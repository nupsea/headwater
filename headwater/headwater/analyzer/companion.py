"""Companion documentation discovery -- finds and parses docs alongside data sources."""

from __future__ import annotations

import csv
import io
import logging
import re
from pathlib import Path

from headwater.core.models import CompanionDoc, SourceConfig

logger = logging.getLogger(__name__)

# File extensions recognized as documentation
_DOC_EXTENSIONS = {".md", ".txt", ".yml", ".yaml"}

# Extensions that are data files, not documentation
_DATA_EXTENSIONS = {".json", ".ndjson", ".parquet", ".avro", ".orc"}

# Filenames that apply to all tables
_GLOBAL_DOC_NAMES = {
    "readme", "data_dictionary", "schema", "dictionary",
    "metadata", "codebook", "data_guide",
}

# Extension -> doc_type mapping
_EXT_TO_TYPE = {
    ".md": "markdown",
    ".txt": "text",
    ".yml": "yaml",
    ".yaml": "yaml",
    ".csv": "csv",
}


def discover_companion_docs(source: SourceConfig) -> list[CompanionDoc]:
    """Scan the source directory for documentation files.

    For file-based sources (path is not None), scans the directory for
    .md, .txt, .yml, .yaml files and .csv files that look like data
    dictionaries (not data files).

    For database sources (uri-only), returns an empty list.
    """
    if not source.path:
        return []

    source_dir = Path(source.path)
    if not source_dir.is_dir():
        # Path might point to a single file; check parent
        source_dir = source_dir.parent
        if not source_dir.is_dir():
            return []

    docs: list[CompanionDoc] = []

    # Scan for known doc extensions
    for ext in _DOC_EXTENSIONS:
        for fp in source_dir.glob(f"*{ext}"):
            if not fp.is_file():
                continue
            try:
                content = parse_doc_file(fp)
                if content.strip():
                    doc_type = _EXT_TO_TYPE.get(ext, "unknown")
                    docs.append(CompanionDoc(
                        filename=fp.name,
                        content=content,
                        doc_type=doc_type,
                    ))
            except Exception as e:
                logger.warning("Failed to read companion doc %s: %s", fp, e)

    # Check .csv files that look like data dictionaries (not data files)
    for fp in source_dir.glob("*.csv"):
        if not fp.is_file():
            continue
        if _is_data_dictionary_csv(fp):
            try:
                content = parse_doc_file(fp)
                if content.strip():
                    docs.append(CompanionDoc(
                        filename=fp.name,
                        content=content,
                        doc_type="csv",
                    ))
            except Exception as e:
                logger.warning(
                    "Failed to read CSV dictionary %s: %s", fp, e,
                )

    if docs:
        logger.info(
            "Found %d companion doc(s) in %s", len(docs), source_dir,
        )

    return docs


def match_docs_to_tables(
    docs: list[CompanionDoc],
    table_names: list[str],
) -> list[CompanionDoc]:
    """Match companion docs to tables by filename, content, and structure.

    Updates matched_tables and confidence on each doc in place.
    """
    table_set = set(table_names)

    for doc in docs:
        matches: dict[str, float] = {}  # table_name -> confidence

        stem = Path(doc.filename).stem.lower()

        # Check if this is a global doc
        stem_clean = re.sub(r"[_\-\s]+", "_", stem)
        is_global = any(gn in stem_clean for gn in _GLOBAL_DOC_NAMES)

        if is_global:
            # Global docs match all tables at lower confidence
            for tname in table_names:
                matches[tname] = max(matches.get(tname, 0), 0.5)

        # Filename-based matching
        for tname in table_names:
            if tname.lower() in stem or stem in tname.lower():
                matches[tname] = max(matches.get(tname, 0), 0.9)

        # Content-based matching: table name appears 2+ times
        content_lower = doc.content.lower()
        for tname in table_names:
            occurrences = content_lower.count(tname.lower())
            if occurrences >= 2:
                matches[tname] = max(matches.get(tname, 0), 0.7)

        # YAML key matching
        if doc.doc_type == "yaml":
            _match_yaml_keys(doc, table_set, matches)

        # Apply matches
        if matches:
            doc.matched_tables = sorted(matches.keys())
            doc.confidence = max(matches.values())

    return docs


def parse_doc_file(path: Path) -> str:
    """Parse a documentation file to plain text.

    - .md/.txt: read as-is
    - .yml/.yaml: parse and flatten to readable text
    - .csv: parse as data dictionary (column_name, description pairs)
    """
    ext = path.suffix.lower()
    text = path.read_text(encoding="utf-8", errors="replace")

    if ext in (".md", ".txt"):
        return text

    if ext in (".yml", ".yaml"):
        return _flatten_yaml(text)

    if ext == ".csv":
        return _parse_csv_dictionary(text)

    return text


def extract_table_context(
    docs: list[CompanionDoc],
    table_name: str,
) -> str | None:
    """Extract the relevant context from companion docs for a specific table.

    Concatenates all matched doc content for the given table.
    Returns None if no relevant context found.
    """
    parts: list[str] = []
    for doc in docs:
        if table_name not in doc.matched_tables:
            continue

        # For global docs, try to extract the relevant section
        if doc.confidence <= 0.5:
            section = _extract_section(doc.content, table_name)
            if section:
                parts.append(f"[From {doc.filename}]\n{section}")
        else:
            parts.append(f"[From {doc.filename}]\n{doc.content}")

    return "\n\n".join(parts) if parts else None


def _is_data_dictionary_csv(path: Path) -> bool:
    """Check if a CSV file looks like a data dictionary (not a data file).

    Heuristic: a data dictionary CSV has columns like 'column', 'description',
    'table', 'field', 'definition', etc. and typically < 500 rows.
    """
    dict_headers = {
        "column", "column_name", "field", "field_name",
        "description", "definition", "meaning", "notes",
        "table", "table_name", "data_type", "type",
    }
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return False
            header_lower = {h.strip().lower() for h in header}
            # At least 2 dict-like headers
            overlap = header_lower & dict_headers
            if len(overlap) >= 2:
                # Count rows to make sure it's not a huge data file
                row_count = sum(1 for _ in reader)
                return row_count < 500
    except Exception:
        return False
    return False


def _flatten_yaml(text: str) -> str:
    """Flatten a YAML document into readable text."""
    try:
        import yaml

        data = yaml.safe_load(text)
        if isinstance(data, dict):
            return _flatten_dict(data)
        return str(data)
    except Exception:
        # If YAML parsing fails, return raw text
        return text


def _flatten_dict(d: dict, prefix: str = "") -> str:
    """Recursively flatten a dict into readable key: value lines."""
    lines: list[str] = []
    for key, value in d.items():
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if isinstance(value, dict):
            lines.append(f"{full_key}:")
            lines.append(_flatten_dict(value, prefix=f"  {full_key}"))
        elif isinstance(value, list):
            lines.append(f"{full_key}: {value}")
        else:
            lines.append(f"{full_key}: {value}")
    return "\n".join(lines)


def _parse_csv_dictionary(text: str) -> str:
    """Parse a CSV data dictionary into readable text."""
    lines: list[str] = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        parts = []
        for key, value in row.items():
            if value and value.strip():
                parts.append(f"{key}: {value.strip()}")
        if parts:
            lines.append(" | ".join(parts))
    return "\n".join(lines)


def _match_yaml_keys(
    doc: CompanionDoc,
    table_set: set[str],
    matches: dict[str, float],
) -> None:
    """Match YAML top-level keys against table names."""
    try:
        import yaml

        data = yaml.safe_load(doc.content)
        if isinstance(data, dict):
            for key in data:
                key_lower = str(key).lower()
                if key_lower in table_set:
                    matches[key_lower] = max(
                        matches.get(key_lower, 0), 0.85,
                    )
    except Exception:
        pass


def _extract_section(content: str, table_name: str) -> str | None:
    """Extract a section from a markdown/text doc that discusses a table.

    Looks for headings or paragraphs mentioning the table name.
    """
    lines = content.split("\n")
    collecting = False
    collected: list[str] = []
    table_lower = table_name.lower()

    for line in lines:
        line_lower = line.lower()

        # Start collecting at a heading or line mentioning the table
        if table_lower in line_lower:
            collecting = True
            collected.append(line)
            continue

        if collecting:
            # Stop at the next heading of same or higher level
            if line.startswith("#") and table_lower not in line_lower:
                break
            # Stop after a blank line following content
            if not line.strip() and collected and not collected[-1].strip():
                break
            collected.append(line)

    result = "\n".join(collected).strip()
    return result if result else None
