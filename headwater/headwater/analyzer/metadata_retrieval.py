"""Metadata retrieval helpers for semantic inference.

The first implementation keeps retrieval local and deterministic: companion
documents, persisted dataset framing, locked column decisions, and lookup-table
candidates. LLM-aided typing can plug into this module later; raw rows should
not be passed to an LLM.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from headwater.core.models import DatasetContext, DiscoveryResult, Relationship, TableInfo

_LOOKUP_ID_RE = re.compile(r"(^id$|_id$|code$|key$|_num$|number$)", re.I)
_TEXTUAL_DTYPES = ("varchar", "char", "text", "string")


@dataclass
class RetrievedMetadata:
    """Structured metadata available to semantic inference and insights."""

    context: DatasetContext | None = None
    glossary: dict[str, str] = field(default_factory=dict)
    lookup_tables: dict[str, dict[str, str]] = field(default_factory=dict)
    enum_mappings: dict[str, dict[str, str]] = field(default_factory=dict)
    locked_roles: dict[tuple[str, str], str] = field(default_factory=dict)


def retrieve_metadata(
    discovery: DiscoveryResult,
    context: DatasetContext | None = None,
) -> RetrievedMetadata:
    """Return local metadata signals for a discovery result."""
    return RetrievedMetadata(
        context=context,
        glossary=_glossary_from_docs(discovery),
        lookup_tables=_lookup_candidates(discovery),
        enum_mappings=_enum_mappings_from_docs(discovery),
        locked_roles={
            (table.name, col.name): col.role or col.semantic_type or ""
            for table in discovery.tables
            for col in table.columns
            if col.locked and (col.role or col.semantic_type)
        },
    )


def _glossary_from_docs(discovery: DiscoveryResult) -> dict[str, str]:
    glossary: dict[str, str] = {}
    for doc in discovery.companion_docs:
        for line in doc.content.splitlines():
            match = re.match(r"\s*([A-Za-z_][\w ]{1,80})\s*[:\-]\s*(.{8,240})", line)
            if match:
                term = " ".join(match.group(1).lower().split())
                description = _normalize_glossary_description(match.group(2).strip())
                if description:
                    glossary.setdefault(term, description)
        for row in _structured_doc_rows(doc.content):
            column_name = row.get("column_name") or row.get("field") or row.get("field_name")
            description = row.get("description") or row.get("definition") or row.get("meaning")
            if column_name and description:
                normalized = _normalize_glossary_description(description.strip())
                if normalized:
                    glossary.setdefault(column_name.lower(), normalized)
    return glossary


def _enum_mappings_from_docs(discovery: DiscoveryResult) -> dict[str, dict[str, str]]:
    mappings: dict[str, dict[str, str]] = {}
    for doc in discovery.companion_docs:
        for line in doc.content.splitlines():
            match = re.match(r"\s*([A-Za-z_][\w ]{1,80})\s*[:\-]\s*(.{8,240})", line)
            if match:
                term = " ".join(match.group(1).lower().split())
                enum_map = _parse_enum_mapping(match.group(2).strip())
                if enum_map:
                    mappings.setdefault(term, enum_map)
        for row in _structured_doc_rows(doc.content):
            column_name = row.get("column_name") or row.get("field") or row.get("field_name")
            description = row.get("description") or row.get("definition") or row.get("meaning")
            if column_name and description:
                enum_map = _parse_enum_mapping(description.strip())
                if enum_map:
                    mappings.setdefault(column_name.lower(), enum_map)
    return mappings


def _lookup_candidates(discovery: DiscoveryResult) -> dict[str, dict[str, str]]:
    candidates: dict[str, dict[str, str]] = {}
    for table in discovery.tables:
        candidate = infer_lookup_candidate(table)
        if candidate is not None:
            candidates[table.name] = candidate
    return candidates


def infer_lookup_candidate(table: TableInfo) -> dict[str, str] | None:
    """Infer a generic code-to-label lookup shape for a small dimension table."""
    if table.row_count > 100_000 or len(table.columns) < 2:
        return None

    id_candidates = [
        (col.name, _lookup_id_score(col.name, col.semantic_type or "", col.role or ""))
        for col in table.columns
    ]
    id_candidates = [item for item in id_candidates if item[1] > 0]
    if not id_candidates:
        return None

    label_candidates = [
        (
            col.name,
            _lookup_label_score(col.name, col.dtype, col.semantic_type or "", col.role or ""),
        )
        for col in table.columns
    ]
    label_candidates = [item for item in label_candidates if item[1] > 0]
    if not label_candidates:
        return None
    if len(table.columns) > len(id_candidates) + len(label_candidates) + 1:
        return None

    id_column = max(id_candidates, key=lambda item: item[1])[0]
    label_options = [item for item in label_candidates if item[0] != id_column]
    if not label_options:
        return None
    label_column, label_score = max(label_options, key=lambda item: item[1])
    if label_score < 6 and len(table.columns) > 2:
        return None

    return {"id_column": id_column, "label_column": label_column}


def lookup_match_keys(name: str) -> list[str]:
    parts = _identifier_tokens(name)
    if not parts:
        return [name.lower()]

    keys: list[str] = []

    def add(value: str) -> None:
        if value and value not in keys:
            keys.append(value)

    add(name.lower())
    add("".join(parts))
    add("_".join(parts))

    suffix_tokens = {"id", "code", "key"}
    directional_prefixes = {
        "origin",
        "destination",
        "dest",
        "from",
        "to",
        "start",
        "end",
        "src",
        "dst",
    }
    if len(parts) >= 2 and parts[-1] in suffix_tokens:
        tail = parts[-2:]
        add("".join(tail))
        add("_".join(tail))
        core = list(parts[:-1])
        while core and core[0] in directional_prefixes:
            core = core[1:]
        if core:
            normalized = core + [parts[-1]]
            add("".join(normalized))
            add("_".join(normalized))
            if len(normalized) >= 2:
                add("".join(normalized[-2:]))
                add("_".join(normalized[-2:]))

    return keys


def build_lookup_index(
    tables: list[TableInfo],
    metadata: RetrievedMetadata | None = None,
    relationships: list[Relationship] | None = None,
) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    lookup_by_table: dict[str, dict[str, str]] = {}
    table_by_name = {table.name: table for table in tables}

    if metadata is not None:
        for table_name, lookup in metadata.lookup_tables.items():
            table = table_by_name.get(table_name)
            details = {
                "table_name": table_name,
                "id_column": lookup["id_column"],
                "label_column": lookup["label_column"],
            }
            if table and table.schema_name:
                details["schema_name"] = table.schema_name
            lookup_by_table[table_name] = details
            for key in lookup_match_keys(lookup["id_column"]):
                index.setdefault(key, details)

    for table in tables:
        candidate = infer_lookup_candidate(table)
        if candidate is None:
            continue
        details = {
            "table_name": table.name,
            "id_column": candidate["id_column"],
            "label_column": candidate["label_column"],
        }
        if table.schema_name:
            details["schema_name"] = table.schema_name
        existing = lookup_by_table.setdefault(table.name, details)
        if table.schema_name and "schema_name" not in existing:
            existing["schema_name"] = table.schema_name
        for key in lookup_match_keys(candidate["id_column"]):
            index.setdefault(key, existing)

    if relationships:
        for rel in relationships:
            lookup = lookup_by_table.get(rel.to_table)
            if not lookup:
                continue
            index.setdefault(
                f"{rel.from_table.lower()}.{rel.from_column.lower()}",
                lookup,
            )
            for key in lookup_match_keys(rel.from_column):
                index.setdefault(key, lookup)

    return index


def lookup_for_column(
    table_name: str,
    column_name: str,
    lookup_index: dict[str, dict[str, str]],
) -> dict[str, str] | None:
    lookup = lookup_index.get(f"{table_name.lower()}.{column_name.lower()}")
    if lookup is not None:
        return lookup
    for key in lookup_match_keys(column_name):
        lookup = lookup_index.get(key)
        if lookup is not None:
            return lookup
    return None


def _lookup_id_score(name: str, semantic_type: str, role: str) -> int:
    score = 0
    lower = name.lower()
    if _LOOKUP_ID_RE.search(name):
        score += 6
    if lower == "id":
        score += 4
    if lower.endswith("_id") or lower.endswith("id"):
        score += 3
    if lower.endswith("_code") or lower.endswith("code"):
        score += 2
    if semantic_type in {"id", "foreign_key"}:
        score += 3
    if role == "identifier":
        score += 2
    return score


def _lookup_label_score(name: str, dtype: str, semantic_type: str, role: str) -> int:
    if _LOOKUP_ID_RE.search(name):
        return -1

    score = 0
    if any(token in (dtype or "").lower() for token in _TEXTUAL_DTYPES):
        score += 4
    if semantic_type == "dimension":
        score += 3
    if role == "dimension":
        score += 2
    if semantic_type in {"id", "foreign_key"} or role == "identifier":
        score -= 5
    return score


def _identifier_tokens(name: str) -> list[str]:
    parts = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    parts = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", parts).lower()
    return [part for part in re.split(r"[_\W]+", parts) if part]


def _structured_doc_rows(content: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in content.splitlines():
        if " | " not in line:
            continue
        row: dict[str, str] = {}
        for part in line.split(" | "):
            if ":" not in part:
                continue
            key, value = part.split(":", 1)
            row[key.strip().lower()] = value.strip()
        if row:
            rows.append(row)
    return rows


def _parse_enum_mapping(text: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for match in re.finditer(
        r"([A-Za-z0-9_-]{1,12})\s*(?:=|:|-)\s*([A-Za-z][^;|]*)",
        text,
    ):
        key = match.group(1).strip()
        value = match.group(2).strip().rstrip(".) ")
        if len(value) < 2:
            continue
        mapping[key] = value
    return mapping if len(mapping) >= 2 else {}


def _normalize_glossary_description(text: str) -> str | None:
    description = text.strip()
    if not description:
        return None
    enum_map = _parse_enum_mapping(description)
    if not enum_map:
        return description

    enum_start = re.search(r"\b[A-Za-z0-9_-]{1,12}\s*(?:=|-)\s*\S", description)
    if enum_start is None:
        return None
    prefix = description[: enum_start.start()].rstrip(" :;,-(").strip()
    if not prefix:
        return None
    words = [word for word in re.split(r"\s+", prefix) if word]
    if len(words) < 2:
        return None
    return prefix
