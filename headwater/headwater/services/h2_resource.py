"""Headwater 2 resource intake and semantic claim fusion.

Accepts Markdown, plain text, and CSV data dictionaries.
Extracts column definitions, enum mappings, aliases, and caveats without
any LLM call — raw resource text is NEVER forwarded to an LLM.

Fusion rules:
  - Locked claims are never overwritten.
  - A new claim for a column that already has a proposed claim with a
    different value is a conflict: both confidences drop and a Resolve card
    is created.
  - Complementary claims (same column, same claim type, same value) are
    idempotent — confidence is nudged up on the second ingestion.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from headwater.core.store import HeadwaterStore

ResourceFormat = Literal["markdown", "text", "csv_dict", "unknown"]
Sensitivity = Literal["safe", "sensitive", "unknown"]
ClaimType = Literal["definition", "enum_mapping", "alias", "caveat", "metric_definition"]

_PII_TERMS = frozenset({
    "ssn", "social_security", "dob", "date_of_birth", "email",
    "phone", "address", "birth", "password", "secret", "api_key",
    "token", "credential", "credit_card", "bank_account", "passport",
})

_DICT_FIELD_HEADERS = frozenset({
    "field", "column", "col", "attribute", "name", "field_name",
    "column_name", "variable", "parameter",
})
_DICT_DESC_HEADERS = frozenset({
    "description", "definition", "meaning", "desc", "notes",
    "note", "comment", "explanation", "details",
})
_DICT_TYPE_HEADERS = frozenset({
    "type", "data_type", "dtype", "datatype", "format",
})
_DICT_EXAMPLE_HEADERS = frozenset({
    "example", "examples", "values", "example_values", "sample",
})

_CLAIM_CONFIDENCE = {
    "exact_match": 0.85,
    "normalized_match": 0.70,
    "conflict": 0.30,
    "complement": 0.90,
}


@dataclass(slots=True)
class ExtractedClaim:
    column_ref: str  # "table.column" or "column"
    claim_type: ClaimType
    value: Any
    confidence: float
    evidence: str  # where in the resource this came from


@dataclass(slots=True)
class ResourceIntakeResult:
    resource_path: str
    resource_format: ResourceFormat
    sensitivity: Sensitivity
    sensitivity_notes: list[str]
    claims_created: int
    claims_updated: int
    claims_skipped_locked: int
    conflicts_detected: int
    notes: list[str]


def ingest_resource(
    store: HeadwaterStore,
    project_id: str,
    resource_path: str | Path,
    *,
    lock_on_ingest: bool = False,
) -> ResourceIntakeResult:
    """Parse a resource file and fuse extracted claims into the project store.

    Does not overwrite locked claims. Detects conflicts between new and
    existing proposed claims and surfaces them as Resolve cards.
    Raw text is never forwarded to any LLM.
    """
    path = Path(resource_path)
    if not path.exists():
        raise FileNotFoundError(f"Resource file not found: {path}")

    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")

    source_name = project_sources[0]["source_name"]
    text = path.read_text(encoding="utf-8", errors="replace")
    fmt = _detect_format(path, text)
    sensitivity, sensitivity_notes = _classify_sensitivity(text)
    extracted = _parse_resource(text, fmt, path.name)
    column_index = _build_column_index(store, source_name)
    existing_claims = _load_existing_claims(store, project_id)

    result_counts = {
        "created": 0,
        "updated": 0,
        "skipped_locked": 0,
        "conflicts": 0,
    }
    notes: list[str] = []

    for claim in extracted:
        matched_cols = _match_columns(claim.column_ref, column_index)
        if not matched_cols:
            notes.append(
                f"No column match for '{claim.column_ref}' in source '{source_name}' — skipped."
            )
            continue
        match_confidence = (
            _CLAIM_CONFIDENCE["exact_match"]
            if _exact_match(claim.column_ref, column_index)
            else _CLAIM_CONFIDENCE["normalized_match"]
        )
        effective_confidence = min(claim.confidence, match_confidence)

        for table_name, col_name in matched_cols:
            outcome = _fuse_claim(
                store=store,
                project_id=project_id,
                source_name=source_name,
                table_name=table_name,
                col_name=col_name,
                claim=claim,
                confidence=effective_confidence,
                resource_path=str(path),
                existing_claims=existing_claims,
                lock_on_ingest=lock_on_ingest,
            )
            result_counts[outcome] = result_counts.get(outcome, 0) + 1
            if outcome == "conflicts":
                _create_conflict_resolve_card(
                    store=store,
                    project_id=project_id,
                    table_name=table_name,
                    col_name=col_name,
                    claim=claim,
                    resource_path=str(path),
                )

    _register_resource(store, project_id, str(path), fmt)

    return ResourceIntakeResult(
        resource_path=str(path),
        resource_format=fmt,
        sensitivity=sensitivity,
        sensitivity_notes=sensitivity_notes,
        claims_created=result_counts["created"],
        claims_updated=result_counts["updated"],
        claims_skipped_locked=result_counts["skipped_locked"],
        conflicts_detected=result_counts["conflicts"],
        notes=notes,
    )


# ── Parsing ──────────────────────────────────────────────────────────────────

def _detect_format(path: Path, text: str) -> ResourceFormat:
    suffix = path.suffix.lower()
    if suffix in (".md", ".markdown"):
        return "markdown"
    if suffix == ".csv":
        return "csv_dict"
    if suffix in (".txt", ".text", ""):
        return "text"
    # Heuristic fallback
    if text.strip().startswith("#") or "**" in text or "```" in text:
        return "markdown"
    if "," in text and "\n" in text:
        first_line = text.splitlines()[0].lower()
        if any(h in first_line for h in _DICT_FIELD_HEADERS | _DICT_DESC_HEADERS):
            return "csv_dict"
    return "text"


def _parse_resource(text: str, fmt: ResourceFormat, filename: str) -> list[ExtractedClaim]:
    if fmt == "markdown":
        return _parse_markdown(text)
    if fmt == "csv_dict":
        return _parse_csv_dict(text, filename)
    return _parse_text(text)


_HEADING_NOISE = frozenset({
    "reference", "codes", "values", "mapping", "dictionary", "definitions",
    "legend", "table", "list", "guide", "overview", "summary", "notes",
})


def _heading_to_col_ref(heading: str) -> str:
    """Convert a section heading like 'Status Code Reference' to 'status_code'."""
    words = re.sub(r"[^a-z0-9\s]", "", heading.lower()).split()
    words = [w for w in words if w not in _HEADING_NOISE]
    return "_".join(words[:3]).strip("_")


def _parse_markdown(text: str) -> list[ExtractedClaim]:
    """Parse Markdown into claims, using section headings as column context for enum tables."""
    heading_re = re.compile(r"^#{1,3}\s+(.+)$", re.MULTILINE)
    # Build list of (heading, section_text) pairs
    sections: list[tuple[str, str]] = []
    prev_heading = ""
    prev_end = 0
    for m in heading_re.finditer(text):
        if prev_end > 0 or m.start() > 0:
            sections.append((prev_heading, text[prev_end: m.start()]))
        prev_heading = m.group(1).strip()
        prev_end = m.end()
    sections.append((prev_heading, text[prev_end:]))

    claims: list[ExtractedClaim] = []
    for heading, section_text in sections:
        raw: list[ExtractedClaim] = []
        raw.extend(_extract_markdown_tables(section_text))
        raw.extend(_extract_markdown_bullets(section_text))
        raw.extend(_extract_inline_definitions(section_text))
        if heading:
            col_hint = _heading_to_col_ref(heading)
            raw = [
                ExtractedClaim(
                    column_ref=col_hint,
                    claim_type=c.claim_type,
                    value=c.value,
                    confidence=round(c.confidence * 0.85, 4),
                    evidence=f"standalone enum under heading '{heading}'",
                )
                if (not c.column_ref and c.claim_type == "enum_mapping" and col_hint)
                else c
                for c in raw
            ]
        claims.extend(raw)
    return [c for c in claims if c.column_ref]


def _extract_markdown_tables(text: str) -> list[ExtractedClaim]:
    """Extract definitions and enum mappings from Markdown tables."""
    claims: list[ExtractedClaim] = []
    table_pattern = re.compile(
        r"(?:^|\n)((?:\|[^\n]+\|\n)+(?:\|[-| :]+\|\n)(?:\|[^\n]+\|\n?)+)",
        re.MULTILINE,
    )
    for match in table_pattern.finditer(text):
        table_text = match.group(1).strip()
        rows = [
            [cell.strip() for cell in row.strip().strip("|").split("|")]
            for row in table_text.splitlines()
            if row.strip().startswith("|") and not re.match(r"^\|[-| :]+\|$", row.strip())
        ]
        if len(rows) < 2:
            continue
        headers = [h.lower().strip("`*_ ") for h in rows[0]]
        data_rows = rows[1:]
        claims.extend(_interpret_table(headers, data_rows))
    return claims


def _interpret_table(headers: list[str], rows: list[list[str]]) -> list[ExtractedClaim]:
    claims: list[ExtractedClaim] = []

    # Data dictionary table: [field_name, description, ...]
    field_col = next((i for i, h in enumerate(headers) if h in _DICT_FIELD_HEADERS), None)
    desc_col = next((i for i, h in enumerate(headers) if h in _DICT_DESC_HEADERS), None)

    if field_col is not None and desc_col is not None:
        for row in rows:
            if len(row) <= max(field_col, desc_col):
                continue
            col_ref = _normalize_col_ref(row[field_col])
            definition = row[desc_col].strip()
            if col_ref and definition:
                claims.append(
                    ExtractedClaim(
                        column_ref=col_ref,
                        claim_type="definition",
                        value=definition,
                        confidence=0.80,
                        evidence=f"markdown table row: {col_ref}",
                    )
                )
                # Check for inline enum hint in other columns
                example_col = next(
                    (i for i, h in enumerate(headers) if h in _DICT_EXAMPLE_HEADERS), None
                )
                if example_col is not None and len(row) > example_col:
                    enum_claims = _extract_inline_enum(col_ref, row[example_col])
                    claims.extend(enum_claims)
        return claims

    # Enum table: 2-column table [code/value, meaning/description]
    if len(headers) == 2:
        code_header = headers[0]
        meaning_header = headers[1]
        if (
            code_header in ("code", "value", "key", "abbr", "abbreviation")
            or meaning_header in _DICT_DESC_HEADERS
        ):
            enum_map: dict[str, str] = {}
            for row in rows:
                if len(row) < 2:
                    continue
                code = row[0].strip()
                meaning = row[1].strip()
                if code and meaning:
                    enum_map[code] = meaning
            if enum_map:
                claims.append(
                    ExtractedClaim(
                        column_ref="",
                        claim_type="enum_mapping",
                        value=enum_map,
                        confidence=0.75,
                        evidence="standalone enum table",
                    )
                )
    return claims


def _extract_markdown_bullets(text: str) -> list[ExtractedClaim]:
    """Extract definitions from bullet list items like: - **col**: definition."""
    claims: list[ExtractedClaim] = []
    pattern = re.compile(
        r"^\s*[-*]\s+(?:\*\*|`)([a-z_][a-z0-9_.]*?)(?:\*\*|`)\s*[-:]\s+(.+)$",
        re.MULTILINE | re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        col_ref = _normalize_col_ref(match.group(1))
        definition = match.group(2).strip()
        if col_ref and definition:
            claims.append(
                ExtractedClaim(
                    column_ref=col_ref,
                    claim_type="definition",
                    value=definition,
                    confidence=0.80,
                    evidence=f"markdown bullet: {match.group(0)[:60]}",
                )
            )
    return claims


def _extract_inline_definitions(text: str) -> list[ExtractedClaim]:
    """Extract plain-text definitions like: col_name: definition (non-table, non-bullet)."""
    claims: list[ExtractedClaim] = []
    pattern = re.compile(
        r"^([a-z_][a-z0-9_.]*)\s*[:\-]\s+([A-Z].{10,})$",
        re.MULTILINE,
    )
    for match in pattern.finditer(text):
        col_ref = _normalize_col_ref(match.group(1))
        definition = match.group(2).strip()
        if col_ref and definition and len(definition) >= 10:
            claims.append(
                ExtractedClaim(
                    column_ref=col_ref,
                    claim_type="definition",
                    value=definition,
                    confidence=0.65,
                    evidence=f"inline: {match.group(0)[:60]}",
                )
            )
    return claims


def _parse_text(text: str) -> list[ExtractedClaim]:
    """Parse plain-text definitions and simple enum patterns."""
    claims: list[ExtractedClaim] = []
    # col_name: definition text (line-oriented)
    def_pattern = re.compile(
        r"^([a-z_][a-z0-9_.]*)\s*[:=]\s+(.{5,})$",
        re.MULTILINE | re.IGNORECASE,
    )
    for match in def_pattern.finditer(text):
        col_ref = _normalize_col_ref(match.group(1))
        value = match.group(2).strip()
        if not col_ref or not value:
            continue
        # Short uppercase values are enum-only; longer text is a definition
        # (but may also embed an inline enum hint in parentheses)
        if len(value) <= 40 and re.match(r"^[A-Z0-9\s,/]+$", value):
            enum_claims = _extract_inline_enum(col_ref, value)
            claims.extend(enum_claims)
        else:
            claims.append(
                ExtractedClaim(
                    column_ref=col_ref,
                    claim_type="definition",
                    value=value,
                    confidence=0.70,
                    evidence=f"text line: {match.group(0)[:60]}",
                )
            )
            # Also try to extract inline enum hints from the definition text
            enum_claims = _extract_inline_enum(col_ref, value)
            claims.extend(enum_claims)
    return claims


def _parse_csv_dict(text: str, filename: str) -> list[ExtractedClaim]:
    """Parse a CSV data dictionary into definition and enum claims."""
    claims: list[ExtractedClaim] = []
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
    except Exception:
        return claims

    if not rows:
        return claims

    raw_headers = list(rows[0].keys())
    field_col = next((h for h in raw_headers if h.lower().strip() in _DICT_FIELD_HEADERS), None)
    desc_col = next((h for h in raw_headers if h.lower().strip() in _DICT_DESC_HEADERS), None)
    example_col = next(
        (h for h in raw_headers if h.lower().strip() in _DICT_EXAMPLE_HEADERS), None
    )

    if field_col is None or desc_col is None:
        # Try as standalone enum table (2-column: code, meaning)
        if len(raw_headers) == 2:
            code_h, meaning_h = raw_headers
            if meaning_h.lower().strip() in _DICT_DESC_HEADERS | {"meaning", "label"}:
                enum_map = {
                    row[code_h].strip(): row[meaning_h].strip()
                    for row in rows
                    if row.get(code_h) and row.get(meaning_h)
                }
                if enum_map:
                    col_ref = Path(filename).stem.lower().replace("-", "_")
                    claims.append(
                        ExtractedClaim(
                            column_ref=col_ref,
                            claim_type="enum_mapping",
                            value=enum_map,
                            confidence=0.70,
                            evidence=f"csv enum table: {filename}",
                        )
                    )
        return claims

    for row in rows:
        col_ref = _normalize_col_ref(row.get(field_col) or "")
        definition = (row.get(desc_col) or "").strip()
        if not col_ref or not definition:
            continue
        claims.append(
            ExtractedClaim(
                column_ref=col_ref,
                claim_type="definition",
                value=definition,
                confidence=0.80,
                evidence=f"csv row: {col_ref}",
            )
        )
        if example_col:
            enum_claims = _extract_inline_enum(col_ref, row.get(example_col) or "")
            claims.extend(enum_claims)
    return claims


def _extract_inline_enum(col_ref: str, text: str) -> list[ExtractedClaim]:
    """Extract enum mappings from inline text like 'A=Active, I=Inactive'."""
    if not text:
        return []
    pattern = re.compile(r"([A-Z0-9_]+)\s*[=:]\s*([^,;|]+?)(?=[,;|]|$)", re.IGNORECASE)
    enum_map: dict[str, str] = {}
    for match in pattern.finditer(text):
        code = match.group(1).strip()
        meaning = match.group(2).strip()
        if code and meaning and len(code) <= 10:
            enum_map[code] = meaning
    if len(enum_map) >= 2:
        return [
            ExtractedClaim(
                column_ref=col_ref,
                claim_type="enum_mapping",
                value=enum_map,
                confidence=0.75,
                evidence=f"inline enum from: {text[:60]}",
            )
        ]
    return []


# ── Sensitivity ───────────────────────────────────────────────────────────────

def _classify_sensitivity(text: str) -> tuple[Sensitivity, list[str]]:
    lowered = text.lower()
    found = [term for term in _PII_TERMS if term in lowered]
    if found:
        return "sensitive", [f"PII-related term(s) found: {', '.join(found[:5])}"]
    return "safe", []


# ── Column matching ───────────────────────────────────────────────────────────

def _build_column_index(
    store: HeadwaterStore,
    source_name: str,
) -> dict[str, list[tuple[str, str]]]:
    """Return {normalized_col_name: [(table_name, col_name), ...]}."""
    index: dict[str, list[tuple[str, str]]] = {}
    for table in store.get_tables(source_name):
        for col in store.get_columns(source_name, table["name"]):
            normalized = _normalize_name(col["name"])
            index.setdefault(normalized, []).append((table["name"], col["name"]))
            # Also index "table.column"
            full = f"{table['name']}.{col['name']}"
            normalized_full = _normalize_name(full)
            index.setdefault(normalized_full, []).append((table["name"], col["name"]))
    return index


def _match_columns(
    col_ref: str,
    column_index: dict[str, list[tuple[str, str]]],
) -> list[tuple[str, str]]:
    if not col_ref:
        return []
    key = _normalize_name(col_ref)
    return list(column_index.get(key, []))


def _exact_match(
    col_ref: str,
    column_index: dict[str, list[tuple[str, str]]],
) -> bool:
    for table_name, col_name in column_index.get(_normalize_name(col_ref), []):
        full = f"{table_name}.{col_name}".lower()
        if col_name.lower() == col_ref.lower() or full == col_ref.lower():
            return True
    return False


def _normalize_name(name: str) -> str:
    return name.lower().replace("-", "_").replace(" ", "_").strip()


def _normalize_col_ref(value: str) -> str:
    value = value.strip().strip("`*_ ")
    return _normalize_name(value)


# ── Claim fusion ──────────────────────────────────────────────────────────────

def _load_existing_claims(
    store: HeadwaterStore,
    project_id: str,
) -> dict[str, dict[str, Any]]:
    """Return {claim_id: claim_dict} for all existing claims in the project."""
    return {c["id"]: c for c in store.list_semantic_claims(project_id)}


def _fuse_claim(
    store: HeadwaterStore,
    project_id: str,
    source_name: str,
    table_name: str,
    col_name: str,
    claim: ExtractedClaim,
    confidence: float,
    resource_path: str,
    existing_claims: dict[str, dict[str, Any]],
    lock_on_ingest: bool,
) -> str:
    """Fuse one extracted claim. Returns 'created', 'updated', 'skipped_locked', or 'conflicts'."""
    claim_id = f"{project_id}:resource:{table_name}.{col_name}:{claim.claim_type}"

    existing = existing_claims.get(claim_id)
    if existing:
        if existing.get("locked"):
            return "skipped_locked"
        existing_value = existing.get("claim", {}).get("value")
        new_value = _canonical_value(claim.value)
        if _values_conflict(existing_value, new_value):
            # Lower confidence on the existing claim
            store.upsert_semantic_claim(
                claim_id,
                project_id=project_id,
                source_name=source_name,
                scope_type="column",
                table_name=table_name,
                column_name=col_name,
                claim_type=claim.claim_type,
                claim={
                    "value": existing_value,
                    "conflict_with": new_value,
                    "source": existing.get("source"),
                },
                status="needs_review",
                confidence=_CLAIM_CONFIDENCE["conflict"],
                source=existing.get("source", "resource"),
                locked=False,
            )
            # Store the conflicting new value under a sibling id
            conflict_id = f"{claim_id}:conflict"
            store.upsert_semantic_claim(
                conflict_id,
                project_id=project_id,
                source_name=source_name,
                scope_type="column",
                table_name=table_name,
                column_name=col_name,
                claim_type=claim.claim_type,
                claim={"value": new_value, "evidence": claim.evidence},
                status="needs_review",
                confidence=_CLAIM_CONFIDENCE["conflict"],
                source=f"resource:{Path(resource_path).name}",
                locked=False,
            )
            existing_claims[claim_id] = store.get_semantic_claim(claim_id) or {}
            existing_claims[conflict_id] = store.get_semantic_claim(conflict_id) or {}
            return "conflicts"
        if _values_equal(existing_value, new_value):
            bumped = min(1.0, float(existing.get("confidence", confidence)) + 0.05)
            store.upsert_semantic_claim(
                claim_id,
                project_id=project_id,
                source_name=source_name,
                scope_type="column",
                table_name=table_name,
                column_name=col_name,
                claim_type=claim.claim_type,
                claim={"value": new_value, "evidence": claim.evidence},
                status="proposed",
                confidence=bumped,
                source=f"resource:{Path(resource_path).name}",
                locked=lock_on_ingest,
            )
            existing_claims[claim_id] = store.get_semantic_claim(claim_id) or {}
            return "updated"

    store.upsert_semantic_claim(
        claim_id,
        project_id=project_id,
        source_name=source_name,
        scope_type="column",
        table_name=table_name,
        column_name=col_name,
        claim_type=claim.claim_type,
        claim={"value": _canonical_value(claim.value), "evidence": claim.evidence},
        status="proposed",
        confidence=confidence,
        source=f"resource:{Path(resource_path).name}",
        locked=lock_on_ingest,
    )
    existing_claims[claim_id] = store.get_semantic_claim(claim_id) or {}
    return "created"


def _canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k).strip(): str(v).strip() for k, v in value.items()}
    return str(value).strip()


def _values_equal(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return False
    if isinstance(a, dict) and isinstance(b, dict):
        return a == b
    return str(a).strip().lower() == str(b).strip().lower()


def _values_conflict(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return False
    if isinstance(a, dict) and isinstance(b, dict):
        # Enum conflict: same keys, different values
        shared = set(a) & set(b)
        return any(str(a[k]).lower() != str(b[k]).lower() for k in shared)
    # Definition conflict: non-empty strings that are different
    sa, sb = str(a).strip().lower(), str(b).strip().lower()
    return bool(sa and sb and sa != sb)


def _create_conflict_resolve_card(
    store: HeadwaterStore,
    project_id: str,
    table_name: str,
    col_name: str,
    claim: ExtractedClaim,
    resource_path: str,
) -> None:
    card_id = f"{project_id}:conflict:{table_name}.{col_name}:{claim.claim_type}"
    store.upsert_resolve_item(
        card_id,
        project_id=project_id,
        issue_kind="structural_ambiguity",
        title=f'Conflicting {claim.claim_type} for "{col_name}"',
        body=(
            f"A new resource (`{Path(resource_path).name}`) provided a {claim.claim_type} "
            f"for `{table_name}.{col_name}` that conflicts with an existing definition. "
            "Review both versions and lock the correct one."
        ),
        priority="medium",
        status="open",
        payload={
            "table": table_name,
            "column": col_name,
            "claim_type": claim.claim_type,
            "resource": Path(resource_path).name,
            "contract_impacts": ["definition_consistent"],
        },
    )


def _register_resource(
    store: HeadwaterStore,
    project_id: str,
    resource_path: str,
    fmt: ResourceFormat,
) -> None:
    """Track ingested resource in the project's resource registry claim."""
    from datetime import datetime

    registry_id = f"{project_id}:resource_registry"
    existing = store.get_semantic_claim(registry_id)
    registry: list[dict[str, Any]] = []
    if existing:
        registry = list(existing.get("claim", {}).get("value") or [])
    entry = {
        "path": resource_path,
        "format": fmt,
        "ingested_at": datetime.now().isoformat(timespec="seconds"),
    }
    # Replace if same path, else append
    registry = [r for r in registry if r.get("path") != resource_path] + [entry]
    store.upsert_semantic_claim(
        registry_id,
        project_id=project_id,
        scope_type="project",
        claim_type="resource_registry",
        claim={"value": registry},
        status="proposed",
        confidence=1.0,
        source="system",
        locked=False,
    )
