"""Metadata retrieval helpers for semantic inference.

The first implementation keeps retrieval local and deterministic: companion
documents, persisted dataset framing, locked column decisions, and lookup-table
candidates. LLM-aided typing can plug into this module later; raw rows should
not be passed to an LLM.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from headwater.core.models import DatasetContext, DiscoveryResult, TableInfo

_LOOKUP_ID_RE = re.compile(r"(^id$|_id$|locationid$|code$|key$|_num$|number$)", re.I)
_LOOKUP_LABEL_RE = re.compile(
    r"(name|label|description|zone|borough|region|title|status|category|type|display|text|value)",
    re.I,
)
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
        (col.name, _lookup_label_score(col.name, col.dtype, col.semantic_type or "", col.role or ""))
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
    if label_score < 6:
        return None

    return {"id_column": id_column, "label_column": label_column}


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
    lower = name.lower()
    if _LOOKUP_ID_RE.search(name):
        return -1

    score = 0
    if _LOOKUP_LABEL_RE.search(name):
        score += 6
    if any(token in (dtype or "").lower() for token in _TEXTUAL_DTYPES):
        score += 4
    if semantic_type == "dimension":
        score += 3
    if role == "dimension":
        score += 2
    if semantic_type in {"id", "foreign_key"} or role == "identifier":
        score -= 5
    return score


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
