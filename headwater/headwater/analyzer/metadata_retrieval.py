"""Metadata retrieval helpers for semantic inference.

The first implementation keeps retrieval local and deterministic: companion
documents, persisted dataset framing, locked column decisions, and lookup-table
candidates. LLM-aided typing can plug into this module later; raw rows should
not be passed to an LLM.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from headwater.core.models import DatasetContext, DiscoveryResult


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
        id_cols = [
            col.name
            for col in table.columns
            if re.search(r"(^id$|_id$|locationid$|code$|key$)", col.name, re.I)
        ]
        label_cols = [
            col.name
            for col in table.columns
            if re.search(r"(name|label|description|zone|borough|region|title)", col.name, re.I)
        ]
        if id_cols and label_cols and table.row_count <= 100_000:
            candidates[table.name] = {
                "id_column": id_cols[0],
                "label_column": label_cols[0],
            }
    return candidates


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
