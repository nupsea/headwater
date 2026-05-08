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
                glossary.setdefault(term, match.group(2).strip())
    return glossary


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
