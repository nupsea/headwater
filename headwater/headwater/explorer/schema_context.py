"""Schema context builder -- rich metadata for exploratory fallback.

When the catalog-driven decomposer cannot fully resolve a question
(outside_scope or low confidence), the system falls back to LLM-based
SQL generation. This module builds a rich context string from
discovery metadata + semantic catalog that is far better than raw
schema dumps.

Used by nl_to_sql.py when the decomposer returns outside_scope or
when no catalog is available.
"""

from __future__ import annotations

import logging

from headwater.core.models import (
    DiscoveryResult,
    GeneratedModel,
    SemanticCatalog,
)

logger = logging.getLogger(__name__)


def build_schema_context(
    discovery: DiscoveryResult,
    models: list[GeneratedModel] | None = None,
    catalog: SemanticCatalog | None = None,
) -> str:
    """Build a rich text context from discovery metadata + catalog.

    Includes:
    - Table descriptions, row counts, domains
    - Column names, types, descriptions, roles
    - Relationships with integrity scores
    - Catalog metrics and dimensions (if available)
    - Mart models (if available)

    The output is suitable for LLM prompts -- structured but concise.
    """
    parts: list[str] = []

    # Tables section
    parts.append("=== TABLES ===")
    for table in discovery.tables:
        desc = table.description or ""
        domain = f" [{table.domain}]" if table.domain else ""
        parts.append(f"\n{table.name} ({table.row_count:,} rows){domain}")
        if desc:
            parts.append(f"  Description: {desc}")

        # Semantic detail
        if table.semantic_detail:
            sd = table.semantic_detail
            if sd.row_semantics:
                parts.append(f"  Row semantics: {sd.row_semantics}")
            if sd.narrative:
                parts.append(f"  Narrative: {sd.narrative[:200]}")

        # Columns with roles and descriptions
        for col in table.columns:
            role = f" ({col.role})" if col.role else ""
            stype = f" [{col.semantic_type}]" if col.semantic_type else ""
            col_desc = f" -- {col.description}" if col.description else ""
            parts.append(f"    {col.name} {col.dtype}{role}{stype}{col_desc}")

    # Relationships
    if discovery.relationships:
        parts.append("\n=== RELATIONSHIPS ===")
        for r in discovery.relationships:
            integrity = f" (integrity={r.referential_integrity:.0%})"
            parts.append(
                f"  {r.from_table}.{r.from_column} -> "
                f"{r.to_table}.{r.to_column} ({r.type}){integrity}"
            )

    # Catalog section (if available)
    if catalog:
        if catalog.entities:
            parts.append("\n=== ENTITIES (queryable) ===")
            for e in catalog.entities:
                parts.append(f"  {e.display_name}: {e.description[:120]}")
                if e.metrics:
                    parts.append(f"    Metrics: {', '.join(e.metrics[:8])}")
                if e.dimensions:
                    parts.append(f"    Dimensions: {', '.join(e.dimensions[:8])}")

        if catalog.metrics:
            parts.append("\n=== METRICS ===")
            for m in catalog.metrics[:20]:
                parts.append(f"  {m.display_name}: {m.expression} on {m.table}")

        if catalog.dimensions:
            parts.append("\n=== DIMENSIONS ===")
            for d in catalog.dimensions[:20]:
                syn_str = f" (synonyms: {', '.join(d.synonyms[:4])})" if d.synonyms else ""
                parts.append(f"  {d.display_name}: {d.column} in {d.table}{syn_str}")

    # Models section
    if models:
        parts.append("\n=== MODELS (prefer these for queries) ===")
        for m in models:
            if m.status == "approved":
                parts.append(f"  {m.name}: {m.description or ''}")

    context = "\n".join(parts)
    logger.debug("Built schema context: %d chars, %d tables", len(context), len(discovery.tables))
    return context
