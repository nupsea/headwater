"""Semantic analyzer -- enriches discovery results with descriptions and domains.

Uses LLM when available, falls back to heuristics.
Never sends raw data rows -- only schema metadata and statistical summaries.
"""

from __future__ import annotations

import asyncio
import logging

from headwater.analyzer.heuristics import (
    build_domain_map,
    enrich_tables,
    generate_deep_table_description,
)
from headwater.analyzer.llm import LLMProvider, NoLLMProvider, make_cache_key
from headwater.core.models import (
    ColumnProfile,
    ColumnSemanticDetail,
    DiscoveryResult,
    Relationship,
    TableInfo,
    TableSemanticDetail,
)

logger = logging.getLogger(__name__)


def analyze(
    discovery: DiscoveryResult,
    provider: LLMProvider | None = None,
) -> DiscoveryResult:
    """Enrich a DiscoveryResult with semantic descriptions and domain classification.

    If provider is None or NoLLMProvider, uses heuristic-only mode.
    If provider is an LLM provider, enriches with LLM + heuristic fallback.
    """
    if provider is None or isinstance(provider, NoLLMProvider):
        return _analyze_heuristic(discovery)

    # LLM mode: run async enrichment
    return asyncio.run(_analyze_with_llm(discovery, provider))


def _analyze_heuristic(discovery: DiscoveryResult) -> DiscoveryResult:
    """Enrich using heuristics only -- no LLM calls."""
    enrich_tables(discovery.tables, discovery.profiles, discovery.relationships)
    discovery.domains = build_domain_map(discovery.tables)

    # Deep semantic descriptions
    _apply_deep_descriptions(discovery)

    return discovery


async def _analyze_with_llm(
    discovery: DiscoveryResult,
    provider: LLMProvider,
) -> DiscoveryResult:
    """Enrich using LLM with heuristic fallback per table."""
    # First apply heuristics as baseline
    enrich_tables(discovery.tables, discovery.profiles, discovery.relationships)

    # Build profile lookup
    profile_map: dict[str, list[ColumnProfile]] = {}
    for p in discovery.profiles:
        profile_map.setdefault(p.table_name, []).append(p)

    # Build companion context lookup
    companion_context_map = _build_companion_context_map(discovery)

    # Check if this is a local (Ollama) provider for prompt simplification
    compact = _is_compact_provider(provider)

    # Enrich each table with LLM
    for table in discovery.tables:
        table_profiles = profile_map.get(table.name, [])
        companion_ctx = companion_context_map.get(table.name)
        await _enrich_table_with_llm(
            table,
            table_profiles,
            discovery.relationships,
            provider,
            companion_ctx,
            compact=compact,
        )

    discovery.domains = build_domain_map(discovery.tables)
    return discovery


def _apply_deep_descriptions(discovery: DiscoveryResult) -> None:
    """Apply heuristic deep descriptions to all unlocked tables."""
    profile_map: dict[str, list[ColumnProfile]] = {}
    for p in discovery.profiles:
        profile_map.setdefault(p.table_name, []).append(p)

    companion_context_map = _build_companion_context_map(discovery)

    for table in discovery.tables:
        if table.locked and table.semantic_detail is not None:
            continue
        table_profiles = profile_map.get(table.name, [])
        companion_ctx = companion_context_map.get(table.name)
        table.semantic_detail = generate_deep_table_description(
            table,
            table_profiles,
            discovery.relationships,
            companion_ctx,
        )


def _build_companion_context_map(discovery: DiscoveryResult) -> dict[str, str]:
    """Build a table_name -> companion context string mapping."""
    context_map: dict[str, str] = {}
    for doc in discovery.companion_docs:
        for table_name in doc.matched_tables:
            existing = context_map.get(table_name, "")
            separator = "\n\n" if existing else ""
            context_map[table_name] = existing + separator + f"[From {doc.filename}]\n{doc.content}"
    return context_map


def _is_compact_provider(provider: LLMProvider) -> bool:
    """Check if the provider needs simplified prompts (e.g., Ollama)."""
    from headwater.analyzer.ollama import OllamaProvider

    return isinstance(provider, OllamaProvider)


async def _enrich_table_with_llm(
    table: TableInfo,
    profiles: list[ColumnProfile],
    relationships: list[Relationship],
    provider: LLMProvider,
    companion_context: str | None = None,
    *,
    compact: bool = False,
) -> None:
    """Enrich a single table using the LLM. Updates table in place.

    Locked tables have their descriptions preserved. Locked columns are skipped
    and their existing descriptions are passed as ground truth to the LLM prompt.
    """
    # Count and log locked columns
    locked_cols = [c for c in table.columns if c.locked]
    if locked_cols:
        logger.info(
            "Skipped enrichment for %d locked column(s) in table %s",
            len(locked_cols),
            table.name,
        )

    if table.locked:
        logger.info("Skipping LLM enrichment for locked table %s", table.name)
        return

    if compact:
        prompt = _build_compact_table_prompt(table, profiles, relationships, companion_context)
    else:
        prompt = _build_deep_table_prompt(table, profiles, relationships, companion_context)

    cache_key = make_cache_key(table.name, [c.name for c in table.columns])

    logger.info("Analyzing table %s (cache_key=%s)", table.name, cache_key)
    result = await provider.analyze(prompt, system=_DEEP_SYSTEM_PROMPT)

    if not result:
        return  # Keep heuristic descriptions

    # Apply LLM results
    _apply_llm_result(table, result, companion_context, compact=compact)


_DEEP_SYSTEM_PROMPT = (
    "You are a data catalog assistant that produces rich semantic documentation. "
    "Analyze database table metadata and provide detailed business-level descriptions. "
    "Respond with valid JSON only. Never guess or hallucinate -- if unsure, say 'Unknown'. "
    "You receive only schema metadata and statistical summaries, never raw data rows."
)


def _build_deep_table_prompt(
    table: TableInfo,
    profiles: list[ColumnProfile],
    relationships: list[Relationship],
    companion_context: str | None = None,
) -> str:
    """Build a deep inference LLM prompt. Uses only metadata, never raw rows."""
    col_lines = _build_column_lines(table, profiles)
    rel_lines = _build_relationship_lines(table, relationships)
    heuristic_lines = _build_heuristic_lines(table)
    locked_section = _build_locked_section(table)
    companion_section = (
        f"\nCompanion documentation (use as context):\n{companion_context}\n"
        if companion_context
        else ""
    )

    return f"""Analyze this database table and provide deep semantic documentation.

Table: {table.name} ({table.row_count:,} rows)

Columns (with profiling statistics):
{chr(10).join(col_lines)}

Relationships:
{chr(10).join(rel_lines) if rel_lines else "  None detected"}

Heuristic classification (use as starting point, correct if wrong):
{chr(10).join(heuristic_lines)}
{companion_section}{locked_section}
Respond as JSON:
{{
  "description": "Concise 1-2 sentence summary",
  "domain": "Business domain",
  "narrative": "3-5 sentence narrative: what this table is, its purpose, related tables",
  "row_semantics": "What each row represents (e.g., 'Each row is a daily air quality reading')",
  "business_process": "The business process this table captures",
  "temporal_grain": "daily|monthly|yearly|event-based|snapshot|none",
  "key_dimensions": ["primary grouping columns"],
  "key_metrics": ["primary measurement columns"],
  "column_groups": {{
    "group_label": ["col1", "col2"]
  }},
  "columns": {{
    "column_name": {{
      "description": "Business-level description",
      "semantic_type": "id|dimension|metric|temporal|geographic|text|pii|foreign_key",
      "business_description": "Rich explanation of what this value means to a business user",
      "semantic_group": "logical grouping name",
      "example_interpretation": "What a specific value means in context"
    }}
  }}
}}"""


def _build_compact_table_prompt(
    table: TableInfo,
    profiles: list[ColumnProfile],
    relationships: list[Relationship],
    companion_context: str | None = None,
) -> str:
    """Build a simplified prompt for smaller/local models (Ollama)."""
    col_lines = _build_column_lines(table, profiles)
    rel_lines = _build_relationship_lines(table, relationships)
    companion_section = (
        f"\nDocumentation context:\n{companion_context}\n" if companion_context else ""
    )

    return f"""Analyze this database table.

Table: {table.name} ({table.row_count:,} rows)

Columns:
{chr(10).join(col_lines)}

Relationships:
{chr(10).join(rel_lines) if rel_lines else "  None detected"}
{companion_section}
Respond as JSON:
{{
  "description": "1-2 sentence description of what this table represents",
  "domain": "Business domain",
  "row_semantics": "What each row represents",
  "columns": {{
    "column_name": {{
      "description": "what this column represents",
      "semantic_type": "id|dimension|metric|temporal|geographic|text|pii|foreign_key"
    }}
  }}
}}"""


def _build_column_lines(
    table: TableInfo,
    profiles: list[ColumnProfile],
) -> list[str]:
    """Build column summary lines with stats for prompt."""
    profile_map = {p.column_name: p for p in profiles}
    col_lines = []
    for col in table.columns:
        p = profile_map.get(col.name)
        line = f"  - {col.name} ({col.dtype})"
        if p:
            parts = [f"nulls={p.null_rate:.0%}", f"distinct={p.distinct_count}"]
            if p.top_values:
                top3 = [v for v, _ in p.top_values[:3]]
                parts.append(f"top_values={top3}")
            if p.min_value is not None:
                parts.append(f"range=[{p.min_value}, {p.max_value}]")
            if p.mean is not None:
                parts.append(f"mean={p.mean:.2f}")
            if p.stddev is not None and p.stddev > 0:
                parts.append(f"stddev={p.stddev:.2f}")
            if p.detected_pattern:
                parts.append(f"pattern={p.detected_pattern}")
            if p.min_date:
                parts.append(f"date_range=[{p.min_date}, {p.max_date}]")
            line += f" -- {', '.join(parts)}"
        col_lines.append(line)
    return col_lines


def _build_relationship_lines(
    table: TableInfo,
    relationships: list[Relationship],
) -> list[str]:
    """Build relationship lines for prompt."""
    rel_lines = []
    for r in relationships:
        if r.from_table == table.name or r.to_table == table.name:
            rel_lines.append(
                f"  - {r.from_table}.{r.from_column} -> "
                f"{r.to_table}.{r.to_column} ({r.type}, integrity={r.referential_integrity:.0%})"
            )
    return rel_lines


def _build_heuristic_lines(table: TableInfo) -> list[str]:
    """Build heuristic classification lines for the deep prompt."""
    lines = []
    for col in table.columns:
        if col.locked:
            continue
        parts = []
        if col.semantic_type:
            parts.append(f"semantic_type={col.semantic_type}")
        if col.role:
            parts.append(f"role={col.role}")
        if col.confidence > 0:
            parts.append(f"confidence={col.confidence:.2f}")
        if parts:
            lines.append(f"  - {col.name}: {', '.join(parts)}")
    return lines


def _build_locked_section(table: TableInfo) -> str:
    """Build locked columns section for prompt."""
    locked_col_lines = [
        f"  - {c.name}: LOCKED -- ground truth: {c.description!r}"
        for c in table.columns
        if c.locked and c.description
    ]
    if not locked_col_lines:
        return ""
    return "\nLocked columns (do not re-classify, use as ground truth):\n" + "\n".join(
        locked_col_lines
    )


def _apply_llm_result(
    table: TableInfo,
    result: dict,
    companion_context: str | None,
    *,
    compact: bool = False,
) -> None:
    """Apply parsed LLM response to table and its columns."""
    from headwater.analyzer.ollama import OllamaProvider  # noqa: F401

    # Basic fields
    if "description" in result:
        table.description = result["description"]
    if "domain" in result:
        table.domain = result["domain"]

    # Column-level descriptions and semantic types
    if "columns" in result and isinstance(result["columns"], dict):
        for col in table.columns:
            if col.locked:
                continue
            col_data = result["columns"].get(col.name, {})
            if isinstance(col_data, dict):
                if "description" in col_data:
                    col.description = col_data["description"]
                if "semantic_type" in col_data:
                    col.semantic_type = col_data["semantic_type"]

    # Build semantic detail from LLM response
    semantic_columns: dict[str, ColumnSemanticDetail] = {}
    if "columns" in result and isinstance(result["columns"], dict):
        for col_name, col_data in result["columns"].items():
            if not isinstance(col_data, dict):
                continue
            semantic_columns[col_name] = ColumnSemanticDetail(
                business_description=col_data.get("business_description"),
                semantic_group=col_data.get("semantic_group"),
                example_interpretation=col_data.get("example_interpretation"),
            )

    confidence = 0.6 if compact else 0.8

    table.semantic_detail = TableSemanticDetail(
        narrative=result.get("narrative"),
        row_semantics=result.get("row_semantics"),
        business_process=result.get("business_process"),
        temporal_grain=result.get("temporal_grain"),
        key_dimensions=result.get("key_dimensions", []),
        key_metrics=result.get("key_metrics", []),
        column_groups=result.get("column_groups", {}),
        semantic_columns=semantic_columns,
        companion_context=companion_context,
        inference_confidence=confidence,
    )
