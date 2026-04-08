"""Semantic analyzer -- enriches discovery results with descriptions and domains.

Uses LLM when available, falls back to heuristics.
Never sends raw data rows -- only schema metadata and statistical summaries.
"""

from __future__ import annotations

import asyncio
import logging

from headwater.analyzer.heuristics import build_domain_map, enrich_tables
from headwater.analyzer.llm import LLMProvider, NoLLMProvider, make_cache_key
from headwater.core.models import ColumnProfile, DiscoveryResult, Relationship, TableInfo

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

    # Enrich each table with LLM
    for table in discovery.tables:
        table_profiles = profile_map.get(table.name, [])
        await _enrich_table_with_llm(table, table_profiles, discovery.relationships, provider)

    discovery.domains = build_domain_map(discovery.tables)
    return discovery


async def _enrich_table_with_llm(
    table: TableInfo,
    profiles: list[ColumnProfile],
    relationships: list[Relationship],
    provider: LLMProvider,
) -> None:
    """Enrich a single table using the LLM. Updates table in place."""
    prompt = _build_table_prompt(table, profiles, relationships)
    cache_key = make_cache_key(table.name, [c.name for c in table.columns])

    logger.info("Analyzing table %s (cache_key=%s)", table.name, cache_key)
    result = await provider.analyze(prompt, system=_SYSTEM_PROMPT)

    if not result:
        return  # Keep heuristic descriptions

    # Apply LLM results, overriding heuristics
    if "description" in result:
        table.description = result["description"]
    if "domain" in result:
        table.domain = result["domain"]
    if "columns" in result and isinstance(result["columns"], dict):
        for col in table.columns:
            col_data = result["columns"].get(col.name, {})
            if isinstance(col_data, dict):
                if "description" in col_data:
                    col.description = col_data["description"]
                if "semantic_type" in col_data:
                    col.semantic_type = col_data["semantic_type"]


_SYSTEM_PROMPT = """You are a data catalog assistant. Analyze database table metadata and provide
descriptions and classifications. Respond with valid JSON only. Never guess or hallucinate --
if unsure, say so. You receive only schema metadata and statistical summaries, never raw data."""


def _build_table_prompt(
    table: TableInfo,
    profiles: list[ColumnProfile],
    relationships: list[Relationship],
) -> str:
    """Build an LLM prompt for table analysis. Uses only metadata, never raw rows."""
    # Column summary with stats
    col_lines = []
    profile_map = {p.column_name: p for p in profiles}
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
            if p.detected_pattern:
                parts.append(f"pattern={p.detected_pattern}")
            line += f" -- {', '.join(parts)}"
        col_lines.append(line)

    # Relationships
    rel_lines = []
    for r in relationships:
        if r.from_table == table.name or r.to_table == table.name:
            rel_lines.append(
                f"  - {r.from_table}.{r.from_column} -> "
                f"{r.to_table}.{r.to_column} ({r.type}, integrity={r.referential_integrity:.0%})"
            )

    return f"""Analyze this database table:

Table: {table.name} ({table.row_count} rows)
Columns:
{chr(10).join(col_lines)}

Relationships:
{chr(10).join(rel_lines) if rel_lines else "  None detected"}

Respond as JSON:
{{
  "description": "1-2 sentence description of what this table represents",
  "domain": "Business domain (e.g. Environmental Monitoring, Public Health, etc.)",
  "columns": {{
    "column_name": {{
      "description": "what this column represents",
      "semantic_type": "id|dimension|metric|temporal|geographic|text|pii|foreign_key"
    }}
  }}
}}"""
