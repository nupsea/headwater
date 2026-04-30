"""Profiler orchestrator -- runs schema extraction, stats, and relationship detection."""

from __future__ import annotations

from datetime import datetime

import duckdb

from headwater.core.models import DiscoveryResult, SourceConfig
from headwater.profiler.key_detection import detect_composite_keys, suggest_primary_keys
from headwater.profiler.relationships import detect_relationships
from headwater.profiler.schema import extract_schema
from headwater.profiler.stats import profile_all

_AUTO_PK_CONFIDENCE = 0.85
_AUTO_COMPOSITE_PK_CONFIDENCE = 0.7


def discover(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    source: SourceConfig,
    sample_size: int = 50_000,
) -> DiscoveryResult:
    """Run the full discovery pipeline: schema + profiles + relationships.

    Returns a DiscoveryResult with all tables, profiles, and relationships populated.
    Descriptions and domains are NOT filled -- that's the analyzer's job.
    """
    tables = extract_schema(con, schema)
    profiles = profile_all(con, schema, tables, sample_size)
    _apply_profile_key_detection(con, schema, tables, profiles)
    relationships = detect_relationships(con, schema, tables)

    return DiscoveryResult(
        source=source,
        tables=tables,
        profiles=profiles,
        relationships=relationships,
        discovered_at=datetime.now(),
    )


def _apply_profile_key_detection(con, schema: str, tables: list, profiles: list) -> None:
    """Promote high-confidence profile-derived PKs before FK detection runs."""
    table_row_counts = {t.name: t.row_count for t in tables}
    profile_by_table: dict[str, list[dict]] = {}
    for p in profiles:
        profile_by_table.setdefault(p.table_name, []).append(
            {
                "column_name": p.column_name,
                "dtype": p.dtype,
                "stats": {
                    "row_count": table_row_counts.get(p.table_name, 0),
                    "distinct_count": p.distinct_count,
                    "null_count": p.null_count,
                    "min": p.min_value,
                    "max": p.max_value,
                },
            }
        )

    for table in tables:
        if any(c.is_primary_key for c in table.columns):
            continue

        table_profiles = profile_by_table.get(table.name, [])
        if not table_profiles:
            continue

        pk_candidates = suggest_primary_keys(table.name, table_profiles)
        if pk_candidates and pk_candidates[0].confidence >= _AUTO_PK_CONFIDENCE:
            best = pk_candidates[0]
            col = next((c for c in table.columns if c.name == best.column), None)
            if col:
                col.is_primary_key = True
                col.semantic_type = "primary_key"
                col.role = "identifier"
                col.confidence = max(col.confidence, best.confidence)
            continue

        composite_candidates = detect_composite_keys(
            con,
            table.name,
            schema,
            table_profiles,
            columns=table.columns,
        )
        if composite_candidates and (
            composite_candidates[0].confidence >= _AUTO_COMPOSITE_PK_CONFIDENCE
        ):
            best_composite = composite_candidates[0]
            for col_name in best_composite.columns:
                col = next((c for c in table.columns if c.name == col_name), None)
                if col:
                    col.is_primary_key = True
                    col.role = "identifier"
                    col.confidence = max(col.confidence, best_composite.confidence)
