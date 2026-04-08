"""Profiler orchestrator -- runs schema extraction, stats, and relationship detection."""

from __future__ import annotations

from datetime import datetime

import duckdb

from headwater.core.models import DiscoveryResult, SourceConfig
from headwater.profiler.relationships import detect_relationships
from headwater.profiler.schema import extract_schema
from headwater.profiler.stats import profile_all


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
    relationships = detect_relationships(con, schema, tables)

    return DiscoveryResult(
        source=source,
        tables=tables,
        profiles=profiles,
        relationships=relationships,
        discovered_at=datetime.now(),
    )
