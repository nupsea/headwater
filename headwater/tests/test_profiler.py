"""Tests for the profiling engine -- stats, relationships, orchestrator."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from headwater.connectors.json_loader import JsonLoader
from headwater.core.models import SourceConfig
from headwater.profiler.engine import discover
from headwater.profiler.relationships import detect_relationships
from headwater.profiler.schema import extract_schema
from headwater.profiler.stats import profile_all

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sample"


@pytest.fixture()
def loaded_ddb() -> duckdb.DuckDBPyConnection:
    """DuckDB with sample data loaded."""
    con = duckdb.connect(":memory:")
    loader = JsonLoader()
    loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
    loader.load_to_duckdb(con, "env_health")
    return con


# -- Statistical profiler ---------------------------------------------------


class TestStatisticalProfiler:
    def test_profile_all_tables(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        profiles = profile_all(loaded_ddb, "env_health", tables)
        assert len(profiles) > 50  # ~103 columns across 8 tables

    def test_numeric_stats(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        profiles = profile_all(loaded_ddb, "env_health", tables)
        # zones.population should have numeric stats
        pop = next(
            (p for p in profiles if p.table_name == "zones" and p.column_name == "population"),
            None,
        )
        assert pop is not None
        assert pop.min_value is not None
        assert pop.max_value is not None
        assert pop.mean is not None
        assert pop.min_value > 0

    def test_string_stats(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        profiles = profile_all(loaded_ddb, "env_health", tables)
        # zones.name should have string stats
        name_prof = next(
            (p for p in profiles if p.table_name == "zones" and p.column_name == "name"),
            None,
        )
        assert name_prof is not None
        assert name_prof.min_length is not None
        assert name_prof.max_length is not None
        assert name_prof.avg_length is not None

    def test_null_rate(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        profiles = profile_all(loaded_ddb, "env_health", tables)
        # complaints.resolution_date has nulls (unresolved complaints)
        res = next(
            (p for p in profiles
             if p.table_name == "complaints" and p.column_name == "resolution_date"),
            None,
        )
        assert res is not None
        assert res.null_rate > 0  # Some complaints are unresolved

    def test_top_values_low_cardinality(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        profiles = profile_all(loaded_ddb, "env_health", tables)
        # zones.type should have top_values (low cardinality)
        zone_type = next(
            (p for p in profiles if p.table_name == "zones" and p.column_name == "type"),
            None,
        )
        assert zone_type is not None
        assert zone_type.top_values is not None
        assert len(zone_type.top_values) > 0

    def test_uniqueness_ratio(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        profiles = profile_all(loaded_ddb, "env_health", tables)
        # zone_id in zones should be unique
        zid = next(
            (p for p in profiles if p.table_name == "zones" and p.column_name == "zone_id"),
            None,
        )
        assert zid is not None
        assert zid.uniqueness_ratio == 1.0


# -- Relationship detector --------------------------------------------------


class TestRelationshipDetector:
    def test_detects_relationships(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        rels = detect_relationships(loaded_ddb, "env_health", tables)
        assert len(rels) >= 5  # At least the main FKs

    def test_sites_to_zones(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        rels = detect_relationships(loaded_ddb, "env_health", tables)
        site_zone = next(
            (r for r in rels if r.from_table == "sites" and r.to_table == "zones"),
            None,
        )
        assert site_zone is not None
        assert site_zone.from_column == "zone_id"
        assert site_zone.referential_integrity > 0.9

    def test_sensors_to_sites(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        rels = detect_relationships(loaded_ddb, "env_health", tables)
        sensor_site = next(
            (r for r in rels if r.from_table == "sensors" and r.to_table == "sites"),
            None,
        )
        assert sensor_site is not None
        assert sensor_site.referential_integrity > 0.9

    def test_confidence_scores(self, loaded_ddb: duckdb.DuckDBPyConnection):
        tables = extract_schema(loaded_ddb, "env_health")
        rels = detect_relationships(loaded_ddb, "env_health", tables)
        for r in rels:
            assert 0.0 <= r.confidence <= 1.0
            assert 0.0 <= r.referential_integrity <= 1.0


# -- Profiler engine (orchestrator) -----------------------------------------


class TestProfilerEngine:
    def test_discover_returns_complete_result(self, loaded_ddb: duckdb.DuckDBPyConnection):
        source = SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR))
        result = discover(loaded_ddb, "env_health", source)

        assert len(result.tables) == 8
        assert len(result.profiles) > 50
        assert len(result.relationships) >= 5
        assert result.discovered_at is not None

    def test_discover_tables_have_columns(self, loaded_ddb: duckdb.DuckDBPyConnection):
        source = SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR))
        result = discover(loaded_ddb, "env_health", source)

        for table in result.tables:
            assert len(table.columns) > 0, f"Table {table.name} has no columns"
