"""Tests for the semantic analyzer and heuristics."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from headwater.analyzer.heuristics import (
    build_domain_map,
    classify_domain,
    classify_semantic_type,
    enrich_tables,
    generate_column_description,
    generate_table_description,
)
from headwater.analyzer.llm import NoLLMProvider, make_cache_key
from headwater.analyzer.semantic import analyze
from headwater.connectors.json_loader import JsonLoader
from headwater.core.models import ColumnInfo, SourceConfig, TableInfo
from headwater.profiler.engine import discover

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sample"


@pytest.fixture()
def discovery_result():
    """Full discovery result from sample data."""
    con = duckdb.connect(":memory:")
    loader = JsonLoader()
    loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
    loader.load_to_duckdb(con, "env_health")
    source = SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR))
    return discover(con, "env_health", source)


# -- Heuristics ------------------------------------------------------------


class TestHeuristics:
    def test_table_description(self):
        table = TableInfo(
            name="sensors",
            row_count=832,
            columns=[
                ColumnInfo(name="sensor_id", dtype="varchar"),
                ColumnInfo(name="site_id", dtype="varchar"),
            ],
        )
        desc = generate_table_description(table)
        assert "832" in desc

    def test_classify_domain_sensors(self):
        table = TableInfo(
            name="sensors",
            columns=[
                ColumnInfo(name="sensor_id", dtype="varchar"),
                ColumnInfo(name="sensor_type", dtype="varchar"),
                ColumnInfo(name="calibration_status", dtype="varchar"),
            ],
        )
        domain = classify_domain(table)
        assert "Environmental" in domain or "Monitoring" in domain

    def test_classify_domain_incidents(self):
        table = TableInfo(
            name="incidents",
            columns=[
                ColumnInfo(name="incident_id", dtype="varchar"),
                ColumnInfo(name="patient_age", dtype="int64"),
                ColumnInfo(name="severity", dtype="varchar"),
            ],
        )
        domain = classify_domain(table)
        assert "Health" in domain

    def test_column_description_id(self):
        desc = generate_column_description("zone_id", "zones")
        assert "identifier" in desc.lower() or "id" in desc.lower()

    def test_column_description_fk(self):
        desc = generate_column_description("zone_id", "sites")
        assert "reference" in desc.lower() or "zone" in desc.lower()

    def test_column_description_date(self):
        desc = generate_column_description("inspection_date", "inspections")
        assert "timestamp" in desc.lower() or "date" in desc.lower()

    def test_semantic_type_id(self):
        assert classify_semantic_type("zone_id") == "id"

    def test_semantic_type_metric(self):
        assert classify_semantic_type("violation_count") == "metric"

    def test_semantic_type_temporal(self):
        assert classify_semantic_type("created_at") == "temporal"

    def test_semantic_type_geographic(self):
        assert classify_semantic_type("latitude") == "geographic"


# -- LLM provider -----------------------------------------------------------


class TestLLMProvider:
    def test_no_llm_provider(self):
        import asyncio

        provider = NoLLMProvider()
        result = asyncio.run(provider.analyze("test prompt"))
        assert result == {}

    def test_cache_key_deterministic(self):
        key1 = make_cache_key("sensors", ["sensor_id", "site_id"])
        key2 = make_cache_key("sensors", ["site_id", "sensor_id"])
        assert key1 == key2  # Order independent

    def test_cache_key_different_tables(self):
        key1 = make_cache_key("sensors", ["sensor_id"])
        key2 = make_cache_key("sites", ["site_id"])
        assert key1 != key2


# -- Semantic analyzer (end-to-end, heuristic mode) -------------------------


class TestSemanticAnalyzer:
    def test_analyze_heuristic_mode(self, discovery_result):
        result = analyze(discovery_result, provider=None)

        # All tables should have descriptions
        for table in result.tables:
            assert table.description is not None, f"{table.name} missing description"
            assert table.domain is not None, f"{table.name} missing domain"

        # Domains should be populated
        assert len(result.domains) > 0

    def test_enrich_sets_semantic_types(self, discovery_result):
        enrich_tables(
            discovery_result.tables,
            discovery_result.profiles,
            discovery_result.relationships,
        )
        # Check that at least some columns got semantic types
        all_types = [
            col.semantic_type
            for t in discovery_result.tables
            for col in t.columns
            if col.semantic_type
        ]
        assert len(all_types) > 10  # Many columns should get types

    def test_domain_map(self, discovery_result):
        enrich_tables(
            discovery_result.tables,
            discovery_result.profiles,
            discovery_result.relationships,
        )
        domains = build_domain_map(discovery_result.tables)
        # Should have multiple domains
        assert len(domains) >= 3
        # All tables should be in some domain
        all_tables = {name for tables in domains.values() for name in tables}
        assert len(all_tables) == 8
