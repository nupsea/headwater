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
from headwater.analyzer.llm import NoLLMProvider, _parse_json_response, make_cache_key
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


# -- LLM response parser (US-604) -------------------------------------------


class TestParseJsonResponse:
    def test_plain_json(self):
        result = _parse_json_response('{"description": "test", "domain": "Health"}')
        assert result["description"] == "test"

    def test_json_in_backtick_fence(self):
        text = '```json\n{"description": "test"}\n```'
        result = _parse_json_response(text)
        assert result["description"] == "test"

    def test_json_in_plain_fence(self):
        text = '```\n{"description": "test"}\n```'
        result = _parse_json_response(text)
        assert result["description"] == "test"

    def test_plain_sql_select(self):
        result = _parse_json_response("SELECT * FROM foo WHERE x > 1")
        assert result == {"sql": "SELECT * FROM foo WHERE x > 1"}

    def test_plain_sql_create(self):
        result = _parse_json_response("CREATE TABLE foo AS SELECT 1")
        assert result == {"sql": "CREATE TABLE foo AS SELECT 1"}

    def test_plain_sql_with(self):
        result = _parse_json_response("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert "sql" in result

    def test_sql_in_backtick_fence(self):
        text = "```sql\nSELECT * FROM foo\n```"
        result = _parse_json_response(text)
        assert result == {"sql": "SELECT * FROM foo"}

    def test_unparseable_returns_empty(self):
        result = _parse_json_response("This is just some random text that isn't JSON or SQL.")
        assert result == {}


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


# -- Semantic locks (US-201) ------------------------------------------------


class TestSemanticLocks:
    def test_locked_column_description_preserved(self):
        """A locked column keeps its description after re-enrichment."""
        table = TableInfo(
            name="sensors",
            row_count=10,
            columns=[
                ColumnInfo(
                    name="sensor_id",
                    dtype="varchar",
                    description="Human-approved: unique sensor identifier",
                    locked=True,
                ),
                ColumnInfo(name="site_id", dtype="varchar"),
            ],
        )
        enrich_tables([table], [], [])
        # Locked column description must be unchanged
        locked_col = next(c for c in table.columns if c.name == "sensor_id")
        assert locked_col.description == "Human-approved: unique sensor identifier"

    def test_unlocked_column_gets_enriched(self):
        """An unlocked column gets a heuristic description."""
        table = TableInfo(
            name="sensors",
            row_count=10,
            columns=[
                ColumnInfo(name="site_id", dtype="varchar", locked=False),
            ],
        )
        enrich_tables([table], [], [])
        col = table.columns[0]
        assert col.description is not None
        assert col.description != ""

    def test_locked_table_description_preserved(self):
        """A locked table keeps its description after re-enrichment."""
        table = TableInfo(
            name="sensors",
            row_count=10,
            columns=[],
            description="Human-approved table description",
            locked=True,
        )
        enrich_tables([table], [], [])
        assert table.description == "Human-approved table description"

    def test_column_locked_flag_default_false(self):
        col = ColumnInfo(name="x", dtype="varchar")
        assert col.locked is False

    def test_table_locked_flag_default_false(self):
        table = TableInfo(name="x", columns=[])
        assert table.locked is False
