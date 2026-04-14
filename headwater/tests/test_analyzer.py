"""Tests for the semantic analyzer and heuristics."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from headwater.analyzer.heuristics import (
    _refine_semantic_type,
    build_domain_map,
    classify_domain,
    classify_domains,
    classify_semantic_type,
    enrich_tables,
    generate_column_description,
    generate_table_description,
)
from headwater.analyzer.llm import NoLLMProvider, _parse_json_response, make_cache_key
from headwater.analyzer.semantic import analyze
from headwater.connectors.json_loader import JsonLoader
from headwater.core.models import ColumnInfo, ColumnProfile, SourceConfig, TableInfo
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

    def test_classify_domain_fallback(self):
        """Standalone classify_domain returns 'General' (use classify_domains instead)."""
        table = TableInfo(
            name="sensors",
            columns=[
                ColumnInfo(name="sensor_id", dtype="varchar"),
                ColumnInfo(name="sensor_type", dtype="varchar"),
                ColumnInfo(name="calibration_status", dtype="varchar"),
            ],
        )
        assert classify_domain(table) == "General"

    def test_classify_domains_relationship_cluster(self):
        """Tables connected by FK relationships are grouped into one domain."""
        from headwater.core.models import Relationship

        tables = [
            TableInfo(
                name="orders",
                row_count=100,
                columns=[
                    ColumnInfo(name="order_id", dtype="varchar"),
                ],
            ),
            TableInfo(
                name="order_items",
                row_count=500,
                columns=[
                    ColumnInfo(name="item_id", dtype="varchar"),
                    ColumnInfo(name="order_id", dtype="varchar"),
                ],
            ),
            TableInfo(
                name="customers",
                row_count=50,
                columns=[
                    ColumnInfo(name="customer_id", dtype="varchar"),
                ],
            ),
        ]
        rels = [
            Relationship(
                from_table="order_items",
                from_column="order_id",
                to_table="orders",
                to_column="order_id",
                type="many_to_one",
                confidence=1.0,
                referential_integrity=1.0,
                source="inferred_name",
            ),
        ]
        result = classify_domains(tables, rels)
        # orders and order_items share the same domain
        assert result["orders"] == result["order_items"]
        # customers is separate
        assert result["customers"] != result["orders"]

    def test_classify_domains_vocabulary_cluster(self):
        """Unconnected tables with shared column tokens are grouped together."""
        tables = [
            TableInfo(
                name="sales_east",
                row_count=10,
                columns=[
                    ColumnInfo(name="region", dtype="varchar"),
                    ColumnInfo(name="product", dtype="varchar"),
                    ColumnInfo(name="revenue", dtype="float64"),
                    ColumnInfo(name="quarter", dtype="varchar"),
                ],
            ),
            TableInfo(
                name="sales_west",
                row_count=10,
                columns=[
                    ColumnInfo(name="region", dtype="varchar"),
                    ColumnInfo(name="product", dtype="varchar"),
                    ColumnInfo(name="revenue", dtype="float64"),
                    ColumnInfo(name="quarter", dtype="varchar"),
                ],
            ),
        ]
        result = classify_domains(tables, [])
        # Should be grouped together via shared vocabulary
        assert result["sales_east"] == result["sales_west"]

    def test_classify_domains_common_prefix(self):
        """Tables sharing a name prefix get a label derived from that prefix."""
        from headwater.core.models import Relationship

        tables = [
            TableInfo(
                name="aqs_sites",
                row_count=10,
                columns=[
                    ColumnInfo(name="site_id", dtype="varchar"),
                ],
            ),
            TableInfo(
                name="aqs_monitors",
                row_count=20,
                columns=[
                    ColumnInfo(name="monitor_id", dtype="varchar"),
                ],
            ),
        ]
        rels = [
            Relationship(
                from_table="aqs_monitors",
                from_column="site_id",
                to_table="aqs_sites",
                to_column="site_id",
                type="many_to_one",
                confidence=1.0,
                referential_integrity=1.0,
                source="inferred_name",
            ),
        ]
        result = classify_domains(tables, rels)
        assert "Aqs" in result["aqs_sites"]
        assert result["aqs_sites"] == result["aqs_monitors"]

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

    def test_complaint_type_311_is_dimension(self):
        assert classify_semantic_type("complaint_type_311") == "dimension"

    def test_census_tract_is_dimension(self):
        assert classify_semantic_type("census_tract") == "dimension"

    def test_site_num_is_dimension(self):
        assert classify_semantic_type("site_num") == "dimension"

    def test_community_board_is_dimension(self):
        assert classify_semantic_type("community_board") == "dimension"

    def test_complaint_no_is_dimension(self):
        assert classify_semantic_type("complaint_no") == "dimension"

    def test_type_suffix_with_version(self):
        """Type columns with numeric suffixes are still dimensions."""
        assert classify_semantic_type("incident_type_2") == "dimension"


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
        # Should have at least one domain
        assert len(domains) >= 1
        # All tables should be in some domain
        all_tables = {name for tables in domains.values() for name in tables}
        assert len(all_tables) == 8


# -- Profile-aware refinement ------------------------------------------------


class TestRefineSemanticType:
    def test_high_uniqueness_numeric_dimension_becomes_id(self):
        """complaint_number: name-pattern says 'dimension' but 0.95 uniqueness -> 'id'."""
        col = ColumnInfo(name="complaint_number", dtype="int64", semantic_type="dimension")
        profile = ColumnProfile(
            table_name="t",
            column_name="complaint_number",
            dtype="int64",
            distinct_count=9500,
            uniqueness_ratio=0.95,
        )
        assert _refine_semantic_type(col, profile, 10000) == "id"

    def test_low_cardinality_null_type_becomes_dimension(self):
        """county: no name-pattern match, 60 distinct / 10K rows -> 'dimension'."""
        col = ColumnInfo(name="county", dtype="varchar", semantic_type=None)
        profile = ColumnProfile(
            table_name="t",
            column_name="county",
            dtype="varchar",
            distinct_count=60,
        )
        assert _refine_semantic_type(col, profile, 10000) == "dimension"

    def test_numeric_moderate_cardinality_with_mean_becomes_metric(self):
        """response_days: no pattern, int64, 500 distinct, has mean -> 'metric'."""
        col = ColumnInfo(name="response_days", dtype="int64", semantic_type=None)
        profile = ColumnProfile(
            table_name="t",
            column_name="response_days",
            dtype="int64",
            distinct_count=500,
            mean=15.3,
        )
        assert _refine_semantic_type(col, profile, 10000) == "metric"

    def test_existing_geographic_type_preserved(self):
        """latitude: already 'geographic' from name-pattern, should not change."""
        col = ColumnInfo(name="latitude", dtype="float64", semantic_type="geographic")
        profile = ColumnProfile(
            table_name="t",
            column_name="latitude",
            dtype="float64",
            distinct_count=8000,
            uniqueness_ratio=0.8,
        )
        assert _refine_semantic_type(col, profile, 10000) == "geographic"

    def test_existing_metric_type_preserved(self):
        """severity_score: already 'metric' from name-pattern, should not change."""
        col = ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric")
        profile = ColumnProfile(
            table_name="t",
            column_name="severity_score",
            dtype="float64",
            distinct_count=500,
            mean=5.2,
        )
        assert _refine_semantic_type(col, profile, 10000) == "metric"

    def test_null_type_high_uniqueness_numeric_becomes_id(self):
        """Unnamed numeric column with 0.95 uniqueness -> 'id'."""
        col = ColumnInfo(name="record_key", dtype="int64", semantic_type=None)
        profile = ColumnProfile(
            table_name="t",
            column_name="record_key",
            dtype="int64",
            distinct_count=9500,
            uniqueness_ratio=0.95,
        )
        assert _refine_semantic_type(col, profile, 10000) == "id"

    def test_adaptive_threshold_scales_with_row_count(self):
        """Small table (100 rows): threshold = sqrt(100) = 10."""
        col = ColumnInfo(name="category", dtype="varchar", semantic_type=None)
        # 15 distinct values > 10 threshold for 100-row table
        profile = ColumnProfile(
            table_name="t",
            column_name="category",
            dtype="varchar",
            distinct_count=15,
        )
        # Should NOT be dimension (15 > 10)
        assert _refine_semantic_type(col, profile, 100) is None

        # 8 distinct values <= 10 threshold
        profile2 = ColumnProfile(
            table_name="t",
            column_name="category",
            dtype="varchar",
            distinct_count=8,
        )
        assert _refine_semantic_type(col, profile2, 100) == "dimension"

    def test_enrich_uses_profiles(self):
        """Integration: enrich_tables uses profiles for refinement."""
        table = TableInfo(
            name="complaints",
            row_count=10000,
            columns=[
                ColumnInfo(name="complaint_number", dtype="int64"),
                ColumnInfo(name="county", dtype="varchar"),
                ColumnInfo(name="latitude", dtype="float64"),
                ColumnInfo(name="severity_score", dtype="float64"),
            ],
        )
        profiles = [
            ColumnProfile(
                table_name="complaints",
                column_name="complaint_number",
                dtype="int64",
                distinct_count=9500,
                uniqueness_ratio=0.95,
            ),
            ColumnProfile(
                table_name="complaints",
                column_name="county",
                dtype="varchar",
                distinct_count=60,
            ),
            ColumnProfile(
                table_name="complaints",
                column_name="latitude",
                dtype="float64",
                distinct_count=8000,
                uniqueness_ratio=0.8,
                mean=40.7,
            ),
            ColumnProfile(
                table_name="complaints",
                column_name="severity_score",
                dtype="float64",
                distinct_count=500,
                mean=5.2,
            ),
        ]
        enriched = enrich_tables([table], profiles, [])

        cols = {c.name: c for c in enriched[0].columns}
        # complaint_number: .*number$ -> dimension -> refined to id
        assert cols["complaint_number"].semantic_type == "id"
        # county: ^county$ -> dimension (name pattern hits now)
        assert cols["county"].semantic_type == "dimension"
        # latitude: .*latitude.* -> geographic (preserved)
        assert cols["latitude"].semantic_type == "geographic"
        # severity_score: .*score.* -> metric (preserved)
        assert cols["severity_score"].semantic_type == "metric"


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
