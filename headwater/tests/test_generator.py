"""Tests for the generation layer: staging, marts, contracts."""

from __future__ import annotations

from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    SourceConfig,
    TableInfo,
)
from headwater.generator.contracts import generate_contracts
from headwater.generator.marts import generate_mart_models
from headwater.generator.staging import generate_staging_models

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _sample_tables() -> list[TableInfo]:
    """Minimal table list covering the environmental health dataset."""
    return [
        TableInfo(
            name="zones",
            row_count=25,
            columns=[
                ColumnInfo(name="zone_id", dtype="varchar", is_primary_key=True),
                ColumnInfo(name="name", dtype="varchar"),
                ColumnInfo(name="population", dtype="int64"),
            ],
        ),
        TableInfo(
            name="sites",
            row_count=500,
            columns=[
                ColumnInfo(name="site_id", dtype="varchar", is_primary_key=True),
                ColumnInfo(name="zone_id", dtype="varchar"),
                ColumnInfo(name="site_type", dtype="varchar"),
            ],
        ),
        TableInfo(
            name="sensors",
            row_count=832,
            columns=[
                ColumnInfo(name="sensor_id", dtype="varchar", is_primary_key=True),
                ColumnInfo(name="site_id", dtype="varchar"),
                ColumnInfo(name="sensor_type", dtype="varchar"),
            ],
        ),
        TableInfo(
            name="readings",
            row_count=49302,
            columns=[
                ColumnInfo(name="reading_id", dtype="varchar", is_primary_key=True),
                ColumnInfo(name="sensor_id", dtype="varchar"),
                ColumnInfo(name="value", dtype="float64"),
                ColumnInfo(name="timestamp", dtype="timestamp"),
            ],
        ),
        TableInfo(
            name="inspections",
            row_count=1243,
            columns=[
                ColumnInfo(name="inspection_id", dtype="varchar", is_primary_key=True),
                ColumnInfo(name="site_id", dtype="varchar"),
                ColumnInfo(name="score", dtype="float64"),
                ColumnInfo(name="result", dtype="varchar"),
            ],
        ),
        TableInfo(
            name="incidents",
            row_count=5000,
            columns=[
                ColumnInfo(name="incident_id", dtype="varchar", is_primary_key=True),
                ColumnInfo(name="zone_id", dtype="varchar"),
                ColumnInfo(name="incident_type", dtype="varchar"),
                ColumnInfo(name="severity", dtype="varchar"),
            ],
        ),
        TableInfo(
            name="complaints",
            row_count=3000,
            columns=[
                ColumnInfo(name="complaint_id", dtype="varchar", is_primary_key=True),
                ColumnInfo(name="zone_id", dtype="varchar"),
                ColumnInfo(name="category", dtype="varchar"),
                ColumnInfo(name="priority", dtype="varchar"),
            ],
        ),
        TableInfo(
            name="programs",
            row_count=10,
            columns=[
                ColumnInfo(name="program_id", dtype="varchar", is_primary_key=True),
                ColumnInfo(name="name", dtype="varchar"),
                ColumnInfo(name="budget_usd", dtype="float64"),
            ],
        ),
    ]


def _sample_discovery() -> DiscoveryResult:
    return DiscoveryResult(
        source=SourceConfig(name="sample", type="json", path="/data/sample"),
        tables=_sample_tables(),
    )


def _sample_profiles() -> list[ColumnProfile]:
    return [
        # Not-null + unique column
        ColumnProfile(
            table_name="zones",
            column_name="zone_id",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=25,
            uniqueness_ratio=1.0,
            min_length=4,
            max_length=8,
            avg_length=6.0,
        ),
        # Not-null, non-unique string with low cardinality
        ColumnProfile(
            table_name="sites",
            column_name="site_type",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=5,
            uniqueness_ratio=0.01,
            top_values=[
                ("restaurant", 200), ("school", 120), ("factory", 80),
                ("park", 60), ("hospital", 40),
            ],
        ),
        # Numeric column with range
        ColumnProfile(
            table_name="inspections",
            column_name="score",
            dtype="float64",
            null_count=0,
            null_rate=0.0,
            distinct_count=90,
            uniqueness_ratio=0.07,
            min_value=20.0,
            max_value=100.0,
            mean=78.5,
            median=80.0,
        ),
        # Column with some nulls (should NOT get not_null contract)
        ColumnProfile(
            table_name="incidents",
            column_name="severity",
            dtype="varchar",
            null_count=10,
            null_rate=0.002,
            distinct_count=4,
            uniqueness_ratio=0.0008,
            top_values=[("high", 1200), ("medium", 2000), ("low", 1500), ("critical", 300)],
        ),
        # Non-negative numeric with zero min
        ColumnProfile(
            table_name="zones",
            column_name="population",
            dtype="int64",
            null_count=0,
            null_rate=0.0,
            distinct_count=25,
            uniqueness_ratio=1.0,
            min_value=0.0,
            max_value=50000.0,
        ),
    ]


# ---------------------------------------------------------------------------
# Staging generator tests
# ---------------------------------------------------------------------------


class TestStagingGenerator:
    def test_generates_one_model_per_table(self):
        tables = _sample_tables()
        models = generate_staging_models(tables, source_schema="env_health")
        assert len(models) == len(tables)

    def test_model_names_prefixed(self):
        models = generate_staging_models(_sample_tables(), source_schema="env_health")
        for m in models:
            assert m.name.startswith("stg_")

    def test_all_auto_approved(self):
        models = generate_staging_models(_sample_tables(), source_schema="env_health")
        for m in models:
            assert m.status == "approved"
            assert m.model_type == "staging"

    def test_sql_contains_source_reference(self):
        models = generate_staging_models(_sample_tables(), source_schema="env_health")
        zones_model = next(m for m in models if m.name == "stg_zones")
        assert "env_health.zones" in zones_model.sql

    def test_sql_contains_loaded_at(self):
        models = generate_staging_models(_sample_tables(), source_schema="env_health")
        for m in models:
            assert "_loaded_at" in m.sql

    def test_target_schema_respected(self):
        models = generate_staging_models(
            _sample_tables(), source_schema="env_health", target_schema="my_staging"
        )
        for m in models:
            assert "my_staging" in m.sql

    def test_timestamp_columns_cast(self):
        tables = [
            TableInfo(
                name="events",
                columns=[
                    ColumnInfo(name="event_id", dtype="varchar"),
                    ColumnInfo(name="created_at", dtype="timestamp"),
                ],
            )
        ]
        models = generate_staging_models(tables, source_schema="raw")
        sql = models[0].sql
        assert 'CAST("created_at" AS TIMESTAMP)' in sql

    def test_source_tables_populated(self):
        models = generate_staging_models(_sample_tables(), source_schema="env_health")
        zones_model = next(m for m in models if m.name == "stg_zones")
        assert zones_model.source_tables == ["zones"]


# ---------------------------------------------------------------------------
# Mart generator tests
# ---------------------------------------------------------------------------


class TestMartGenerator:
    def test_generates_at_least_one_mart_for_rich_discovery(self):
        """Full sample dataset should produce at least one mart proposal."""
        discovery = _sample_discovery()
        models = generate_mart_models(discovery)
        assert len(models) >= 1

    def test_all_proposed_status(self):
        models = generate_mart_models(_sample_discovery())
        for m in models:
            assert m.status == "proposed"
            assert m.model_type == "mart"

    def test_each_has_assumptions_and_questions(self):
        models = generate_mart_models(_sample_discovery())
        for m in models:
            assert len(m.assumptions) > 0
            assert len(m.questions) > 0

    def test_target_schema_substituted(self):
        models = generate_mart_models(_sample_discovery(), target_schema="analytics")
        for m in models:
            assert "analytics." in m.sql
            assert "{{target_schema}}" not in m.sql

    def test_depends_on_staging(self):
        models = generate_mart_models(_sample_discovery())
        for m in models:
            for dep in m.depends_on:
                assert dep.startswith("stg_")

    def test_no_relationships_no_metrics_produces_zero_proposals(self):
        """US-503: Source with no relationships and no metric/temporal columns = 0 marts."""
        from headwater.core.models import ColumnInfo as CI

        discovery = DiscoveryResult(
            source=SourceConfig(name="simple", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="tags",
                    row_count=10,
                    columns=[
                        CI(name="tag_id", dtype="varchar", semantic_type="id"),
                        CI(name="tag_name", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[],
        )
        models = generate_mart_models(discovery)
        assert len(models) == 0

    def test_temporal_column_gets_period_comparison(self):
        """US-501: A source with a temporal column gets a period_comparison proposal."""
        from headwater.core.models import ColumnInfo as CI, Relationship

        discovery = DiscoveryResult(
            source=SourceConfig(name="events", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="events",
                    row_count=500,
                    columns=[
                        CI(name="event_id", dtype="varchar", semantic_type="id"),
                        CI(name="event_date", dtype="timestamp", semantic_type="temporal"),
                        CI(name="revenue", dtype="float64", semantic_type="metric"),
                    ],
                ),
            ],
            relationships=[],
        )
        models = generate_mart_models(discovery)
        archetypes = {m.name for m in models}
        assert any("by_period" in n for n in archetypes), (
            f"Expected a period_comparison mart. Got: {archetypes}"
        )

    def test_metric_with_fk_gets_entity_summary(self):
        """US-501: A source with metric columns + FK to dimension gets entity_summary."""
        from headwater.core.models import ColumnInfo as CI, Relationship

        discovery = DiscoveryResult(
            source=SourceConfig(name="sales", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="orders",
                    row_count=1000,
                    columns=[
                        CI(name="order_id", dtype="varchar", semantic_type="id"),
                        CI(name="customer_id", dtype="varchar", semantic_type="foreign_key"),
                        CI(name="amount", dtype="float64", semantic_type="metric"),
                        CI(name="quantity", dtype="int64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="customers",
                    row_count=200,
                    columns=[
                        CI(name="customer_id", dtype="varchar", semantic_type="id"),
                        CI(name="country", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[
                Relationship(
                    from_table="orders",
                    from_column="customer_id",
                    to_table="customers",
                    to_column="customer_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.99,
                    source="inferred_name",
                ),
                Relationship(
                    from_table="orders",
                    from_column="customer_id",
                    to_table="customers",
                    to_column="customer_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.99,
                    source="inferred_value",
                ),
            ],
        )
        models = generate_mart_models(discovery)
        archetypes = {m.name for m in models}
        assert any("by_customers" in n for n in archetypes), (
            f"Expected an entity_summary mart. Got: {archetypes}"
        )


# ---------------------------------------------------------------------------
# Contract generator tests
# ---------------------------------------------------------------------------


class TestContractGenerator:
    def test_not_null_contract(self):
        profiles = _sample_profiles()
        rules = generate_contracts(profiles)
        not_null_rules = [r for r in rules if r.rule_type == "not_null"]
        # zone_id, site_type, score, population all have 0 nulls
        assert len(not_null_rules) >= 3

    def test_no_not_null_for_nullable_column(self):
        profiles = _sample_profiles()
        rules = generate_contracts(profiles)
        # severity has nulls so should NOT get a not_null rule
        severity_nn = [
            r for r in rules if r.rule_type == "not_null" and r.column_name == "severity"
        ]
        assert len(severity_nn) == 0

    def test_unique_contract(self):
        profiles = _sample_profiles()
        rules = generate_contracts(profiles)
        unique_rules = [r for r in rules if r.rule_type == "unique"]
        # zone_id has 100% uniqueness and >1 distinct
        zone_unique = [r for r in unique_rules if r.column_name == "zone_id"]
        assert len(zone_unique) == 1

    def test_range_contract(self):
        profiles = _sample_profiles()
        rules = generate_contracts(profiles)
        range_rules = [r for r in rules if r.rule_type == "range"]
        # score and population have numeric min/max
        assert len(range_rules) >= 2

    def test_range_headroom(self):
        """Range contract should have headroom beyond observed values."""
        profiles = _sample_profiles()
        rules = generate_contracts(profiles)
        score_range = next(
            r for r in rules if r.rule_type == "range" and r.column_name == "score"
        )
        # Observed range is 20-100, headroom = 40, so lower=0 (clamped), upper=140
        assert "BETWEEN" in score_range.expression

    def test_cardinality_contract(self):
        profiles = _sample_profiles()
        rules = generate_contracts(profiles)
        card_rules = [r for r in rules if r.rule_type == "cardinality"]
        # site_type has 5 distinct values, varchar, <=30 distinct
        site_type_card = [r for r in card_rules if r.column_name == "site_type"]
        assert len(site_type_card) == 1
        assert "restaurant" in site_type_card[0].expression

    def test_no_cardinality_for_high_cardinality(self):
        """score has 90 distinct values -- should NOT get cardinality contract."""
        profiles = _sample_profiles()
        rules = generate_contracts(profiles)
        score_card = [
            r for r in rules if r.rule_type == "cardinality" and r.column_name == "score"
        ]
        assert len(score_card) == 0

    def test_all_proposed_status(self):
        rules = generate_contracts(_sample_profiles())
        for r in rules:
            assert r.status == "proposed"

    def test_all_have_ids(self):
        rules = generate_contracts(_sample_profiles())
        ids = [r.id for r in rules]
        assert all(id_ is not None for id_ in ids)
        # All IDs should be unique
        assert len(set(ids)) == len(ids)

    def test_model_prefix(self):
        rules = generate_contracts(_sample_profiles(), model_prefix="raw_")
        for r in rules:
            assert "raw_" in r.model_name

    def test_schema_qualified_model_name(self):
        rules = generate_contracts(_sample_profiles(), target_schema="my_schema")
        for r in rules:
            assert r.model_name.startswith("my_schema.")

    def test_non_negative_range_floor(self):
        """For non-negative columns, range lower bound should not go below 0."""
        profiles = _sample_profiles()
        rules = generate_contracts(profiles)
        pop_range = next(
            r for r in rules if r.rule_type == "range" and r.column_name == "population"
        )
        # min_value=0, max_value=50000, headroom=25000, lower=max(0, -25000)=0
        assert "BETWEEN 0.00" in pop_range.expression
