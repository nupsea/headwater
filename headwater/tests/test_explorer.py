"""Tests for the explorer layer -- suggestions, statistical insights, NL-to-SQL, visualization."""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
import polars as pl
import pytest

from headwater.analyzer.llm import LLMProvider
from headwater.api.routes.insights import compute_semantic_highlights, compute_top_insights
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    ContractCheckResult,
    ContractRule,
    DatasetContext,
    DiscoveryResult,
    ExplorationResult,
    GeneratedModel,
    Relationship,
    SourceConfig,
    StatisticalInsight,
    SuggestedQuestion,
    TableInfo,
    VisualizationSpec,
)
from headwater.explorer.nl_to_sql import (
    _build_vocabulary,
    _check_grounding,
    _heuristic_sql,
    _is_read_only,
    _match_suggestion,
    _questions_similar,
    _repair_loop,
    ask,
)
from headwater.explorer.statistical import (
    _detect_change_points_for_column,
    _detect_correlations,
    _detect_temporal_anomalies,
    _find_metric_columns,
    _find_temporal_columns,
    detect_insights,
    detect_insights_with_diagnostics,
)
from headwater.explorer.suggestions import generate_suggestions
from headwater.explorer.visualization import _classify_columns, recommend_visualization

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_discovery() -> DiscoveryResult:
    """A minimal discovery result for testing."""
    return DiscoveryResult(
        source=SourceConfig(name="test", type="json", path="/data"),
        tables=[
            TableInfo(
                name="readings",
                row_count=1000,
                columns=[
                    ColumnInfo(name="reading_id", dtype="int64", semantic_type="id"),
                    ColumnInfo(name="site_id", dtype="varchar", semantic_type="foreign_key"),
                    ColumnInfo(name="value", dtype="float64", semantic_type="metric"),
                    ColumnInfo(name="timestamp", dtype="timestamp", semantic_type="temporal"),
                    ColumnInfo(name="sensor_type", dtype="varchar", semantic_type="dimension"),
                ],
                domain="Environmental Monitoring",
            ),
            TableInfo(
                name="sites",
                row_count=50,
                columns=[
                    ColumnInfo(name="site_id", dtype="varchar", semantic_type="id"),
                    ColumnInfo(name="name", dtype="varchar", semantic_type="dimension"),
                    ColumnInfo(name="zone_id", dtype="varchar", semantic_type="foreign_key"),
                ],
                domain="Environmental Monitoring",
            ),
        ],
        profiles=[
            ColumnProfile(
                table_name="readings",
                column_name="value",
                dtype="float64",
                null_count=0,
                distinct_count=800,
                min_value=0.5,
                max_value=150.0,
                mean=35.0,
                stddev=20.0,
            ),
        ],
        relationships=[
            Relationship(
                from_table="readings",
                from_column="site_id",
                to_table="sites",
                to_column="site_id",
                type="many_to_one",
                confidence=0.95,
                referential_integrity=0.98,
                source="inferred_name",
            ),
        ],
    )


@pytest.fixture()
def sample_models() -> list[GeneratedModel]:
    """Sample models for testing."""
    return [
        GeneratedModel(
            name="stg_readings",
            model_type="staging",
            sql="SELECT * FROM readings",
            description="Staging readings",
            source_tables=["readings"],
            status="executed",
        ),
        GeneratedModel(
            name="mart_air_quality_daily",
            model_type="mart",
            sql="SELECT ... FROM staging.stg_readings",
            description="Daily air quality averages",
            source_tables=["readings", "sensors", "sites", "zones"],
            status="executed",
            assumptions=["Only valid readings included"],
            questions=["Are AQI breakpoints correct?"],
        ),
    ]


@pytest.fixture()
def duckdb_con() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with sample data."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")

    # Create a time-series table with a deliberate anomaly
    base_date = date(2024, 1, 1)
    dates = [base_date + timedelta(days=i) for i in range(180)]
    values = []
    for i, _d in enumerate(dates):
        # Normal baseline ~50, anomaly around day 90-100 (spike to ~120)
        if 90 <= i <= 100:
            values.append(120.0 + (i % 5) * 2)
        else:
            values.append(50.0 + (i % 7) * 3)
    zones = ["Zone A" if i % 2 == 0 else "Zone B" for i in range(180)]

    df = pl.DataFrame(
        {
            "reading_date": dates,
            "avg_value": values,
            "zone_name": zones,
            "reading_count": [10 + i % 5 for i in range(180)],
        }
    )
    arrow = df.to_arrow()
    con.register("_tmp_mart", arrow)
    con.execute("CREATE TABLE marts.mart_air_quality_daily AS SELECT * FROM _tmp_mart")
    con.unregister("_tmp_mart")

    # Create a staging table
    df2 = pl.DataFrame(
        {
            "site_id": ["S1", "S2", "S3"] * 60,
            "value": [float(i) + 10.5 for i in range(180)],
            "sensor_type": ["pm25", "ozone", "no2"] * 60,
        }
    )
    arrow2 = df2.to_arrow()
    con.register("_tmp_stg", arrow2)
    con.execute("CREATE TABLE staging.stg_readings AS SELECT * FROM _tmp_stg")
    con.unregister("_tmp_stg")

    yield con
    con.close()


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestModels:
    def test_suggested_question_model(self):
        q = SuggestedQuestion(
            question="What is the average PM2.5?",
            source="mart",
            category="Air Quality",
            relevant_tables=["readings"],
            sql_hint="SELECT AVG(value) FROM readings",
        )
        assert q.source == "mart"
        assert q.sql_hint is not None

    def test_statistical_insight_model(self):
        i = StatisticalInsight(
            metric="avg_value",
            table_name="mart_air_quality_daily",
            insight_type="temporal_anomaly",
            description="Spike detected",
            magnitude=35.5,
            z_score=2.8,
            p_value=0.005,
            confidence_level="99%",
            severity="warning",
        )
        assert i.insight_type == "temporal_anomaly"
        assert i.severity == "warning"

    def test_visualization_spec_model(self):
        v = VisualizationSpec(
            chart_type="line",
            title="Air Quality Trends",
            x_axis="reading_date",
            y_axis="avg_value",
        )
        assert v.chart_type == "line"

    def test_exploration_result_model(self):
        r = ExplorationResult(
            question="What is the average?",
            sql="SELECT AVG(value) FROM t",
            data=[{"avg": 42.5}],
            row_count=1,
        )
        assert r.row_count == 1
        assert r.error is None


# ---------------------------------------------------------------------------
# Suggestion tests
# ---------------------------------------------------------------------------


class TestSuggestions:
    def test_generates_mart_suggestions(self, sample_discovery, sample_models):
        questions = generate_suggestions(
            discovery=sample_discovery,
            models=sample_models,
        )
        mart_qs = [q for q in questions if q.source == "mart"]
        assert len(mart_qs) > 0
        assert all(q.sql_hint is not None for q in mart_qs)
        assert all(q.category for q in mart_qs)
        assert not any("key metrics" in q.question.lower() for q in mart_qs)

    def test_mart_suggestions_available_when_proposed(self, sample_discovery):
        """Mart questions should appear even when mart status is proposed."""
        models = [
            GeneratedModel(
                name="mart_air_quality_daily",
                model_type="mart",
                sql="SELECT ...",
                description="test",
                status="proposed",
            ),
        ]
        questions = generate_suggestions(
            discovery=sample_discovery,
            models=models,
        )
        mart_qs = [q for q in questions if q.source == "mart"]
        assert len(mart_qs) > 0
        assert any("air quality" in q.question.lower() for q in mart_qs)
        assert not any("key metrics" in q.question.lower() for q in mart_qs)

    def test_mart_suggestions_use_materialized_columns(self, sample_discovery, sample_models):
        con = duckdb.connect(":memory:")
        try:
            con.execute("CREATE SCHEMA marts")
            con.execute(
                "CREATE TABLE marts.mart_air_quality_daily AS "
                "SELECT * FROM (VALUES "
                "(DATE '2026-01-01', 10.0), "
                "(DATE '2026-01-02', 12.0)"
                ") AS t(period, avg_value)"
            )

            questions = generate_suggestions(
                discovery=sample_discovery,
                models=sample_models,
                con=con,
            )
        finally:
            con.close()

        mart_qs = [q for q in questions if q.source == "mart"]
        assert any("changed over time" in q.question.lower() for q in mart_qs)
        assert not any("key metrics" in q.question.lower() for q in mart_qs)

    def test_business_signal_questions_are_prioritized(self, sample_discovery):
        business_insights = [
            {
                "id": "temporal_peak:readings:timestamp",
                "table": "readings",
                "column": "value",
                "group_by_column": "timestamp",
                "group_by_grain": "day",
                "metric": "period_total",
                "chart_type": "line",
            },
            {
                "id": "metric_driver:readings:sensor_type:value",
                "table": "readings",
                "column": "sensor_type",
                "group_by_column": "sensor_type",
                "metric": "value",
                "chart_type": "bar",
            },
        ]

        questions = generate_suggestions(
            discovery=sample_discovery,
            business_insights=business_insights,
        )

        assert questions
        assert questions[0].source == "business"
        assert questions[0].sql_hint is not None
        assert any("changed over time" in q.question.lower() for q in questions)

    def test_encoded_dimension_questions_use_readable_labels(self, duckdb_con):
        duckdb_con.execute(
            """
            CREATE TABLE staging.stg_yellow_trips AS
            SELECT * FROM (VALUES
                (1, 2),
                (1, 3),
                (2, 1),
                (2, 4)
            ) AS t(payment_type, passenger_count)
            """
        )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="yellow_trips",
                    row_count=4,
                    columns=[
                        ColumnInfo(name="payment_type", dtype="int64", semantic_type="dimension"),
                        ColumnInfo(name="passenger_count", dtype="int64", semantic_type="metric"),
                    ],
                )
            ],
            profiles=[
                ColumnProfile(
                    table_name="yellow_trips",
                    column_name="payment_type",
                    dtype="int64",
                    null_count=0,
                    distinct_count=2,
                ),
                ColumnProfile(
                    table_name="yellow_trips",
                    column_name="passenger_count",
                    dtype="int64",
                    null_count=0,
                    distinct_count=4,
                ),
            ],
        )
        models = [
            GeneratedModel(
                name="stg_yellow_trips",
                model_type="staging",
                sql="SELECT * FROM yellow_trips",
                description="Yellow trips staging",
                source_tables=["yellow_trips"],
                status="executed",
            )
        ]

        questions = generate_suggestions(
            discovery=discovery,
            models=models,
            con=duckdb_con,
        )
        question = next(
            q for q in questions if "payment method" in q.question.lower()
        )

        result = ask(
            question=question.question,
            con=duckdb_con,
            discovery=discovery,
            models=models,
            suggestions=questions,
        )

        assert result.error is None
        assert result.data
        assert result.data[0]["dimension"] in {"Credit card", "Cash"}
        assert all(not isinstance(row["dimension"], int) for row in result.data)

    def test_distribution_questions_return_bucketed_results(self, duckdb_con):
        duckdb_con.execute(
            """
            CREATE TABLE staging.stg_measurements AS
            SELECT i AS value
            FROM range(1, 101) AS t(i)
            """
        )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="measurements",
                    row_count=100,
                    columns=[
                        ColumnInfo(name="value", dtype="int64", semantic_type="metric"),
                    ],
                )
            ],
            profiles=[
                ColumnProfile(
                    table_name="measurements",
                    column_name="value",
                    dtype="int64",
                    null_count=0,
                    distinct_count=100,
                    min_value=1,
                    max_value=100,
                    mean=50.5,
                ),
            ],
        )
        models = [
            GeneratedModel(
                name="stg_measurements",
                model_type="staging",
                sql="SELECT * FROM measurements",
                description="Measurements staging",
                source_tables=["measurements"],
                status="executed",
            )
        ]

        questions = generate_suggestions(
            discovery=discovery,
            models=models,
            con=duckdb_con,
        )
        question = next(
            q for q in questions if "distribution of value" in q.question.lower()
        )

        result = ask(
            question=question.question,
            con=duckdb_con,
            discovery=discovery,
            models=models,
            suggestions=questions,
        )

        assert result.error is None
        assert result.row_count > 1
        assert result.visualization is not None
        assert result.visualization.chart_type == "bar"
        assert {"bucket", "records"} <= set(result.data[0])

    def test_suggestions_diversify_repetitive_trends(self):
        tables = []
        profiles = []
        for idx in range(8):
            table_name = f"events_{idx}"
            tables.append(
                TableInfo(
                    name=table_name,
                    row_count=100,
                    columns=[
                        ColumnInfo(name="event_date", dtype="date", semantic_type="temporal"),
                        ColumnInfo(name="value", dtype="float64", semantic_type="metric"),
                        ColumnInfo(name="category", dtype="varchar", semantic_type="dimension"),
                    ],
                )
            )
            profiles.extend(
                [
                    ColumnProfile(
                        table_name=table_name,
                        column_name="value",
                        dtype="float64",
                        null_count=0,
                        distinct_count=80,
                    ),
                    ColumnProfile(
                        table_name=table_name,
                        column_name="category",
                        dtype="varchar",
                        null_count=0,
                        distinct_count=5,
                    ),
                ]
            )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=tables,
            profiles=profiles,
        )

        questions = generate_suggestions(discovery=discovery)
        trend_questions = [
            q for q in questions if "changed over time" in q.question.lower()
        ]

        assert len(trend_questions) <= 3
        assert any("highest value" in q.question.lower() for q in questions)

    def test_generates_domain_questions(self, sample_discovery):
        questions = generate_suggestions(discovery=sample_discovery)
        semantic_qs = [q for q in questions if q.source == "semantic"]
        assert len(semantic_qs) > 0
        # Questions should be BI-oriented, not column-level
        for q in semantic_qs:
            assert q.sql_hint is not None

    def test_generates_relationship_suggestions_with_sql(self, sample_discovery):
        questions = generate_suggestions(discovery=sample_discovery)
        rel_qs = [q for q in questions if q.source == "relationship"]
        assert len(rel_qs) > 0
        assert any("readings" in q.relevant_tables for q in rel_qs)
        # All relationship questions should have SQL hints
        assert all(q.sql_hint is not None for q in rel_qs)

    def test_generates_quality_suggestions(self, sample_discovery):
        contracts = [
            ContractRule(
                id="c1",
                model_name="stg_readings",
                column_name="value",
                rule_type="not_null",
                expression='"value" IS NOT NULL',
            ),
        ]
        results = [
            ContractCheckResult(
                rule_id="c1",
                model_name="stg_readings",
                passed=False,
                message="5 nulls found",
            ),
        ]
        questions = generate_suggestions(
            discovery=sample_discovery,
            contracts=contracts,
            quality_results=results,
        )
        quality_qs = [q for q in questions if q.source == "quality"]
        assert len(quality_qs) > 0
        assert "Data Quality" in quality_qs[0].category

    def test_all_questions_have_sql_hints(self, sample_discovery, sample_models):
        questions = generate_suggestions(
            discovery=sample_discovery,
            models=sample_models,
        )
        for q in questions:
            assert q.sql_hint is not None, f"Missing SQL hint: {q.question}"

    def test_empty_discovery_returns_empty(self):
        empty = DiscoveryResult(
            source=SourceConfig(name="empty", type="json", path="/x"),
        )
        questions = generate_suggestions(discovery=empty)
        assert isinstance(questions, list)

    def test_no_latitude_in_suggested_questions(self):
        """Latitude/longitude should never appear as metrics in suggestions."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="sites",
                    row_count=1000,
                    columns=[
                        ColumnInfo(
                            name="site_id",
                            dtype="varchar",
                            semantic_type="id",
                        ),
                        ColumnInfo(
                            name="site_name",
                            dtype="varchar",
                            semantic_type="dimension",
                        ),
                        ColumnInfo(
                            name="latitude",
                            dtype="float64",
                            semantic_type="geographic",
                        ),
                        ColumnInfo(
                            name="longitude",
                            dtype="float64",
                            semantic_type="geographic",
                        ),
                        ColumnInfo(
                            name="reading_value",
                            dtype="float64",
                            semantic_type="metric",
                        ),
                        ColumnInfo(
                            name="created_date",
                            dtype="date",
                            semantic_type="temporal",
                        ),
                    ],
                ),
            ],
            profiles=[
                ColumnProfile(
                    table_name="sites",
                    column_name="latitude",
                    dtype="float64",
                    distinct_count=900,
                ),
                ColumnProfile(
                    table_name="sites",
                    column_name="longitude",
                    dtype="float64",
                    distinct_count=900,
                ),
                ColumnProfile(
                    table_name="sites",
                    column_name="reading_value",
                    dtype="float64",
                    distinct_count=500,
                ),
            ],
        )
        questions = generate_suggestions(discovery=discovery)
        for q in questions:
            assert "latitude" not in q.question.lower(), (
                f"latitude should not appear in: {q.question}"
            )
            assert "longitude" not in q.question.lower(), (
                f"longitude should not appear in: {q.question}"
            )


class TestCrossTableSuggestions:
    """Tests for SchemaGraph-based cross-table suggestion generation."""

    def test_cross_table_suggestions_generated(self):
        """Cross-table suggestions should be generated when tables are joinable."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="orders",
                    row_count=5000,
                    columns=[
                        ColumnInfo(
                            name="order_id", dtype="int64",
                            semantic_type="id", is_primary_key=True,
                        ),
                        ColumnInfo(name="customer_id", dtype="int64", semantic_type="foreign_key"),
                        ColumnInfo(name="amount", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="customers",
                    row_count=200,
                    columns=[
                        ColumnInfo(
                            name="customer_id", dtype="int64",
                            semantic_type="id", is_primary_key=True,
                        ),
                        ColumnInfo(name="region", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            profiles=[
                ColumnProfile(
                    table_name="orders",
                    column_name="amount",
                    dtype="float64",
                    null_count=0,
                    distinct_count=3000,
                    min_value=1.0,
                    max_value=1000.0,
                    mean=150.0,
                ),
                ColumnProfile(
                    table_name="customers",
                    column_name="region",
                    dtype="varchar",
                    null_count=0,
                    distinct_count=10,
                    top_values=[("East", 80), ("West", 60), ("North", 40)],
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
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
            ],
        )
        questions = generate_suggestions(discovery=discovery)
        cross_qs = [q for q in questions if q.source == "cross_table"]
        assert len(cross_qs) > 0, "Should generate cross-table suggestions when join paths exist"
        assert all(q.sql_hint is not None for q in cross_qs)
        assert all(len(q.relevant_tables) == 2 for q in cross_qs)

    def test_cross_table_uses_count_when_only_weak_numeric_attribute_exists(self):
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="incidents",
                    row_count=5000,
                    columns=[
                        ColumnInfo(name="incident_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="foreign_key"),
                        ColumnInfo(name="patient_age", dtype="int64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="complaints",
                    row_count=200,
                    columns=[
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="category", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            profiles=[
                ColumnProfile(
                    table_name="incidents",
                    column_name="patient_age",
                    dtype="int64",
                    null_count=0,
                    distinct_count=90,
                    min_value=1,
                    max_value=95,
                    mean=42,
                ),
                ColumnProfile(
                    table_name="complaints",
                    column_name="category",
                    dtype="varchar",
                    null_count=0,
                    distinct_count=8,
                ),
            ],
            relationships=[
                Relationship(
                    from_table="incidents",
                    from_column="complaint_id",
                    to_table="complaints",
                    to_column="complaint_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
            ],
        )

        questions = generate_suggestions(discovery=discovery)
        cross_qs = [q for q in questions if q.source == "cross_table"]

        assert cross_qs
        assert not any("average patient age" in q.question.lower() for q in cross_qs)
        assert any("how many incidents records" in q.question.lower() for q in cross_qs)

    def test_cross_table_suggestions_require_direct_join(self):
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="incidents",
                    row_count=5000,
                    columns=[
                        ColumnInfo(name="incident_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="zone_id", dtype="int64", semantic_type="foreign_key"),
                        ColumnInfo(name="patient_age", dtype="int64", semantic_type="metric"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="zones",
                    row_count=20,
                    columns=[
                        ColumnInfo(name="zone_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="site_id", dtype="int64", semantic_type="foreign_key"),
                        ColumnInfo(name="borough", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
                TableInfo(
                    name="sites",
                    row_count=200,
                    columns=[
                        ColumnInfo(name="site_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="unit", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            profiles=[
                ColumnProfile(
                    table_name="incidents",
                    column_name="patient_age",
                    dtype="int64",
                    null_count=0,
                    distinct_count=90,
                ),
                ColumnProfile(
                    table_name="incidents",
                    column_name="severity_score",
                    dtype="float64",
                    null_count=0,
                    distinct_count=400,
                ),
                ColumnProfile(
                    table_name="zones",
                    column_name="borough",
                    dtype="varchar",
                    null_count=0,
                    distinct_count=5,
                ),
                ColumnProfile(
                    table_name="sites",
                    column_name="unit",
                    dtype="varchar",
                    null_count=0,
                    distinct_count=20,
                ),
            ],
            relationships=[
                Relationship(
                    from_table="incidents",
                    from_column="zone_id",
                    to_table="zones",
                    to_column="zone_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
                Relationship(
                    from_table="zones",
                    from_column="site_id",
                    to_table="sites",
                    to_column="site_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
            ],
        )

        questions = generate_suggestions(discovery=discovery)
        cross_qs = [q for q in questions if q.source == "cross_table"]

        assert any("by borough" in q.question.lower() for q in cross_qs)
        assert not any("by unit" in q.question.lower() for q in cross_qs)

    def test_cross_table_prefers_business_metric_over_age(self):
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="incidents",
                    row_count=5000,
                    columns=[
                        ColumnInfo(name="incident_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="foreign_key"),
                        ColumnInfo(name="patient_age", dtype="int64", semantic_type="metric"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="complaints",
                    row_count=200,
                    columns=[
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="category", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            profiles=[
                ColumnProfile(
                    table_name="incidents",
                    column_name="patient_age",
                    dtype="int64",
                    null_count=0,
                    distinct_count=90,
                ),
                ColumnProfile(
                    table_name="incidents",
                    column_name="severity_score",
                    dtype="float64",
                    null_count=0,
                    distinct_count=500,
                ),
                ColumnProfile(
                    table_name="complaints",
                    column_name="category",
                    dtype="varchar",
                    null_count=0,
                    distinct_count=8,
                ),
            ],
            relationships=[
                Relationship(
                    from_table="incidents",
                    from_column="complaint_id",
                    to_table="complaints",
                    to_column="complaint_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
            ],
        )

        questions = generate_suggestions(discovery=discovery)
        cross_qs = [q for q in questions if q.source == "cross_table"]

        assert any("average severity score" in q.question.lower() for q in cross_qs)
        assert not any("average patient age" in q.question.lower() for q in cross_qs)

    def test_cross_table_no_join_path_no_suggestions(self):
        """Cross-table suggestions should not be generated without join paths."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="orders",
                    row_count=5000,
                    columns=[
                        ColumnInfo(name="order_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="amount", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="products",
                    row_count=100,
                    columns=[
                        ColumnInfo(name="product_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="category", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            profiles=[],
            relationships=[],
        )
        questions = generate_suggestions(discovery=discovery)
        cross_qs = [q for q in questions if q.source == "cross_table"]
        assert len(cross_qs) == 0, "Should not generate cross-table suggestions without join paths"

    def test_extra_relationships_enrich_suggestions(self):
        """Confirmed PK/FK relationships should produce additional suggestions."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="invoices",
                    row_count=3000,
                    columns=[
                        ColumnInfo(name="invoice_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="vendor_id", dtype="int64", semantic_type="foreign_key"),
                        ColumnInfo(name="total", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="vendors",
                    row_count=50,
                    columns=[
                        ColumnInfo(
                            name="vendor_id", dtype="int64",
                            semantic_type="id", is_primary_key=True,
                        ),
                        ColumnInfo(name="industry", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            profiles=[
                ColumnProfile(
                    table_name="vendors",
                    column_name="industry",
                    dtype="varchar",
                    null_count=0,
                    distinct_count=8,
                    top_values=[("Tech", 20), ("Finance", 15)],
                ),
            ],
            relationships=[],  # No auto-detected relationships
        )

        # Without extra_relationships: no cross-table or relationship suggestions
        qs_without = generate_suggestions(discovery=discovery)
        cross_without = [q for q in qs_without if q.source in ("cross_table", "relationship")]

        # With confirmed FK relationship
        extra = [
            Relationship(
                from_table="invoices",
                from_column="vendor_id",
                to_table="vendors",
                to_column="vendor_id",
                type="many_to_one",
                confidence=1.0,
                referential_integrity=1.0,
                source="declared",
            ),
        ]
        qs_with = generate_suggestions(discovery=discovery, extra_relationships=extra)
        cross_with = [q for q in qs_with if q.source in ("cross_table", "relationship")]

        assert len(cross_with) > len(cross_without), (
            "Confirmed relationships should produce additional cross-table suggestions"
        )


# ---------------------------------------------------------------------------
# Statistical tests
# ---------------------------------------------------------------------------


class TestStatistical:
    def test_find_temporal_columns(self):
        df = pl.DataFrame(
            {
                "d": [date(2024, 1, 1), date(2024, 1, 2)],
                "v": [1.0, 2.0],
            }
        )
        assert _find_temporal_columns(df) == ["d"]

    def test_find_metric_columns(self):
        df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "site_id": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
                "value": [1.0, 2.5, 3.0, 4.5, 5.0, 6.5, 7.0, 8.5, 9.0, 10.5],
                "count": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            }
        )
        metrics = _find_metric_columns(df)
        assert "value" in metrics
        assert "count" in metrics
        assert "site_id" not in metrics  # *_id excluded
        assert "id" not in metrics

    def test_detect_temporal_anomalies(self, duckdb_con):
        arrow = duckdb_con.execute("SELECT * FROM marts.mart_air_quality_daily").arrow()
        df = pl.from_arrow(arrow)
        insights = _detect_temporal_anomalies(
            df, "mart_air_quality_daily", "reading_date", "avg_value"
        )
        # Should detect the anomaly around day 90-100
        anomalies = [i for i in insights if i.insight_type == "temporal_anomaly"]
        assert len(anomalies) > 0
        # The spike should be positive deviation
        assert any(i.magnitude > 0 for i in anomalies)

    def test_detect_change_points(self, duckdb_con):
        arrow = duckdb_con.execute("SELECT * FROM marts.mart_air_quality_daily").arrow()
        df = pl.from_arrow(arrow)
        insights = _detect_change_points_for_column(
            df, "mart_air_quality_daily", "reading_date", "avg_value"
        )
        # Change points may or may not be detected depending on data shape
        assert isinstance(insights, list)

    def test_detect_correlations(self):
        # Create a DataFrame with correlated columns
        import random

        random.seed(42)
        n = 100
        x = [float(i) for i in range(n)]
        y = [v * 2.0 + random.gauss(0, 5) for v in x]
        z = [random.gauss(50, 10) for _ in range(n)]
        df = pl.DataFrame({"x": x, "y": y, "z": z})
        insights = _detect_correlations(df, "test_table", ["x", "y", "z"])
        # x and y should be correlated
        corr_insights = [i for i in insights if i.insight_type == "correlation"]
        assert len(corr_insights) > 0
        xy = [i for i in corr_insights if "x" in i.metric and "y" in i.metric]
        assert len(xy) > 0
        assert xy[0].p_value is not None
        assert xy[0].p_value < 0.05

    def test_detect_insights_integration(self, duckdb_con):
        insights = detect_insights(duckdb_con, schema="marts")
        assert isinstance(insights, list)
        # Should find at least some insights from the anomalous data
        assert len(insights) > 0

    def test_detect_insights_empty_schema(self, duckdb_con):
        duckdb_con.execute("CREATE SCHEMA IF NOT EXISTS empty_schema")
        insights = detect_insights(duckdb_con, schema="empty_schema")
        assert insights == []

    def test_insight_severity_levels(self, duckdb_con):
        insights = detect_insights(duckdb_con, schema="marts")
        for i in insights:
            assert i.severity in ("info", "warning", "critical")
            if i.p_value is not None:
                assert 0 <= i.p_value <= 1

    def test_semantic_temporal_roles_cast_string_and_year_columns(self, duckdb_con):
        duckdb_con.execute(
            "CREATE TABLE staging.stg_string_dates AS "
            "SELECT * FROM (VALUES "
            "('2026-01-01', 1.0), ('2026-01-02', 2.0)"
            ") AS t(date_filed, amount)"
        )
        duckdb_con.execute(
            "CREATE TABLE staging.stg_years AS "
            "SELECT * FROM (VALUES (2025, 10.0), (2026, 11.0)) AS t(year, value)"
        )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="csv"),
            tables=[
                TableInfo(
                    name="string_dates",
                    row_count=2,
                    columns=[
                        ColumnInfo(
                            name="date_filed",
                            dtype="varchar",
                            role="temporal",
                            locked=True,
                        ),
                        ColumnInfo(name="amount", dtype="double"),
                    ],
                ),
                TableInfo(
                    name="years",
                    row_count=2,
                    columns=[
                        ColumnInfo(
                            name="year",
                            dtype="bigint",
                            role="temporal",
                            locked=True,
                        ),
                        ColumnInfo(name="value", dtype="double"),
                    ],
                ),
            ],
        )

        insights = detect_insights(duckdb_con, schema="staging", discovery=discovery)

        coverage_tables = {
            insight.table_name
            for insight in insights
            if insight.insight_type == "coverage_period"
        }
        assert {"string_dates", "years"}.issubset(coverage_tables)

    def test_semantic_diagnostics_report_generated_and_skipped_families(self, duckdb_con):
        duckdb_con.execute(
            "CREATE TABLE staging.stg_events AS "
            "SELECT * FROM (VALUES "
            "('2026-01-01', 1.0), ('2026-01-02', 2.0)"
            ") AS t(event_date, value)"
        )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="csv"),
            tables=[
                TableInfo(
                    name="events",
                    row_count=2,
                    columns=[
                        ColumnInfo(
                            name="event_date",
                            dtype="varchar",
                            role="temporal",
                            locked=True,
                        ),
                        ColumnInfo(name="value", dtype="double"),
                    ],
                )
            ],
        )

        result = detect_insights_with_diagnostics(
            duckdb_con,
            schema="staging",
            discovery=discovery,
        )

        statuses = {(d.family, d.status) for d in result.diagnostics}
        assert ("coverage", "generated") in statuses
        assert ("duration", "skipped") in statuses
        duration = next(d for d in result.diagnostics if d.family == "duration")
        assert duration.required_roles == ["duration_min"]
        assert duration.reason

    def test_semantic_model_lineage_maps_mart_table(self, duckdb_con):
        duckdb_con.execute(
            "CREATE TABLE marts.mart_event_summary AS "
            "SELECT * FROM (VALUES "
            "('2026-01-01', 1.0), ('2026-01-02', 2.0)"
            ") AS t(event_date, value)"
        )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="csv"),
            tables=[
                TableInfo(
                    name="events",
                    row_count=2,
                    columns=[
                        ColumnInfo(
                            name="event_date",
                            dtype="varchar",
                            role="temporal",
                            locked=True,
                        ),
                        ColumnInfo(name="value", dtype="double"),
                    ],
                )
            ],
        )
        models = [
            GeneratedModel(
                name="mart_event_summary",
                model_type="mart",
                sql="SELECT event_date, value FROM staging.stg_events",
                description="Event summary",
                source_tables=["events"],
                status="executed",
            )
        ]

        result = detect_insights_with_diagnostics(
            duckdb_con,
            schema="marts",
            discovery=discovery,
            models=models,
        )

        assert not any(
            d.reason == "No matching discovered source table for physical table."
            for d in result.diagnostics
            if d.physical_table == "mart_event_summary"
        )
        assert any(
            d.physical_table == "mart_event_summary" and d.table_name == "events"
            for d in result.diagnostics
        )

    def test_semantic_diagnostics_ignore_tables_outside_model_scope(self, duckdb_con):
        duckdb_con.execute(
            "CREATE TABLE staging.stg_events AS "
            "SELECT * FROM (VALUES ('2026-01-01'), ('2026-01-02')) AS t(event_date)"
        )
        duckdb_con.execute("CREATE TABLE staging.stg_stale_table AS SELECT 1 AS id")
        duckdb_con.execute("CREATE TABLE staging.stg_other AS SELECT 1 AS id")
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="csv"),
            tables=[
                TableInfo(
                    name="events",
                    row_count=2,
                    columns=[
                        ColumnInfo(
                            name="event_date",
                            dtype="varchar",
                            role="temporal",
                            locked=True,
                        ),
                    ],
                )
            ],
        )
        models = [
            GeneratedModel(
                name="stg_events",
                model_type="staging",
                sql="SELECT event_date FROM events",
                description="Events staging",
                source_tables=["events"],
                status="executed",
            ),
            GeneratedModel(
                name="stg_other",
                model_type="staging",
                sql="SELECT id FROM other",
                description="Other stale model",
                source_tables=["other"],
                status="executed",
            ),
        ]

        result = detect_insights_with_diagnostics(
            duckdb_con,
            schema="staging",
            discovery=discovery,
            models=models,
        )

        assert "stg_stale_table" not in {d.physical_table for d in result.diagnostics}
        assert "stg_other" not in {d.physical_table for d in result.diagnostics}

    def test_semantic_families_generate_actionable_lifecycle_insights(self, duckdb_con):
        duckdb_con.execute(
            """
            CREATE TABLE staging.stg_trips AS
            WITH base AS (
                SELECT
                    i,
                    CASE
                        WHEN i < 140 THEN TIMESTAMP '2026-01-05 18:00:00'
                        WHEN i < 220 THEN TIMESTAMP '2026-01-06 04:00:00'
                        WHEN i < 260 THEN TIMESTAMP '2026-01-06 07:00:00'
                        ELSE TIMESTAMP '2026-01-04 10:00:00'
                    END AS pickup_datetime,
                    CASE WHEN i < 260 THEN 24 ELSE 12 END AS duration_min,
                    CASE
                        WHEN i < 140 THEN 4
                        WHEN i < 260 THEN 12
                        ELSE 3
                    END AS wait_min,
                    CASE
                        WHEN i < 140 THEN 'JFK'
                        WHEN i < 260 THEN 'Downtown'
                        ELSE NULL
                    END AS pu_location_id
                FROM range(300) AS t(i)
            )
            SELECT
                pickup_datetime,
                pickup_datetime + duration_min * INTERVAL 1 MINUTE AS dropoff_datetime,
                pickup_datetime - wait_min * INTERVAL 1 MINUTE AS request_datetime,
                pu_location_id,
                'Manhattan' AS do_location_id,
                CASE
                    WHEN i < 140 THEN 'Yellow'
                    WHEN i < 260 THEN 'HVFHV'
                    ELSE 'FHV'
                END AS service_type,
                10.0 AS trip_miles
            FROM base
            """
        )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="csv"),
            tables=[
                TableInfo(
                    name="trips",
                    row_count=300,
                    columns=[
                        ColumnInfo(name="pickup_datetime", dtype="timestamp"),
                        ColumnInfo(name="dropoff_datetime", dtype="timestamp"),
                        ColumnInfo(name="request_datetime", dtype="timestamp"),
                        ColumnInfo(name="pu_location_id", dtype="varchar"),
                        ColumnInfo(name="do_location_id", dtype="varchar"),
                        ColumnInfo(name="service_type", dtype="varchar"),
                        ColumnInfo(name="trip_miles", dtype="double"),
                    ],
                )
            ],
        )

        insights = detect_insights(
            duckdb_con,
            schema="staging",
            discovery=discovery,
        )
        descriptions = " ".join(i.description for i in insights)
        types = {i.insight_type for i in insights}

        assert "volume_distribution" in types
        assert "peak_period" in types
        assert "geographic_hotspot" in types
        assert "duration_distribution" in types
        assert "data_quality" in types
        assert "6 PM is the busiest" in descriptions
        assert "Weekday trips average" in descriptions
        assert "JFK has the longest high-volume" in descriptions
        assert "HVFHV wait time is highest around 4 AM and 7 AM" in descriptions
        assert "FHV location analysis is unreliable" in descriptions
        assert all(i.support_count is not None for i in insights)

    def test_semantic_families_generalize_beyond_taxi_tables(self, duckdb_con):
        duckdb_con.execute(
            """
            CREATE TABLE staging.stg_work_orders AS
            WITH base AS (
                SELECT
                    i,
                    CASE
                        WHEN i < 120 THEN TIMESTAMP '2026-02-02 08:00:00'
                        WHEN i < 200 THEN TIMESTAMP '2026-02-03 15:00:00'
                        ELSE TIMESTAMP '2026-02-01 11:00:00'
                    END AS started_at,
                    CASE
                        WHEN i < 120 THEN 70
                        WHEN i < 200 THEN 30
                        ELSE 20
                    END AS duration_min,
                    CASE
                        WHEN i < 120 THEN 18
                        WHEN i < 200 THEN 7
                        ELSE 4
                    END AS wait_min,
                    CASE
                        WHEN i < 140 THEN 'Plant A'
                        WHEN i < 220 THEN 'Plant B'
                        ELSE 'Plant C'
                    END AS site_id,
                    CASE
                        WHEN i < 150 THEN 'Emergency'
                        ELSE 'Routine'
                    END AS work_type
                FROM range(260) AS t(i)
            )
            SELECT
                started_at - wait_min * INTERVAL 1 MINUTE AS requested_at,
                started_at,
                started_at + duration_min * INTERVAL 1 MINUTE AS resolved_at,
                site_id,
                work_type
            FROM base
            """
        )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="csv"),
            tables=[
                TableInfo(
                    name="work_orders",
                    row_count=260,
                    columns=[
                        ColumnInfo(name="requested_at", dtype="timestamp"),
                        ColumnInfo(name="started_at", dtype="timestamp"),
                        ColumnInfo(name="resolved_at", dtype="timestamp"),
                        ColumnInfo(name="site_id", dtype="varchar"),
                        ColumnInfo(name="work_type", dtype="varchar"),
                    ],
                )
            ],
        )

        insights = detect_insights(
            duckdb_con,
            schema="staging",
            discovery=discovery,
        )
        descriptions = " ".join(i.description for i in insights)
        types = {i.insight_type for i in insights}

        assert "volume_distribution" in types
        assert "geographic_hotspot" in types
        assert "duration_distribution" in types
        assert "8 AM is the busiest" in descriptions
        assert "Plant A has the longest high-volume" in descriptions
        assert "Emergency wait time is highest around 8 AM and 3 PM" in descriptions
        assert all(i.support_count is not None for i in insights)

    def test_semantic_highlights_use_context_and_value_oriented_findings(self, duckdb_con):
        duckdb_con.execute(
            """
            CREATE TABLE staging.stg_trips AS
            WITH base AS (
                SELECT
                    i,
                    CASE
                        WHEN i < 140 THEN TIMESTAMP '2026-01-05 18:00:00'
                        WHEN i < 220 THEN TIMESTAMP '2026-01-06 04:00:00'
                        WHEN i < 260 THEN TIMESTAMP '2026-01-06 07:00:00'
                        ELSE TIMESTAMP '2026-01-04 10:00:00'
                    END AS pickup_datetime,
                    CASE WHEN i < 260 THEN 24 ELSE 12 END AS duration_min,
                    CASE
                        WHEN i < 140 THEN 4
                        WHEN i < 260 THEN 12
                        ELSE 3
                    END AS wait_min,
                    CASE
                        WHEN i < 140 THEN 'JFK'
                        WHEN i < 260 THEN 'Downtown'
                        ELSE NULL
                    END AS pu_location_id
                FROM range(300) AS t(i)
            )
            SELECT
                pickup_datetime,
                pickup_datetime + duration_min * INTERVAL 1 MINUTE AS dropoff_datetime,
                pickup_datetime - wait_min * INTERVAL 1 MINUTE AS request_datetime,
                pu_location_id,
                'Manhattan' AS do_location_id,
                CASE
                    WHEN i < 140 THEN 'Yellow'
                    WHEN i < 260 THEN 'HVFHV'
                    ELSE 'FHV'
                END AS service_type,
                10.0 AS trip_miles
            FROM base
            """
        )
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="csv"),
            tables=[
                TableInfo(
                    name="trips",
                    row_count=300,
                    columns=[
                        ColumnInfo(name="pickup_datetime", dtype="timestamp"),
                        ColumnInfo(name="dropoff_datetime", dtype="timestamp"),
                        ColumnInfo(name="request_datetime", dtype="timestamp"),
                        ColumnInfo(name="pu_location_id", dtype="varchar"),
                        ColumnInfo(name="do_location_id", dtype="varchar"),
                        ColumnInfo(name="service_type", dtype="varchar"),
                        ColumnInfo(name="trip_miles", dtype="double"),
                    ],
                )
            ],
        )
        models = [
            GeneratedModel(
                name="stg_trips",
                model_type="staging",
                sql="SELECT * FROM trips",
                description="Trips staging",
                source_tables=["trips"],
                status="executed",
            )
        ]
        context = DatasetContext(
            source_name="test",
            row_represents="trip",
            decisions="Operations and dispatch planning",
        )

        highlights = compute_semantic_highlights(
            duckdb_con,
            discovery,
            context,
            models,
        )

        assert highlights
        assert any(h["decision_lens"] == "Operations" for h in highlights)
        assert any("HVFHV wait time is highest" in h["detail"] for h in highlights)
        assert any("FHV location analysis is unreliable" in h["title"] for h in highlights)
        assert len({h["insight_type"] for h in highlights}) >= 3

    def test_top_insights_skip_opaque_code_segments_without_lookup(self, duckdb_con):
        duckdb_con.execute(
            """
            CREATE TABLE staging.stg_trip_codes AS
            SELECT * FROM (VALUES
                ('B03404', 100.0, TIMESTAMP '2026-02-01 08:00:00'),
                ('B03404', 120.0, TIMESTAMP '2026-02-01 09:00:00'),
                ('B09999', 80.0, TIMESTAMP '2026-02-01 10:00:00')
            ) AS t(originating_base_num, trip_miles, pickup_datetime)
            """
        )
        tables = [
            TableInfo(
                name="stg_trip_codes",
                schema_name="staging",
                row_count=3,
                columns=[
                    ColumnInfo(
                        name="originating_base_num",
                        dtype="varchar",
                        semantic_type="dimension",
                    ),
                    ColumnInfo(name="trip_miles", dtype="double", semantic_type="metric"),
                    ColumnInfo(name="pickup_datetime", dtype="timestamp", semantic_type="temporal"),
                ],
            )
        ]
        profiles = [
            ColumnProfile(
                table_name="stg_trip_codes",
                column_name="originating_base_num",
                dtype="varchar",
                null_count=0,
                distinct_count=2,
                top_values=[("B03404", 2), ("B09999", 1)],
            ),
            ColumnProfile(
                table_name="stg_trip_codes",
                column_name="trip_miles",
                dtype="double",
                null_count=0,
                distinct_count=3,
                min_value=80.0,
                max_value=120.0,
                mean=100.0,
            ),
        ]

        insights = compute_top_insights(duckdb_con, tables, profiles)

        assert not any("B03404" in insight["title"] for insight in insights)
        assert not any("B03404" in insight["detail"] for insight in insights)


# ---------------------------------------------------------------------------
# NL-to-SQL tests
# ---------------------------------------------------------------------------


class TestNlToSql:
    def test_is_read_only_select(self):
        assert _is_read_only("SELECT * FROM foo") is True

    def test_is_read_only_with_clause(self):
        assert _is_read_only("WITH cte AS (SELECT 1) SELECT * FROM cte") is True

    def test_rejects_insert(self):
        assert _is_read_only("INSERT INTO foo VALUES (1)") is False

    def test_rejects_drop(self):
        assert _is_read_only("DROP TABLE foo") is False

    def test_rejects_update(self):
        assert _is_read_only("UPDATE foo SET x = 1") is False

    def test_rejects_delete(self):
        assert _is_read_only("DELETE FROM foo") is False

    def test_rejects_select_with_drop(self):
        assert _is_read_only("SELECT 1; DROP TABLE foo") is False

    def test_questions_similar_exact(self):
        assert _questions_similar("what is the average", "what is the average") is True

    def test_questions_similar_contains(self):
        # "average pm25" (scalar) should NOT match "average pm25 by zone" (grouped)
        # because adding "by zone" changes the query structure entirely.
        assert _questions_similar("average pm25", "what is the average pm25 by zone") is False

    def test_questions_similar_contains_same_structure(self):
        # Both scalar -- substring match should work
        assert _questions_similar("average pm25", "what is the average pm25") is True

    def test_questions_dissimilar(self):
        assert _questions_similar("hello world", "goodbye moon") is False

    def test_match_suggestion_exact(self):
        suggestions = [
            SuggestedQuestion(
                question="What is the average PM2.5?",
                source="mart",
                category="Air Quality",
                sql_hint="SELECT AVG(value) FROM t",
            ),
        ]
        sql = _match_suggestion("What is the average PM2.5?", suggestions)
        assert sql == "SELECT AVG(value) FROM t"

    def test_match_suggestion_no_match(self):
        suggestions = [
            SuggestedQuestion(
                question="What is the average PM2.5?",
                source="mart",
                category="Air Quality",
                sql_hint="SELECT AVG(value) FROM t",
            ),
        ]
        sql = _match_suggestion("How many sites are there?", suggestions)
        assert sql is None

    def test_ask_with_suggestion_match(self, duckdb_con, sample_discovery):
        suggestions = [
            SuggestedQuestion(
                question="How many rows in stg_readings?",
                source="semantic",
                category="Test",
                sql_hint="SELECT COUNT(*) AS cnt FROM staging.stg_readings",
            ),
        ]
        result = ask(
            question="How many rows in stg_readings?",
            con=duckdb_con,
            discovery=sample_discovery,
            suggestions=suggestions,
        )
        assert result.error is None
        assert result.row_count == 1
        assert result.data[0]["cnt"] == 180

    def test_ask_no_match_no_llm(self, duckdb_con, sample_discovery):
        result = ask(
            question="some random question with no match",
            con=duckdb_con,
            discovery=sample_discovery,
        )
        assert result.error is not None
        assert "Could not generate SQL" in result.error

    def test_ask_blocks_write_query(self, duckdb_con, sample_discovery):
        suggestions = [
            SuggestedQuestion(
                question="Drop the table",
                source="semantic",
                category="Test",
                sql_hint="DROP TABLE staging.stg_readings",
            ),
        ]
        result = ask(
            question="Drop the table",
            con=duckdb_con,
            discovery=sample_discovery,
            suggestions=suggestions,
        )
        assert result.error is not None
        assert "write operations" in result.error


# ---------------------------------------------------------------------------
# Auto-repair tests
# ---------------------------------------------------------------------------


class _FixingProvider(LLMProvider):
    """Test provider that returns a corrected SQL on first call."""

    def __init__(self, fixed_sql: str):
        self._fixed_sql = fixed_sql

    async def analyze(self, prompt: str, system: str = "") -> dict:
        return {"sql": self._fixed_sql}


class _FailingProvider(LLMProvider):
    """Test provider that always returns None (no repair)."""

    async def analyze(self, prompt: str, system: str = "") -> dict:
        return {}


class _WriteProvider(LLMProvider):
    """Test provider that returns a non-read-only SQL (should be blocked)."""

    async def analyze(self, prompt: str, system: str = "") -> dict:
        return {"sql": "DROP TABLE staging.stg_readings"}


class TestAutoRepair:
    def test_repair_succeeds_on_first_attempt(self, duckdb_con):
        """Repair loop fixes a bad query on the first try."""
        bad_sql = "SELECT * FROM staging.nonexistent_table"
        good_sql = "SELECT COUNT(*) AS cnt FROM staging.stg_readings"
        provider = _FixingProvider(good_sql)
        result = _repair_loop(
            question="How many readings?",
            original_sql=bad_sql,
            original_error="Table not found: nonexistent_table",
            con=duckdb_con,
            context="",
            provider=provider,
        )
        assert result.error is None
        assert result.repaired is True
        assert result.row_count == 1
        assert result.data[0]["cnt"] == 180
        assert len(result.repair_history) == 1  # original attempt only

    def test_repair_exhausts_attempts(self, duckdb_con):
        """When LLM returns no fix, repair stops and returns error."""
        provider = _FailingProvider()
        result = _repair_loop(
            question="Bad query",
            original_sql="SELECT * FROM staging.ghost",
            original_error="Table not found",
            con=duckdb_con,
            context="",
            provider=provider,
        )
        assert result.error is not None
        assert "auto-repair was unsuccessful" in result.error

    def test_repair_blocks_write_sql(self, duckdb_con):
        """Repair stops if LLM suggests a write operation."""
        provider = _WriteProvider()
        result = _repair_loop(
            question="Bad query",
            original_sql="SELECT * FROM staging.ghost",
            original_error="Table not found",
            con=duckdb_con,
            context="",
            provider=provider,
        )
        assert result.error is not None
        assert "auto-repair was unsuccessful" in result.error

    def test_repair_history_tracks_attempts(self, duckdb_con):
        """Repair history records each failed attempt."""
        # Provider returns a query that is valid SQL but hits wrong table
        provider = _FixingProvider("SELECT * FROM staging.also_nonexistent")
        result = _repair_loop(
            question="Bad query",
            original_sql="SELECT * FROM staging.ghost",
            original_error="Table not found: ghost",
            con=duckdb_con,
            context="",
            provider=provider,
        )
        assert result.error is not None
        # Original + up to MAX_REPAIR_ATTEMPTS failures
        assert len(result.repair_history) >= 2

    def test_ask_triggers_repair_with_llm(self, duckdb_con, sample_discovery):
        """ask() calls repair when LLM is available and execution fails."""
        good_sql = "SELECT COUNT(*) AS cnt FROM staging.stg_readings"
        provider = _FixingProvider(good_sql)

        # Suggestion with bad SQL that will fail execution
        suggestions = [
            SuggestedQuestion(
                question="Count the readings",
                source="semantic",
                category="Test",
                sql_hint="SELECT * FROM staging.nonexistent_xyz",
            ),
        ]
        result = ask(
            question="Count the readings",
            con=duckdb_con,
            discovery=sample_discovery,
            suggestions=suggestions,
            provider=provider,
        )
        assert result.error is None
        assert result.repaired is True
        assert result.data[0]["cnt"] == 180

    def test_exploration_result_repair_fields(self):
        """ExplorationResult correctly stores repair metadata."""
        r = ExplorationResult(
            question="test",
            sql="SELECT 1",
            repaired=True,
            repair_history=[
                {"sql": "SELECT bad", "error": "table not found"},
            ],
        )
        assert r.repaired is True
        assert len(r.repair_history) == 1
        assert r.repair_history[0]["error"] == "table not found"


# ---------------------------------------------------------------------------
# Grounding check tests
# ---------------------------------------------------------------------------


class TestGrounding:
    def test_vocabulary_includes_table_and_column_names(self, sample_discovery):
        vocab = _build_vocabulary(sample_discovery, [])
        assert "readings" in vocab
        assert "sites" in vocab
        assert "value" in vocab
        assert "site" in vocab  # from site_id split on _
        assert "sensor" in vocab  # from sensor_type

    def test_vocabulary_includes_descriptions_and_domains(self, sample_discovery):
        vocab = _build_vocabulary(sample_discovery, [])
        assert "environmental" in vocab
        assert "monitoring" in vocab

    def test_vocabulary_includes_model_names(self, sample_discovery, sample_models):
        vocab = _build_vocabulary(sample_discovery, sample_models)
        assert "air" in vocab  # from mart_air_quality_daily
        assert "quality" in vocab

    def test_grounded_question_returns_no_warnings(self, sample_discovery):
        warnings = _check_grounding(
            "What is the average reading value by sensor type?",
            sample_discovery,
            [],
        )
        assert len(warnings) == 0

    def test_ungrounded_term_returns_warning(self, sample_discovery):
        warnings = _check_grounding(
            "How are toys distributed across zones?",
            sample_discovery,
            [],
        )
        assert len(warnings) >= 1
        assert "toys" in warnings[0].lower()

    def test_multiple_ungrounded_terms_strong_warning(self, sample_discovery):
        warnings = _check_grounding(
            "Show me banana prices by galaxy cluster",
            sample_discovery,
            [],
        )
        assert len(warnings) >= 2
        assert "unreliable" in warnings[-1].lower()

    def test_stop_words_and_analytical_words_ignored(self, sample_discovery):
        # A question with only stop words + analytical words + known terms
        warnings = _check_grounding(
            "What is the average value per site?",
            sample_discovery,
            [],
        )
        assert len(warnings) == 0

    def test_grounding_warns_on_unrecognized_terms(self, duckdb_con, sample_discovery):
        """Free-form query with terms absent from schema and suggestions gets warned."""
        # LLM provider that returns valid SQL (simulates LLM ignoring "toys")
        provider = _FixingProvider("SELECT COUNT(*) AS cnt FROM staging.stg_readings")
        result = ask(
            question="How are toys trending?",
            con=duckdb_con,
            discovery=sample_discovery,
            suggestions=[],
            provider=provider,
        )
        assert result.error is None
        assert len(result.warnings) >= 1
        assert "toys" in result.warnings[0].lower()

    def test_grounded_suggestion_has_no_warnings(self, duckdb_con, sample_discovery):
        """A well-grounded curated question should produce no warnings."""
        suggestions = [
            SuggestedQuestion(
                question="What is the average reading value by sensor type?",
                source="semantic",
                category="Test",
                sql_hint=(
                    "SELECT sensor_type, AVG(value) FROM staging.stg_readings GROUP BY sensor_type"
                ),
            ),
        ]
        result = ask(
            question="What is the average reading value by sensor type?",
            con=duckdb_con,
            discovery=sample_discovery,
            suggestions=suggestions,
        )
        assert result.error is None
        assert len(result.warnings) == 0


# ---------------------------------------------------------------------------
# Heuristic SQL builder tests
# ---------------------------------------------------------------------------


class TestHeuristicSql:
    def test_trend_query(self, sample_discovery):
        sql = _heuristic_sql(
            "Are readings increasing over time?",
            sample_discovery,
            [],
        )
        assert sql is not None
        assert "FROM staging.stg_readings" in sql
        assert "period" in sql.lower()
        assert "GROUP BY" in sql

    def test_breakdown_query(self, sample_discovery):
        sql = _heuristic_sql(
            "What is the average value by sensor type?",
            sample_discovery,
            [],
        )
        assert sql is not None
        assert "sensor_type" in sql
        assert "AVG" in sql

    def test_count_query(self, sample_discovery):
        sql = _heuristic_sql(
            "How many readings per sensor type?",
            sample_discovery,
            [],
        )
        assert sql is not None
        assert "COUNT" in sql

    def test_ungrounded_question_returns_none(self, sample_discovery):
        sql = _heuristic_sql(
            "Tell me about toys and galaxies",
            sample_discovery,
            [],
        )
        assert sql is None

    def test_ask_uses_heuristic_without_llm(self, duckdb_con, sample_discovery):
        """ask() falls back to heuristic when no suggestion match and no LLM."""
        result = ask(
            question="How many readings by sensor type?",
            con=duckdb_con,
            discovery=sample_discovery,
        )
        assert result.error is None
        assert result.row_count > 0

    def test_uses_mart_when_available(self, sample_discovery, sample_models):
        sql = _heuristic_sql(
            "What are the reading trends over time?",
            sample_discovery,
            sample_models,
        )
        assert sql is not None
        # Should prefer the executed mart over staging
        assert "marts." in sql

    def test_join_query_with_fk_relationship(self, sample_discovery, duckdb_con):
        """Cross-table question builds JOIN when FK relationship exists."""
        # Create the sites staging table so it can resolve
        import polars as pl

        df_sites = pl.DataFrame(
            {
                "site_id": ["S1", "S2", "S3"],
                "name": ["Alpha Station", "Beta Station", "Gamma Station"],
                "zone_id": ["Z1", "Z2", "Z1"],
            }
        )
        duckdb_con.register("_tmp_sites", df_sites.to_arrow())
        duckdb_con.execute("CREATE TABLE staging.stg_sites AS SELECT * FROM _tmp_sites")
        duckdb_con.unregister("_tmp_sites")

        sql = _heuristic_sql(
            "Give me readings per site?",
            sample_discovery,
            [],
            con=duckdb_con,
        )
        assert sql is not None
        assert "JOIN" in sql
        assert "site" in sql.lower()

    def test_indirect_join_through_shared_table(self, duckdb_con):
        """Two tables related through a shared intermediate table produce a multi-hop JOIN."""
        # Schema: complaints --(zone_id)--> zones <--(zone_id)-- sites
        # No direct FK between complaints and sites.
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="complaints",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="zone_id", dtype="varchar", semantic_type="foreign_key"),
                        ColumnInfo(name="severity", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="score", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="sites",
                    row_count=50,
                    columns=[
                        ColumnInfo(name="site_id", dtype="varchar", semantic_type="id"),
                        ColumnInfo(name="site_name", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="zone_id", dtype="varchar", semantic_type="foreign_key"),
                    ],
                ),
                TableInfo(
                    name="zones",
                    row_count=10,
                    columns=[
                        ColumnInfo(name="zone_id", dtype="varchar", semantic_type="id"),
                        ColumnInfo(name="zone_name", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[
                Relationship(
                    from_table="complaints",
                    from_column="zone_id",
                    to_table="zones",
                    to_column="zone_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
                Relationship(
                    from_table="sites",
                    from_column="zone_id",
                    to_table="zones",
                    to_column="zone_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
            ],
        )

        # Create the tables in DuckDB
        duckdb_con.execute(
            "CREATE TABLE staging.stg_complaints AS "
            "SELECT * FROM (VALUES "
            "  (1, 'Z1', 'high', 8.5), (2, 'Z1', 'low', 3.0), "
            "  (3, 'Z2', 'high', 9.0), (4, 'Z2', 'medium', 5.5)"
            ") AS t(complaint_id, zone_id, severity, score)"
        )
        duckdb_con.execute(
            "CREATE TABLE staging.stg_zones AS "
            "SELECT * FROM (VALUES ('Z1', 'Downtown'), ('Z2', 'Uptown')) "
            "AS t(zone_id, zone_name)"
        )
        duckdb_con.execute(
            "CREATE TABLE staging.stg_sites AS "
            "SELECT * FROM (VALUES "
            "  ('S1', 'Alpha', 'Z1'), ('S2', 'Beta', 'Z2'), ('S3', 'Gamma', 'Z1')"
            ") AS t(site_id, site_name, zone_id)"
        )

        sql = _heuristic_sql(
            "Give me complaints per site?",
            discovery,
            [],
            con=duckdb_con,
        )
        assert sql is not None
        assert "JOIN" in sql
        # Should have two JOINs (through zones)
        assert sql.count("JOIN") == 2

        # Verify it actually executes
        result = duckdb_con.execute(sql).fetchall()
        assert len(result) > 0

    def test_no_join_path_falls_back_to_single_table(self):
        """When secondary table found but no FK path, fall through to single-table."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="complaints",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="severity", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="score", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="sites",
                    row_count=50,
                    columns=[
                        ColumnInfo(name="site_id", dtype="varchar", semantic_type="id"),
                        ColumnInfo(name="site_name", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[],  # No relationships at all
        )
        sql = _heuristic_sql(
            "Give me complaints per site?",
            discovery,
            [],
        )
        # Should NOT return None -- should fall back to single-table analysis
        # (whichever table _match_table picks as primary)
        assert sql is not None
        assert "COUNT" in sql or "AVG" in sql

    def test_complaints_per_county_picks_correct_columns(self):
        """'complaints per county' must group by county, not complaint_number.

        Validates that:
        1. Table-name words ('env', 'complaints') don't shadow column matching
        2. 'county' matches the county column, not complaint_number
        3. latitude is excluded from metrics (geographic)
        4. complaint_number is excluded from metrics (_number suffix)
        """
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_number", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(
                            name="complaint_type",
                            dtype="varchar",
                            semantic_type="dimension",
                        ),
                        ColumnInfo(name="status", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="latitude", dtype="float64", semantic_type="geographic"),
                        ColumnInfo(name="longitude", dtype="float64", semantic_type="geographic"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                        ColumnInfo(name="response_days", dtype="int64", semantic_type="metric"),
                    ],
                ),
            ],
            relationships=[],
        )
        sql = _heuristic_sql("Give me complaints per county", discovery, [])
        assert sql is not None
        assert '"county"' in sql, f"Expected county in SQL, got: {sql}"
        assert "latitude" not in sql, f"latitude should not be a metric: {sql}"
        assert "complaint_number" not in sql, f"complaint_number should not appear: {sql}"
        assert "GROUP BY" in sql

    def test_various_question_patterns(self):
        """Validate multiple question patterns against the same schema."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_number", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(
                            name="complaint_type",
                            dtype="varchar",
                            semantic_type="dimension",
                        ),
                        ColumnInfo(name="received_date", dtype="date", semantic_type="temporal"),
                        ColumnInfo(name="latitude", dtype="float64", semantic_type="geographic"),
                        ColumnInfo(name="longitude", dtype="float64", semantic_type="geographic"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
            ],
            relationships=[],
        )

        # "complaints by type" -> group by complaint_type
        sql = _heuristic_sql("How many complaints by type?", discovery, [])
        assert sql is not None
        assert '"complaint_type"' in sql

        # "top counties" -> group by county, order desc
        sql = _heuristic_sql("Top counties by severity score?", discovery, [])
        assert sql is not None
        assert '"county"' in sql
        assert "severity_score" in sql

        # "complaints over time" -> trend query
        sql = _heuristic_sql("How have complaints changed over time?", discovery, [])
        assert sql is not None
        assert "period" in sql.lower()

    def test_join_query_executes(self, sample_discovery, duckdb_con):
        """JOIN query actually runs against DuckDB without error."""
        import polars as pl

        df_sites = pl.DataFrame(
            {
                "site_id": ["S1", "S2", "S3"],
                "name": ["Alpha Station", "Beta Station", "Gamma Station"],
                "zone_id": ["Z1", "Z2", "Z1"],
            }
        )
        duckdb_con.register("_tmp_sites2", df_sites.to_arrow())
        duckdb_con.execute("CREATE OR REPLACE TABLE staging.stg_sites AS SELECT * FROM _tmp_sites2")
        duckdb_con.unregister("_tmp_sites2")

        result = ask(
            question="Give me readings per site?",
            con=duckdb_con,
            discovery=sample_discovery,
        )
        assert result.error is None
        assert result.row_count > 0

    def test_trend_prefers_date_over_year(self):
        """Trend query should prefer date/timestamp columns over 'year'."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="aqi_by_county",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="year", dtype="int64", semantic_type="temporal"),
                        ColumnInfo(name="date_local", dtype="date", semantic_type="temporal"),
                        ColumnInfo(name="days_with_aqi", dtype="int64", semantic_type="metric"),
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[],
        )
        sql = _heuristic_sql(
            "How has days with aqi changed over time?",
            discovery,
            [],
        )
        assert sql is not None
        # Should prefer date_local (actual date) over year (integer)
        assert "date_local" in sql, f"Expected date_local in trend SQL, got: {sql}"

    def test_trend_year_only_uses_raw_value(self):
        """When 'year' is the only temporal column, use raw value (no CAST)."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="aqi_by_county",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="year", dtype="int64", semantic_type="temporal"),
                        ColumnInfo(name="days_with_aqi", dtype="int64", semantic_type="metric"),
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[],
        )
        sql = _heuristic_sql(
            "How has days with aqi changed over time?",
            discovery,
            [],
        )
        assert sql is not None
        assert '"year"' in sql
        # Should NOT try to CAST year as DATE
        assert "CAST" not in sql, f"Should not CAST integer year as DATE: {sql}"

    def test_fallback_returns_none_for_missing_column(self):
        """'complaints per county' when table has no county column -> None."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_number", dtype="int64", semantic_type="id"),
                        ColumnInfo(
                            name="complaint_type_311",
                            dtype="varchar",
                            semantic_type="dimension",
                        ),
                        ColumnInfo(name="status", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
            ],
            relationships=[],
        )
        sql = _heuristic_sql("complaints per county", discovery, [])
        assert sql is None, f"Should return None when 'county' column not found, got: {sql}"

    def test_fallback_works_for_vague_question(self):
        """'show me complaints' (no specific column) gets a summary."""
        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_number", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="status", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
            ],
            relationships=[],
        )
        sql = _heuristic_sql("show me complaints", discovery, [])
        assert sql is not None, "Vague question should get a fallback summary"
        assert "GROUP BY" in sql


# ---------------------------------------------------------------------------
# Suggestion quality tests
# ---------------------------------------------------------------------------


class TestSuggestionQuality:
    def test_humanize_strips_numeric_suffix(self):
        from headwater.explorer.suggestions import _humanize

        assert _humanize("complaint_type_311") == "complaint type"
        assert _humanize("incident_type_2") == "incident type"
        # Regular names should not be affected
        assert _humanize("severity_score") == "severity score"
        # Model prefixes stripped as before
        assert _humanize("mart_complaints_by_period") == "complaints by period"

    def test_prefer_name_over_code(self):
        from headwater.explorer.suggestions import _prefer_display_dim

        cols = ["state_code", "state_name", "borough", "zip_code"]
        ranked = _prefer_display_dim(cols)
        # state_name should come before state_code and zip_code
        assert ranked.index("state_name") < ranked.index("state_code")
        assert ranked.index("state_name") < ranked.index("zip_code")
        # borough (plain) should come before code columns
        assert ranked.index("borough") < ranked.index("state_code")


# ---------------------------------------------------------------------------
# Visualization tests
# ---------------------------------------------------------------------------


class TestVisualization:
    def test_kpi_single_metric(self):
        viz = recommend_visualization(
            ["avg_value"],
            [{"avg_value": 42.5}],
        )
        assert viz.chart_type == "kpi"

    def test_line_temporal_metric(self):
        data = [
            {"reading_date": "2024-01-01", "avg_value": 42.5},
            {"reading_date": "2024-01-02", "avg_value": 43.0},
        ]
        viz = recommend_visualization(["reading_date", "avg_value"], data)
        assert viz.chart_type == "line"
        assert viz.x_axis == "reading_date"
        assert viz.y_axis == "avg_value"

    def test_bar_dimension_metric(self):
        data = [
            {"zone_name": "Zone A", "count": 100},
            {"zone_name": "Zone B", "count": 200},
        ]
        viz = recommend_visualization(["zone_name", "count"], data)
        assert viz.chart_type == "bar"
        assert viz.x_axis == "zone_name"

    def test_scatter_two_metrics(self):
        data = [
            {"value": 10.0, "score": 85.0},
            {"value": 20.0, "score": 90.0},
        ]
        viz = recommend_visualization(["value", "score"], data)
        assert viz.chart_type == "scatter"

    def test_table_fallback(self):
        data = [{"a": "x", "b": "y", "c": "z"}]
        viz = recommend_visualization(["a", "b", "c"], data)
        assert viz.chart_type == "table"

    def test_empty_data(self):
        viz = recommend_visualization([], [])
        assert viz.chart_type == "table"

    def test_classify_columns_temporal_by_name(self):
        types = _classify_columns(
            ["reading_date", "count"],
            [{"reading_date": "2024-01-01", "count": 10}],
        )
        assert types["reading_date"] == "temporal"
        assert types["count"] == "metric"

    def test_classify_columns_dimension_by_name(self):
        types = _classify_columns(
            ["zone_name", "population"],
            [{"zone_name": "Zone A", "population": 1000}],
        )
        assert types["zone_name"] == "dimension"
        assert types["population"] == "metric"

    def test_title_from_question(self):
        viz = recommend_visualization(
            ["zone_name", "count"],
            [{"zone_name": "A", "count": 10}],
            "What is the count by zone",
        )
        assert "count by zone" in viz.title.lower()

    def test_line_with_groupby(self):
        data = [
            {"month": "2024-01", "zone_name": "A", "incidents": 5},
            {"month": "2024-01", "zone_name": "B", "incidents": 8},
            {"month": "2024-02", "zone_name": "A", "incidents": 3},
            {"month": "2024-02", "zone_name": "B", "incidents": 7},
        ]
        viz = recommend_visualization(["month", "zone_name", "incidents"], data)
        assert viz.chart_type == "line"
        assert viz.group_by == "zone_name"


# ---------------------------------------------------------------------------
# Schema graph tests
# ---------------------------------------------------------------------------


class TestSchemaGraph:
    def test_resolve_table_exact(self, sample_discovery):
        from headwater.explorer.schema_graph import SchemaGraph

        graph = SchemaGraph(sample_discovery)
        matches = graph.resolve_table("readings")
        assert len(matches) > 0
        assert matches[0].table_name == "readings"
        assert matches[0].match_type == "exact"

    def test_resolve_table_stem(self, sample_discovery):
        from headwater.explorer.schema_graph import SchemaGraph

        graph = SchemaGraph(sample_discovery)
        matches = graph.resolve_table("reading")
        assert len(matches) > 0
        assert matches[0].table_name == "readings"
        assert matches[0].match_type == "stem"

    def test_resolve_column_same_table(self, sample_discovery):
        from headwater.explorer.schema_graph import SchemaGraph

        graph = SchemaGraph(sample_discovery)
        matches = graph.resolve_column("sensor_type", preferred_table="readings")
        assert len(matches) > 0
        assert matches[0].table_name == "readings"
        assert matches[0].column_name == "sensor_type"
        assert matches[0].role == "dimension"

    def test_resolve_column_cross_table(self):
        """Resolve 'county' when it exists in a different table."""
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="borough", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="aqi_by_county",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="state", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="days_with_aqi", dtype="int64", semantic_type="metric"),
                    ],
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        matches = graph.resolve_column("county")
        assert len(matches) > 0
        assert matches[0].column_name == "county"
        assert matches[0].table_name == "aqi_by_county"

    def test_column_role_classification(self, sample_discovery):
        from headwater.explorer.schema_graph import (
            ROLE_DIMENSION,
            ROLE_IDENTIFIER,
            ROLE_METRIC,
            ROLE_TEMPORAL,
            SchemaGraph,
        )

        graph = SchemaGraph(sample_discovery)
        readings = graph.tables["readings"]
        assert readings.columns["reading_id"].role == ROLE_IDENTIFIER
        assert readings.columns["value"].role == ROLE_METRIC
        assert readings.columns["timestamp"].role == ROLE_TEMPORAL
        assert readings.columns["sensor_type"].role == ROLE_DIMENSION

    def test_find_join_path_direct(self, sample_discovery):
        from headwater.explorer.schema_graph import SchemaGraph

        graph = SchemaGraph(sample_discovery)
        path = graph.find_join_path("readings", "sites")
        assert path is not None
        assert len(path) == 1
        assert path[0].from_table == "readings"
        assert path[0].to_table == "sites"
        assert path[0].from_column == "site_id"

    def test_find_join_path_indirect(self):
        """Two-hop join: complaints -> zones -> sites."""
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="complaints",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="zone_id", dtype="varchar", semantic_type="foreign_key"),
                    ],
                ),
                TableInfo(
                    name="zones",
                    row_count=10,
                    columns=[
                        ColumnInfo(name="zone_id", dtype="varchar", semantic_type="id"),
                        ColumnInfo(name="zone_name", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
                TableInfo(
                    name="sites",
                    row_count=50,
                    columns=[
                        ColumnInfo(name="site_id", dtype="varchar", semantic_type="id"),
                        ColumnInfo(name="zone_id", dtype="varchar", semantic_type="foreign_key"),
                        ColumnInfo(name="site_name", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[
                Relationship(
                    from_table="complaints",
                    from_column="zone_id",
                    to_table="zones",
                    to_column="zone_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
                Relationship(
                    from_table="sites",
                    from_column="zone_id",
                    to_table="zones",
                    to_column="zone_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        path = graph.find_join_path("complaints", "sites")
        assert path is not None
        assert len(path) == 2

    def test_find_join_path_no_path(self):
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(name="a", row_count=10, columns=[]),
                TableInfo(name="b", row_count=10, columns=[]),
            ],
            relationships=[],
        )
        graph = SchemaGraph(discovery)
        assert graph.find_join_path("a", "b") is None

    def test_get_best_dimension_prefers_table_name(self):
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="aqi_by_county",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="state", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="days_with_aqi", dtype="int64", semantic_type="metric"),
                    ],
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        best = graph.get_best_dimension("aqi_by_county")
        assert best is not None
        assert best.info.name == "county"  # county matches table name


# ---------------------------------------------------------------------------
# Query planner tests
# ---------------------------------------------------------------------------


class TestQueryPlanner:
    def test_complaints_per_county(self):
        """The foundational case: 'complaints per county' with county in the table."""
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_number", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(
                            name="complaint_type_311",
                            dtype="varchar",
                            semantic_type="dimension",
                        ),
                        ColumnInfo(name="bin", dtype="int64", semantic_type=None),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
            ],
            profiles=[
                ColumnProfile(
                    table_name="env_complaints",
                    column_name="bin",
                    dtype="int64",
                    distinct_count=8000,
                    uniqueness_ratio=0.8,
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("complaints per county?")
        assert sql is not None
        assert '"county"' in sql, f"Expected county in SQL, got: {sql}"
        assert "COUNT(*)" in sql
        assert "GROUP BY" in sql
        # Must NOT pick bin or complaint_type_311
        assert "bin" not in sql.lower() or '"county"' in sql
        assert "complaint_type_311" not in sql

    def test_average_metric_by_dimension(self):
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="readings",
                    row_count=1000,
                    columns=[
                        ColumnInfo(name="reading_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="value", dtype="float64", semantic_type="metric"),
                        ColumnInfo(name="sensor_type", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("average value by sensor type?")
        assert sql is not None
        assert "AVG" in sql
        assert '"sensor_type"' in sql
        assert '"value"' in sql

    def test_trend_query(self, sample_discovery):
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        graph = SchemaGraph(sample_discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("How have readings changed over time?")
        assert sql is not None
        assert "FROM" in sql
        assert "GROUP BY" in sql

    def test_top_query(self):
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_number", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("top counties by severity score?")
        assert sql is not None
        assert '"county"' in sql
        assert "severity_score" in sql
        assert "DESC" in sql

    def test_cross_table_join(self):
        """Question references a column from another table -- planner finds join."""
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="readings",
                    row_count=1000,
                    columns=[
                        ColumnInfo(name="reading_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="site_id", dtype="varchar", semantic_type="foreign_key"),
                        ColumnInfo(name="value", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="sites",
                    row_count=50,
                    columns=[
                        ColumnInfo(name="site_id", dtype="varchar", semantic_type="id"),
                        ColumnInfo(name="site_name", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[
                Relationship(
                    from_table="readings",
                    from_column="site_id",
                    to_table="sites",
                    to_column="site_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("readings per site name?")
        assert sql is not None
        assert "JOIN" in sql
        assert "site_name" in sql

    def test_unrelated_question_returns_none(self):
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="readings",
                    row_count=100,
                    columns=[
                        ColumnInfo(name="value", dtype="float64", semantic_type="metric"),
                    ],
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("tell me about bananas and galaxies")
        assert sql is None

    def test_planner_uses_mart_when_available(self, sample_discovery, sample_models):
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        graph = SchemaGraph(sample_discovery, sample_models)
        planner = QueryPlanner(graph, models=sample_models)
        sql = planner.plan_sql("reading trends over time?")
        assert sql is not None
        assert "marts." in sql

    def test_bin_excluded_as_metric_with_high_uniqueness(self):
        """bin (building ID number) with high uniqueness should be classified as identifier."""
        from headwater.explorer.schema_graph import ROLE_IDENTIFIER, SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="bin", dtype="int64", semantic_type=None),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
            ],
            profiles=[
                ColumnProfile(
                    table_name="env_complaints",
                    column_name="bin",
                    dtype="int64",
                    distinct_count=8000,
                    uniqueness_ratio=0.8,
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        assert graph.tables["env_complaints"].columns["bin"].role == ROLE_IDENTIFIER

    def test_planner_executes_against_duckdb(self, duckdb_con, sample_discovery):
        """Planner-generated SQL actually runs without errors."""
        from headwater.explorer.nl_to_sql import _planned_sql

        sql = _planned_sql(
            "How many readings per sensor type?",
            sample_discovery,
            [],
            con=duckdb_con,
        )
        assert sql is not None
        result = duckdb_con.execute(sql).fetchall()
        assert len(result) > 0

    def test_subject_predicate_split_drives_table_selection(self):
        """'complaints per county' picks env_complaints, not aqi_by_county.

        Without subject/predicate splitting, 'county' matches the table name
        aqi_by_county (score 10) and hijacks table selection away from the
        actual subject 'complaints'.
        """
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="borough", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(
                            name="complaint_type",
                            dtype="varchar",
                            semantic_type="dimension",
                        ),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="aqi_by_county",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="state", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="days_with_aqi", dtype="int64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="aqs_sites",
                    row_count=200,
                    columns=[
                        ColumnInfo(name="site_number", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="county_code", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="county_name", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="state_name", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("complaints per county?")
        assert sql is not None
        # Primary table must be env_complaints (the subject)
        assert "env_complaints" in sql or "stg_env_complaints" in sql, (
            f"Expected env_complaints as primary table, got: {sql}"
        )
        # Should NOT pick aqi_by_county or aqs_sites as primary
        assert "aqi_by_county" not in sql.split("FROM")[1].split("JOIN")[0], (
            f"aqi_by_county should not be the primary table: {sql}"
        )

    def test_complaints_per_county_cross_table_join(self):
        """When env_complaints has no county column but a joinable table does,
        planner should find the join path and generate a JOIN query."""
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="zone_id", dtype="varchar", semantic_type="foreign_key"),
                        ColumnInfo(name="borough", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric"),
                    ],
                ),
                TableInfo(
                    name="zones",
                    row_count=10,
                    columns=[
                        ColumnInfo(name="zone_id", dtype="varchar", semantic_type="id"),
                        ColumnInfo(name="zone_name", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
            ],
            relationships=[
                Relationship(
                    from_table="env_complaints",
                    from_column="zone_id",
                    to_table="zones",
                    to_column="zone_id",
                    type="many_to_one",
                    confidence=0.95,
                    referential_integrity=0.98,
                    source="inferred_name",
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("complaints per county?")
        assert sql is not None
        # Should JOIN to zones to get the county column
        assert "JOIN" in sql, f"Expected JOIN for cross-table county: {sql}"
        assert "county" in sql.lower()
        assert "COUNT" in sql

    def test_aqi_by_county_is_primary_when_subject(self):
        """'AQI by county' should pick aqi_by_county as primary."""
        from headwater.explorer.query_planner import QueryPlanner
        from headwater.explorer.schema_graph import SchemaGraph

        discovery = DiscoveryResult(
            source=SourceConfig(name="test", type="json", path="/data"),
            tables=[
                TableInfo(
                    name="env_complaints",
                    row_count=10000,
                    columns=[
                        ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id"),
                        ColumnInfo(name="borough", dtype="varchar", semantic_type="dimension"),
                    ],
                ),
                TableInfo(
                    name="aqi_by_county",
                    row_count=500,
                    columns=[
                        ColumnInfo(name="county", dtype="varchar", semantic_type="dimension"),
                        ColumnInfo(name="days_with_aqi", dtype="int64", semantic_type="metric"),
                    ],
                ),
            ],
        )
        graph = SchemaGraph(discovery)
        planner = QueryPlanner(graph)
        sql = planner.plan_sql("AQI by county?")
        assert sql is not None
        assert "aqi_by_county" in sql or "stg_aqi_by_county" in sql, (
            f"Expected aqi_by_county as primary table, got: {sql}"
        )
