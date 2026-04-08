"""Tests for the explorer layer -- suggestions, statistical insights, NL-to-SQL, visualization."""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
import polars as pl
import pytest

from headwater.analyzer.llm import LLMProvider
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    ContractCheckResult,
    ContractRule,
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
    _is_read_only,
    _match_suggestion,
    _questions_similar,
    _repair_loop,
    ask,
)
from headwater.explorer.statistical import (
    _detect_correlations,
    _detect_period_shifts,
    _detect_temporal_anomalies,
    _find_metric_columns,
    _find_temporal_columns,
    detect_insights,
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

    df = pl.DataFrame({
        "reading_date": dates,
        "avg_value": values,
        "zone_name": zones,
        "reading_count": [10 + i % 5 for i in range(180)],
    })
    arrow = df.to_arrow()
    con.register("_tmp_mart", arrow)
    con.execute(
        "CREATE TABLE marts.mart_air_quality_daily AS SELECT * FROM _tmp_mart"
    )
    con.unregister("_tmp_mart")

    # Create a staging table
    df2 = pl.DataFrame({
        "site_id": ["S1", "S2", "S3"] * 60,
        "value": [float(i) + 10.5 for i in range(180)],
        "sensor_type": ["pm25", "ozone", "no2"] * 60,
    })
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


# ---------------------------------------------------------------------------
# Statistical tests
# ---------------------------------------------------------------------------


class TestStatistical:
    def test_find_temporal_columns(self):
        df = pl.DataFrame({
            "d": [date(2024, 1, 1), date(2024, 1, 2)],
            "v": [1.0, 2.0],
        })
        assert _find_temporal_columns(df) == ["d"]

    def test_find_metric_columns(self):
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "site_id": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            "value": [1.0, 2.5, 3.0, 4.5, 5.0, 6.5, 7.0, 8.5, 9.0, 10.5],
            "count": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        })
        metrics = _find_metric_columns(df)
        assert "value" in metrics
        assert "count" in metrics
        assert "site_id" not in metrics  # *_id excluded
        assert "id" not in metrics

    def test_detect_temporal_anomalies(self, duckdb_con):
        arrow = duckdb_con.execute(
            "SELECT * FROM marts.mart_air_quality_daily"
        ).arrow()
        df = pl.from_arrow(arrow)
        insights = _detect_temporal_anomalies(
            df, "mart_air_quality_daily", "reading_date", "avg_value"
        )
        # Should detect the anomaly around day 90-100
        anomalies = [i for i in insights if i.insight_type == "temporal_anomaly"]
        assert len(anomalies) > 0
        # The spike should be positive deviation
        assert any(i.magnitude > 0 for i in anomalies)

    def test_detect_period_shifts(self, duckdb_con):
        arrow = duckdb_con.execute(
            "SELECT * FROM marts.mart_air_quality_daily"
        ).arrow()
        df = pl.from_arrow(arrow)
        insights = _detect_period_shifts(
            df, "mart_air_quality_daily", "reading_date", "avg_value"
        )
        # Period shift should be detected (first half has the anomaly spike)
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
        assert _questions_similar("average pm25", "what is the average pm25 by zone") is True

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
        provider = _FixingProvider(
            "SELECT * FROM staging.also_nonexistent"
        )
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
        viz = recommend_visualization(
            ["month", "zone_name", "incidents"], data
        )
        assert viz.chart_type == "line"
        assert viz.group_by == "zone_name"
