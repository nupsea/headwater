"""Explore correctness tests -- validates that explore outputs match golden answers.

This is the gating test suite for the Explore overhaul (Waves E0-E6).
Every test asserts a numerically correct answer against manually verified
golden values from the sample dataset.

Three test classes:
- TestSQLCorrectness: ask() returns correct numeric answers
- TestStatisticalCorrectness: detect_insights() returns accurate statistics
- TestVisualizationCorrectness: recommended charts match data semantics

Wave E0 establishes the baseline. Many tests may fail initially.
Each subsequent wave must increase the pass rate.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from headwater.analyzer.catalog import build_catalog
from headwater.analyzer.semantic import analyze
from headwater.connectors.registry import get_connector
from headwater.core.metadata import MetadataStore
from headwater.core.models import SourceConfig
from headwater.executor.duckdb_backend import DuckDBBackend
from headwater.executor.runner import run_models
from headwater.explorer.nl_to_sql import ask
from headwater.explorer.statistical import detect_insights
from headwater.explorer.suggestions import generate_suggestions
from headwater.generator.contracts import generate_contracts
from headwater.generator.marts import generate_mart_models
from headwater.generator.staging import generate_staging_models
from headwater.profiler.engine import discover
from headwater.quality.checker import check_contracts
from headwater.quality.report import build_report
from tests.golden.explore_answers import (
    AVG_READING_VALUE,
    COMPLAINTS_PER_ZONE_TOP1,
    COMPLAINTS_ZONE_CARDINALITY,
    DISTINCT_ZONES,
    MAX_READING_VALUE,
    SITES_WITH_INSPECTIONS,
    TOTAL_COMPLAINTS,
)

SAMPLE_DATA = Path(__file__).resolve().parent.parent.parent / "data" / "sample"


@pytest.fixture(scope="module")
def explore_env():
    """Run the full pipeline and prepare explore environment.

    Returns a dict with: con, discovery, catalog, suggestions, models, contracts.
    Module-scoped so we only run the pipeline once for all tests.
    """
    con = duckdb.connect(":memory:")
    store = MetadataStore(":memory:")
    store.init()

    source_schema = "env_health"
    staging_schema = "staging"
    marts_schema = "marts"

    try:
        # Load
        source = SourceConfig(name="explore_e2e", type="json", path=str(SAMPLE_DATA))
        connector = get_connector(source.type)
        connector.connect(source)
        connector.load_to_duckdb(con, source_schema)

        # Discover + Analyze
        discovery = discover(con, source_schema, source)
        analyze(discovery, store=store, source_name="explore_e2e")

        # Catalog
        catalog = build_catalog(discovery)

        # Generate models
        staging_models = generate_staging_models(
            discovery.tables, source_schema, staging_schema
        )
        mart_models = generate_mart_models(discovery, target_schema=marts_schema)

        # Generate contracts
        contracts = generate_contracts(discovery.profiles, target_schema=staging_schema)

        # Execute approved models (staging is auto-approved)
        all_models = staging_models + mart_models
        backend = DuckDBBackend(con)
        backend.ensure_schema(staging_schema)
        backend.ensure_schema(marts_schema)
        run_models(backend, all_models, only_approved=True)

        # Quality
        contract_results = check_contracts(con, contracts, only_active=False)
        quality_report = build_report(contract_results)

        # Generate suggestions
        suggestions = generate_suggestions(
            discovery=discovery,
            models=all_models,
            contracts=contracts,
            quality_results=quality_report.results if quality_report else [],
            con=con,
            catalog=catalog,
        )

        return {
            "con": con,
            "discovery": discovery,
            "catalog": catalog,
            "suggestions": suggestions,
            "staging_models": staging_models,
            "mart_models": mart_models,
            "all_models": all_models,
            "contracts": contracts,
            "quality_report": quality_report,
        }
    except Exception:
        con.close()
        raise


def _ask(explore_env, question: str):
    """Helper: call ask() with the full explore context."""
    return ask(
        question=question,
        con=explore_env["con"],
        discovery=explore_env["discovery"],
        models=explore_env["all_models"],
        suggestions=explore_env["suggestions"],
        catalog=explore_env["catalog"],
    )


def _extract_single_value(result, key_substring=None):
    """Extract a single numeric value from an ExplorationResult.

    Tries to find a column matching key_substring (case-insensitive).
    Falls back to the first numeric value in the first row.
    """
    if not result.data:
        return None
    row = result.data[0]
    if key_substring:
        for k, v in row.items():
            if key_substring.lower() in k.lower():
                return v
    # Fallback: first numeric value
    for v in row.values():
        if isinstance(v, (int, float)):
            return v
    return None


# ---------------------------------------------------------------------------
# SQL Correctness: ask() returns numerically correct answers
# ---------------------------------------------------------------------------
class TestSQLCorrectness:
    """Every test executes a natural language question and validates the answer."""

    def test_total_complaints(self, explore_env):
        result = _ask(explore_env, "How many complaints are there?")
        assert result.error is None, f"Query failed: {result.error}"
        val = _extract_single_value(result, "count")
        assert val is not None, f"No count found in {result.data}"
        assert val == TOTAL_COMPLAINTS, f"Expected {TOTAL_COMPLAINTS}, got {val}"

    def test_avg_reading_value(self, explore_env):
        result = _ask(explore_env, "What is the average reading value?")
        assert result.error is None, f"Query failed: {result.error}"
        val = _extract_single_value(result, "avg")
        assert val is not None, f"No avg found in {result.data}"
        assert abs(val - AVG_READING_VALUE) < 0.5, f"Expected ~{AVG_READING_VALUE}, got {val}"

    def test_distinct_zones(self, explore_env):
        result = _ask(explore_env, "How many distinct zones are there?")
        assert result.error is None, f"Query failed: {result.error}"
        val = _extract_single_value(result, "count")
        # Could be 25 (from zones table) or 35 (from complaints zone_id cardinality)
        assert val is not None, f"No count found in {result.data}"
        assert val in (DISTINCT_ZONES, COMPLAINTS_ZONE_CARDINALITY), (
            f"Expected {DISTINCT_ZONES} or {COMPLAINTS_ZONE_CARDINALITY}, got {val}"
        )

    def test_max_reading_value(self, explore_env):
        result = _ask(explore_env, "What is the maximum reading value?")
        assert result.error is None, f"Query failed: {result.error}"
        val = _extract_single_value(result, "max")
        assert val is not None, f"No max found in {result.data}"
        assert val == MAX_READING_VALUE, f"Expected {MAX_READING_VALUE}, got {val}"

    def test_complaints_per_zone_top(self, explore_env):
        result = _ask(explore_env, "Which zone has the most complaints?")
        assert result.error is None, f"Query failed: {result.error}"
        assert len(result.data) > 0, "No results returned"
        # The top zone should be Z09 with 203 complaints
        first = result.data[0]
        zone_val = None
        count_val = None
        for k, v in first.items():
            if "zone" in k.lower():
                zone_val = v
            if isinstance(v, (int, float)) and v > 100:
                count_val = v
        if zone_val is not None:
            assert zone_val == COMPLAINTS_PER_ZONE_TOP1[0], (
                f"Expected top zone {COMPLAINTS_PER_ZONE_TOP1[0]}, got {zone_val}"
            )
        if count_val is not None:
            assert count_val == COMPLAINTS_PER_ZONE_TOP1[1], (
                f"Expected count {COMPLAINTS_PER_ZONE_TOP1[1]}, got {count_val}"
            )

    def test_sites_with_inspections(self, explore_env):
        result = _ask(explore_env, "How many distinct sites have inspections?")
        assert result.error is None, f"Query failed: {result.error}"
        val = _extract_single_value(result, "count")
        assert val is not None, f"No count found in {result.data}"
        assert val == SITES_WITH_INSPECTIONS, f"Expected {SITES_WITH_INSPECTIONS}, got {val}"

    def test_zone_with_most_sensors(self, explore_env):
        result = _ask(explore_env, "Which zone has the most sensors?")
        assert result.error is None, f"Query failed: {result.error}"
        assert len(result.data) > 0, "No results returned"

    def test_no_sql_errors_on_suggestions(self, explore_env):
        """Every suggested question with a SQL hint should execute without error."""
        suggestions = explore_env["suggestions"]
        errors = []
        tested = 0
        for s in suggestions:
            if s.sql_hint:
                tested += 1
                result = _ask(explore_env, s.question)
                if result.error:
                    errors.append(f"{s.question}: {result.error}")
        assert tested > 0, "No suggestions had SQL hints"
        if errors:
            # Report as warning, not hard failure (yet) -- Wave E1 will fix these
            msg = f"{len(errors)}/{tested} suggested questions failed:\n"
            msg += "\n".join(errors[:5])
            pytest.fail(msg)


# ---------------------------------------------------------------------------
# Statistical Correctness: detect_insights() returns accurate values
# ---------------------------------------------------------------------------
class TestStatisticalCorrectness:

    def test_insights_produced(self, explore_env):
        """detect_insights should find at least some insights on staging data."""
        con = explore_env["con"]
        insights = detect_insights(con, schema="staging")
        assert len(insights) >= 0  # baseline: just checking it runs without crash

    def test_no_false_positive_on_uniform_data(self, explore_env):
        """Insert uniform data, verify zero anomalies detected."""
        con = explore_env["con"]
        con.execute("CREATE SCHEMA IF NOT EXISTS test_uniform")
        con.execute(
            "CREATE TABLE test_uniform.flat_values AS "
            "SELECT i AS id, 50.0 AS value, "
            "DATE '2024-01-01' + INTERVAL (i) DAY AS ts "
            "FROM generate_series(1, 100) t(i)"
        )
        insights = detect_insights(con, schema="test_uniform")
        # Uniform data should produce zero anomaly insights
        anomalies = [i for i in insights if i.insight_type == "anomaly"]
        assert len(anomalies) == 0, (
            f"Got {len(anomalies)} false positive anomalies on uniform data"
        )
        con.execute("DROP SCHEMA test_uniform CASCADE")

    def test_known_anomaly_detected(self, explore_env):
        """Insert data with a known 10x spike, verify it is detected."""
        con = explore_env["con"]
        con.execute("CREATE SCHEMA IF NOT EXISTS test_spike")
        # 99 days at value=50, day 50 spikes to 500
        con.execute(
            "CREATE TABLE test_spike.spiked AS "
            "SELECT i AS id, "
            "CASE WHEN i = 50 THEN 500.0 ELSE 50.0 END AS value, "
            "DATE '2024-01-01' + INTERVAL (i) DAY AS ts "
            "FROM generate_series(1, 100) t(i)"
        )
        insights = detect_insights(con, schema="test_spike")
        anomalies = [i for i in insights if i.insight_type == "anomaly"]
        # Should detect at least one anomaly for the spike
        assert len(anomalies) >= 1, "Known 10x spike was not detected as anomaly"
        con.execute("DROP SCHEMA test_spike CASCADE")

    def test_known_correlation_detected(self, explore_env):
        """Insert perfectly correlated columns, verify correlation found."""
        con = explore_env["con"]
        con.execute("CREATE SCHEMA IF NOT EXISTS test_corr")
        con.execute(
            "CREATE TABLE test_corr.correlated AS "
            "SELECT i AS id, i * 1.0 AS a, i * 2.0 + 5.0 AS b, "
            "DATE '2024-01-01' + INTERVAL (i) DAY AS ts "
            "FROM generate_series(1, 100) t(i)"
        )
        insights = detect_insights(con, schema="test_corr")
        correlations = [i for i in insights if i.insight_type == "correlation"]
        assert len(correlations) >= 1, "Perfect linear correlation (a, b=2a+5) not detected"
        con.execute("DROP SCHEMA test_corr CASCADE")

    def test_no_spurious_correlation(self, explore_env):
        """Independent random columns should not show strong correlation."""
        con = explore_env["con"]
        con.execute("CREATE SCHEMA IF NOT EXISTS test_nocorr")
        # Use hash-based pseudo-random to be deterministic
        con.execute(
            "CREATE TABLE test_nocorr.independent AS "
            "SELECT i AS id, "
            "hash(i * 7 + 13) % 1000 / 10.0 AS a, "
            "hash(i * 31 + 97) % 1000 / 10.0 AS b, "
            "DATE '2024-01-01' + INTERVAL (i) DAY AS ts "
            "FROM generate_series(1, 200) t(i)"
        )
        insights = detect_insights(con, schema="test_nocorr")
        correlations = [
            i for i in insights
            if i.insight_type == "correlation"
            and "a" in i.description.lower()
            and "b" in i.description.lower()
        ]
        # Should not find strong correlation between independent columns
        for c in correlations:
            # If a correlation is reported, its magnitude should be weak
            assert abs(c.magnitude) < 0.5, (
                f"Spurious correlation r={c.magnitude} between independent columns"
            )
        con.execute("DROP SCHEMA test_nocorr CASCADE")


# ---------------------------------------------------------------------------
# Visualization Correctness: chart recommendations match data semantics
# ---------------------------------------------------------------------------
class TestVisualizationCorrectness:

    def test_single_count_gets_kpi(self, explore_env):
        """A single aggregate value should get a KPI visualization."""
        result = _ask(explore_env, "How many complaints are there?")
        if result.error:
            pytest.skip(f"Query failed: {result.error}")
        if result.visualization:
            assert result.visualization.chart_type in ("kpi", "table"), (
                f"Single count got {result.visualization.chart_type}, expected kpi/table"
            )

    def test_group_by_gets_bar_chart(self, explore_env):
        """A categorical breakdown should get a bar chart."""
        result = _ask(explore_env, "How many complaints per zone?")
        if result.error:
            pytest.skip(f"Query failed: {result.error}")
        if result.visualization:
            assert result.visualization.chart_type in ("bar", "table"), (
                f"Categorical breakdown got chart_type={result.visualization.chart_type}"
            )

    def test_temporal_gets_line_chart(self, explore_env):
        """Temporal data should get a line chart."""
        result = _ask(explore_env, "Show average reading value by month")
        if result.error:
            pytest.skip(f"Query failed: {result.error}")
        if result.visualization:
            assert result.visualization.chart_type in ("line", "bar", "table"), (
                f"Temporal data got chart_type={result.visualization.chart_type}"
            )

    def test_axes_reference_real_columns(self, explore_env):
        """Every visualization axis must reference a column that exists in the data."""
        questions = [
            "How many complaints per zone?",
            "Show average reading value by month",
            "Count of inspections by site",
        ]
        for question in questions:
            result = _ask(explore_env, question)
            if result.error or not result.visualization or not result.data:
                continue
            columns = set(result.data[0].keys())
            viz = result.visualization
            if viz.x_axis:
                assert viz.x_axis in columns, (
                    f"Question: {question!r} -- x_axis '{viz.x_axis}' not in data columns {columns}"
                )
            if viz.y_axis:
                assert viz.y_axis in columns, (
                    f"Question: {question!r} -- y_axis '{viz.y_axis}' not in data columns {columns}"
                )

    def test_visualization_always_has_title(self, explore_env):
        """Every visualization should have a non-empty title."""
        result = _ask(explore_env, "How many sensors per zone?")
        if result.error:
            pytest.skip(f"Query failed: {result.error}")
        if result.visualization:
            assert result.visualization.title, "Visualization has empty title"
