"""E2E heartbeat test -- runs the full pipeline against sample data.

This is the product's heartbeat test. If this test fails, nothing else matters.
Every pipeline stage is exercised and its output validated:

  Load -> Discover -> Analyze -> Generate -> Execute -> Quality
"""

from __future__ import annotations

import time
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
from headwater.generator.contracts import generate_contracts
from headwater.generator.marts import generate_mart_models
from headwater.generator.staging import generate_staging_models
from headwater.profiler.engine import discover
from headwater.quality.checker import check_contracts
from headwater.quality.report import build_report

SAMPLE_DATA = Path(__file__).resolve().parent.parent.parent / "data" / "sample"

# Expected table count in sample dataset
EXPECTED_TABLES = 8
# Minimum relationships we know exist in the environmental health dataset
MIN_RELATIONSHIPS = 3
# Minimum contracts generated from profiling
MIN_CONTRACTS = 5


@pytest.fixture(scope="module")
def pipeline_result():
    """Run the full pipeline once for all tests in this module.

    Returns a dict with every intermediate result for assertion.
    """
    start = time.monotonic()
    con = duckdb.connect(":memory:")
    store = MetadataStore(":memory:")
    store.init()

    source_schema = "env_health"
    staging_schema = "staging"
    marts_schema = "marts"

    try:
        # ---- Stage 1: Load ----
        source = SourceConfig(
            name="sample_e2e",
            type="json",
            path=str(SAMPLE_DATA),
        )
        connector = get_connector(source.type)
        connector.connect(source)
        tables_loaded = connector.load_to_duckdb(con, source_schema)

        # ---- Stage 2: Discover ----
        discovery = discover(con, source_schema, source)

        # ---- Stage 3: Analyze (heuristic only -- no LLM) ----
        analyze(discovery, store=store, source_name="sample_e2e")

        # ---- Stage 4: Build catalog ----
        catalog = build_catalog(discovery)

        # ---- Stage 5: Generate staging models ----
        staging_models = generate_staging_models(
            discovery.tables, source_schema, staging_schema
        )

        # ---- Stage 6: Generate mart models ----
        mart_models = generate_mart_models(discovery, target_schema=marts_schema)

        # ---- Stage 7: Generate contracts ----
        contracts = generate_contracts(discovery.profiles, target_schema=staging_schema)

        # ---- Stage 8: Execute approved models ----
        all_models = staging_models + mart_models
        backend = DuckDBBackend(con)
        backend.ensure_schema(staging_schema)
        backend.ensure_schema(marts_schema)
        execution_results = run_models(backend, all_models, only_approved=True)

        # ---- Stage 9: Check quality contracts (all, not just active) ----
        contract_results = check_contracts(con, contracts, only_active=False)
        quality_report = build_report(contract_results)

        elapsed = time.monotonic() - start

        return {
            "con": con,
            "tables_loaded": tables_loaded,
            "discovery": discovery,
            "catalog": catalog,
            "staging_models": staging_models,
            "mart_models": mart_models,
            "contracts": contracts,
            "all_models": all_models,
            "execution_results": execution_results,
            "quality_report": quality_report,
            "elapsed_seconds": elapsed,
        }
    except Exception:
        con.close()
        raise


# ---------------------------------------------------------------------------
# Stage 1: Load
# ---------------------------------------------------------------------------
class TestLoad:
    def test_tables_loaded(self, pipeline_result):
        assert len(pipeline_result["tables_loaded"]) >= EXPECTED_TABLES


# ---------------------------------------------------------------------------
# Stage 2: Discover
# ---------------------------------------------------------------------------
class TestDiscovery:
    def test_table_count(self, pipeline_result):
        discovery = pipeline_result["discovery"]
        assert len(discovery.tables) == EXPECTED_TABLES

    def test_every_table_has_columns(self, pipeline_result):
        for table in pipeline_result["discovery"].tables:
            assert len(table.columns) > 0, f"Table {table.name} has no columns"

    def test_profiles_exist_for_every_column(self, pipeline_result):
        discovery = pipeline_result["discovery"]
        profiled_cols = {(p.table_name, p.column_name) for p in discovery.profiles}
        for table in discovery.tables:
            for col in table.columns:
                assert (table.name, col.name) in profiled_cols, (
                    f"No profile for {table.name}.{col.name}"
                )

    def test_relationships_detected(self, pipeline_result):
        assert len(pipeline_result["discovery"].relationships) >= MIN_RELATIONSHIPS


# ---------------------------------------------------------------------------
# Stage 3: Analyze
# ---------------------------------------------------------------------------
class TestAnalysis:
    def test_tables_have_descriptions(self, pipeline_result):
        for table in pipeline_result["discovery"].tables:
            assert table.description, f"Table {table.name} has no description after analysis"

    def test_columns_have_semantic_types(self, pipeline_result):
        """At least some columns should have semantic type after heuristic analysis."""
        typed_count = sum(
            1
            for table in pipeline_result["discovery"].tables
            for col in table.columns
            if col.semantic_type
        )
        assert typed_count > 0, "No columns received semantic types"


# ---------------------------------------------------------------------------
# Stage 4: Catalog
# ---------------------------------------------------------------------------
class TestCatalog:
    def test_catalog_has_metrics(self, pipeline_result):
        catalog = pipeline_result["catalog"]
        assert len(catalog.metrics) > 0, "Catalog has no metrics"

    def test_catalog_has_dimensions(self, pipeline_result):
        catalog = pipeline_result["catalog"]
        assert len(catalog.dimensions) > 0, "Catalog has no dimensions"

    def test_catalog_has_entities(self, pipeline_result):
        catalog = pipeline_result["catalog"]
        assert len(catalog.entities) > 0, "Catalog has no entities"


# ---------------------------------------------------------------------------
# Stage 5 & 6: Generate
# ---------------------------------------------------------------------------
class TestGeneration:
    def test_staging_model_count_matches_tables(self, pipeline_result):
        assert len(pipeline_result["staging_models"]) == EXPECTED_TABLES

    def test_staging_models_auto_approved(self, pipeline_result):
        for model in pipeline_result["staging_models"]:
            assert model.status == "approved", (
                f"Staging model {model.name} is {model.status}, expected approved"
            )

    def test_staging_models_have_sql(self, pipeline_result):
        for model in pipeline_result["staging_models"]:
            assert model.sql, f"Staging model {model.name} has no SQL"
            assert "CREATE" in model.sql.upper()

    def test_mart_models_generated(self, pipeline_result):
        assert len(pipeline_result["mart_models"]) >= 1, "No mart models generated"

    def test_mart_models_are_proposed(self, pipeline_result):
        for model in pipeline_result["mart_models"]:
            assert model.status == "proposed", (
                f"Mart model {model.name} is {model.status}, expected proposed"
            )

    def test_contracts_generated(self, pipeline_result):
        assert len(pipeline_result["contracts"]) >= MIN_CONTRACTS


# ---------------------------------------------------------------------------
# Stage 8: Execute
# ---------------------------------------------------------------------------
class TestExecution:
    def test_staging_models_executed(self, pipeline_result):
        """All staging models (approved) should have execution results."""
        staging_names = {m.name for m in pipeline_result["staging_models"]}
        executed_names = {r.model_name for r in pipeline_result["execution_results"]}
        assert staging_names.issubset(executed_names), (
            f"Missing executions for: {staging_names - executed_names}"
        )

    def test_all_executions_succeeded(self, pipeline_result):
        for result in pipeline_result["execution_results"]:
            assert result.success, (
                f"Model {result.model_name} failed: {result.error}"
            )

    def test_all_executions_have_rows(self, pipeline_result):
        for result in pipeline_result["execution_results"]:
            if result.success:
                assert result.row_count is None or result.row_count > 0, (
                    f"Model {result.model_name} produced 0 rows"
                )


# ---------------------------------------------------------------------------
# Stage 9: Quality
# ---------------------------------------------------------------------------
class TestQuality:
    def test_quality_report_has_results(self, pipeline_result):
        report = pipeline_result["quality_report"]
        assert report.total_contracts > 0

    def test_no_checker_errors(self, pipeline_result):
        """Contract violations are OK; checker crashes are NOT."""
        for result in pipeline_result["quality_report"].results:
            if not result.passed:
                assert "Check error:" not in (result.message or ""), (
                    f"Checker crashed on {result.model_name}: {result.message}"
                )

    def test_pass_rate_reasonable(self, pipeline_result):
        """At least some contracts should pass on clean sample data."""
        report = pipeline_result["quality_report"]
        if report.total_contracts > 0:
            pass_rate = report.passed / report.total_contracts
            assert pass_rate > 0, "Zero contracts passed -- data or generator is broken"


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------
class TestPerformance:
    def test_total_time_under_60_seconds(self, pipeline_result):
        assert pipeline_result["elapsed_seconds"] < 60, (
            f"Pipeline took {pipeline_result['elapsed_seconds']:.1f}s, expected < 60s"
        )
