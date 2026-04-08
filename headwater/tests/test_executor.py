"""Tests for the execution layer: DuckDB backend, model runner."""

from __future__ import annotations

import duckdb

from headwater.core.models import GeneratedModel
from headwater.executor.duckdb_backend import DuckDBBackend
from headwater.executor.runner import run_models, topological_sort

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_con() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with test data."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute(
        "CREATE TABLE raw.users ("
        "  user_id VARCHAR, name VARCHAR, age INTEGER"
        ")"
    )
    con.execute(
        "INSERT INTO raw.users VALUES "
        "('u1', 'Alice', 30), ('u2', 'Bob', 25), ('u3', 'Carol', 35)"
    )
    con.execute(
        "CREATE TABLE raw.orders ("
        "  order_id VARCHAR, user_id VARCHAR, amount DOUBLE"
        ")"
    )
    con.execute(
        "INSERT INTO raw.orders VALUES "
        "('o1', 'u1', 100.0), ('o2', 'u2', 200.0), ('o3', 'u1', 50.0)"
    )
    return con


def _staging_users() -> GeneratedModel:
    return GeneratedModel(
        name="stg_users",
        model_type="staging",
        sql=(
            "CREATE OR REPLACE TABLE staging.stg_users AS "
            "SELECT user_id, name, age, CURRENT_TIMESTAMP AS _loaded_at "
            "FROM raw.users"
        ),
        description="Staging users",
        source_tables=["users"],
        status="approved",
    )


def _staging_orders() -> GeneratedModel:
    return GeneratedModel(
        name="stg_orders",
        model_type="staging",
        sql=(
            "CREATE OR REPLACE TABLE staging.stg_orders AS "
            "SELECT order_id, user_id, amount, CURRENT_TIMESTAMP AS _loaded_at "
            "FROM raw.orders"
        ),
        description="Staging orders",
        source_tables=["orders"],
        status="approved",
    )


def _mart_user_spend() -> GeneratedModel:
    return GeneratedModel(
        name="mart_user_spend",
        model_type="mart",
        sql=(
            "CREATE OR REPLACE TABLE staging.mart_user_spend AS "
            "SELECT u.user_id, u.name, SUM(o.amount) AS total_spend "
            "FROM staging.stg_users u "
            "JOIN staging.stg_orders o ON u.user_id = o.user_id "
            "GROUP BY u.user_id, u.name"
        ),
        description="User spend mart",
        source_tables=["users", "orders"],
        depends_on=["stg_users", "stg_orders"],
        status="approved",
    )


# ---------------------------------------------------------------------------
# DuckDB Backend tests
# ---------------------------------------------------------------------------


class TestDuckDBBackend:
    def test_execute_returns_dataframe(self):
        con = _make_con()
        backend = DuckDBBackend(con)
        df = backend.execute("SELECT * FROM raw.users")
        assert len(df) == 3
        assert "user_id" in df.columns

    def test_materialize_success(self):
        con = _make_con()
        backend = DuckDBBackend(con)
        result = backend.materialize(_staging_users())
        assert result.success is True
        assert result.row_count == 3
        assert result.execution_time_ms > 0
        assert result.error is None

    def test_materialize_failure(self):
        con = _make_con()
        backend = DuckDBBackend(con)
        bad_model = GeneratedModel(
            name="bad_model",
            model_type="staging",
            sql="CREATE OR REPLACE TABLE staging.bad AS SELECT * FROM nonexistent",
            description="Should fail",
            status="approved",
        )
        result = backend.materialize(bad_model)
        assert result.success is False
        assert result.error is not None

    def test_ensure_schema(self):
        con = duckdb.connect(":memory:")
        backend = DuckDBBackend(con)
        backend.ensure_schema("my_schema")
        con.execute("CREATE TABLE my_schema.test (id INT)")
        # Should not raise

    def test_table_exists(self):
        con = _make_con()
        backend = DuckDBBackend(con)
        assert backend.table_exists("raw", "users") is True
        assert backend.table_exists("raw", "nonexistent") is False


# ---------------------------------------------------------------------------
# Topological sort tests
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def test_staging_before_marts(self):
        models = [_mart_user_spend(), _staging_users(), _staging_orders()]
        sorted_models = topological_sort(models)
        names = [m.name for m in sorted_models]
        # Both staging models must come before the mart
        assert names.index("stg_users") < names.index("mart_user_spend")
        assert names.index("stg_orders") < names.index("mart_user_spend")

    def test_no_deps_stable_order(self):
        models = [_staging_orders(), _staging_users()]
        sorted_models = topological_sort(models)
        names = [m.name for m in sorted_models]
        # Alphabetical ordering when no dependencies between them
        assert names == ["stg_orders", "stg_users"]

    def test_empty_list(self):
        assert topological_sort([]) == []

    def test_single_model(self):
        models = [_staging_users()]
        sorted_models = topological_sort(models)
        assert len(sorted_models) == 1


# ---------------------------------------------------------------------------
# Model runner tests
# ---------------------------------------------------------------------------


class TestModelRunner:
    def test_runs_all_approved(self):
        con = _make_con()
        backend = DuckDBBackend(con)
        models = [_staging_users(), _staging_orders(), _mart_user_spend()]
        results = run_models(backend, models)
        assert len(results) == 3
        assert all(r.success for r in results)

    def test_skips_proposed_models(self):
        con = _make_con()
        backend = DuckDBBackend(con)
        proposed_mart = GeneratedModel(
            name="mart_proposed",
            model_type="mart",
            sql="CREATE OR REPLACE TABLE staging.mart_proposed AS SELECT 1",
            description="Proposed only",
            status="proposed",
        )
        models = [_staging_users(), proposed_mart]
        results = run_models(backend, models)
        assert len(results) == 1
        assert results[0].model_name == "stg_users"

    def test_run_all_flag(self):
        con = _make_con()
        backend = DuckDBBackend(con)
        proposed = GeneratedModel(
            name="mart_any",
            model_type="mart",
            sql="CREATE OR REPLACE TABLE staging.mart_any AS SELECT 1 AS x",
            description="Run anyway",
            status="proposed",
        )
        results = run_models(backend, [proposed], only_approved=False)
        assert len(results) == 1
        assert results[0].success is True

    def test_materialized_tables_queryable(self):
        con = _make_con()
        backend = DuckDBBackend(con)
        models = [_staging_users(), _staging_orders(), _mart_user_spend()]
        run_models(backend, models)
        # Verify mart result
        df = backend.execute("SELECT * FROM staging.mart_user_spend ORDER BY total_spend DESC")
        assert len(df) == 2  # Alice (150) and Bob (200)
        assert df["total_spend"].to_list() == [200.0, 150.0]
