"""DuckDB execution backend -- runs SQL, returns results."""

from __future__ import annotations

import logging
import time

import duckdb
import polars as pl

from headwater.core.models import ExecutionResult, GeneratedModel

logger = logging.getLogger(__name__)


class DuckDBBackend:
    """Execute SQL statements against a DuckDB connection."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    def execute(self, sql: str) -> pl.DataFrame:
        """Execute arbitrary SQL and return result as a Polars DataFrame."""
        result = self._con.execute(sql)
        arrow = result.to_arrow_table()
        return pl.from_arrow(arrow)

    def execute_sql(self, sql: str) -> None:
        """Execute SQL without returning results."""
        self._con.execute(sql)

    def materialize(self, model: GeneratedModel) -> ExecutionResult:
        """Execute a model's SQL and return the execution result."""
        start = time.monotonic()
        try:
            self._con.execute(model.sql)
            elapsed_ms = (time.monotonic() - start) * 1000

            # Try to get row count from the materialized table
            row_count = self._get_row_count(model)

            logger.info(
                "Materialized %s: %s rows in %.0fms",
                model.name,
                row_count,
                elapsed_ms,
            )
            return ExecutionResult(
                model_name=model.name,
                success=True,
                row_count=row_count,
                execution_time_ms=elapsed_ms,
            )
        except Exception as e:
            elapsed_ms = (time.monotonic() - start) * 1000
            logger.error("Failed to materialize %s: %s", model.name, e)
            return ExecutionResult(
                model_name=model.name,
                success=False,
                execution_time_ms=elapsed_ms,
                error=str(e),
            )

    def _get_row_count(self, model: GeneratedModel) -> int | None:
        """Extract row count from a materialized table."""
        # Parse the target table from the SQL (CREATE OR REPLACE TABLE schema.name AS ...)
        sql_upper = model.sql.upper()
        if "CREATE OR REPLACE TABLE" in sql_upper:
            # Find the table name between TABLE and AS
            idx_table = sql_upper.index("TABLE") + len("TABLE")
            idx_as = sql_upper.index(" AS", idx_table)
            table_ref = model.sql[idx_table:idx_as].strip()
            try:
                result = self._con.execute(f"SELECT COUNT(*) FROM {table_ref}")
                return result.fetchone()[0]
            except Exception:
                return None
        return None

    def ensure_schema(self, schema: str) -> None:
        """Create a schema if it doesn't exist."""
        self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if a table exists in the given schema."""
        try:
            result = self._con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            )
            return result.fetchone()[0] > 0
        except Exception:
            return False
