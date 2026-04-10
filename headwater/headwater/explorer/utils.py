"""Shared utilities for the explorer layer."""

from __future__ import annotations

import duckdb

from headwater.core.models import GeneratedModel


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """Return True if the table exists in DuckDB."""
    try:
        con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ? LIMIT 1",
            [schema, table],
        )
        return bool(con.fetchone())
    except Exception:
        return False


def resolve_table_ref(
    table_name: str,
    con: duckdb.DuckDBPyConnection,
    models: list[GeneratedModel],
) -> str:
    """Return the best available table reference for a given source table name.

    Resolution order (first that exists in DuckDB wins):
    1. An executed mart that covers this table
    2. staging.stg_{table_name}
    3. public.{table_name}  (source table loaded directly)
    4. {table_name}         (unqualified fallback)
    """
    for m in models:
        if (
            m.model_type == "mart"
            and table_name in m.source_tables
            and m.status == "executed"
            and table_exists(con, "marts", m.name)
        ):
            return f"marts.{m.name}"

    staging_name = f"stg_{table_name}"
    if table_exists(con, "staging", staging_name):
        return f"staging.{staging_name}"

    if table_exists(con, "public", table_name):
        return f"public.{table_name}"

    return table_name
