"""Headwater 2 answer execution — closing the data loop.

Generated answer SQL (from ``h2_answer``) is only worth anything once it runs.
This module rebuilds a project's source into a DuckDB analytical store and
executes the answer SQL, returning real result rows plus a statistical summary
that downstream certification (and the LLM judge) consume as evidence.

Invariants honored here:
  - I-1: DuckDB holds analytical data only; metadata stays in SQLite.  The
    analytical store is rebuilt fresh, in-memory, per request — no single-writer
    lock contention, no stale data.  This also realizes the "refresh from the
    beginning" contract: every execution reflects the current source.
  - I-2: data flows Polars <-> DuckDB via Arrow; no Pandas, no CSV intermediary.
  - I-3: the raw result rows returned here are for the UI/executor only.  The
    ``stats`` summary is the I-3-safe artifact the LLM judge consumes; raw rows
    are never handed to an LLM.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import Any

import duckdb
import polars as pl

from headwater.connectors.registry import get_connector
from headwater.core.models import SourceConfig
from headwater.core.store import HeadwaterStore
from headwater.executor.duckdb_backend import DuckDBBackend

# Source types whose data can be materialized locally for execution.  Warehouse
# connectors (postgres/mysql/snowflake/redshift) do pushdown profiling only and
# raise NotImplementedError from load_to_duckdb.
_MATERIALIZABLE = frozenset({"json", "csv", "duckdb", "sqlite", "parquet"})

# Cap rows returned to the UI.  Stats are still computed over the full result.
_MAX_PREVIEW_ROWS = 500


@dataclass(slots=True)
class ExecutedResult:
    """Outcome of executing one answer's SQL against the analytical store."""

    question_id: str
    sql_text: str | None
    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)  # capped preview
    row_count: int = 0
    truncated: bool = False
    stats: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _schema_name(source_name: str) -> str:
    # Mirror the transform used by h2_source.discover_and_persist so the
    # schema/table names match what discovery recorded.
    return source_name.replace("-", "_").replace(".", "_")


def materialize_source(
    store: HeadwaterStore, source_name: str
) -> tuple[duckdb.DuckDBPyConnection, str]:
    """Rebuild a source's tables into a fresh in-memory DuckDB.

    Returns ``(con, schema)`` with the default schema set so unqualified table
    names in answer SQL resolve.  Caller owns the connection and must close it.

    Raises ValueError for sources whose data cannot be materialized locally.
    """
    src = store.get_source(source_name)
    if src is None:
        raise ValueError(f"Source '{source_name}' not found.")

    src_type = str(src["type"])
    if src_type not in _MATERIALIZABLE:
        raise ValueError(
            f"Source type '{src_type}' cannot be executed locally yet — "
            "execution supports file/embedded sources "
            "(json, csv, duckdb, sqlite, parquet)."
        )

    config = SourceConfig(
        name=src["name"],
        type=src_type,  # type: ignore[arg-type]
        path=src.get("path"),
        uri=src.get("uri"),
        mode="generate",
    )
    connector = get_connector(config.type)
    connector.connect(config)

    con = duckdb.connect(":memory:")
    schema = _schema_name(source_name)
    try:
        connector.load_to_duckdb(con, schema)
        # Answer SQL references bare table names (e.g. "events"); set the default
        # schema so those resolve against the loaded tables.
        con.execute(f'USE "{schema}"')
    except Exception:
        con.close()
        raise
    finally:
        close = getattr(connector, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()
    return con, schema


def execute_one(
    con: duckdb.DuckDBPyConnection,
    question_id: str,
    sql_text: str | None,
) -> ExecutedResult:
    """Execute a single answer's SQL on an already-materialized connection."""
    if not sql_text:
        return ExecutedResult(question_id=question_id, sql_text=sql_text)

    backend = DuckDBBackend(con)
    try:
        df = backend.execute(sql_text)
    except Exception as exc:  # surfaced to the UI as a non-certifiable result
        return ExecutedResult(
            question_id=question_id, sql_text=sql_text, error=str(exc)
        )

    row_count = df.height
    truncated = row_count > _MAX_PREVIEW_ROWS
    preview = df.head(_MAX_PREVIEW_ROWS)
    return ExecutedResult(
        question_id=question_id,
        sql_text=sql_text,
        columns=list(df.columns),
        rows=_records(preview),
        row_count=row_count,
        truncated=truncated,
        stats=result_stats(df),
    )


def execute_answers(
    store: HeadwaterStore,
    source_name: str,
    items: list[tuple[str, str | None]],
) -> dict[str, ExecutedResult]:
    """Materialize the source once and execute many (question_id, sql) pairs."""
    results: dict[str, ExecutedResult] = {}
    con: duckdb.DuckDBPyConnection | None = None
    try:
        con, _ = materialize_source(store, source_name)
        for question_id, sql_text in items:
            results[question_id] = execute_one(con, question_id, sql_text)
    finally:
        if con is not None:
            con.close()
    return results


def execute_project_answers(
    store: HeadwaterStore, project_id: str
) -> dict[str, ExecutedResult]:
    """Execute the latest persisted answer artifact for every project question."""
    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")
    source_name = project_sources[0]["source_name"]

    items: list[tuple[str, str | None]] = []
    for question in store.list_questions(project_id):
        artifact = store.get_answer_artifact(f"{question['id']}:answer:latest")
        if artifact is None:
            continue
        items.append((question["id"], artifact.get("sql_text")))

    if not items:
        return {}
    return execute_answers(store, source_name, items)


# ── Statistical summary (I-3-safe evidence) ───────────────────────────────────


def result_stats(df: pl.DataFrame) -> dict[str, Any]:
    """Summarize a result frame for certification evidence and the LLM judge.

    Aggregates only — never raw rows.  Per column: dtype, null/distinct counts,
    and numeric/temporal min-max-mean where applicable.
    """
    columns: dict[str, Any] = {}
    for name in df.columns:
        series = df[name]
        info: dict[str, Any] = {
            "dtype": str(series.dtype),
            "null_count": int(series.null_count()),
        }
        with contextlib.suppress(Exception):
            info["distinct_count"] = int(series.n_unique())
        if series.dtype.is_numeric():
            info["min"] = _scalar(series.min())
            info["max"] = _scalar(series.max())
            info["mean"] = _scalar(series.mean())
        elif series.dtype.is_temporal():
            info["min"] = _scalar(series.min())
            info["max"] = _scalar(series.max())
        columns[name] = info

    return {
        "row_count": df.height,
        "column_count": df.width,
        "columns": columns,
    }


def _records(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert a frame to JSON-friendly records (dates/decimals -> str/float)."""
    return [{k: _scalar(v) for k, v in row.items()} for row in df.to_dicts()]


def _scalar(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    # datetime/date/Decimal and any other exotic type -> stable string/float.
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    return str(value)
