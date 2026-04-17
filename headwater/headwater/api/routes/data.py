"""Data viewing API routes -- preview tables and run read-only queries."""

from __future__ import annotations

import logging
import re

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from headwater.explorer.utils import resolve_table_ref, table_exists

router = APIRouter()
logger = logging.getLogger(__name__)

_MAX_ROWS = 500

_MUTATING_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXEC)\b",
    re.IGNORECASE,
)

# Schemas that DuckDB uses internally; hide from the catalog.
_INTERNAL_SCHEMAS = {"information_schema", "pg_catalog"}


def _serialize_value(val):
    """Convert a value to a JSON-safe representation."""
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def _empty_query_result(sql: str, error: str) -> dict:
    return {
        "columns": [],
        "data": [],
        "row_count": 0,
        "sql": sql,
        "error": error,
    }


def _get_schemas(con) -> list[str]:
    """Return all user-created schemas in DuckDB (excludes internal ones)."""
    rows = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    return [r[0] for r in rows if r[0] not in _INTERNAL_SCHEMAS]


class QueryRequest(BaseModel):
    """Body for the POST /data/query endpoint."""

    sql: str = Field(..., description="Read-only SQL query to execute against DuckDB.")
    limit: int = Field(default=100, ge=1, le=_MAX_ROWS, description="Max rows to return.")


class KeyPersistRequest(BaseModel):
    """Body for PATCH /tables/{table}/keys."""

    confirm_pks: list[str] = Field(default_factory=list)
    reject_pks: list[str] = Field(default_factory=list)
    confirm_fks: list[dict] = Field(default_factory=list)
    reject_fk_ids: list[int] = Field(default_factory=list)


def _require_discovery(request: Request):
    """Raise 400 if no discovery has been run."""
    pipeline = request.app.state.pipeline
    if pipeline.get("discovery") is None:
        raise HTTPException(
            status_code=400,
            detail="No discovery data available. Run the pipeline first.",
        )


def _find_table(table_name: str, request: Request) -> str | None:
    """Return the schema-qualified ref if the table exists, else None."""
    con = request.app.state.duckdb_con
    pipeline = request.app.state.pipeline
    models = pipeline.get("staging_models", []) + pipeline.get("mart_models", [])

    ref = resolve_table_ref(table_name, con, models)

    # resolve_table_ref falls back to unqualified name -- verify it actually exists
    if "." in ref:
        schema, tbl = ref.split(".", 1)
        if table_exists(con, schema, tbl):
            return ref
    else:
        # Unqualified fallback -- check if DuckDB can find it
        try:
            con.execute(f"SELECT 1 FROM {ref} LIMIT 0")  # noqa: S608
            return ref
        except Exception:
            pass
    return None


@router.get("/data/catalog")
def get_catalog(request: Request):
    """Return all tables across all schemas in DuckDB with column info.

    Unlike insights (which only shows enriched/discovered tables), this
    lists every table that physically exists in the analytical database.
    """
    con = request.app.state.duckdb_con

    rows = con.execute(
        "SELECT table_schema, table_name "
        "FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' "
        "ORDER BY table_schema, table_name"
    ).fetchall()

    tables = []
    for schema, tname in rows:
        if schema in _INTERNAL_SCHEMAS:
            continue

        # Get column info
        col_rows = con.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema, tname],
        ).fetchall()

        # Get row count
        try:
            count = con.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{tname}"'  # noqa: S608
            ).fetchone()[0]
        except Exception:
            count = None

        tables.append(
            {
                "schema": schema,
                "table_name": tname,
                "qualified_name": f"{schema}.{tname}",
                "row_count": count,
                "column_count": len(col_rows),
                "columns": [{"name": c[0], "dtype": c[1]} for c in col_rows],
            }
        )

    schemas = sorted({t["schema"] for t in tables})
    return {"schemas": schemas, "tables": tables, "total": len(tables)}


@router.get("/data/{table_name}/preview")
def preview_table(request: Request, table_name: str, limit: int = 100):
    """Return the first N rows from a staging or mart table.

    Query params:
        limit: number of rows (default 100, max 500).
    """
    _require_discovery(request)
    limit = min(max(limit, 1), _MAX_ROWS)

    ref = _find_table(table_name, request)
    if ref is None:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

    con = request.app.state.duckdb_con

    try:
        result = con.execute(f"SELECT * FROM {ref} LIMIT {limit}")  # noqa: S608
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Query failed: {exc}") from exc

    # Get total row count
    try:
        total_result = con.execute(f"SELECT COUNT(*) FROM {ref}")  # noqa: S608
        total_rows = total_result.fetchone()[0]
    except Exception:
        total_rows = len(rows)

    data = [
        {col: _serialize_value(val) for col, val in zip(columns, row, strict=False)} for row in rows
    ]

    return {
        "columns": columns,
        "data": data,
        "row_count": len(rows),
        "total_rows": total_rows,
        "sql": f"SELECT * FROM {ref} LIMIT {limit}",
    }


@router.post("/data/query")
def run_query(request: Request, body: QueryRequest):
    """Execute a read-only SQL query against DuckDB and return results.

    Returns error information in the response body rather than raising HTTP errors,
    so the UI can display the message inline.
    """
    _require_discovery(request)

    sql = body.sql.strip().rstrip(";")
    if not sql:
        return _empty_query_result(body.sql, "SQL query must not be empty.")

    if _MUTATING_PATTERN.search(sql):
        return _empty_query_result(
            sql,
            "Blocked: only read-only queries are allowed. "
            "INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, "
            "and EXEC statements are rejected.",
        )

    limit = body.limit
    con = request.app.state.duckdb_con

    # Set search path so unqualified table names resolve across all user schemas
    schemas = _get_schemas(con)
    if schemas:
        path = ",".join(schemas)
        con.execute(f"SET search_path = '{path}'")

    # Wrap in a limited subquery to enforce row cap
    wrapped_sql = f"SELECT * FROM ({sql}) AS _q LIMIT {limit}"

    try:
        result = con.execute(wrapped_sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
    except Exception as exc:
        return _empty_query_result(sql, f"Query failed: {exc}")

    data = [
        {col: _serialize_value(val) for col, val in zip(columns, row, strict=False)} for row in rows
    ]

    return {
        "columns": columns,
        "data": data,
        "row_count": len(rows),
        "sql": sql,
        "error": None,
    }


@router.get("/tables/{table_name}/pk-fk-suggestions")
def get_pk_fk_suggestions(request: Request, table_name: str):
    """Auto-detect PK/FK candidates from data profiles."""
    _require_discovery(request)
    discovery = request.app.state.pipeline["discovery"]

    # Find row_count for tables
    table_row_counts = {t.name: t.row_count for t in discovery.tables}

    def _profile_to_dict(p, tbl_name: str) -> dict:
        """Convert ColumnProfile to the dict format key_detection expects."""
        return {
            "column_name": p.column_name,
            "dtype": p.dtype,
            "stats": {
                "row_count": table_row_counts.get(tbl_name, 0),
                "distinct_count": p.distinct_count,
                "null_count": p.null_count,
                "min": p.min_value,
                "max": p.max_value,
            },
        }

    # Find the table's profiles
    table_profiles = [
        _profile_to_dict(p, table_name)
        for p in discovery.profiles
        if p.table_name == table_name
    ]
    if not table_profiles:
        raise HTTPException(status_code=404, detail=f"No profiles found for table '{table_name}'.")

    from headwater.profiler.key_detection import suggest_foreign_keys, suggest_primary_keys

    pk_candidates = suggest_primary_keys(table_name, table_profiles)

    # Build profiles for all tables for FK detection
    all_profiles: dict[str, list[dict]] = {}
    for p in discovery.profiles:
        tbl = p.table_name
        if tbl not in all_profiles:
            all_profiles[tbl] = []
        all_profiles[tbl].append(_profile_to_dict(p, tbl))

    # Get known PKs from metadata
    store = request.app.state.metadata_store
    source_name = discovery.source.name if discovery.source else ""
    pk_columns: dict[str, list[str]] = {}
    if source_name:
        for tbl_info in discovery.tables:
            if not hasattr(store, "get_columns"):
                continue
            cols = store.get_columns(tbl_info.name, source_name)
            pks = [c["name"] for c in cols if c.get("is_primary_key")]
            if pks:
                pk_columns[tbl_info.name] = pks

    fk_candidates = suggest_foreign_keys(all_profiles, pk_columns if pk_columns else None)
    # Filter FK candidates to those relevant to this table
    fk_candidates = [fk for fk in fk_candidates if fk.from_table == table_name]

    return {
        "table": table_name,
        "pk_candidates": [c.model_dump() for c in pk_candidates],
        "fk_candidates": [c.model_dump() for c in fk_candidates],
    }


@router.patch("/tables/{table_name}/keys")
def persist_keys(request: Request, table_name: str, body: KeyPersistRequest):
    """Confirm or reject PK/FK suggestions, persisting to metadata."""
    _require_discovery(request)
    discovery = request.app.state.pipeline["discovery"]
    source_name = discovery.source.name if discovery.source else ""

    if not source_name:
        raise HTTPException(status_code=400, detail="No source name found in discovery.")

    store = request.app.state.metadata_store
    result = store.persist_pk_fk(
        table_name,
        source_name,
        confirm_pks=body.confirm_pks or None,
        reject_pks=body.reject_pks or None,
        confirm_fks=body.confirm_fks or None,
        reject_fk_ids=body.reject_fk_ids or None,
    )

    store.log_activity(
        "keys_updated",
        f"Updated keys for {table_name}: {result}",
        artifact_type="table",
        artifact_id=table_name,
    )

    return result
