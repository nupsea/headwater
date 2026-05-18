"""Data viewing API routes -- preview tables and run read-only queries."""

from __future__ import annotations

import logging
import re

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from headwater.api.project_scope import scoped_pipeline
from headwater.core.models import Relationship
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
_SOURCE_SCHEMAS = ("public", "main")
_SQL_QUOTE_TRANSLATION = str.maketrans(
    {
        "\u201c": '"',
        "\u201d": '"',
        "\u201e": '"',
        "\u201f": '"',
        "\u2018": "'",
        "\u2019": "'",
        "\u201a": "'",
        "\u201b": "'",
    }
)


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


def _normalize_sql(sql: str) -> str:
    """Normalize paste-friendly SQL punctuation before sending it to DuckDB."""
    return sql.translate(_SQL_QUOTE_TRANSLATION).strip().rstrip(";")


def _get_schemas(con) -> list[str]:
    """Return all user-created schemas in DuckDB (excludes internal ones)."""
    rows = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    return [r[0] for r in rows if r[0] not in _INTERNAL_SCHEMAS]


def _find_source_table_ref(con, table_names: list[str]) -> str | None:
    """Return a quoted ref for the first matching loaded source table."""
    for schema in _SOURCE_SCHEMAS:
        for table_name in table_names:
            if table_exists(con, schema, table_name):
                return f'"{schema}"."{table_name}"'
    return None


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


def _require_discovery(request: Request, project_id: str | None = None):
    """Raise 400 if no discovery has been run."""
    pipeline = scoped_pipeline(request, project_id)
    if pipeline.get("discovery") is None:
        raise HTTPException(
            status_code=400,
            detail="No discovery data available. Run the pipeline first.",
        )
    return pipeline


def _find_table(table_name: str, request: Request, pipeline: dict | None = None) -> str | None:
    """Return the schema-qualified ref if the table exists, else None."""
    con = request.app.state.duckdb_con
    pipeline = pipeline or request.app.state.pipeline
    models = pipeline.get("staging_models", []) + pipeline.get("mart_models", [])

    if "." in table_name:
        schema, tbl = table_name.split(".", 1)
        if table_exists(con, schema, tbl):
            return f'"{schema}"."{tbl}"'
        if schema == "staging" and tbl.startswith("stg_"):
            source_tbl = tbl.removeprefix("stg_")
            source_ref = _find_source_table_ref(con, [source_tbl, tbl])
            if source_ref:
                return source_ref

    if table_name.startswith("stg_") and table_exists(con, "staging", table_name):
        return f'"staging"."{table_name}"'
    if table_name.startswith("stg_"):
        source_tbl = table_name.removeprefix("stg_")
        source_ref = _find_source_table_ref(con, [source_tbl, table_name])
        if source_ref:
            return source_ref

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
def get_catalog(request: Request, project_id: str | None = None):
    """Return all tables across all schemas in DuckDB with column info.

    Unlike insights (which only shows enriched/discovered tables), this
    lists every table that physically exists in the analytical database.
    """
    con = request.app.state.duckdb_con
    project_tables: set[tuple[str | None, str]] | None = None
    if project_id:
        discovery = scoped_pipeline(request, project_id).get("discovery")
        project_tables = (
            {(table.schema_name, table.name) for table in discovery.tables}
            if discovery
            else set()
        )

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
        if project_tables is not None and (schema, tname) not in project_tables:
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
def preview_table(
    request: Request,
    table_name: str,
    limit: int = 100,
    project_id: str | None = None,
):
    """Return the first N rows from a staging or mart table.

    Query params:
        limit: number of rows (default 100, max 500).
    """
    pipeline = _require_discovery(request, project_id)
    limit = min(max(limit, 1), _MAX_ROWS)

    ref = _find_table(table_name, request, pipeline)
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
def run_query(request: Request, body: QueryRequest, project_id: str | None = None):
    """Execute a read-only SQL query against DuckDB and return results.

    Returns error information in the response body rather than raising HTTP errors,
    so the UI can display the message inline.
    """
    _require_discovery(request, project_id)

    sql = _normalize_sql(body.sql)
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
def get_pk_fk_suggestions(
    request: Request,
    table_name: str,
    project_id: str | None = None,
):
    """Auto-detect PK/FK candidates from data profiles."""
    pipeline = _require_discovery(request, project_id)
    discovery = pipeline["discovery"]

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

    store = request.app.state.metadata_store
    source_name = discovery.source.name if discovery.source else ""
    table_info = next((t for t in discovery.tables if t.name == table_name), None)
    existing_pks = {c.name for c in table_info.columns if c.is_primary_key} if table_info else set()
    rejected_pks: set[str] = set()
    if source_name and hasattr(store, "get_decisions"):
        for col in table_profiles:
            col_name = col["column_name"]
            decisions = store.get_decisions(
                "pk_candidate",
                f"{source_name}.{table_name}.{col_name}",
            )
            if decisions and decisions[0].get("action") == "rejected":
                rejected_pks.add(col_name)

    pk_candidates = [
        c
        for c in suggest_primary_keys(table_name, table_profiles)
        if c.column not in existing_pks and c.column not in rejected_pks
    ]

    # Build profiles for all tables for FK detection
    all_profiles: dict[str, list[dict]] = {}
    for p in discovery.profiles:
        tbl = p.table_name
        if tbl not in all_profiles:
            all_profiles[tbl] = []
        all_profiles[tbl].append(_profile_to_dict(p, tbl))

    # Get known PKs from metadata
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
    existing_fks = {
        (r.from_table, r.from_column, r.to_table, r.to_column)
        for r in discovery.relationships
    }
    fk_candidates = [
        fk
        for fk in fk_candidates
        if fk.from_table == table_name
        and (fk.from_table, fk.from_column, fk.to_table, fk.to_column) not in existing_fks
    ]

    return {
        "table": table_name,
        "pk_candidates": [c.model_dump() for c in pk_candidates],
        "fk_candidates": [c.model_dump() for c in fk_candidates],
    }


@router.patch("/tables/{table_name}/keys")
def persist_keys(
    request: Request,
    table_name: str,
    body: KeyPersistRequest,
    project_id: str | None = None,
):
    """Confirm or reject PK/FK suggestions, persisting to metadata."""
    pipeline = _require_discovery(request, project_id)
    discovery = pipeline["discovery"]
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

    _apply_key_changes_to_discovery(discovery, table_name, body)

    store.log_activity(
        "keys_updated",
        f"Updated keys for {table_name}: {result}",
        artifact_type="table",
        artifact_id=table_name,
    )

    return result


def _apply_key_changes_to_discovery(discovery, table_name: str, body: KeyPersistRequest) -> None:
    """Keep active discovery state in sync with key confirmations."""
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if table is None:
        return

    for col_name in body.confirm_pks:
        col = next((c for c in table.columns if c.name == col_name), None)
        if col:
            col.is_primary_key = True
            col.semantic_type = "primary_key"
            col.role = "identifier"
            col.confidence = max(col.confidence, 1.0)

    for col_name in body.reject_pks:
        col = next((c for c in table.columns if c.name == col_name), None)
        if col:
            col.is_primary_key = False
            if col.semantic_type == "primary_key":
                col.semantic_type = None

    for fk in body.confirm_fks:
        key = (table_name, fk["from_col"], fk["to_table"], fk["to_col"])
        exists = any(
            (r.from_table, r.from_column, r.to_table, r.to_column) == key
            for r in discovery.relationships
        )
        if not exists:
            discovery.relationships.append(
                Relationship(
                    from_table=table_name,
                    from_column=fk["from_col"],
                    to_table=fk["to_table"],
                    to_column=fk["to_col"],
                    type="many_to_one",
                    confidence=1.0,
                    referential_integrity=0.0,
                    source="declared",
                )
            )

        col = next((c for c in table.columns if c.name == fk["from_col"]), None)
        if col:
            col.semantic_type = "foreign_key"
            col.role = "identifier"
            col.confidence = max(col.confidence, 1.0)

    if body.reject_fk_ids:
        reject_ids = set(body.reject_fk_ids)
        discovery.relationships = [
            r for r in discovery.relationships if r.id not in reject_ids
        ]
