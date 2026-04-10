"""Discovery API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.heuristics import build_domain_map, enrich_tables
from headwater.connectors.registry import get_connector
from headwater.core.models import SourceConfig
from headwater.profiler.engine import discover

router = APIRouter()


@router.post("/discover")
async def run_discovery(
    request: Request,
    source_path: str,
    source_type: str = "json",
    source_name: str = "source",
    source_schema: str = "env_health",
):
    """Run the discovery pipeline on a data source."""
    from pathlib import Path

    data_path = Path(source_path).resolve()
    if not data_path.exists():
        raise HTTPException(status_code=400, detail=f"Path not found: {data_path}")

    con = request.app.state.duckdb_con
    source = SourceConfig(name=source_name, type=source_type, path=str(data_path))

    connector = get_connector(source.type)
    connector.connect(source)
    connector.load_to_duckdb(con, source_schema)

    discovery = discover(con, source_schema, source)
    enrich_tables(discovery.tables, discovery.profiles, discovery.relationships)
    discovery.domains = build_domain_map(discovery.tables)

    request.app.state.pipeline["discovery"] = discovery

    return {
        "tables": len(discovery.tables),
        "profiles": len(discovery.profiles),
        "relationships": len(discovery.relationships),
        "domains": discovery.domains,
    }


@router.get("/tables")
async def list_tables(request: Request):
    """List discovered tables."""
    discovery = request.app.state.pipeline["discovery"]
    if not discovery:
        raise HTTPException(
            status_code=400, detail="No discovery run yet. POST /api/discover first."
        )
    return [
        {
            "name": t.name,
            "row_count": t.row_count,
            "columns": len(t.columns),
            "domain": t.domain,
            "description": t.description,
        }
        for t in discovery.tables
    ]


@router.get("/tables/{table_name}")
async def get_table(request: Request, table_name: str):
    """Get table detail including columns."""
    discovery = request.app.state.pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")
    return table.model_dump()


@router.get("/tables/{table_name}/profile")
async def get_table_profile(request: Request, table_name: str):
    """Get column profiles for a table."""
    discovery = request.app.state.pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    profiles = [p for p in discovery.profiles if p.table_name == table_name]
    if not profiles:
        raise HTTPException(status_code=404, detail=f"No profiles for '{table_name}'.")
    return [p.model_dump() for p in profiles]


@router.get("/relationships")
async def list_relationships(request: Request):
    """List all detected relationships."""
    discovery = request.app.state.pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    return [r.model_dump() for r in discovery.relationships]


class ColumnPatchRequest(BaseModel):
    """Payload for PATCH /api/columns/{source_name}/{table_name}/{column_name}."""

    description: str | None = None
    locked: bool | None = None


@router.patch("/columns/{source_name}/{table_name}/{column_name}")
async def patch_column(
    request: Request,
    source_name: str,
    table_name: str,
    column_name: str,
    body: ColumnPatchRequest,
) -> dict:
    """Update and optionally lock a column description.

    Setting description automatically locks the column (locked=true).
    Setting locked=false clears the lock without changing description.
    """
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    # Check column exists in in-memory discovery
    discovery = request.app.state.pipeline.get("discovery")
    if discovery is None:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    table = next((t for t in discovery.tables if t.name == table_name), None)
    if table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")
    col = next((c for c in table.columns if c.name == column_name), None)
    if col is None:
        raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found.")

    # Apply changes
    should_lock = body.locked if body.locked is not None else (body.description is not None)
    if should_lock:
        if body.description is not None:
            col.description = body.description
        store.lock_column(
            table_name, source_name, column_name,
            locked=True, description=body.description,
        )
        store.record_decision(
            "column", f"{source_name}.{table_name}.{column_name}", "locked",
            payload={"description": body.description},
        )
    elif body.locked is False:
        store.lock_column(table_name, source_name, column_name, locked=False)
        store.record_decision(
            "column", f"{source_name}.{table_name}.{column_name}", "unlocked",
        )
    elif body.description is not None:
        col.description = body.description

    return {
        "source_name": source_name,
        "table_name": table_name,
        "column_name": column_name,
        "description": col.description,
        "locked": should_lock,
    }
