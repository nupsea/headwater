"""Sources API -- multi-source connector list, registration, and sync history.

Backs the /sources page in the UI. The legacy single-source flow on /api/discover
still works; this layer adds first-class CRUD over the `sources` SQLite table plus
the connector picker catalog and a per-source sync event log.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.connectors.registry import (
    get_connector,
    list_connector_catalog,
)
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig

logger = logging.getLogger(__name__)
router = APIRouter()


# ---- Schemas --------------------------------------------------------------


class SourceCreate(BaseModel):
    name: str
    type: str
    host: str | None = None
    uri: str | None = None
    path: str | None = None
    display_name: str | None = None
    auto_sync: bool = False
    config: dict | None = None


class SourceSummary(BaseModel):
    name: str
    display_name: str | None
    type: str
    host: str | None
    status: str
    health: int | None
    last_sync_at: str | None
    drift_count: int
    auto_sync: bool
    tables: int
    rows: int
    schemas: int


# ---- Helpers --------------------------------------------------------------


def _aggregate_counts(store, name: str) -> tuple[int, int, int]:
    """Return (schemas, tables, rows) summary for a source from the metadata store."""
    try:
        rows = store.get_active_tables(name)
    except Exception:
        rows = []
    schemas = {r.get("schema_name") for r in rows if r.get("schema_name")}
    return (
        len(schemas) or (1 if rows else 0),
        len(rows),
        sum(int(r.get("row_count") or 0) for r in rows),
    )


def _row_to_summary(store, row: dict) -> dict:
    schemas, tables, rows_count = _aggregate_counts(store, row["name"])
    return {
        "name": row["name"],
        "display_name": row.get("display_name") or row["name"],
        "type": row["type"],
        "host": row.get("host"),
        "status": row.get("status") or "idle",
        "health": row.get("health"),
        "last_sync_at": row.get("last_sync_at"),
        "drift_count": row.get("drift_count") or 0,
        "auto_sync": bool(row.get("auto_sync")),
        "tables": tables,
        "rows": rows_count,
        "schemas": schemas,
    }


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---- Routes ---------------------------------------------------------------


@router.get("/connector-catalog")
async def connector_catalog():
    """Return the catalog of connector types shown in the UI picker."""
    return {"connectors": list_connector_catalog()}


@router.get("/sources")
async def list_sources_route(request: Request):
    """List all registered sources with health, drift, and table summaries."""
    store = request.app.state.metadata_store
    return {"sources": [_row_to_summary(store, r) for r in store.list_sources()]}


@router.get("/sources/{name}")
async def get_source_route(request: Request, name: str):
    store = request.app.state.metadata_store
    row = store.get_source(name)
    if not row:
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")
    summary = _row_to_summary(store, row)
    summary["events"] = store.list_sync_events(source_name=name, limit=20)
    return summary


@router.post("/sources", status_code=201)
async def create_source(request: Request, body: SourceCreate):
    """Register a new data source. Does not run discovery -- call /sources/{name}/sync."""
    store = request.app.state.metadata_store

    # Validate the connector type exists in the catalog.
    catalog = {c["id"] for c in list_connector_catalog()}
    if body.type not in catalog:
        raise HTTPException(status_code=400, detail=f"Unknown connector type: {body.type}")

    # Persist the row -- auto-sync flag is honored by the future scheduler;
    # for now, status starts as 'idle' until the first sync runs.
    store.upsert_source(body.name, body.type, body.path, body.uri, mode="generate")
    store.upsert_source_meta(
        body.name,
        display_name=body.display_name or body.name,
        host=body.host,
        config=body.config,
        status="idle",
        auto_sync=body.auto_sync,
    )
    store.insert_sync_event(
        body.name,
        "registered",
        f"Source '{body.display_name or body.name}' registered ({body.type})",
        severity="info",
    )

    row = store.get_source(body.name)
    return _row_to_summary(store, row)


@router.post("/sources/{name}/sync")
async def sync_source(request: Request, name: str):
    """Trigger a sync for an existing source.

    For the POC this delegates to the existing pipeline -- it tests the connection
    and records a sync_event. Full discovery remains on /api/pipeline/run.
    """
    store = request.app.state.metadata_store
    row = store.get_source(name)
    if not row:
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")

    try:
        connector = get_connector(row["type"])
        config = SourceConfig(
            name=row["name"],
            type=row["type"],
            path=row.get("path"),
            uri=row.get("uri"),
        )
        connector.connect(config)
        # Health is binary at this layer -- 100 if connect succeeds, lowered by drift.
        health = max(0, 100 - 5 * (row.get("drift_count") or 0))
        store.upsert_source_meta(
            name,
            status="healthy",
            health=health,
            last_sync_at=_now_iso(),
        )
        store.insert_sync_event(name, "sync_complete", "Connection verified", severity="info")
        return {"name": name, "status": "healthy", "health": health}
    except ConnectorError as e:
        store.upsert_source_meta(name, status="error", health=0, last_sync_at=_now_iso())
        store.insert_sync_event(name, "sync_failed", str(e), severity="error")
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001 - any failure becomes an error event
        logger.exception("Sync failed for source '%s'", name)
        store.upsert_source_meta(name, status="error", health=0, last_sync_at=_now_iso())
        store.insert_sync_event(name, "sync_failed", str(e), severity="error")
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.delete("/sources/{name}")
async def delete_source_route(request: Request, name: str):
    store = request.app.state.metadata_store
    if not store.get_source(name):
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")
    store.delete_source(name)
    return {"name": name, "deleted": True}


@router.get("/sources/{name}/events")
async def list_source_events(request: Request, name: str, limit: int = 50):
    store = request.app.state.metadata_store
    if not store.get_source(name):
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")
    return {"events": store.list_sync_events(source_name=name, limit=limit)}


@router.get("/sync-events")
async def list_all_events(request: Request, limit: int = 50):
    """Cross-source activity feed shown on the Sources page footer."""
    store = request.app.state.metadata_store
    return {"events": store.list_sync_events(limit=limit)}
