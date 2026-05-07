"""Sources API -- multi-source connector list, registration, and sync history.

Backs the /sources page in the UI. The legacy single-source flow on /api/discover
still works; this layer adds first-class CRUD over the `sources` SQLite table plus
the connector picker catalog and a per-source sync event log.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.api.project_scope import project_sources
from headwater.connectors.registry import (
    connector_status,
    list_connector_catalog,
)
from headwater.core.events import EventType
from headwater.core.exceptions import ConnectorError
from headwater.services.source_evaluation import evaluate_all_connectors, evaluate_source
from headwater.services.source_sync import SourceNotFoundError, SourceSyncService

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
    quality_failed: int
    quality_score: float | None
    latest_run_status: str | None
    latest_run_duration_ms: int | None
    latest_error: str | None
    auto_sync: bool
    tables: int
    rows: int
    schemas: int
    evaluation: dict


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
    latest_run = store.get_latest_sync_run(row["name"])
    latest_quality = store.get_latest_quality_report(row["name"])
    evaluation_row = {
        **row,
        "latest_run_status": latest_run["status"] if latest_run else None,
        "quality_failed": latest_quality["failed"] if latest_quality else 0,
        "quality_score": latest_quality["score"] if latest_quality else None,
    }
    return {
        "name": row["name"],
        "display_name": row.get("display_name") or row["name"],
        "type": row["type"],
        "host": row.get("host"),
        "status": row.get("status") or "idle",
        "health": row.get("health"),
        "last_sync_at": row.get("last_sync_at"),
        "drift_count": row.get("drift_count") or 0,
        "quality_failed": latest_quality["failed"] if latest_quality else 0,
        "quality_score": latest_quality["score"] if latest_quality else None,
        "latest_run_status": latest_run["status"] if latest_run else None,
        "latest_run_duration_ms": latest_run.get("duration_ms") if latest_run else None,
        "latest_error": latest_run.get("error") if latest_run else None,
        "auto_sync": bool(row.get("auto_sync")),
        "tables": tables,
        "rows": rows_count,
        "schemas": schemas,
        "evaluation": evaluate_source(
            evaluation_row,
            schemas=schemas,
            tables=tables,
            rows=rows_count,
        ),
    }


# ---- Routes ---------------------------------------------------------------


@router.get("/connector-catalog")
async def connector_catalog():
    """Return the catalog of connector types shown in the UI picker."""
    return {"connectors": list_connector_catalog()}


@router.get("/source-evaluations")
async def source_evaluations():
    """Return OLTP/OLAP evaluation templates for available connector types."""
    return {"evaluations": evaluate_all_connectors()}


@router.get("/sources")
async def list_sources_route(request: Request, project_id: str | None = None):
    """List all registered sources with health, drift, and table summaries."""
    store = request.app.state.metadata_store
    rows = store.list_sources()
    if project_id:
        project = store.get_project(project_id)
        if not project:
            raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
        allowed = set(project_sources(project, store))
        rows = [row for row in rows if row["name"] in allowed]
    return {"sources": [_row_to_summary(store, r) for r in rows]}


@router.get("/sources/{name}")
async def get_source_route(request: Request, name: str):
    store = request.app.state.metadata_store
    row = store.get_source(name)
    if not row:
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")
    summary = _row_to_summary(store, row)
    summary["events"] = store.list_events(source_name=name, limit=20)
    summary["runs"] = store.list_sync_runs(source_name=name, limit=10)
    return summary


@router.get("/sources/{name}/evaluation")
async def get_source_evaluation(request: Request, name: str):
    store = request.app.state.metadata_store
    row = store.get_source(name)
    if not row:
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")
    summary = _row_to_summary(store, row)
    return summary["evaluation"]


@router.post("/sources", status_code=201)
async def create_source(request: Request, body: SourceCreate):
    """Register a new data source. Does not run discovery -- call /sources/{name}/sync."""
    store = request.app.state.metadata_store

    # Validate the connector type exists in the catalog.
    catalog = {c["id"] for c in list_connector_catalog()}
    if body.type not in catalog:
        raise HTTPException(status_code=400, detail=f"Unknown connector type: {body.type}")
    connector = next((c for c in list_connector_catalog() if c["id"] == body.type), None)
    status = connector_status(body.type)
    if not connector or not connector.get("supported"):
        raise HTTPException(
            status_code=400,
            detail=f"Connector '{body.type}' is {status}, not supported in this build.",
        )

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
    SourceSyncService(request).record_event(
        EventType.SOURCE_REGISTERED,
        f"Source '{body.display_name or body.name}' registered ({body.type})",
        source_name=body.name,
        invalidates=["sources", "briefing"],
    )

    row = store.get_source(body.name)
    return _row_to_summary(store, row)


@router.post("/sources/{name}/test")
async def test_source(request: Request, name: str):
    """Test connection for an existing source without running discovery."""
    service = SourceSyncService(request)
    try:
        return service.test(name)
    except SourceNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ConnectorError as e:
        service.record_event(
            EventType.CONNECTION_TEST_FAILED,
            str(e),
            source_name=name,
            severity="error",
            invalidates=["sources", "briefing"],
        )
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.post("/sources/{name}/sync")
async def sync_source(request: Request, name: str):
    """Trigger a sync for an existing source.

    Runs the source-scoped pipeline using the persisted source path/URI and records
    durable sync run + event history.
    """
    service = SourceSyncService(request)
    try:
        return service.sync(name)
    except SourceNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ConnectorError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.delete("/sources/{name}")
async def delete_source_route(request: Request, name: str):
    store = request.app.state.metadata_store
    if not store.get_source(name):
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")
    store.delete_source(name)
    pipeline = getattr(request.app.state, "pipeline", None)
    if pipeline and pipeline.get("discovery"):
        active_source = pipeline["discovery"].source.name if pipeline["discovery"].source else None
        if active_source == name:
            pipeline["discovery"] = None
            pipeline["catalog"] = None
            pipeline["staging_models"] = []
            pipeline["mart_models"] = []
            pipeline["contracts"] = []
            pipeline["execution_results"] = []
            pipeline["quality_report"] = None
    return {"name": name, "deleted": True}


@router.get("/sources/{name}/events")
async def list_source_events(request: Request, name: str, limit: int = 50):
    store = request.app.state.metadata_store
    if not store.get_source(name):
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")
    return {"events": store.list_events(source_name=name, limit=limit)}


@router.get("/events")
async def list_events(request: Request, source: str | None = None, limit: int = 50):
    """Return normalized operational events across sources."""
    store = request.app.state.metadata_store
    if source and not store.get_source(source):
        raise HTTPException(status_code=404, detail=f"Source '{source}' not found.")
    return {"events": store.list_events(source_name=source, limit=limit)}


@router.get("/sync-events")
async def list_all_events(request: Request, limit: int = 50):
    """Cross-source activity feed shown on the Sources page footer."""
    store = request.app.state.metadata_store
    return {"events": store.list_sync_events(limit=limit)}
