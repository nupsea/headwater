"""Sources API -- multi-source connector list, registration, and sync history.

Backs the /sources page in the UI. The legacy single-source flow on /api/discover
still works; this layer adds first-class CRUD over the `sources` SQLite table plus
the connector picker catalog and a per-source sync event log.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import duckdb
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.connectors.registry import (
    connector_status,
    get_connector,
    list_connector_catalog,
)
from headwater.core.config import get_settings
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


def _source_value(row: dict) -> str:
    """Return URI or path for a persisted source row."""
    value = row.get("uri") or row.get("path")
    if not value:
        raise ConnectorError("Source has no uri or path configured.")
    return value


def _default_source_schema(row: dict) -> str:
    """Choose a default schema for current source sync."""
    return "public" if row.get("uri") else "env_health"


def _record_event(
    store,
    event_type: str,
    summary: str,
    *,
    source_name: str,
    severity: str = "info",
    payload: dict | None = None,
    invalidates: list[str] | None = None,
    artifact_type: str | None = None,
    artifact_id: str | None = None,
    detail: str | None = None,
) -> None:
    """Write normalized and legacy source events."""
    try:
        store.insert_event(
            event_type,
            summary,
            source_name=source_name,
            severity=severity,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            detail=detail,
            payload=payload,
            invalidates=invalidates,
        )
    except Exception:
        logger.exception("Failed to write normalized event '%s'", event_type)
    try:
        store.insert_sync_event(
            source_name,
            event_type,
            summary,
            severity=severity,
            payload=payload,
        )
    except Exception:
        logger.exception("Failed to write legacy sync_event '%s'", event_type)


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
    summary["runs"] = store.list_sync_runs(source_name=name, limit=10)
    return summary


@router.post("/sources", status_code=201)
async def create_source(request: Request, body: SourceCreate):
    """Register a new data source. Does not run discovery -- call /sources/{name}/sync."""
    store = request.app.state.metadata_store

    # Validate the connector type exists in the catalog.
    catalog = {c["id"] for c in list_connector_catalog()}
    if body.type not in catalog:
        raise HTTPException(status_code=400, detail=f"Unknown connector type: {body.type}")
    status = connector_status(body.type)
    if status != "supported":
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
    _record_event(
        store,
        "source_registered",
        f"Source '{body.display_name or body.name}' registered ({body.type})",
        source_name=body.name,
        invalidates=["sources", "briefing"],
    )

    row = store.get_source(body.name)
    return _row_to_summary(store, row)


@router.post("/sources/{name}/test")
async def test_source(request: Request, name: str):
    """Test connection for an existing source without running discovery."""
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
        table_count = None
        if hasattr(connector, "list_tables"):
            try:
                table_count = len(connector.list_tables())
            except Exception:
                table_count = None
        if hasattr(connector, "close"):
            connector.close()
        _record_event(
            store,
            "connection_tested",
            "Connection verified",
            source_name=name,
            payload={"table_count": table_count},
            invalidates=["sources"],
        )
        return {"name": name, "status": "ok", "tables": table_count}
    except ConnectorError as e:
        _record_event(
            store,
            "connection_test_failed",
            str(e),
            source_name=name,
            severity="error",
            invalidates=["sources", "briefing"],
        )
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001
        logger.exception("Connection test failed for source '%s'", name)
        _record_event(
            store,
            "connection_test_failed",
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
    store = request.app.state.metadata_store
    row = store.get_source(name)
    if not row:
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")

    run_id = store.start_sync_run(name, mode="full")
    store.upsert_source_meta(name, status="syncing", last_sync_at=_now_iso())
    _record_event(
        store,
        "sync_started",
        "Source sync started",
        source_name=name,
        payload={"run_id": run_id},
        invalidates=["sources", "briefing"],
    )

    try:
        from headwater.api.routes.pipeline import _run_pipeline_inner

        source_path = _source_value(row)
        pipeline = request.app.state.pipeline
        source_schema = _default_source_schema(row)
        target_schema = "staging"

        if getattr(request.app.state, "_in_memory", False):
            result = _run_pipeline_inner(
                request.app.state.duckdb_con,
                request,
                pipeline,
                source_path,
                row["type"],
                name,
                source_schema,
                target_schema,
            )
        else:
            settings = get_settings()
            con = duckdb.connect(str(settings.analytical_db_path))
            try:
                result = _run_pipeline_inner(
                    con,
                    request,
                    pipeline,
                    source_path,
                    row["type"],
                    name,
                    source_schema,
                    target_schema,
                )
            finally:
                con.close()

        latest = store.get_source(name) or row
        drift_count = latest.get("drift_count") or 0
        drift_health = max(0, 100 - 5 * drift_count)
        quality_failed = int(result.get("quality_failed") or 0)
        quality_score = int(round(result.get("quality_score", 100)))
        health = min(drift_health, quality_score)
        final_status = "warning" if drift_count or quality_failed else "healthy"
        quality_run_id = result.get("quality_run_id")
        if quality_run_id:
            store.attach_quality_run_to_sync(int(quality_run_id), run_id)
        store.finish_sync_run(
            run_id,
            tables_seen=result.get("tables_discovered", 0),
            profiles_written=result.get("profiles", 0),
            contracts_checked=result.get("quality_total", 0),
            payload=result,
        )
        store.upsert_source_meta(
            name,
            status=final_status,
            health=health,
            last_sync_at=_now_iso(),
        )
        _record_event(
            store,
            "sync_completed",
            f"Source sync completed: {result.get('tables_discovered', 0)} table(s) discovered",
            source_name=name,
            payload={"run_id": run_id, **result},
            invalidates=["sources", "briefing", "health", "insights", "models", "quality"],
        )
        return {"name": name, "status": final_status, "health": health, "run_id": run_id, **result}
    except ConnectorError as e:
        store.fail_sync_run(run_id, str(e))
        store.upsert_source_meta(name, status="error", health=0, last_sync_at=_now_iso())
        _record_event(
            store,
            "sync_failed",
            str(e),
            source_name=name,
            severity="error",
            payload={"run_id": run_id},
            invalidates=["sources", "briefing"],
        )
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001 - any failure becomes an error event
        logger.exception("Sync failed for source '%s'", name)
        store.fail_sync_run(run_id, str(e))
        store.upsert_source_meta(name, status="error", health=0, last_sync_at=_now_iso())
        _record_event(
            store,
            "sync_failed",
            str(e),
            source_name=name,
            severity="error",
            payload={"run_id": run_id},
            invalidates=["sources", "briefing"],
        )
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
