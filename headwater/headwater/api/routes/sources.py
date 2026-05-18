"""Sources API -- multi-source connector list, registration, and sync history.

Backs the /sources page in the UI. The legacy single-source flow on /api/discover
still works; this layer adds first-class CRUD over the `sources` SQLite table plus
the connector picker catalog and a per-source sync event log.
"""

from __future__ import annotations

import json
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
from headwater.core.redaction import redact_secrets
from headwater.core.runtime_state import get_runtime_state
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


class SourceUpdate(BaseModel):
    type: str | None = None
    host: str | None = None
    uri: str | None = None
    path: str | None = None
    display_name: str | None = None
    auto_sync: bool | None = None
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
        "latest_error": redact_secrets(latest_run.get("error")) if latest_run else None,
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


def _redact_source_config(config: object) -> dict:
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (TypeError, ValueError):
            config = {}
    if not isinstance(config, dict):
        return {}
    return redact_secrets(config)


def _sanitize_sync_run(row: dict) -> dict:
    sanitized = dict(row)
    sanitized["error"] = redact_secrets(sanitized.get("error"))
    if "payload" in sanitized:
        sanitized["payload"] = redact_secrets(sanitized.get("payload"))
    return sanitized


def _sanitize_event(row: dict) -> dict:
    sanitized = dict(row)
    sanitized["summary"] = redact_secrets(sanitized.get("summary"))
    sanitized["detail"] = redact_secrets(sanitized.get("detail"))
    if "payload" in sanitized:
        sanitized["payload"] = redact_secrets(sanitized.get("payload"))
    return sanitized


def _row_to_detail(store, row: dict) -> dict:
    return {
        **_row_to_summary(store, row),
        "uri": redact_secrets(row.get("uri")),
        "path": row.get("path"),
        "config": _redact_source_config(row.get("config_json")),
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
    detail = _row_to_detail(store, row)
    detail["events"] = [_sanitize_event(event) for event in store.list_events(source_name=name, limit=20)]
    detail["runs"] = [_sanitize_sync_run(run) for run in store.list_sync_runs(source_name=name, limit=10)]
    return detail


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


@router.patch("/sources/{name}")
async def update_source(request: Request, name: str, body: SourceUpdate):
    """Update a registered source without running sync."""
    store = request.app.state.metadata_store
    row = store.get_source(name)
    if not row:
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")

    next_type = body.type or row["type"]
    catalog = {c["id"] for c in list_connector_catalog()}
    if next_type not in catalog:
        raise HTTPException(status_code=400, detail=f"Unknown connector type: {next_type}")
    connector = next((c for c in list_connector_catalog() if c["id"] == next_type), None)
    status = connector_status(next_type)
    if not connector or not connector.get("supported"):
        raise HTTPException(
            status_code=400,
            detail=f"Connector '{next_type}' is {status}, not supported in this build.",
        )

    store.upsert_source(
        name,
        next_type,
        body.path if body.path is not None else row.get("path"),
        body.uri if body.uri is not None else row.get("uri"),
        mode=row.get("mode") or "generate",
    )
    store.upsert_source_meta(
        name,
        display_name=body.display_name if body.display_name is not None else row.get("display_name"),
        host=body.host if body.host is not None else row.get("host"),
        config=body.config if body.config is not None else None,
        status="idle",
        auto_sync=body.auto_sync if body.auto_sync is not None else bool(row.get("auto_sync")),
    )
    SourceSyncService(request).record_event(
        EventType.SOURCE_REGISTERED,
        f"Source '{name}' configuration updated",
        source_name=name,
        invalidates=["sources", "briefing"],
    )
    updated = store.get_source(name)
    return _row_to_detail(store, updated)


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


@router.post("/sources/{name}/preview")
async def preview_source(request: Request, name: str):
    """Preview what a sync would discover without running the full pipeline.

    Connects to the source, applies schema/table filters from the source config,
    lists tables and schemas, estimates row counts where available, and returns
    a summary for the user to confirm before syncing.
    """
    store = request.app.state.metadata_store
    row = store.get_source(name)
    if not row:
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")

    from headwater.connectors.registry import get_connector
    from headwater.connectors.schema_filter import SchemaTableFilter
    from headwater.core.models import SourceConfig
    from headwater.services.source_sync import _source_config

    config = _source_config(row)
    source_type = row["type"]
    uri = row.get("uri")
    path = row.get("path")

    try:
        connector = get_connector(source_type)
        source_cfg = SourceConfig(
            name=name, type=source_type,
            uri=uri, path=path,
        )
        connector.connect(source_cfg)
    except ConnectorError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    try:
        # Apply schema filter from config.
        sf = SchemaTableFilter.from_config(config)
        if hasattr(connector, "set_schema_filter") and not sf.is_empty:
            connector.set_schema_filter(config)

        # List schemas if supported.
        schemas: list[str] = []
        if hasattr(connector, "list_schemas"):
            try:
                schemas = connector.list_schemas()
            except Exception:
                schemas = []

        # List tables.
        all_tables = connector.list_tables()

        # Apply filter for connectors that don't have built-in filter support.
        if not hasattr(connector, "set_schema_filter") and not sf.is_empty:
            all_tables = sf.filter_tables(all_tables)

        max_tables = config.get("max_tables", 50)
        sample_rows = config.get("sample_rows", 10000)
        tables_considered = all_tables[:max_tables]
        tables_skipped = all_tables[max_tables:]

        # Estimate row counts where possible.
        table_details: list[dict] = []
        total_estimated_rows = 0
        has_row_estimates = hasattr(connector, "estimate_row_count")
        for table_name in tables_considered:
            row_estimate = None
            if has_row_estimates:
                try:
                    row_estimate = connector.estimate_row_count(table_name)
                except Exception:
                    row_estimate = None
            if row_estimate is not None:
                total_estimated_rows += row_estimate
            table_details.append({
                "name": table_name,
                "estimated_rows": row_estimate,
            })

        # Estimate total sample data size.
        sample_rows_total = min(sample_rows, max(1, total_estimated_rows)) * len(tables_considered)
        if not has_row_estimates:
            sample_rows_total = sample_rows * len(tables_considered)

        return {
            "source_name": name,
            "source_type": source_type,
            "schemas_found": len(schemas),
            "schemas": schemas[:50],
            "tables_found": len(all_tables),
            "tables_considered": len(tables_considered),
            "tables_skipped": len(tables_skipped),
            "tables_skipped_names": tables_skipped[:20],
            "tables": table_details,
            "total_estimated_rows": total_estimated_rows if has_row_estimates else None,
            "has_row_estimates": has_row_estimates,
            "config": {
                "max_tables": max_tables,
                "sample_rows": sample_rows,
                "schema_filter": sf.describe() if not sf.is_empty else None,
            },
            "sample_rows_per_table": sample_rows,
            "estimated_sample_total": sample_rows_total,
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    finally:
        if hasattr(connector, "close"):
            connector.close()


@router.delete("/sources/{name}")
async def delete_source_route(request: Request, name: str):
    store = request.app.state.metadata_store
    if not store.get_source(name):
        raise HTTPException(status_code=404, detail=f"Source '{name}' not found.")
    store.delete_source(name)
    get_runtime_state(request).clear_for_source(name)
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
