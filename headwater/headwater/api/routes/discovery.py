"""Discovery API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.catalog import build_catalog
from headwater.analyzer.companion import (
    discover_companion_docs,
    match_docs_to_tables,
)
from headwater.analyzer.eval import evaluate_catalog
from headwater.analyzer.semantic import analyze
from headwater.analyzer.semantic_schema import ambiguous_roles, infer_semantic_schema
from headwater.api.project_scope import scoped_pipeline
from headwater.connectors.registry import get_connector
from headwater.core.events import EventType
from headwater.core.models import DatasetContext, SourceConfig
from headwater.core.runtime_state import get_runtime_state
from headwater.drift.schema import build_snapshot_from_discovery, compare_schemas
from headwater.profiler.engine import discover
from headwater.services.discovery_persistence import (
    persist_catalog_data,
    persist_discovery_data,
    persist_semantic_data,
)
from headwater.services.model_impacts import (
    compute_schema_drift_model_impacts,
    invalidated_model_names,
)
from headwater.services.pipeline_assets import build_graph_and_index

logger = logging.getLogger(__name__)

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

    logger.info(
        "Discovery starting: path=%s, type=%s, name=%s, schema=%s",
        data_path,
        source_type,
        source_name,
        source_schema,
    )

    connector = get_connector(source.type)
    connector.connect(source)
    logger.info("Connector connected, loading to DuckDB schema '%s'", source_schema)
    connector.load_to_duckdb(con, source_schema)
    logger.info("Data loaded to DuckDB")

    discovery = discover(con, source_schema, source)
    logger.info(
        "Discovery complete: %d tables, %d profiles, %d relationships",
        len(discovery.tables),
        len(discovery.profiles),
        len(discovery.relationships),
    )

    # Companion doc discovery
    companion_docs = discover_companion_docs(source)
    if companion_docs:
        table_names = [t.name for t in discovery.tables]
        match_docs_to_tables(companion_docs, table_names)
        discovery.companion_docs = companion_docs
    logger.info("Companion docs: %d found", len(discovery.companion_docs))

    # Semantic analysis (heuristic-only in discovery route)
    analyze(discovery)
    logger.info("Semantic analysis complete")

    store = request.app.state.metadata_store
    store.apply_key_decisions_to_discovery(discovery)
    logger.info("Applied persisted key decisions")

    # Build semantic catalog (heuristic tier 0)
    catalog = build_catalog(discovery)
    logger.info(
        "Catalog built: %d metrics, %d dimensions, %d entities (confidence=%.2f)",
        len(catalog.metrics),
        len(catalog.dimensions),
        len(catalog.entities),
        catalog.confidence,
    )

    # Evaluate catalog quality
    evaluation = evaluate_catalog(catalog, discovery.tables, discovery.profiles)
    logger.info("Catalog evaluation: overall=%.2f", evaluation.confidence)

    # Build graph store (Kuzu) with table nodes and FK edges
    assets = build_graph_and_index(discovery, catalog, source_name)
    catalog_data = assets.summary
    runtime_state = get_runtime_state(request)
    runtime_state["graph_store"] = assets.graph_store
    runtime_state["vector_store"] = assets.vector_store

    # Persist all discovery data to metadata store
    logger.info("Persisting discovery data to metadata store...")
    persist_discovery_data(request.app.state.metadata_store, discovery, source_name)

    # Persist semantic details and companion docs
    persist_semantic_data(request.app.state.metadata_store, discovery, source_name)

    # Persist catalog to metadata store
    persist_catalog_data(request.app.state.metadata_store, catalog, evaluation, source_name)
    logger.info("Persistence complete")

    runtime_state["discovery"] = discovery
    runtime_state["catalog"] = catalog

    return {
        "tables": len(discovery.tables),
        "profiles": len(discovery.profiles),
        "relationships": len(discovery.relationships),
        "domains": discovery.domains,
        "companion_docs": len(discovery.companion_docs),
        "catalog": {
            "metrics": len(catalog.metrics),
            "dimensions": len(catalog.dimensions),
            "entities": len(catalog.entities),
            "confidence": catalog.confidence,
            "evaluation": evaluation.confidence,
        },
        **catalog_data,
    }


@router.get("/tables")
async def list_tables(request: Request, project_id: str | None = None):
    """List discovered tables."""
    discovery = scoped_pipeline(request, project_id)["discovery"]
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
            "has_semantic_detail": t.semantic_detail is not None,
        }
        for t in discovery.tables
    ]


@router.get("/tables/{table_name}")
async def get_table(request: Request, table_name: str, project_id: str | None = None):
    """Get table detail including columns."""
    discovery = scoped_pipeline(request, project_id)["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")
    return table.model_dump()


@router.get("/tables/{table_name}/semantic-detail")
async def get_table_semantic_detail(
    request: Request,
    table_name: str,
    project_id: str | None = None,
):
    """Get deep semantic detail for a table."""
    discovery = scoped_pipeline(request, project_id).get("discovery")
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if not table:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")
    if not table.semantic_detail:
        raise HTTPException(
            status_code=404,
            detail=f"No semantic detail available for '{table_name}'.",
        )
    return table.semantic_detail.model_dump()


@router.get("/tables/{table_name}/profile")
async def get_table_profile(request: Request, table_name: str, project_id: str | None = None):
    """Get column profiles for a table."""
    discovery = scoped_pipeline(request, project_id)["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    profiles = [p for p in discovery.profiles if p.table_name == table_name]
    if not profiles:
        raise HTTPException(status_code=404, detail=f"No profiles for '{table_name}'.")
    return [p.model_dump() for p in profiles]


@router.get("/relationships")
async def list_relationships(request: Request, project_id: str | None = None):
    """List all detected relationships."""
    discovery = scoped_pipeline(request, project_id)["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    return [r.model_dump() for r in discovery.relationships]


class ColumnPatchRequest(BaseModel):
    """Payload for PATCH /api/columns/{source_name}/{table_name}/{column_name}."""

    description: str | None = None
    locked: bool | None = None


class SemanticConfirmRequest(BaseModel):
    """Payload for confirming inferred semantic roles."""

    min_confidence: float = 0.8
    table_name: str | None = None


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
    discovery = get_runtime_state(request).get("discovery")
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
            table_name,
            source_name,
            column_name,
            locked=True,
            description=body.description,
        )
        store.record_decision(
            "column",
            f"{source_name}.{table_name}.{column_name}",
            "locked",
            payload={"description": body.description},
        )
    elif body.locked is False:
        store.lock_column(table_name, source_name, column_name, locked=False)
        store.record_decision(
            "column",
            f"{source_name}.{table_name}.{column_name}",
            "unlocked",
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


@router.get("/dataset-context")
async def get_dataset_context(request: Request, project_id: str | None = None):
    """Return optional dataset framing context for the active source."""
    discovery = scoped_pipeline(request, project_id)["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    store = request.app.state.metadata_store
    row = store.get_dataset_context(discovery.source.name)
    if row:
        return DatasetContext(**row).model_dump(mode="json")
    return DatasetContext(source_name=discovery.source.name).model_dump(mode="json")


@router.put("/dataset-context")
async def put_dataset_context(
    body: DatasetContext,
    request: Request,
    project_id: str | None = None,
):
    """Persist optional dataset framing context for the active source."""
    discovery = scoped_pipeline(request, project_id)["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    source_name = discovery.source.name
    context = body.model_copy(update={"source_name": source_name})
    payload = context.model_dump(mode="json", exclude={"updated_at"})
    store = request.app.state.metadata_store
    store.upsert_dataset_context(source_name, payload)
    store.record_decision("dataset_context", source_name, "updated", payload=payload)
    return store.get_dataset_context(source_name) or context.model_dump(mode="json")


@router.get("/semantic-schema")
async def get_semantic_schema(request: Request, project_id: str | None = None):
    """Return inferred canonical roles and derived fields for the active source."""
    discovery = scoped_pipeline(request, project_id)["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    store = request.app.state.metadata_store
    context_row = store.get_dataset_context(discovery.source.name)
    context = DatasetContext(**context_row) if context_row else None
    schema = infer_semantic_schema(discovery, context)
    return {
        **schema.model_dump(mode="json"),
        "ambiguous_count": len(ambiguous_roles(schema)),
    }


@router.post("/semantic-schema/confirm")
async def confirm_semantic_schema(
    body: SemanticConfirmRequest,
    request: Request,
    project_id: str | None = None,
):
    """Lock high-confidence inferred roles, leaving ambiguous columns for review."""
    pipeline = scoped_pipeline(request, project_id)
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    store = request.app.state.metadata_store
    context_row = store.get_dataset_context(discovery.source.name)
    context = DatasetContext(**context_row) if context_row else None
    schema = infer_semantic_schema(discovery, context)

    confirmed = 0
    updates_by_table: dict[str, list[dict]] = {}
    for role in schema.columns:
        if role.locked or role.confidence < body.min_confidence:
            continue
        if body.table_name and role.table_name != body.table_name:
            continue
        updates_by_table.setdefault(role.table_name, []).append(
            {
                "name": role.column_name,
                "role": _dictionary_role_for_canonical(role.canonical_role),
                "semantic_type": _semantic_type_for_canonical(role.canonical_role),
                "confidence": role.confidence,
            }
        )

    for table_name, updates in updates_by_table.items():
        store.bulk_update_columns(table_name, discovery.source.name, updates, lock=True)
        confirmed += len(updates)
        table = next((t for t in discovery.tables if t.name == table_name), None)
        if table:
            by_col = {update["name"]: update for update in updates}
            for col in table.columns:
                update = by_col.get(col.name)
                if update:
                    col.role = update["role"]
                    col.semantic_type = update["semantic_type"]
                    col.confidence = update["confidence"]
                    col.locked = True

    store.record_decision(
        "semantic_schema",
        discovery.source.name,
        "confirmed",
        payload={"columns_confirmed": confirmed, "min_confidence": body.min_confidence},
    )
    return {"columns_confirmed": confirmed}


def _dictionary_role_for_canonical(role: str) -> str:
    if role.endswith("_ts"):
        return "temporal"
    if role in {"origin_id", "destination_id", "location_id"}:
        return "geographic"
    if role in {"service_type"}:
        return "dimension"
    if role in {"distance", "duration", "amount", "tip_amount", "count", "measure"}:
        return "metric"
    return "dimension"


def _semantic_type_for_canonical(role: str) -> str:
    if role.endswith("_ts"):
        return "temporal"
    if role in {"origin_id", "destination_id", "location_id"}:
        return "geographic"
    if role in {"service_type"}:
        return "dimension"
    if role in {"distance", "duration", "amount", "tip_amount", "count", "measure"}:
        return "metric"
    return "dimension"


def _persist_discovery_data(request: Request, discovery, source_name: str) -> None:
    """Backwards-compatible wrapper around the discovery persistence service."""
    persist_discovery_data(
        getattr(request.app.state, "metadata_store", None),
        discovery,
        source_name,
    )


def _persist_schema_drift(store, discovery, source_name: str, run_id: int) -> None:
    """Persist schema snapshot and drift report for a discovery run.

    The first run establishes a baseline snapshot only. Subsequent runs compare
    against the latest prior snapshot and persist a drift report.
    """
    current_snapshot = build_snapshot_from_discovery(discovery)
    previous_record = store.get_latest_snapshot_record(source_name, before_run_id=run_id)
    store.save_snapshot(run_id, source_name, current_snapshot)

    if previous_record is None:
        logger.info("Schema drift baseline saved for source '%s'", source_name)
        return

    diff = compare_schemas(
        previous_record["snapshot"],
        current_snapshot,
        source_name,
        run_id_from=previous_record["run_id"],
        run_id_to=run_id,
    )
    report_id = store.save_drift_report(
        source_name,
        previous_record["run_id"],
        run_id,
        diff.model_dump(),
    )
    if not diff.no_changes:
        _persist_model_impacts_for_drift(
            store,
            source_name,
            report_id,
            diff.model_dump(),
        )
        try:
            store.insert_event(
                EventType.SCHEMA_DRIFT_DETECTED,
                "Schema drift detected",
                source_name=source_name,
                severity="warning",
                artifact_type="source",
                artifact_id=source_name,
                payload={"report_id": report_id, **diff.model_dump()},
                invalidates=["sources", "briefing", "health", "insights", "models"],
            )
        except Exception:
            logger.exception("Failed to write normalized drift event for '%s'", source_name)


def _persist_model_impacts_for_drift(
    store,
    source_name: str,
    drift_report_id: int,
    diff: dict,
) -> None:
    models = store.get_models(source_name)
    if not models:
        return
    impacts = compute_schema_drift_model_impacts(
        source_name=source_name,
        drift_report_id=drift_report_id,
        diff=diff,
        models=models,
    )
    if not impacts:
        return

    impact_ids = store.save_model_impacts(impacts)
    invalidated = invalidated_model_names(impacts)
    for model_name in invalidated:
        store.update_model_status(model_name, "invalidated")
        try:
            store.insert_event(
                EventType.MODEL_IMPACTED,
                f"Model '{model_name}' impacted by schema drift",
                source_name=source_name,
                severity="warning",
                artifact_type="model",
                artifact_id=model_name,
                payload={
                    "drift_report_id": drift_report_id,
                    "impact_ids": [
                        impact_id
                        for impact_id, impact in zip(impact_ids, impacts, strict=False)
                        if impact["model_name"] == model_name
                    ],
                },
                invalidates=["models", "briefing", "health"],
            )
        except Exception:
            logger.exception("Failed to write model impact event for '%s'", model_name)


def _persist_semantic_data(request: Request, discovery, source_name: str) -> None:
    """Backwards-compatible wrapper around the discovery persistence service."""
    persist_semantic_data(
        getattr(request.app.state, "metadata_store", None),
        discovery,
        source_name,
    )


def _build_graph_and_index(
    request: Request,
    discovery,
    catalog,
    source_name: str,
    evaluation,
) -> dict:
    """Backwards-compatible wrapper around the runtime asset builder."""
    del evaluation
    assets = build_graph_and_index(discovery, catalog, source_name)
    runtime_state = get_runtime_state(request)
    runtime_state["graph_store"] = assets.graph_store
    runtime_state["vector_store"] = assets.vector_store
    return assets.summary


def _persist_catalog_data(request: Request, catalog, evaluation, source_name: str) -> None:
    """Backwards-compatible wrapper around the discovery persistence service."""
    persist_catalog_data(
        getattr(request.app.state, "metadata_store", None),
        catalog,
        evaluation,
        source_name,
    )
