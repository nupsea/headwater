"""Discovery API routes."""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.catalog import build_catalog, index_catalog
from headwater.analyzer.companion import (
    discover_companion_docs,
    match_docs_to_tables,
)
from headwater.analyzer.eval import evaluate_catalog
from headwater.analyzer.semantic import analyze
from headwater.analyzer.semantic_schema import ambiguous_roles, infer_semantic_schema
from headwater.api.project_scope import project_for_source, scoped_pipeline
from headwater.connectors.registry import get_connector
from headwater.core.events import EventType
from headwater.core.models import DatasetContext, SourceConfig
from headwater.core.redaction import redact_secrets
from headwater.drift.schema import build_snapshot_from_discovery, compare_schemas
from headwater.profiler.engine import discover
from headwater.services.model_impacts import (
    compute_schema_drift_model_impacts,
    invalidated_model_names,
)

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
    catalog_data = _build_graph_and_index(
        request,
        discovery,
        catalog,
        source_name,
        evaluation,
    )

    # Persist all discovery data to metadata store
    logger.info("Persisting discovery data to metadata store...")
    _persist_discovery_data(request, discovery, source_name)

    # Persist semantic details and companion docs
    _persist_semantic_data(request, discovery, source_name)

    # Persist catalog to metadata store
    _persist_catalog_data(request, catalog, evaluation, source_name)
    logger.info("Persistence complete")

    request.app.state.pipeline["discovery"] = discovery
    request.app.state.pipeline["catalog"] = catalog

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
    """Persist tables, columns, profiles, and relationships to the metadata store."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        logger.warning("_persist_discovery_data: no metadata store available, skipping")
        return

    source = discovery.source
    logger.info(
        "Persisting source: name=%s, type=%s, path=%s, uri=%s, mode=%s",
        source_name,
        source.type,
        redact_secrets(source.path),
        redact_secrets(source.uri),
        source.mode,
    )
    store.upsert_source(source_name, source.type, source.path, source.uri, mode=source.mode)
    run_id = store.start_run(source_name)
    logger.info("Started discovery run_id=%d", run_id)

    total_cols = 0
    for table in discovery.tables:
        store.upsert_table(
            table.name,
            source_name,
            schema_name=table.schema_name,
            row_count=table.row_count,
            description=table.description,
            domain=table.domain,
            tags=table.tags,
            run_id=run_id,
        )
        for i, col in enumerate(table.columns):
            store.upsert_column(
                table.name,
                source_name,
                col.name,
                col.dtype,
                nullable=col.nullable,
                is_primary_key=col.is_primary_key,
                description=col.description,
                semantic_type=col.semantic_type,
                role=col.role,
                confidence=col.confidence,
                ordinal=i,
            )
            total_cols += 1
    logger.info("Persisted %d tables, %d columns", len(discovery.tables), total_cols)

    for profile in discovery.profiles:
        stats = profile.model_dump(
            exclude={"table_name", "column_name", "dtype"},
        )
        store.upsert_profile(
            profile.table_name,
            profile.column_name,
            source_name,
            profile.dtype,
            stats,
            run_id=run_id,
        )
    logger.info("Persisted %d profiles", len(discovery.profiles))

    # Clear old relationships before re-inserting to avoid duplicates
    cleared = store.clear_relationships(source_name)
    logger.info("Cleared %d old relationships", cleared)
    for rel in discovery.relationships:
        rel_id = store.insert_relationship(
            source_name,
            rel.from_table,
            rel.from_column,
            rel.to_table,
            rel.to_column,
            rel.type,
            rel.confidence,
            rel.referential_integrity,
            rel.source,
            run_id=run_id,
        )
        rel.id = rel_id
    logger.info("Persisted %d relationships", len(discovery.relationships))

    removed = store.mark_removed_tables(
        source_name,
        [t.name for t in discovery.tables],
        run_id,
    )
    if removed:
        logger.info("Marked %d tables as removed: %s", len(removed), removed)
    _persist_schema_drift(store, discovery, source_name, run_id)
    store.finish_run(run_id, table_count=len(discovery.tables))
    logger.info("Discovery run_id=%d finished", run_id)


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
    """Persist semantic details and companion docs to metadata store."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        return

    for table in discovery.tables:
        if table.semantic_detail:
            store.upsert_semantic_detail(
                table.name,
                source_name,
                table.semantic_detail.model_dump(),
            )

    for doc in discovery.companion_docs:
        store.upsert_companion_doc(
            source_name=source_name,
            filename=doc.filename,
            content=doc.content,
            doc_type=doc.doc_type,
            matched_tables=doc.matched_tables,
            confidence=doc.confidence,
        )


def _build_graph_and_index(
    request: Request,
    discovery,
    catalog,
    source_name: str,
    evaluation,
) -> dict:
    """Build Kuzu graph store and LanceDB index. Returns summary dict."""
    result: dict = {"graph": {}, "vector_index": 0}

    # Graph store: load tables + relationships
    try:
        from headwater.core.config import get_settings
        from headwater.core.graph_store import GraphStore

        settings = get_settings()
        graph = GraphStore(settings.graph_store_path)
        graph.clear()  # Fresh graph for each discovery run

        table_dicts = [
            {
                "name": t.name,
                "row_count": t.row_count,
                "domain": t.domain or "",
                "description": t.description or "",
            }
            for t in discovery.tables
        ]
        node_count = graph.load_tables(table_dicts)

        rel_dicts = [
            {
                "from_table": r.from_table,
                "from_column": r.from_column,
                "to_table": r.to_table,
                "to_column": r.to_column,
                "rel_type": r.type,
                "confidence": r.confidence,
                "ref_integrity": r.referential_integrity,
            }
            for r in discovery.relationships
        ]
        edge_count = graph.load_relationships(rel_dicts)

        # Run pattern discovery
        conformed = graph.find_conformed_dimensions()
        stars = graph.find_star_schemas()
        chains = graph.find_chains()
        nullable_warnings = graph.find_nullable_fk_warnings()

        result["graph"] = {
            "nodes": node_count,
            "edges": edge_count,
            "conformed_dimensions": len(conformed),
            "star_schemas": len(stars),
            "chains": len(chains),
            "nullable_fk_warnings": len(nullable_warnings),
        }

        request.app.state.pipeline["graph_store"] = graph
        logger.info(
            "Graph built: %d nodes, %d edges, %d conformed dims, %d stars",
            node_count,
            edge_count,
            len(conformed),
            len(stars),
        )
    except Exception:
        logger.exception("Failed to build graph store")

    # LanceDB vector index
    try:
        from headwater.core.vector_store import VectorStore

        settings = get_settings()
        vs = VectorStore(settings.vector_store_path)
        indexed = index_catalog(catalog, source_name, vs)
        result["vector_index"] = indexed
        request.app.state.pipeline["vector_store"] = vs
        logger.info("Indexed %d catalog entries in LanceDB", indexed)
    except Exception:
        logger.exception("Failed to build vector index")

    return result


def _persist_catalog_data(request: Request, catalog, evaluation, source_name: str) -> None:
    """Persist catalog metrics, dimensions, and entities to metadata store."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        return

    linked_project = project_for_source(store, source_name)
    project_id = linked_project["id"] if linked_project else source_name

    # Ensure project record exists for direct source runs. Source syncs launched
    # from a real project persist catalog rows under that project id.
    if linked_project:
        store.upsert_project(
            id_=project_id,
            slug=linked_project["slug"],
            display_name=linked_project["display_name"],
            description=linked_project.get("description", ""),
            sources_json=json.dumps(linked_project.get("sources") or []),
            maturity="profiled",
            catalog_confidence=evaluation.confidence,
        )
    else:
        store.upsert_project(
            id_=project_id,
            slug=project_id,
            display_name=project_id,
            maturity="profiled",
            catalog_confidence=evaluation.confidence,
        )

    # Clear previous catalog for this project before re-inserting
    store.clear_catalog(project_id)

    for m in catalog.metrics:
        store.upsert_catalog_metric(
            project_id=project_id,
            name=m.name,
            display_name=m.display_name,
            description=m.description,
            expression=m.expression,
            table_name=m.table,
            agg_type=m.agg_type,
            column_name=m.column,
            synonyms=m.synonyms,
            confidence=m.confidence,
            status=m.status,
            source=m.source,
        )

    for d in catalog.dimensions:
        store.upsert_catalog_dimension(
            project_id=project_id,
            name=d.name,
            display_name=d.display_name,
            description=d.description,
            column_name=d.column,
            table_name=d.table,
            dtype=d.dtype,
            synonyms=d.synonyms,
            sample_values=d.sample_values,
            cardinality=d.cardinality,
            confidence=d.confidence,
            status=d.status,
            source=d.source,
            join_path=d.join_path,
            join_nullable=d.join_nullable,
        )

    for e in catalog.entities:
        store.upsert_catalog_entity(
            project_id=project_id,
            name=e.name,
            display_name=e.display_name,
            description=e.description,
            table_name=e.table,
            row_semantics=e.row_semantics,
            metrics=e.metrics,
            dimensions=e.dimensions,
            temporal_grain=e.temporal_grain,
        )

    logger.info(
        "Persisted catalog: %d metrics, %d dimensions, %d entities",
        len(catalog.metrics),
        len(catalog.dimensions),
        len(catalog.entities),
    )
