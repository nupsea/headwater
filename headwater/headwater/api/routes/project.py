"""Project API -- CRUD, maturity tracking, and progress dashboard."""

from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from headwater.api.project_scope import (
    catalog_ids_for_project,
    project_sources,
    resolve_project,
    scoped_pipeline,
    visible_projects,
)
from headwater.core.runtime_state import get_runtime_state
from headwater.services.context_import import import_context_exports
from headwater.services.context_projection import build_context_exports

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateProjectRequest(BaseModel):
    display_name: str
    source_path: str | None = None
    description: str | None = None
    sources: list[str] | None = None


class RenameProjectRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None


class UpdateProjectRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    sources: list[str] | None = None


class AddContextResourceRequest(BaseModel):
    resource_type: str
    title: str
    location: str | None = None
    status: str = "active"
    metadata: dict = Field(default_factory=dict)


class UpdateContextItemRequest(BaseModel):
    status: Literal["proposed", "approved", "rejected", "locked", "needs_review"] | None = None
    value: dict | None = None
    name: str | None = None
    title: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    reason: str | None = None


class ContextItemDecisionRequest(BaseModel):
    reason: str | None = None
    value: dict | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)


class ImportProjectContextRequest(BaseModel):
    files: dict[str, str] = Field(default_factory=dict)
    source_name: str | None = None


def _slugify(name: str) -> str:
    """Convert display name to a URL-friendly slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def _source_ids_for_project(project: dict, store) -> list[str]:
    return catalog_ids_for_project(project, store)


def _compute_progress(
    discovery,
    pipeline: dict,
    store,
    project: dict | str,
) -> dict:
    """Compute live progress counters for a project."""
    if isinstance(project, str):
        project = store.get_project(project) or {"id": project, "sources": []}
    project_id = project["id"]
    catalog_ids = _source_ids_for_project(project, store) or [project_id]
    tables = discovery.tables if discovery else []
    profiles = discovery.profiles if discovery else []
    relationships = discovery.relationships if discovery else []
    contracts = pipeline.get("contracts", [])
    staging_models = pipeline.get("staging_models", [])
    mart_models = pipeline.get("mart_models", [])
    execution_results = pipeline.get("execution_results", [])
    source_name = getattr(getattr(discovery, "source", None), "name", None) or project_id

    tables_discovered = len(tables)
    tables_profiled = sum(1 for _ in profiles) and tables_discovered  # profiled if profiles exist
    tables_reviewed = sum(1 for t in tables if t.review_status == "reviewed")
    tables_modeled = len({m.source_tables[0] for m in staging_models if m.source_tables})
    tables_mart_ready = sum(1 for m in mart_models if m.status in {"approved", "executed"})
    mart_models_review_pending = sum(1 for m in mart_models if m.status == "proposed")
    invalidated_models = sum(1 for m in staging_models + mart_models if m.status == "invalidated")
    materialized_models = sum(1 for r in execution_results if getattr(r, "success", False))

    columns_total = sum(len(t.columns) for t in tables)
    columns_described = sum(
        1 for t in tables for c in t.columns if c.description and c.description != c.name
    )
    columns_confirmed = sum(1 for t in tables for c in t.columns if c.locked)

    relationships_detected = len(relationships)
    relationships_confirmed = sum(
        1 for r in relationships if r.source == "declared" or r.confidence >= 0.95
    )

    # Catalog counts from metadata store
    metrics = [
        metric
        for catalog_id in catalog_ids
        for metric in store.get_catalog_metrics(catalog_id)
    ]
    dimensions = [
        dimension
        for catalog_id in catalog_ids
        for dimension in store.get_catalog_dimensions(catalog_id)
    ]
    table_names = set(pipeline.get("table_names") or [])
    if table_names:
        metrics = [metric for metric in metrics if metric.get("table_name") in table_names]
        dimensions = [
            dimension for dimension in dimensions if dimension.get("table_name") in table_names
        ]
    metrics_defined = len(metrics)
    metrics_confirmed = sum(1 for m in metrics if m.get("status") == "confirmed")
    dimensions_defined = len(dimensions)
    dimensions_confirmed = sum(1 for d in dimensions if d.get("status") == "confirmed")

    # Contracts
    quality_contracts = len(contracts)
    contracts_enforcing = sum(1 for c in contracts if c.status in {"enforced", "enforcing"})
    contracts_observing = sum(1 for c in contracts if c.status == "observing")
    contracts_failing = sum(1 for c in contracts if c.status == "failing")
    contracts_recovered = sum(1 for c in contracts if c.status == "recovered")
    latest_quality = pipeline.get("quality_report")
    if latest_quality:
        quality_failed = int(getattr(latest_quality, "failed", 0) or 0)
        total_contracts = int(getattr(latest_quality, "total_contracts", 0) or 0)
        passed_contracts = int(getattr(latest_quality, "passed", 0) or 0)
        quality_score = (passed_contracts / total_contracts) * 100 if total_contracts else 100.0
    else:
        latest_quality_row = store.get_latest_quality_report(source_name)
        quality_failed = int((latest_quality_row or {}).get("failed") or 0)
        quality_score = float((latest_quality_row or {}).get("score") or 100.0)
    source_row = store.get_source(source_name)
    source_drift_count = int((source_row or {}).get("drift_count") or 0)
    persisted_impacts = store.list_model_impacts(source_name=source_name, limit=200)
    impacted_models = len({impact["model_name"] for impact in persisted_impacts})

    # Catalog coverage: % of analytical columns referenced in catalog
    catalog_columns = set()
    for m in metrics:
        if m.get("column_name"):
            catalog_columns.add((m["table_name"], m["column_name"]))
    for d in dimensions:
        catalog_columns.add((d["table_name"], d["column_name"]))
    analytical_columns = max(columns_total, 1)
    catalog_coverage = round(len(catalog_columns) / analytical_columns, 3)

    progress = {
        "tables_discovered": tables_discovered,
        "tables_profiled": tables_profiled,
        "tables_reviewed": tables_reviewed,
        "tables_modeled": tables_modeled,
        "tables_mart_ready": tables_mart_ready,
        "mart_models_review_pending": mart_models_review_pending,
        "materialized_models": materialized_models,
        "invalidated_models": invalidated_models,
        "impacted_models": impacted_models,
        "columns_total": columns_total,
        "columns_described": columns_described,
        "columns_confirmed": columns_confirmed,
        "relationships_detected": relationships_detected,
        "relationships_confirmed": relationships_confirmed,
        "metrics_defined": metrics_defined,
        "metrics_confirmed": metrics_confirmed,
        "dimensions_defined": dimensions_defined,
        "dimensions_confirmed": dimensions_confirmed,
        "quality_contracts": quality_contracts,
        "contracts_enforcing": contracts_enforcing,
        "contracts_observing": contracts_observing,
        "contracts_failing": contracts_failing,
        "contracts_recovered": contracts_recovered,
        "quality_failed": quality_failed,
        "quality_score": quality_score,
        "source_drift_count": source_drift_count,
        "catalog_coverage": catalog_coverage,
    }
    progress["maturity_blockers"] = _maturity_blockers(progress)
    progress["next_actions"] = _next_actions(progress)
    return progress


def _project_context_payload(project: dict, store) -> dict:
    ids = _context_ids_for_project(project, store)

    dataset_contexts = []
    items: list[dict] = []
    resources: list[dict] = []
    seen_item_ids: set[str] = set()
    seen_resource_ids: set[str] = set()
    source_names = []

    for source_name in project_sources(project, store):
        source_names.append(source_name)
        context = store.get_dataset_context(source_name)
        if context:
            dataset_contexts.append(context)

    for context_id in ids:
        for item in store.list_project_context_items(context_id):
            if item["id"] in seen_item_ids:
                continue
            seen_item_ids.add(item["id"])
            items.append(item)
        for resource in store.list_project_context_resources(context_id):
            if resource["id"] in seen_resource_ids:
                continue
            seen_resource_ids.add(resource["id"])
            resources.append(resource)

    item_types: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for item in items:
        item_types[item["item_type"]] = item_types.get(item["item_type"], 0) + 1
        status = item.get("status") or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1

    return {
        "project_id": project["id"],
        "source_names": source_names,
        "dataset_contexts": dataset_contexts,
        "items": items,
        "resources": resources,
        "summary": {
            "item_count": len(items),
            "resource_count": len(resources),
            "dataset_context_count": len(dataset_contexts),
            "item_types": item_types,
            "status_counts": status_counts,
        },
    }


def _context_ids_for_project(project: dict, store) -> list[str]:
    ids = _source_ids_for_project(project, store)
    if project["id"] not in ids:
        ids.insert(0, project["id"])
    return ids


def _find_project_context_item(project: dict, store, item_id: str) -> dict | None:
    for context_id in _context_ids_for_project(project, store):
        item = store.get_project_context_item(item_id, project_id=context_id)
        if item is not None:
            return item
    return None


def _update_context_item(
    project: dict,
    store,
    item_id: str,
    *,
    status: str | None = None,
    value: dict | None = None,
    name: str | None = None,
    title: str | None = None,
    confidence: float | None = None,
    reason: str | None = None,
) -> dict:
    item = _find_project_context_item(project, store, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Context item '{item_id}' not found.")
    updated = store.update_project_context_item(
        item_id,
        project_id=item["project_id"],
        status=status if status is not None else item.get("status"),
        value=value if value is not None else item.get("value"),
        name=name if name is not None else item.get("name"),
        title=title if title is not None else item.get("title"),
        confidence=confidence if confidence is not None else item.get("confidence"),
        source="user",
    )
    assert updated is not None
    store.record_decision(
        "project_context_item",
        item_id,
        updated["status"],
        reason=reason,
        payload={
            "before": item,
            "after": updated,
        },
    )
    store.log_activity(
        "project_context_item_reviewed",
        f"Updated context item '{item_id}' to {updated['status']}",
        artifact_type="project_context_item",
        artifact_id=item_id,
    )
    return updated


def _compute_maturity(progress: dict) -> tuple[str, float]:
    """Derive maturity level and score from progress counters."""
    tables = max(progress["tables_discovered"], 1)
    columns = max(progress["columns_total"], 1)
    quality_contracts = max(progress["quality_contracts"], 1)

    profile_score = _ratio(progress["tables_profiled"], tables)
    documentation_score = (
        _ratio(progress["columns_confirmed"], columns) * 0.6
        + _ratio(progress["tables_reviewed"], tables) * 0.2
        + min(progress["catalog_coverage"] / 0.8, 1.0) * 0.2
    )
    model_score = (
        _ratio(progress["tables_modeled"], tables) * 0.3
        + _ratio(
            progress["tables_mart_ready"],
            max(
                progress["tables_mart_ready"] + progress["mart_models_review_pending"],
                1,
            ),
        )
        * 0.4
        + _ratio(progress["materialized_models"], max(progress["tables_modeled"], 1)) * 0.3
    )
    quality_score = (
        _ratio(progress["contracts_enforcing"] + progress["contracts_recovered"], quality_contracts)
        * 0.5
        + (float(progress.get("quality_score") or 100.0) / 100.0) * 0.5
    )
    drift_score = 0.0 if progress["source_drift_count"] else 1.0

    score = (
        profile_score * 0.2
        + documentation_score * 0.2
        + model_score * 0.25
        + quality_score * 0.25
        + drift_score * 0.1
    )
    if (
        progress["invalidated_models"]
        or progress["contracts_failing"]
        or progress["quality_failed"]
    ):
        score -= 0.15
    if progress["source_drift_count"]:
        score -= 0.1
    score = max(0.0, min(score, 1.0))

    if score >= 0.8 and not _critical_blockers(progress):
        level = "production"
    elif score >= 0.6 and progress["tables_mart_ready"]:
        level = "modeled"
    elif score >= 0.4 and progress["columns_described"]:
        level = "documented"
    elif score >= 0.2 and progress["tables_profiled"]:
        level = "profiled"
    else:
        level = "raw"
    return level, round(score, 3)


def _ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return max(0.0, min(float(numerator) / float(denominator), 1.0))


def _critical_blockers(progress: dict) -> list[dict]:
    return [
        blocker
        for blocker in _maturity_blockers(progress)
        if blocker["severity"] == "critical"
    ]


def _maturity_blockers(progress: dict) -> list[dict]:
    blockers = []
    if progress["source_drift_count"]:
        blockers.append(
            {
                "title": "Schema drift needs review",
                "detail": f"{progress['source_drift_count']} unresolved drift event(s)",
                "severity": "critical",
                "route": "/models",
            }
        )
    if progress["invalidated_models"] or progress["impacted_models"]:
        affected = max(progress["invalidated_models"], progress["impacted_models"])
        blockers.append(
            {
                "title": "Models impacted by source changes",
                "detail": f"{affected} model(s) affected",
                "severity": "critical",
                "route": "/models",
            }
        )
    if progress["quality_failed"] or progress["contracts_failing"]:
        failing = max(progress["quality_failed"], progress["contracts_failing"])
        blockers.append(
            {
                "title": "Quality contracts failing",
                "detail": f"{failing} failing check(s)",
                "severity": "critical",
                "route": "/quality",
            }
        )
    if progress["mart_models_review_pending"]:
        blockers.append(
            {
                "title": "Mart models awaiting review",
                "detail": f"{progress['mart_models_review_pending']} proposed mart model(s)",
                "severity": "warning",
                "route": "/models",
            }
        )
    if progress["columns_confirmed"] < progress["columns_total"]:
        confirmed = progress["columns_confirmed"]
        total = progress["columns_total"]
        blockers.append(
            {
                "title": "Column descriptions need confirmation",
                "detail": f"{confirmed} of {total} confirmed",
                "severity": "warning",
                "route": "/discovery",
            }
        )
    if progress["contracts_enforcing"] == 0 and progress["quality_contracts"]:
        blockers.append(
            {
                "title": "Contracts are not enforced",
                "detail": f"{progress['quality_contracts']} contract(s) still below enforcement",
                "severity": "info",
                "route": "/quality",
            }
        )
    return blockers[:5]


def _next_actions(progress: dict) -> list[dict]:
    return [
        {
            "title": blocker["title"],
            "priority": blocker["severity"],
            "route": blocker["route"],
        }
        for blocker in _maturity_blockers(progress)[:3]
    ]


@router.post("/projects", status_code=201)
async def create_project(body: CreateProjectRequest, request: Request):
    """Create a new project."""
    store = request.app.state.metadata_store
    id_ = str(uuid.uuid4())
    slug = _slugify(body.display_name)
    store.upsert_project(
        id_,
        slug,
        body.display_name,
        description=body.description or "",
        sources_json=json.dumps(body.sources or []),
    )
    store.log_activity(
        "project_created",
        f"Created project '{body.display_name}'",
        artifact_type="project",
        artifact_id=id_,
    )
    project = store.get_project(id_)
    logger.info("Created project %s (%s)", id_, body.display_name)
    return project


@router.get("/projects")
async def list_projects(request: Request):
    """List all projects with summary info."""
    store = request.app.state.metadata_store
    try:
        project_rows = store.list_projects()
        source_rows = store.list_sources()
        projects = [
            _hydrate_project(project, store, sources=source_rows)
            for project in visible_projects(store, projects=project_rows, sources=source_rows)
        ]
    except Exception:
        logger.exception("Failed to list projects from metadata store")
        raise
    logger.info("Listed %d projects", len(projects))
    return {"projects": projects}


@router.get("/projects/{project_id}")
async def get_project(project_id: str, request: Request):
    """Get a single project with full progress and maturity."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    project = _hydrate_project(project, store)

    pipeline = scoped_pipeline(request, project_id)
    discovery = pipeline.get("discovery")

    progress = _compute_progress(discovery, pipeline, store, project)
    maturity, maturity_score = _compute_maturity(progress)

    # Update maturity if changed
    maturity_changed = maturity != project.get("maturity")
    score_changed = abs(maturity_score - project.get("maturity_score", 0)) > 0.01
    if maturity_changed or score_changed:
        store.update_project_maturity(project_id, maturity, maturity_score)
        project["maturity"] = maturity
        project["maturity_score"] = maturity_score

    return {
        **project,
        "progress": progress,
        "maturity": maturity,
        "maturity_score": maturity_score,
    }


def _hydrate_project(project: dict, store, *, sources: list[dict] | None = None) -> dict:
    hydrated = dict(project)
    hydrated["sources"] = project_sources(project, store, sources=sources)
    return hydrated


def _persist_project_update(
    store,
    project_id: str,
    project: dict,
    body: UpdateProjectRequest,
) -> dict:
    display_name = body.display_name or project["display_name"]
    description = (
        body.description
        if body.description is not None
        else project.get("description", "")
    )
    sources = body.sources if body.sources is not None else project.get("sources", [])
    store.upsert_project(
        project_id,
        _slugify(display_name),
        display_name,
        description=description,
        sources_json=json.dumps(sources),
        maturity=project.get("maturity", "raw"),
        maturity_score=project.get("maturity_score", 0.0),
        catalog_confidence=project.get("catalog_confidence", 0.0),
    )
    return store.get_project(project_id)


@router.get("/projects/{project_id}/progress")
async def get_project_progress(project_id: str, request: Request):
    """Get live progress counters for a project."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    pipeline = scoped_pipeline(request, project_id)
    discovery = pipeline.get("discovery")

    try:
        progress = _compute_progress(discovery, pipeline, store, project)
        maturity, maturity_score = _compute_maturity(progress)
    except Exception:
        logger.exception("Failed to compute progress for project '%s'", project_id)
        raise

    return {
        "project_id": project_id,
        "progress": progress,
        "maturity": maturity,
        "maturity_score": maturity_score,
    }


@router.get("/projects/{project_id}/catalog")
async def get_project_catalog(project_id: str, request: Request):
    """Get the semantic catalog (metrics, dimensions, entities) for a project."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    catalog_ids = _source_ids_for_project(project, store) or [project_id]
    pipeline = scoped_pipeline(request, project_id)
    table_names = set(pipeline.get("table_names") or [])
    metrics = [
        metric
        for catalog_id in catalog_ids
        for metric in store.get_catalog_metrics(catalog_id)
    ]
    dimensions = [
        dimension
        for catalog_id in catalog_ids
        for dimension in store.get_catalog_dimensions(catalog_id)
    ]
    entities = [
        entity
        for catalog_id in catalog_ids
        for entity in store.get_catalog_entities(catalog_id)
    ]
    if table_names:
        metrics = [metric for metric in metrics if metric.get("table_name") in table_names]
        dimensions = [
            dimension for dimension in dimensions if dimension.get("table_name") in table_names
        ]
        entities = [entity for entity in entities if entity.get("table_name") in table_names]

    return {
        "project_id": project_id,
        "metrics": metrics,
        "dimensions": dimensions,
        "entities": entities,
        "catalog_confidence": project.get("catalog_confidence", 0.0),
    }


@router.delete("/projects/{project_id}")
async def delete_project(project_id: str, request: Request):
    """Delete a project and its catalog data."""
    store = request.app.state.metadata_store
    project = store.get_project(project_id)
    if project:
        store.clear_catalog(project_id)
        deleted = store.delete_project(project_id)
        if not deleted:
            raise HTTPException(status_code=500, detail="Failed to delete project.")

        logger.info("Deleted project %s (%s)", project_id, project.get("display_name"))
        return {"deleted": project_id}

    source = store.get_source(project_id)
    if not source:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    deleted = store.delete_source(project_id)
    if not deleted:
        raise HTTPException(status_code=500, detail="Failed to delete source-backed project.")
    get_runtime_state(request).clear_for_source(project_id)
    logger.info("Deleted source-backed project %s (%s)", project_id, source.get("display_name"))
    return {"deleted": project_id}


@router.patch("/projects/{project_id}/rename")
async def rename_project(project_id: str, body: RenameProjectRequest, request: Request):
    """Rename or update description of a project."""
    store = request.app.state.metadata_store
    project = store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    updated = _persist_project_update(
        store,
        project_id,
        project,
        UpdateProjectRequest(
            display_name=body.display_name,
            description=body.description,
        ),
    )
    store.log_activity(
        "project_renamed",
        f"Renamed project to '{updated['display_name']}'",
        artifact_type="project",
        artifact_id=project_id,
    )
    logger.info("Renamed project %s to '%s'", project_id, updated["display_name"])
    return updated


@router.patch("/projects/{project_id}")
async def update_project(project_id: str, body: UpdateProjectRequest, request: Request):
    """Update project metadata and linked sources."""
    store = request.app.state.metadata_store
    project = store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    updated = _persist_project_update(store, project_id, project, body)
    store.log_activity(
        "project_updated",
        f"Updated project '{updated['display_name']}'",
        artifact_type="project",
        artifact_id=project_id,
    )
    logger.info("Updated project %s", project_id)
    return updated


@router.get("/projects/{project_id}/context")
async def get_project_context(project_id: str, request: Request):
    """Return reviewable and machine-usable project context."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    return _project_context_payload(project, store)


@router.patch("/projects/{project_id}/context/items/{item_id}")
async def update_project_context_item(
    project_id: str,
    item_id: str,
    body: UpdateContextItemRequest,
    request: Request,
):
    """Update or review a canonical context item."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    if (
        body.status is None
        and body.value is None
        and body.name is None
        and body.title is None
        and body.confidence is None
    ):
        raise HTTPException(status_code=400, detail="No context item changes were provided.")
    return _update_context_item(
        project,
        store,
        item_id,
        status=body.status,
        value=body.value,
        name=body.name,
        title=body.title,
        confidence=body.confidence,
        reason=body.reason,
    )


@router.post("/projects/{project_id}/context/items/{item_id}/approve")
async def approve_project_context_item(
    project_id: str,
    item_id: str,
    request: Request,
    body: ContextItemDecisionRequest | None = None,
):
    """Approve a canonical context item."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    body = body or ContextItemDecisionRequest()
    return _update_context_item(
        project,
        store,
        item_id,
        status="approved",
        value=body.value,
        confidence=body.confidence,
        reason=body.reason,
    )


@router.post("/projects/{project_id}/context/items/{item_id}/reject")
async def reject_project_context_item(
    project_id: str,
    item_id: str,
    request: Request,
    body: ContextItemDecisionRequest | None = None,
):
    """Reject a canonical context item."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    body = body or ContextItemDecisionRequest()
    return _update_context_item(
        project,
        store,
        item_id,
        status="rejected",
        value=body.value,
        confidence=body.confidence,
        reason=body.reason,
    )


@router.post("/projects/{project_id}/context/items/{item_id}/lock")
async def lock_project_context_item(
    project_id: str,
    item_id: str,
    request: Request,
    body: ContextItemDecisionRequest | None = None,
):
    """Lock a canonical context item."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    body = body or ContextItemDecisionRequest()
    return _update_context_item(
        project,
        store,
        item_id,
        status="locked",
        value=body.value,
        confidence=body.confidence,
        reason=body.reason,
    )


@router.get("/projects/{project_id}/context/export")
async def export_project_context(
    project_id: str,
    request: Request,
    include_proposed: bool = Query(default=True),
):
    """Export canonical context as machine and review projections."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    payload = _project_context_payload(project, store)
    return {
        "project_id": project["id"],
        "include_proposed": include_proposed,
        "files": build_context_exports(payload, include_proposed=include_proposed),
    }


@router.post("/projects/{project_id}/context/import")
async def import_project_context(
    project_id: str,
    body: ImportProjectContextRequest,
    request: Request,
):
    """Import machine-reviewable context files back into canonical state."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    if not body.files:
        raise HTTPException(status_code=400, detail="No context files were provided.")
    result = import_context_exports(
        store,
        project,
        files=body.files,
        source_name=body.source_name,
    )
    store.record_decision(
        "project_context",
        project["id"],
        "imported",
        payload=result,
    )
    store.log_activity(
        "project_context_imported",
        f"Imported context files for project '{project['id']}'",
        artifact_type="project",
        artifact_id=project["id"],
    )
    return {
        **result,
        "context": _project_context_payload(project, store),
    }


@router.post("/projects/{project_id}/context/resources")
async def add_project_context_resource(
    project_id: str,
    body: AddContextResourceRequest,
    request: Request,
):
    """Attach an external context resource to a project."""
    store = request.app.state.metadata_store
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    source_names = project_sources(project, store) or [project_id]
    resource = {
        "id": f"resource:{project_id}:{uuid.uuid4().hex}",
        "project_id": project["id"],
        "source_name": source_names[0] if source_names else None,
        "resource_type": body.resource_type,
        "title": body.title,
        "location": body.location,
        "status": body.status,
        "source": "user",
        "metadata": body.metadata,
    }
    store.upsert_project_context_resource(**resource)
    store.record_decision(
        "project_context_resource",
        resource["id"],
        "added",
        payload=resource,
    )
    return resource


@router.get("/activity")
async def get_activity(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
):
    """Return recent activity feed."""
    store = request.app.state.metadata_store
    activities = store.get_activity(limit=limit)
    return {"activities": activities}
