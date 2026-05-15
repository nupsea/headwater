"""Project API -- CRUD, maturity tracking, and progress dashboard."""

from __future__ import annotations

import json
import logging
import re
import uuid

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from headwater.api.project_scope import (
    catalog_ids_for_project,
    project_sources,
    resolve_project,
    scoped_pipeline,
    visible_projects,
)

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
        projects = [_hydrate_project(project, store) for project in visible_projects(store)]
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


def _hydrate_project(project: dict, store) -> dict:
    hydrated = dict(project)
    hydrated["sources"] = project_sources(project, store)
    return hydrated


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
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    store.clear_catalog(project_id)
    deleted = store.delete_project(project_id)
    if not deleted:
        raise HTTPException(status_code=500, detail="Failed to delete project.")

    logger.info("Deleted project %s (%s)", project_id, project.get("display_name"))
    return {"deleted": project_id}


@router.patch("/projects/{project_id}/rename")
async def rename_project(project_id: str, body: RenameProjectRequest, request: Request):
    """Rename or update description of a project."""
    store = request.app.state.metadata_store
    project = store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    display_name = body.display_name or project["display_name"]
    description = body.description if body.description is not None else project.get("description")
    slug = _slugify(display_name)

    store.upsert_project(
        project_id,
        slug,
        display_name,
        description=description,
    )
    store.log_activity(
        "project_renamed",
        f"Renamed project to '{display_name}'",
        artifact_type="project",
        artifact_id=project_id,
    )
    updated = store.get_project(project_id)
    logger.info("Renamed project %s to '%s'", project_id, display_name)
    return updated


@router.get("/activity")
async def get_activity(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
):
    """Return recent activity feed."""
    store = request.app.state.metadata_store
    activities = store.get_activity(limit=limit)
    return {"activities": activities}
