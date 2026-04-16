"""Project API -- CRUD, maturity tracking, and progress dashboard."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()
logger = logging.getLogger(__name__)


def _compute_progress(
    discovery,
    pipeline: dict,
    store,
    project_id: str,
) -> dict:
    """Compute live progress counters for a project."""
    tables = discovery.tables if discovery else []
    profiles = discovery.profiles if discovery else []
    relationships = discovery.relationships if discovery else []
    contracts = pipeline.get("contracts", [])
    staging_models = pipeline.get("staging_models", [])
    mart_models = pipeline.get("mart_models", [])

    tables_discovered = len(tables)
    tables_profiled = sum(1 for _ in profiles) and tables_discovered  # profiled if profiles exist
    tables_reviewed = sum(1 for t in tables if t.review_status == "reviewed")
    tables_modeled = len({m.source_tables[0] for m in staging_models if m.source_tables})
    tables_mart_ready = sum(1 for m in mart_models if m.status == "approved")

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
    metrics = store.get_catalog_metrics(project_id)
    dimensions = store.get_catalog_dimensions(project_id)
    metrics_defined = len(metrics)
    metrics_confirmed = sum(1 for m in metrics if m.get("status") == "confirmed")
    dimensions_defined = len(dimensions)
    dimensions_confirmed = sum(1 for d in dimensions if d.get("status") == "confirmed")

    # Contracts
    quality_contracts = len(contracts)
    contracts_enforcing = sum(1 for c in contracts if c.status == "enforcing")

    # Catalog coverage: % of analytical columns referenced in catalog
    catalog_columns = set()
    for m in metrics:
        if m.get("column_name"):
            catalog_columns.add((m["table_name"], m["column_name"]))
    for d in dimensions:
        catalog_columns.add((d["table_name"], d["column_name"]))
    analytical_columns = max(columns_total, 1)
    catalog_coverage = round(len(catalog_columns) / analytical_columns, 3)

    return {
        "tables_discovered": tables_discovered,
        "tables_profiled": tables_profiled,
        "tables_reviewed": tables_reviewed,
        "tables_modeled": tables_modeled,
        "tables_mart_ready": tables_mart_ready,
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
        "catalog_coverage": catalog_coverage,
    }


def _compute_maturity(progress: dict) -> tuple[str, float]:
    """Derive maturity level and score from progress counters.

    Levels:
        raw        -> Data loaded, not yet profiled
        profiled   -> Schema extracted, stats computed
        documented -> Dictionary reviewed, catalog generated
        modeled    -> Staging + mart models generated and approved
        production -> Quality contracts enforcing
    """
    score = 0.0
    level = "raw"

    total = max(progress["tables_discovered"], 1)

    # Profiled: tables have been discovered and profiled
    if progress["tables_profiled"] > 0:
        level = "profiled"
        score = 0.2

    # Documented: >=60% columns described AND catalog exists
    desc_ratio = progress["columns_described"] / max(progress["columns_total"], 1)
    if desc_ratio >= 0.6 and progress["metrics_defined"] > 0:
        level = "documented"
        score = 0.4 + 0.1 * min(progress["catalog_coverage"] / 0.8, 1.0)

    # Modeled: at least one mart approved
    if progress["tables_mart_ready"] >= 1:
        level = "modeled"
        score = 0.6 + 0.1 * min(progress["tables_mart_ready"] / total, 1.0)

    # Production: at least one contract enforcing
    if progress["contracts_enforcing"] >= 1:
        level = "production"
        score = 0.8 + 0.2 * min(
            progress["contracts_enforcing"] / max(progress["quality_contracts"], 1), 1.0
        )

    return level, round(score, 3)


@router.get("/projects")
async def list_projects(request: Request):
    """List all projects with summary info."""
    store = request.app.state.metadata_store
    try:
        projects = store.list_projects()
    except Exception:
        logger.exception("Failed to list projects from metadata store")
        raise
    logger.info("Listed %d projects", len(projects))
    return {"projects": projects}


@router.get("/projects/{project_id}")
async def get_project(project_id: str, request: Request):
    """Get a single project with full progress and maturity."""
    store = request.app.state.metadata_store
    project = store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    pipeline = request.app.state.pipeline
    discovery = pipeline.get("discovery")

    progress = _compute_progress(discovery, pipeline, store, project_id)
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


@router.get("/projects/{project_id}/progress")
async def get_project_progress(project_id: str, request: Request):
    """Get live progress counters for a project."""
    store = request.app.state.metadata_store
    project = store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    pipeline = request.app.state.pipeline
    discovery = pipeline.get("discovery")

    try:
        progress = _compute_progress(discovery, pipeline, store, project_id)
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
    project = store.get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    metrics = store.get_catalog_metrics(project_id)
    dimensions = store.get_catalog_dimensions(project_id)
    entities = store.get_catalog_entities(project_id)

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
