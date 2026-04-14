"""Discovery API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.companion import (
    discover_companion_docs,
    match_docs_to_tables,
)
from headwater.analyzer.semantic import analyze
from headwater.connectors.registry import get_connector
from headwater.core.models import SourceConfig
from headwater.profiler.engine import discover

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

    logger.info("Discovery starting: path=%s, type=%s, name=%s, schema=%s",
                data_path, source_type, source_name, source_schema)

    connector = get_connector(source.type)
    connector.connect(source)
    logger.info("Connector connected, loading to DuckDB schema '%s'", source_schema)
    connector.load_to_duckdb(con, source_schema)
    logger.info("Data loaded to DuckDB")

    discovery = discover(con, source_schema, source)
    logger.info("Discovery complete: %d tables, %d profiles, %d relationships",
                len(discovery.tables), len(discovery.profiles),
                len(discovery.relationships))

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

    # Persist all discovery data to metadata store
    logger.info("Persisting discovery data to metadata store...")
    _persist_discovery_data(request, discovery, source_name)

    # Persist semantic details and companion docs
    _persist_semantic_data(request, discovery, source_name)
    logger.info("Persistence complete")

    request.app.state.pipeline["discovery"] = discovery

    return {
        "tables": len(discovery.tables),
        "profiles": len(discovery.profiles),
        "relationships": len(discovery.relationships),
        "domains": discovery.domains,
        "companion_docs": len(discovery.companion_docs),
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
            "has_semantic_detail": t.semantic_detail is not None,
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


@router.get("/tables/{table_name}/semantic-detail")
async def get_table_semantic_detail(request: Request, table_name: str):
    """Get deep semantic detail for a table."""
    discovery = request.app.state.pipeline.get("discovery")
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


def _persist_discovery_data(request: Request, discovery, source_name: str) -> None:
    """Persist tables, columns, profiles, and relationships to the metadata store."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        logger.warning("_persist_discovery_data: no metadata store available, skipping")
        return

    source = discovery.source
    logger.info("Persisting source: name=%s, type=%s, path=%s, uri=%s, mode=%s",
                source_name, source.type, source.path, source.uri, source.mode)
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
            profile.table_name, profile.column_name, source_name,
            profile.dtype, stats, run_id=run_id,
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
        source_name, [t.name for t in discovery.tables], run_id,
    )
    if removed:
        logger.info("Marked %d tables as removed: %s", len(removed), removed)
    store.finish_run(run_id, table_count=len(discovery.tables))
    logger.info("Discovery run_id=%d finished", run_id)


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
