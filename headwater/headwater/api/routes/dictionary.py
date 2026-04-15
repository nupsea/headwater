"""Data Dictionary API -- review and confirm table/column metadata before exploration.

The data dictionary is a gate between discovery/enrichment and the explorer.
Users review column classifications (role, semantic_type), descriptions,
PKs/FKs, and confirm them. Only reviewed tables are available for NL-to-SQL
exploration.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.heuristics import generate_clarifying_questions
from headwater.core.models import (
    DataDictionaryColumn,
    DataDictionaryTable,
    Relationship,
    ReviewSummary,
    TableReviewRequest,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _build_dictionary_table(
    table,
    source_name: str,
    profiles,
    relationships,
    clarifying_questions: dict[str, list[str]],
) -> DataDictionaryTable:
    """Build a DataDictionaryTable from a TableInfo and supporting data."""
    # Build FK index for this table
    fk_map: dict[str, str] = {}
    for rel in relationships:
        if rel.from_table == table.name:
            fk_map[rel.from_column] = f"{rel.to_table}.{rel.to_column}"

    cols = []
    for col in table.columns:
        needs_review = not col.locked and col.confidence < 0.7
        cols.append(
            DataDictionaryColumn(
                name=col.name,
                dtype=col.dtype,
                nullable=col.nullable,
                is_primary_key=col.is_primary_key,
                is_foreign_key=col.name in fk_map,
                fk_references=fk_map.get(col.name),
                semantic_type=col.semantic_type,
                role=col.role,
                description=col.description,
                confidence=col.confidence,
                locked=col.locked,
                needs_review=needs_review,
            )
        )

    table_rels = [
        r for r in relationships if r.from_table == table.name or r.to_table == table.name
    ]

    return DataDictionaryTable(
        name=table.name,
        source_name=source_name,
        row_count=table.row_count,
        description=table.description,
        domain=table.domain,
        review_status=table.review_status,
        columns=cols,
        relationships=table_rels,
        questions=clarifying_questions.get(table.name, []),
    )


@router.get("/dictionary")
async def get_dictionary(request: Request):
    """Return the full data dictionary for the current source."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    source_name = discovery.source.name
    clarifying = generate_clarifying_questions(discovery.tables, discovery.profiles)

    tables = []
    for table in discovery.tables:
        tables.append(
            _build_dictionary_table(
                table,
                source_name,
                discovery.profiles,
                discovery.relationships,
                clarifying,
            )
        )

    return {"tables": [t.model_dump() for t in tables]}


@router.get("/dictionary/summary")
async def get_review_summary(request: Request):
    """Return review progress summary."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    counts = {"pending": 0, "in_review": 0, "reviewed": 0, "skipped": 0}
    for table in discovery.tables:
        counts[table.review_status] = counts.get(table.review_status, 0) + 1

    total = len(discovery.tables)
    reviewed = counts["reviewed"]
    return ReviewSummary(
        total=total,
        reviewed=reviewed,
        pending=counts["pending"],
        in_review=counts["in_review"],
        skipped=counts["skipped"],
        pct_complete=round(reviewed / total * 100, 1) if total > 0 else 0.0,
    ).model_dump()


# -- Catalog review routes (MUST be before /{table_name} wildcard) -----------


class CatalogItemAction(BaseModel):
    """Payload for confirming, editing, or rejecting a catalog metric or dimension."""

    action: Literal["confirmed", "rejected"]
    synonyms: list[str] | None = None  # For dimensions: add/replace synonyms


@router.get("/dictionary/catalog")
async def get_catalog_for_review(request: Request):
    """Return catalog metrics and dimensions with their review status."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    store = request.app.state.metadata_store
    source_name = discovery.source.name
    metrics = store.get_catalog_metrics(source_name)
    dimensions = store.get_catalog_dimensions(source_name)
    entities = store.get_catalog_entities(source_name)

    return {
        "metrics": metrics,
        "dimensions": dimensions,
        "entities": entities,
        "summary": {
            "metrics_total": len(metrics),
            "metrics_confirmed": sum(1 for m in metrics if m.get("status") == "confirmed"),
            "metrics_rejected": sum(1 for m in metrics if m.get("status") == "rejected"),
            "dimensions_total": len(dimensions),
            "dimensions_confirmed": sum(
                1 for d in dimensions if d.get("status") == "confirmed"
            ),
            "dimensions_rejected": sum(
                1 for d in dimensions if d.get("status") == "rejected"
            ),
        },
    }


@router.patch("/dictionary/catalog/metrics/{metric_name}")
async def review_catalog_metric(
    metric_name: str,
    body: CatalogItemAction,
    request: Request,
):
    """Confirm or reject a catalog metric."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    store = request.app.state.metadata_store
    project_id = discovery.source.name

    confidence = 0.95 if body.action == "confirmed" else 0.0
    updated = store.update_catalog_metric_status(
        project_id,
        metric_name,
        status=body.action,
        confidence=confidence,
        source="human",
    )
    if not updated:
        raise HTTPException(
            status_code=404,
            detail=f"Metric '{metric_name}' not found in catalog.",
        )

    store.record_decision(
        artifact_type="catalog_metric",
        artifact_id=f"{project_id}.{metric_name}",
        action=body.action,
    )

    return {"metric": metric_name, "status": body.action, "confidence": confidence}


@router.patch("/dictionary/catalog/dimensions/{dimension_name}")
async def review_catalog_dimension(
    dimension_name: str,
    body: CatalogItemAction,
    request: Request,
):
    """Confirm or reject a catalog dimension. Optionally update synonyms."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    store = request.app.state.metadata_store
    project_id = discovery.source.name

    confidence = 0.95 if body.action == "confirmed" else 0.0
    updated = store.update_catalog_dimension_status(
        project_id,
        dimension_name,
        status=body.action,
        confidence=confidence,
        source="human",
        synonyms=body.synonyms,
    )
    if not updated:
        raise HTTPException(
            status_code=404,
            detail=f"Dimension '{dimension_name}' not found in catalog.",
        )

    store.record_decision(
        artifact_type="catalog_dimension",
        artifact_id=f"{project_id}.{dimension_name}",
        action=body.action,
        payload={"synonyms": body.synonyms} if body.synonyms else None,
    )

    return {
        "dimension": dimension_name,
        "status": body.action,
        "confidence": confidence,
    }


# -- Table-level routes (wildcard MUST come after specific routes) -----------


@router.get("/dictionary/{table_name}")
async def get_dictionary_table(table_name: str, request: Request):
    """Return data dictionary for a single table."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    source_name = discovery.source.name
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

    clarifying = generate_clarifying_questions([table], discovery.profiles)
    result = _build_dictionary_table(
        table,
        source_name,
        discovery.profiles,
        discovery.relationships,
        clarifying,
    )
    return result.model_dump()


class SkipRequest(BaseModel):
    reason: str | None = None


@router.post("/dictionary/{table_name}/review")
async def review_table(table_name: str, body: TableReviewRequest, request: Request):
    """Submit a review for a table's data dictionary.

    Updates column classifications, descriptions, PKs. If confirm=True,
    locks all columns and marks the table as reviewed.
    """
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    source_name = discovery.source.name
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

    store = request.app.state.metadata_store
    col_lookup = {c.name: c for c in table.columns}

    # Apply column updates to in-memory model
    for review in body.columns:
        col = col_lookup.get(review.name)
        if col is None:
            continue
        if review.semantic_type is not None:
            col.semantic_type = review.semantic_type
        if review.role is not None:
            col.role = review.role
        if review.description is not None:
            col.description = review.description
        if review.is_primary_key is not None:
            col.is_primary_key = review.is_primary_key

    # Apply table-level updates
    if body.table_description is not None:
        table.description = body.table_description
    if body.table_domain is not None:
        table.domain = body.table_domain

    if body.confirm:
        # Lock all columns and mark table as reviewed
        for col in table.columns:
            col.locked = True
            col.confidence = max(col.confidence, 0.9)  # Human-confirmed = high confidence
        table.review_status = "reviewed"
        table.reviewed_at = datetime.now()
        table.locked = True
    else:
        table.review_status = "in_review"

    # Persist to SQLite
    updates = []
    for col in table.columns:
        updates.append(
            {
                "name": col.name,
                "description": col.description,
                "semantic_type": col.semantic_type,
                "role": col.role,
                "is_primary_key": col.is_primary_key,
                "confidence": col.confidence,
            }
        )
    store.bulk_update_columns(
        table_name,
        source_name,
        updates,
        lock=body.confirm,
    )
    store.update_table_review_status(table_name, source_name, table.review_status)

    # Record decision for audit trail
    store.record_decision(
        artifact_type="table",
        artifact_id=f"{source_name}.{table_name}",
        action="reviewed" if body.confirm else "in_review",
        payload={
            "columns_updated": len(body.columns),
            "confirmed": body.confirm,
        },
    )

    return {
        "table": table_name,
        "review_status": table.review_status,
        "columns_updated": len(body.columns),
        "locked": body.confirm,
    }


@router.post("/dictionary/{table_name}/skip")
async def skip_table(table_name: str, request: Request, body: SkipRequest | None = None):
    """Skip review for a table. Explorer will not include this table."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    source_name = discovery.source.name
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

    table.review_status = "skipped"

    store = request.app.state.metadata_store
    store.update_table_review_status(table_name, source_name, "skipped")
    store.record_decision(
        artifact_type="table",
        artifact_id=f"{source_name}.{table_name}",
        action="skipped",
        reason=body.reason if body else None,
    )

    return {"table": table_name, "review_status": "skipped"}


@router.post("/dictionary/confirm-all")
async def confirm_all_tables(request: Request):
    """Bulk confirm all pending tables with their current classifications."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    source_name = discovery.source.name
    store = request.app.state.metadata_store
    confirmed = 0

    for table in discovery.tables:
        if table.review_status in ("reviewed", "skipped"):
            continue

        # Lock all columns
        for col in table.columns:
            col.locked = True
            col.confidence = max(col.confidence, 0.9)
        table.review_status = "reviewed"
        table.reviewed_at = datetime.now()
        table.locked = True

        # Persist
        updates = [
            {
                "name": col.name,
                "description": col.description,
                "semantic_type": col.semantic_type,
                "role": col.role,
                "confidence": col.confidence,
            }
            for col in table.columns
        ]
        store.bulk_update_columns(table.name, source_name, updates, lock=True)
        store.update_table_review_status(table.name, source_name, "reviewed")
        confirmed += 1

    return {"confirmed": confirmed, "total": len(discovery.tables)}


# ---------------------------------------------------------------------------
# Post-review editing endpoints
# ---------------------------------------------------------------------------


@router.post("/dictionary/{table_name}/unlock")
async def unlock_table(table_name: str, request: Request):
    """Unlock a reviewed table so it can be re-reviewed with updated metadata."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    source_name = discovery.source.name
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

    store = request.app.state.metadata_store

    # Unlock in-memory
    table.review_status = "pending"
    table.locked = False
    table.reviewed_at = None
    for col in table.columns:
        col.locked = False

    # Persist unlock
    store.update_table_review_status(table_name, source_name, "pending")
    for col in table.columns:
        store.lock_column(table_name, source_name, col.name, locked=False)

    store.record_decision(
        artifact_type="table",
        artifact_id=f"{source_name}.{table_name}",
        action="unlocked",
    )

    return {"table": table_name, "review_status": "pending", "locked": False}


class ColumnEditRequest(BaseModel):
    """Payload for editing a single column post-review."""

    semantic_type: str | None = None
    role: str | None = None
    description: str | None = None
    is_primary_key: bool | None = None


@router.patch("/dictionary/{table_name}/columns/{column_name}")
async def edit_column(
    table_name: str,
    column_name: str,
    body: ColumnEditRequest,
    request: Request,
):
    """Edit a single column's metadata. Updates both in-memory and metadata store."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    source_name = discovery.source.name
    table = next((t for t in discovery.tables if t.name == table_name), None)
    if table is None:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

    col = next((c for c in table.columns if c.name == column_name), None)
    if col is None:
        raise HTTPException(
            status_code=404,
            detail=f"Column '{column_name}' not found in '{table_name}'.",
        )

    # Apply changes to in-memory model
    if body.semantic_type is not None:
        col.semantic_type = body.semantic_type
    if body.role is not None:
        col.role = body.role
    if body.description is not None:
        col.description = body.description
    if body.is_primary_key is not None:
        col.is_primary_key = body.is_primary_key

    # Persist to metadata store
    store = request.app.state.metadata_store
    update = {"name": column_name}
    for field in ("semantic_type", "role", "description", "is_primary_key"):
        val = getattr(body, field)
        if val is not None:
            update[field] = val
    store.bulk_update_columns(table_name, source_name, [update], lock=False)

    store.record_decision(
        artifact_type="column",
        artifact_id=f"{source_name}.{table_name}.{column_name}",
        action="edited",
        payload=update,
    )

    return {
        "table": table_name,
        "column": column_name,
        "semantic_type": col.semantic_type,
        "role": col.role,
        "description": col.description,
        "is_primary_key": col.is_primary_key,
    }


class RelationshipCreateRequest(BaseModel):
    """Payload for adding a foreign key relationship."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    rel_type: str = "many_to_one"


@router.post("/dictionary/relationships")
async def add_relationship(body: RelationshipCreateRequest, request: Request):
    """Add a new FK relationship between two columns."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    source_name = discovery.source.name
    table_names = {t.name for t in discovery.tables}

    # Validate tables exist
    for tname in (body.from_table, body.to_table):
        if tname not in table_names:
            raise HTTPException(status_code=404, detail=f"Table '{tname}' not found.")

    # Validate columns exist
    from_table = next(t for t in discovery.tables if t.name == body.from_table)
    to_table = next(t for t in discovery.tables if t.name == body.to_table)
    if not any(c.name == body.from_column for c in from_table.columns):
        raise HTTPException(
            status_code=404,
            detail=f"Column '{body.from_column}' not found in '{body.from_table}'.",
        )
    if not any(c.name == body.to_column for c in to_table.columns):
        raise HTTPException(
            status_code=404,
            detail=f"Column '{body.to_column}' not found in '{body.to_table}'.",
        )

    store = request.app.state.metadata_store
    rel_id = store.insert_relationship(
        source_name,
        body.from_table,
        body.from_column,
        body.to_table,
        body.to_column,
        body.rel_type,
        confidence=1.0,
        ref_integrity=1.0,
        detection_source="declared",
    )

    rel = Relationship(
        id=rel_id,
        from_table=body.from_table,
        from_column=body.from_column,
        to_table=body.to_table,
        to_column=body.to_column,
        type=body.rel_type,
        confidence=1.0,
        referential_integrity=1.0,
        source="declared",
    )
    discovery.relationships.append(rel)

    store.record_decision(
        artifact_type="relationship",
        artifact_id=f"{source_name}.{body.from_table}.{body.from_column}->{body.to_table}.{body.to_column}",
        action="added",
    )

    return rel.model_dump()


@router.delete("/dictionary/relationships/{relationship_id}")
async def remove_relationship(relationship_id: int, request: Request):
    """Remove a foreign key relationship."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    store = request.app.state.metadata_store

    # Verify it exists in the store
    existing = store.get_relationship(relationship_id)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Relationship {relationship_id} not found.")

    # Remove from in-memory
    discovery.relationships = [r for r in discovery.relationships if r.id != relationship_id]

    # Remove from store
    store.delete_relationship(relationship_id)
    store.record_decision(
        artifact_type="relationship",
        artifact_id=str(relationship_id),
        action="deleted",
        payload={
            "from": f"{existing['from_table']}.{existing['from_column']}",
            "to": f"{existing['to_table']}.{existing['to_column']}",
        },
    )

    return {"deleted": relationship_id}
