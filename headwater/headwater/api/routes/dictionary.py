"""Data Dictionary API -- review and confirm table/column metadata before exploration.

The data dictionary is a gate between discovery/enrichment and the explorer.
Users review column classifications (role, semantic_type), descriptions,
PKs/FKs, and confirm them. Only reviewed tables are available for NL-to-SQL
exploration.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.heuristics import generate_clarifying_questions
from headwater.core.models import (
    DataDictionaryColumn,
    DataDictionaryTable,
    ReviewSummary,
    TableReviewRequest,
)

router = APIRouter()


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
        needs_review = (
            not col.locked
            and col.confidence < 0.7
        )
        cols.append(DataDictionaryColumn(
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
        ))

    table_rels = [
        r for r in relationships
        if r.from_table == table.name or r.to_table == table.name
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
        tables.append(_build_dictionary_table(
            table, source_name, discovery.profiles,
            discovery.relationships, clarifying,
        ))

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
        table, source_name, discovery.profiles,
        discovery.relationships, clarifying,
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
        updates.append({
            "name": col.name,
            "description": col.description,
            "semantic_type": col.semantic_type,
            "role": col.role,
            "is_primary_key": col.is_primary_key,
            "confidence": col.confidence,
        })
    store.bulk_update_columns(
        table_name, source_name, updates, lock=body.confirm,
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
        updates = [{
            "name": col.name,
            "description": col.description,
            "semantic_type": col.semantic_type,
            "role": col.role,
            "confidence": col.confidence,
        } for col in table.columns]
        store.bulk_update_columns(table.name, source_name, updates, lock=True)
        store.update_table_review_status(table.name, source_name, "reviewed")
        confirmed += 1

    return {"confirmed": confirmed, "total": len(discovery.tables)}
