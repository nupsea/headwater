"""Warehouse insight planning API."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from headwater.core.exceptions import ConnectorError
from headwater.services.warehouse_insights import (
    PlanExecutionRejectedError,
    PlanNotFoundError,
    SourceNotFoundError,
    build_dry_run_plan,
    execute_approved_plan,
)

router = APIRouter()


class InsightPlanRequest(BaseModel):
    max_queries: int | None = None
    max_tables: int | None = None
    max_sample_rows: int | None = None
    max_estimated_rows: int | None = None
    require_time_filter_above_rows: int | None = None
    allow_full_scan: bool | None = None


class InsightPlanExecutionRequest(BaseModel):
    approved: bool = False
    max_queries: int | None = None
    query_tag: str | None = None
    statement_timeout_seconds: int | None = None


@router.post("/sources/{source_name}/insight-plan/dry-run")
async def dry_run_source_insight_plan(
    source_name: str,
    request: Request,
    body: InsightPlanRequest | None = None,
):
    """Create a dry-run, cost-aware insight plan for a warehouse source."""
    store = request.app.state.metadata_store
    try:
        return build_dry_run_plan(
            store,
            source_name,
            body.model_dump(exclude_none=True) if body else None,
        )
    except SourceNotFoundError:
        raise HTTPException(status_code=404, detail=f"Source '{source_name}' not found.") from None


@router.post("/warehouse-insight-plans/{plan_id}/execute")
async def execute_warehouse_insight_plan(
    plan_id: int,
    body: InsightPlanExecutionRequest,
    request: Request,
):
    """Execute an approved dry-run plan through read-only connector hooks."""
    store = request.app.state.metadata_store
    try:
        return execute_approved_plan(
            store,
            plan_id,
            approved=body.approved,
            max_queries=body.max_queries,
            query_tag=body.query_tag,
            statement_timeout_seconds=body.statement_timeout_seconds,
        )
    except PlanNotFoundError:
        raise HTTPException(status_code=404, detail=f"Plan '{plan_id}' not found.") from None
    except SourceNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Source '{exc}' not found.") from None
    except PlanExecutionRejectedError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except ConnectorError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@router.get("/warehouse-insight-plans")
async def list_warehouse_insight_plans(
    request: Request,
    source: str | None = None,
    limit: int = Query(20, ge=1, le=100),
):
    store = request.app.state.metadata_store
    return {"plans": store.list_warehouse_insight_plans(source_name=source, limit=limit)}


@router.get("/evidence")
async def list_evidence(
    request: Request,
    source: str | None = None,
    plan_id: int | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    store = request.app.state.metadata_store
    return {
        "records": store.list_evidence_records(source_name=source, plan_id=plan_id, limit=limit)
    }
