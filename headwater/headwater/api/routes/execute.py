"""Execution API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from headwater.core.runtime_state import get_runtime_state
from headwater.executor.duckdb_backend import DuckDBBackend
from headwater.executor.runner import run_models

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/execute")
async def execute_models(request: Request):
    """Execute all approved models."""
    pipeline = get_runtime_state(request)
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    if not all_models:
        raise HTTPException(status_code=400, detail="No models generated yet.")

    con = request.app.state.duckdb_con
    backend = DuckDBBackend(con)
    backend.ensure_schema("staging")

    results = run_models(backend, all_models, only_approved=True)
    pipeline["execution_results"] = results
    store = getattr(request.app.state, "metadata_store", None)
    model_by_name = {m.name: m for m in all_models}
    for result in results:
        if result.success and result.model_name in model_by_name:
            model_by_name[result.model_name].status = "executed"
        if store is not None:
            store.save_execution_result(
                model_name=result.model_name,
                success=result.success,
                row_count=result.row_count,
                execution_time_ms=result.execution_time_ms,
                error=result.error,
            )
            if result.success:
                store.update_model_status(result.model_name, "executed")

    return [
        {
            "model_name": r.model_name,
            "success": r.success,
            "row_count": r.row_count,
            "execution_time_ms": round(r.execution_time_ms, 1),
            "error": r.error,
        }
        for r in results
    ]
