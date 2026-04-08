"""Execution API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from headwater.executor.duckdb_backend import DuckDBBackend
from headwater.executor.runner import run_models

router = APIRouter()


@router.post("/execute")
async def execute_models(request: Request):
    """Execute all approved models."""
    pipeline = request.app.state.pipeline
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    if not all_models:
        raise HTTPException(status_code=400, detail="No models generated yet.")

    con = request.app.state.duckdb_con
    backend = DuckDBBackend(con)
    backend.ensure_schema("staging")

    results = run_models(backend, all_models, only_approved=True)
    pipeline["execution_results"] = results

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
