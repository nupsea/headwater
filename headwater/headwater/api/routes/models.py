"""Model API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from headwater.generator.contracts import generate_contracts
from headwater.generator.marts import generate_mart_models
from headwater.generator.staging import generate_staging_models

router = APIRouter()


@router.post("/generate")
async def generate_models(
    request: Request,
    source_schema: str = "env_health",
    target_schema: str = "staging",
):
    """Generate staging models, mart models, and quality contracts."""
    discovery = request.app.state.pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    staging = generate_staging_models(
        discovery.tables, source_schema=source_schema, target_schema=target_schema
    )
    marts = generate_mart_models(discovery, target_schema=target_schema)
    contracts = generate_contracts(discovery.profiles, target_schema=target_schema)

    request.app.state.pipeline["staging_models"] = staging
    request.app.state.pipeline["mart_models"] = marts
    request.app.state.pipeline["contracts"] = contracts

    return {
        "staging_models": len(staging),
        "mart_models": len(marts),
        "contracts": len(contracts),
    }


@router.get("/models")
async def list_models(request: Request):
    """List all generated models."""
    pipeline = request.app.state.pipeline
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    return [
        {
            "name": m.name,
            "model_type": m.model_type,
            "status": m.status,
            "description": m.description,
            "source_tables": m.source_tables,
            "questions": m.questions,
            "assumptions": m.assumptions,
        }
        for m in all_models
    ]


@router.get("/models/{model_name}")
async def get_model(request: Request, model_name: str):
    """Get a specific model with full SQL."""
    pipeline = request.app.state.pipeline
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    model = next((m for m in all_models if m.name == model_name), None)
    if not model:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found.")
    return model.model_dump()


@router.post("/models/{model_name}/approve")
async def approve_model(request: Request, model_name: str):
    """Approve a proposed model for execution."""
    pipeline = request.app.state.pipeline
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    model = next((m for m in all_models if m.name == model_name), None)
    if not model:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found.")
    if model.status != "proposed":
        raise HTTPException(
            status_code=400,
            detail=f"Model is '{model.status}', not 'proposed'.",
        )
    prev_status = model.status
    model.status = "approved"
    store = getattr(request.app.state, "metadata_store", None)
    if store is not None:
        store.record_decision(
            "model", model_name, "approved",
            payload={"previous_status": prev_status},
        )
    return {"name": model.name, "status": model.status}


@router.post("/models/{model_name}/reject")
async def reject_model(request: Request, model_name: str):
    """Reject a proposed model."""
    pipeline = request.app.state.pipeline
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    model = next((m for m in all_models if m.name == model_name), None)
    if not model:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found.")
    prev_status = model.status
    model.status = "rejected"
    store = getattr(request.app.state, "metadata_store", None)
    if store is not None:
        store.record_decision(
            "model", model_name, "rejected",
            payload={"previous_status": prev_status},
        )
    return {"name": model.name, "status": model.status}
