"""Model API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.core.events import EventType
from headwater.generator.contracts import generate_contracts
from headwater.generator.marts import generate_mart_models
from headwater.generator.staging import generate_staging_models
from headwater.services.model_maturity import build_model_impact_report
from headwater.services.source_sync import SourceSyncService

router = APIRouter()
logger = logging.getLogger(__name__)


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

    store = getattr(request.app.state, "metadata_store", None)
    if store is not None:
        _persist_models_and_contracts(
            store,
            staging + marts,
            contracts,
            source_name=discovery.source.name,
        )

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


@router.get("/models/impact")
async def model_impact(request: Request):
    """Return model maturity and impact analysis for the current pipeline."""
    pipeline = request.app.state.pipeline
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    store = getattr(request.app.state, "metadata_store", None)
    latest_quality = store.get_latest_quality_report() if store is not None else None
    sources = store.list_sources() if store is not None else []
    discovery = pipeline.get("discovery")
    table_sources = {}
    if discovery is not None:
        table_sources = {table.name: discovery.source.name for table in discovery.tables}
    return build_model_impact_report(
        models=all_models,
        contracts=pipeline.get("contracts", []),
        execution_results=pipeline.get("execution_results", []),
        latest_quality=latest_quality,
        sources=sources,
        table_sources=table_sources,
    )


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
        store.update_model_status(model_name, "approved")
        store.record_decision(
            "model",
            model_name,
            "approved",
            payload={"previous_status": prev_status},
        )
        SourceSyncService(request).record_event(
            EventType.MODEL_REVIEWED,
            f"Model '{model_name}' approved",
            source_name=_current_source_name(request),
            severity="info",
            artifact_type="model",
            artifact_id=model_name,
            invalidates=["models", "briefing"],
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
        store.update_model_status(model_name, "rejected")
        store.record_decision(
            "model",
            model_name,
            "rejected",
            payload={"previous_status": prev_status},
        )
        SourceSyncService(request).record_event(
            EventType.MODEL_REVIEWED,
            f"Model '{model_name}' rejected",
            source_name=_current_source_name(request),
            severity="info",
            artifact_type="model",
            artifact_id=model_name,
            invalidates=["models", "briefing"],
        )
    return {"name": model.name, "status": model.status}


def _current_source_name(request: Request) -> str:
    discovery = request.app.state.pipeline.get("discovery")
    if discovery is not None:
        return discovery.source.name
    store = getattr(request.app.state, "metadata_store", None)
    sources = store.list_sources() if store is not None else []
    return sources[-1]["name"] if sources else "source"


class AnswerItem(BaseModel):
    question_index: int
    answer: str


class AnswersPayload(BaseModel):
    answers: list[AnswerItem]


@router.post("/models/{model_name}/answers")
async def save_model_answers(request: Request, model_name: str, body: AnswersPayload):
    """Save answers to a model's clarifying questions."""
    pipeline = request.app.state.pipeline
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    model = next((m for m in all_models if m.name == model_name), None)
    if not model:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found.")

    store = getattr(request.app.state, "metadata_store", None)
    answers_dicts = [a.model_dump() for a in body.answers]
    saved = 0
    if store is not None:
        saved = store.save_model_answers(model_name, answers_dicts)
        store.log_activity(
            "question_answered",
            f"Answered {len(body.answers)} questions for {model_name}",
            artifact_type="model",
            artifact_id=model_name,
        )
    return {"model_name": model_name, "answers_saved": saved}


@router.get("/models/{model_name}/answers")
async def get_model_answers(request: Request, model_name: str):
    """Retrieve saved answers for a model's clarifying questions."""
    store = getattr(request.app.state, "metadata_store", None)
    answers = []
    if store is not None:
        answers = store.get_model_answers(model_name)
    return {"model_name": model_name, "answers": answers}


def _persist_models_and_contracts(
    store,
    models: list,
    contracts: list,
    *,
    source_name: str,
) -> None:
    for model in models:
        store.upsert_model(
            name=model.name,
            source_name=source_name,
            model_type=model.model_type,
            sql_text=model.sql,
            description=model.description,
            source_tables=model.source_tables,
            depends_on=model.depends_on,
            status=model.status,
            assumptions=getattr(model, "assumptions", []),
            questions=getattr(model, "questions", []),
        )
    for contract in contracts:
        store.upsert_contract(
            id_=contract.id or f"{contract.model_name}_{contract.rule_type}_{contract.column_name}",
            model_name=contract.model_name,
            rule_type=contract.rule_type,
            expression=contract.expression,
            column_name=contract.column_name,
            severity=contract.severity,
            description=contract.description,
            confidence=contract.confidence,
            status=contract.status,
        )
