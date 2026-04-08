"""Explore API -- NL questions, statistical insights, and data exploration."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.llm import NoLLMProvider, get_provider
from headwater.core.config import get_settings
from headwater.explorer.nl_to_sql import ask
from headwater.explorer.statistical import detect_insights
from headwater.explorer.suggestions import generate_suggestions

router = APIRouter()


class AskRequest(BaseModel):
    question: str


@router.get("/explore/suggestions")
async def get_suggestions(request: Request):
    """Return auto-generated suggested questions and statistical insights."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    contracts = pipeline["contracts"]
    quality_report = pipeline["quality_report"]
    quality_results = quality_report.results if quality_report else []

    suggestions = generate_suggestions(
        discovery=discovery,
        models=all_models,
        contracts=contracts,
        quality_results=quality_results,
    )

    # Statistical insights from materialized marts
    con = request.app.state.duckdb_con
    insights = detect_insights(con, schema="staging")
    insights.extend(detect_insights(con, schema="marts"))

    return {
        "suggestions": [s.model_dump() for s in suggestions],
        "insights": [i.model_dump() for i in insights],
    }


@router.post("/explore/ask")
async def ask_question(request: Request, body: AskRequest):
    """Answer a natural language question by generating and executing SQL."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    con = request.app.state.duckdb_con
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    contracts = pipeline["contracts"]
    quality_report = pipeline["quality_report"]
    quality_results = quality_report.results if quality_report else []

    # Generate suggestions for matching
    suggestions = generate_suggestions(
        discovery=discovery,
        models=all_models,
        contracts=contracts,
        quality_results=quality_results,
    )

    # Get LLM provider if configured
    try:
        settings = get_settings()
        provider = get_provider(settings)
    except Exception:
        provider = NoLLMProvider()

    result = ask(
        question=body.question,
        con=con,
        discovery=discovery,
        models=all_models,
        suggestions=suggestions,
        provider=provider,
    )

    return result.model_dump()


@router.get("/explore/insights")
async def get_statistical_insights(request: Request):
    """Return only statistical insights from materialized data."""
    pipeline = request.app.state.pipeline
    if not pipeline["discovery"]:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    con = request.app.state.duckdb_con
    insights = detect_insights(con, schema="staging")
    insights.extend(detect_insights(con, schema="marts"))

    return {
        "insights": [i.model_dump() for i in insights],
        "total": len(insights),
    }
