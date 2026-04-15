"""Explore API -- NL questions, statistical insights, and data exploration.

v2: Exploration is non-blocking. No dictionary review gate. Confidence
badges signal quality rather than hard gates. Only mart model approval
blocks execution (I-4).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.llm import NoLLMProvider, get_provider
from headwater.core.config import get_settings
from headwater.explorer.nl_to_sql import ask
from headwater.explorer.statistical import detect_insights
from headwater.explorer.suggestions import generate_suggestions

router = APIRouter()
logger = logging.getLogger(__name__)


class AskRequest(BaseModel):
    question: str


@router.get("/explore/suggestions")
async def get_suggestions(request: Request):
    """Return auto-generated suggested questions and statistical insights.

    v2: No review gate. Suggestions are always returned. If few tables
    are reviewed, a soft signal is included but exploration is not blocked.
    """
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    reviewed = {t.name for t in discovery.tables if t.review_status in ("reviewed", "skipped")}
    review_pct = len(reviewed) / len(discovery.tables) * 100 if discovery.tables else 0

    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    contracts = pipeline["contracts"]
    quality_report = pipeline["quality_report"]
    quality_results = quality_report.results if quality_report else []

    catalog = pipeline.get("catalog")
    con = request.app.state.duckdb_con
    suggestions = generate_suggestions(
        discovery=discovery,
        models=all_models,
        contracts=contracts,
        quality_results=quality_results,
        con=con,
        catalog=catalog,
    )

    # Statistical insights from materialized marts
    insights = detect_insights(con, schema="staging")
    insights.extend(detect_insights(con, schema="marts"))

    return {
        "suggestions": [s.model_dump() for s in suggestions],
        "insights": [i.model_dump() for i in insights],
        "review_pct": round(review_pct, 1),
    }


@router.post("/explore/ask")
async def ask_question(request: Request, body: AskRequest):
    """Answer a natural language question by generating and executing SQL.

    v2: No review gate. Questions are always processed. Low-confidence
    answers show warnings rather than errors.
    """
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
        con=con,
    )

    # Get LLM provider if configured
    try:
        settings = get_settings()
        provider = get_provider(settings)
    except Exception:
        provider = NoLLMProvider()

    catalog = pipeline.get("catalog")
    vector_store = pipeline.get("vector_store")

    result = ask(
        question=body.question,
        con=con,
        discovery=discovery,
        models=all_models,
        suggestions=suggestions,
        provider=provider,
        catalog=catalog,
        vector_store=vector_store,
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
