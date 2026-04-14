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


def _get_reviewed_tables(discovery) -> set[str] | None:
    """Get set of reviewed table names. Returns None if all are reviewed."""
    reviewed = {t.name for t in discovery.tables if t.review_status in ("reviewed", "skipped")}
    # If all tables are reviewed, don't filter (no gate needed)
    if len(reviewed) == len(discovery.tables):
        return None
    return reviewed


@router.get("/explore/suggestions")
async def get_suggestions(request: Request):
    """Return auto-generated suggested questions and statistical insights."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    reviewed = _get_reviewed_tables(discovery)

    # If no tables reviewed, return empty with a message
    if reviewed is not None and len(reviewed) == 0:
        return {
            "suggestions": [],
            "insights": [],
            "review_required": True,
            "message": (
                "No tables have been reviewed yet. "
                "Visit the Data Dictionary to review table metadata before exploring."
            ),
        }

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

    # Filter suggestions to only reference reviewed tables
    if reviewed is not None:
        suggestions = [
            s
            for s in suggestions
            if not s.relevant_tables or any(t in reviewed for t in s.relevant_tables)
        ]

    # Statistical insights from materialized marts
    insights = detect_insights(con, schema="staging")
    insights.extend(detect_insights(con, schema="marts"))

    return {
        "suggestions": [s.model_dump() for s in suggestions],
        "insights": [i.model_dump() for i in insights],
        "review_required": reviewed is not None and len(reviewed) < len(discovery.tables),
    }


@router.post("/explore/ask")
async def ask_question(request: Request, body: AskRequest):
    """Answer a natural language question by generating and executing SQL."""
    pipeline = request.app.state.pipeline
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    reviewed = _get_reviewed_tables(discovery)

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
        reviewed_tables=reviewed,
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
