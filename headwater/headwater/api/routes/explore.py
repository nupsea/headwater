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
from headwater.core.models import Relationship
from headwater.explorer.nl_to_sql import ask
from headwater.explorer.statistical import detect_insights
from headwater.explorer.suggestions import generate_suggestions

router = APIRouter()
logger = logging.getLogger(__name__)

_INSIGHTS_ENDPOINT_LIMIT = 50


def _rank_statistical_insights(insights):
    severity_rank = {"critical": 0, "warning": 1, "info": 2}
    return sorted(
        insights,
        key=lambda insight: (
            severity_rank.get(insight.severity, 3),
            insight.p_value if insight.p_value is not None else 1.0,
            -abs(insight.magnitude),
        ),
    )


def _serialize_statistical_insights(insights, limit: int) -> list[dict]:
    return [i.model_dump() for i in _rank_statistical_insights(insights)[:limit]]


def _load_confirmed_relationships(request: Request) -> list[Relationship]:
    """Load human-confirmed FK relationships from metadata store.

    These supplement auto-detected relationships for richer cross-table
    suggestions and join-path resolution.
    """
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        return []
    try:
        rows = store._exec(
            "SELECT from_table, from_column, to_table, to_column "
            "FROM relationships WHERE detection_source = 'confirmed'"
        )
        rels: list[Relationship] = []
        for row in rows:
            rels.append(
                Relationship(
                    from_table=row["from_table"],
                    from_column=row["from_column"],
                    to_table=row["to_table"],
                    to_column=row["to_column"],
                    type="many_to_one",
                    confidence=1.0,
                    referential_integrity=1.0,
                    source="declared",
                )
            )
        return rels
    except Exception:
        logger.debug("No confirmed FK relationships available")
        return []


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
    extra_rels = _load_confirmed_relationships(request)
    suggestions = generate_suggestions(
        discovery=discovery,
        models=all_models,
        contracts=contracts,
        quality_results=quality_results,
        con=request.app.state.duckdb_con,
        catalog=catalog,
        extra_relationships=extra_rels,
    )

    return {
        "suggestions": [s.model_dump() for s in suggestions],
        "insights": [],
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

    # Load confirmed relationships and merge into discovery for richer suggestions
    extra_rels = _load_confirmed_relationships(request)

    # Generate suggestions for matching (with confirmed relationships)
    suggestions = generate_suggestions(
        discovery=discovery,
        models=all_models,
        contracts=contracts,
        quality_results=quality_results,
        con=con,
        extra_relationships=extra_rels,
    )

    # Get LLM provider if configured
    try:
        settings = get_settings()
        provider = get_provider(settings)
    except Exception:
        provider = NoLLMProvider()

    catalog = pipeline.get("catalog")
    vector_store = pipeline.get("vector_store")

    # Enrich discovery with confirmed relationships for NL-to-SQL
    enriched_discovery = discovery
    if extra_rels:
        existing_pairs = {
            (r.from_table, r.from_column, r.to_table, r.to_column)
            for r in discovery.relationships
        }
        new_rels = [
            r for r in extra_rels
            if (r.from_table, r.from_column, r.to_table, r.to_column) not in existing_pairs
        ]
        if new_rels:
            enriched_discovery = discovery.model_copy(
                update={"relationships": list(discovery.relationships) + new_rels}
            )

    result = ask(
        question=body.question,
        con=con,
        discovery=enriched_discovery,
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
        "insights": _serialize_statistical_insights(insights, _INSIGHTS_ENDPOINT_LIMIT),
        "total": len(insights),
    }
