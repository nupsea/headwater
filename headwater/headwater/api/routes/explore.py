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
from headwater.analyzer.metadata_retrieval import retrieve_metadata
from headwater.api.project_scope import scoped_pipeline
from headwater.api.routes.insights import compute_semantic_highlights, compute_top_insights
from headwater.core.config import get_settings
from headwater.core.models import DatasetContext, Relationship
from headwater.explorer.nl_to_sql import ask
from headwater.explorer.statistical import (
    detect_insights_with_diagnostics,
    insight_type_priority_weights,
)
from headwater.explorer.suggestions import generate_suggestions

router = APIRouter()
logger = logging.getLogger(__name__)

_INSIGHTS_ENDPOINT_LIMIT = 50
_INSIGHT_TYPE_LIMITS = {
    "temporal_anomaly": 2,
    "change_point": 2,
    "correlation": 2,
    "coverage_period": 2,
    "volume_distribution": 3,
    "peak_period": 3,
    "duration_distribution": 3,
    "geographic_hotspot": 3,
    "route_pair": 3,
    "congestion_proxy": 2,
    "data_quality": 3,
}


def _rank_statistical_insights(insights):
    severity_weight = {"critical": 3, "warning": 2, "info": 1}
    type_weight = insight_type_priority_weights()
    return sorted(
        insights,
        key=lambda insight: (
            -type_weight.get(insight.insight_type, 0),
            -severity_weight.get(insight.severity, 0),
            -(insight.support_count or 0),
            -abs(insight.magnitude),
            insight.p_value if insight.p_value is not None else 1.0,
            insight.table_name,
            insight.metric,
        ),
    )


def _serialize_statistical_insights(insights, limit: int) -> list[dict]:
    return [i.model_dump() for i in _diversify_statistical_insights(insights, limit)]


def _diversify_statistical_insights(insights, limit: int):
    result = []
    type_counts: dict[str, int] = {}
    seen_table_type: set[tuple[str, str]] = set()

    for insight in _rank_statistical_insights(insights):
        table_type = (insight.table_name, insight.insight_type)
        if table_type in seen_table_type:
            continue
        type_limit = _INSIGHT_TYPE_LIMITS.get(insight.insight_type, 3)
        if type_counts.get(insight.insight_type, 0) >= type_limit:
            continue
        result.append(insight)
        seen_table_type.add(table_type)
        type_counts[insight.insight_type] = type_counts.get(insight.insight_type, 0) + 1
        if len(result) >= limit:
            break

    return result


def _serialize_diagnostics(diagnostics) -> list[dict]:
    return [d.model_dump() for d in diagnostics]


def _dataset_context_for_pipeline(request: Request, pipeline: dict) -> DatasetContext | None:
    store = getattr(request.app.state, "metadata_store", None)
    discovery = pipeline.get("discovery")
    if store is None or discovery is None:
        return None
    try:
        row = store.get_dataset_context(discovery.source.name)
        return DatasetContext(**row) if row else None
    except Exception:
        logger.debug("Dataset context unavailable for insights")
        return None


def _load_confirmed_relationships(
    request: Request,
    source_names: list[str] | None = None,
) -> list[Relationship]:
    """Load human-confirmed FK relationships from metadata store.

    These supplement auto-detected relationships for richer cross-table
    suggestions and join-path resolution.
    """
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        return []
    try:
        if source_names:
            placeholders = ", ".join("?" for _ in source_names)
            rows = store.con.execute(
                "SELECT from_table, from_column, to_table, to_column "
                "FROM relationships WHERE detection_source = 'confirmed' "
                f"AND source_name IN ({placeholders})",
                tuple(source_names),
            ).fetchall()
        else:
            rows = store.con.execute(
                "SELECT from_table, from_column, to_table, to_column "
                "FROM relationships WHERE detection_source = 'confirmed'"
            ).fetchall()
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
async def get_suggestions(request: Request, project_id: str | None = None):
    """Return auto-generated suggested questions and statistical insights.

    v2: No review gate. Suggestions are always returned. If few tables
    are reviewed, a soft signal is included but exploration is not blocked.
    """
    pipeline = scoped_pipeline(request, project_id)
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
    extra_rels = _load_confirmed_relationships(request, pipeline.get("source_names"))
    context = _dataset_context_for_pipeline(request, pipeline)
    metadata = retrieve_metadata(discovery, context)
    business_insights = compute_top_insights(
        request.app.state.duckdb_con,
        discovery.tables,
        discovery.profiles,
    )
    semantic_highlights = compute_semantic_highlights(
        request.app.state.duckdb_con,
        discovery,
        _dataset_context_for_pipeline(request, pipeline),
        all_models,
    )
    suggestions = generate_suggestions(
        discovery=discovery,
        models=all_models,
        contracts=contracts,
        quality_results=quality_results,
        con=request.app.state.duckdb_con,
        catalog=catalog,
        extra_relationships=extra_rels,
        business_insights=business_insights,
        metadata=metadata,
    )
    con = request.app.state.duckdb_con
    staging_result = detect_insights_with_diagnostics(
        con,
        schema="staging",
        discovery=discovery,
        dataset_context=context,
        models=all_models,
    )
    marts_result = detect_insights_with_diagnostics(
        con,
        schema="marts",
        discovery=discovery,
        dataset_context=context,
        models=all_models,
    )
    statistical_insights = staging_result.insights + marts_result.insights
    diagnostics = staging_result.diagnostics + marts_result.diagnostics

    return {
        "suggestions": [s.model_dump() for s in suggestions],
        "business_insights": business_insights,
        "semantic_highlights": semantic_highlights,
        "insights": _serialize_statistical_insights(statistical_insights, 10),
        "diagnostics": _serialize_diagnostics(diagnostics),
        "review_pct": round(review_pct, 1),
    }


@router.post("/explore/ask")
async def ask_question(request: Request, body: AskRequest, project_id: str | None = None):
    """Answer a natural language question by generating and executing SQL.

    v2: No review gate. Questions are always processed. Low-confidence
    answers show warnings rather than errors.
    """
    pipeline = scoped_pipeline(request, project_id)
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    con = request.app.state.duckdb_con
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    contracts = pipeline["contracts"]
    quality_report = pipeline["quality_report"]
    quality_results = quality_report.results if quality_report else []

    # Load confirmed relationships and merge into discovery for richer suggestions
    extra_rels = _load_confirmed_relationships(request, pipeline.get("source_names"))
    context = _dataset_context_for_pipeline(request, pipeline)
    metadata = retrieve_metadata(discovery, context)

    # Generate suggestions for matching (with confirmed relationships)
    suggestions = generate_suggestions(
        discovery=discovery,
        models=all_models,
        contracts=contracts,
        quality_results=quality_results,
        con=con,
        extra_relationships=extra_rels,
        business_insights=compute_top_insights(con, discovery.tables, discovery.profiles),
        metadata=metadata,
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
async def get_statistical_insights(request: Request, project_id: str | None = None):
    """Return only statistical insights from materialized data."""
    pipeline = scoped_pipeline(request, project_id)
    if not pipeline["discovery"]:
        raise HTTPException(status_code=400, detail="No discovery run yet.")

    con = request.app.state.duckdb_con
    discovery = pipeline["discovery"]
    all_models = pipeline["staging_models"] + pipeline["mart_models"]
    context = _dataset_context_for_pipeline(request, pipeline)
    business_insights = compute_top_insights(con, discovery.tables, discovery.profiles)
    semantic_highlights = compute_semantic_highlights(con, discovery, context, all_models)
    staging_result = detect_insights_with_diagnostics(
        con,
        schema="staging",
        discovery=discovery,
        dataset_context=context,
        models=all_models,
    )
    marts_result = detect_insights_with_diagnostics(
        con,
        schema="marts",
        discovery=discovery,
        dataset_context=context,
        models=all_models,
    )
    insights = staging_result.insights + marts_result.insights
    diagnostics = staging_result.diagnostics + marts_result.diagnostics

    return {
        "business_insights": business_insights,
        "semantic_highlights": semantic_highlights,
        "insights": _serialize_statistical_insights(insights, _INSIGHTS_ENDPOINT_LIMIT),
        "diagnostics": _serialize_diagnostics(diagnostics),
        "total": len(insights),
    }
