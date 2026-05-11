"""Suggestion engine -- auto-generates BI-oriented questions from metadata.

Generates analytical questions that a data professional would actually ask,
derived entirely from the discovered schema: column names, dtypes, semantic types,
detected relationships, and mart model definitions.

No hardcoded table names, column names, or domain knowledge -- everything is
inferred from what the data actually contains.

Priority order (highest to lowest):
  mart > cross_table (schema-graph) > relationship > semantic > quality

Quality questions are intentionally de-prioritized and capped:
  - Only for numeric metric columns with actual nulls present
  - Hard cap of MAX_QUALITY_SUGGESTIONS regardless of how many contracts exist

Total output is capped at MAX_TOTAL_SUGGESTIONS, deduplicated.
"""

from __future__ import annotations

import logging
import re

import duckdb

from headwater.analyzer.metadata_retrieval import RetrievedMetadata, retrieve_metadata
from headwater.analyzer.semantic_schema import infer_semantic_schema, roles_for_table
from headwater.core.classification import is_dimension_column, is_metric_column
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    ContractCheckResult,
    ContractRule,
    DiscoveryResult,
    GeneratedModel,
    Relationship,
    SuggestedQuestion,
    TableInfo,
)
from headwater.explorer.schema_graph import SchemaGraph
from headwater.explorer.utils import resolve_table_ref, table_exists

logger = logging.getLogger(__name__)

MAX_TOTAL_SUGGESTIONS = 15
MAX_QUALITY_SUGGESTIONS = 3
MAX_TREND_SUGGESTIONS = 3
MAX_AVERAGE_SUGGESTIONS = 5
MAX_COUNT_SUGGESTIONS = 5

# Numeric dtypes that represent measurable quantities
_NUMERIC_DTYPES = ("int", "float", "double", "decimal", "numeric", "real", "bigint", "hugeint")

# Temporal dtype/name patterns
_TEMPORAL_DTYPES = ("timestamp", "date", "time", "datetime")
_TEMPORAL_NAME_RE = re.compile(
    r"(date|time|month|year|day|week|quarter|period|_at$|_ts$)", re.IGNORECASE
)

# Column name patterns that indicate IDs/codes -- not useful as metrics
_ID_NAME_RE = re.compile(
    r"(_id|_key|_fk|_pk|^id$|^key$|^uuid$|code$|flag$|indicator$)", re.IGNORECASE
)

_ENUM_LABEL_REGISTRY: dict[str, dict[object, str]] = {
    "payment_type": {
        0: "Flex fare",
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    },
}
_ENUM_DIMENSION_LABELS = {
    "payment_type": "payment method",
}
_AGGREGATE_METRIC_RE = re.compile(
    r"^(avg|average|mean|median|min|max|p\d+|pct|percent|rate|ratio|share|count|total|sum)_",
    re.IGNORECASE,
)
_SUMMARY_NAME_RE = re.compile(r"(summary|overview|snapshot|totals?)", re.IGNORECASE)
_LOW_SIGNAL_DIMENSION_TOKENS = (
    "flag",
    "indicator",
    "store_and_fwd",
    "source_file",
    "source_system",
    "load_batch",
    "ingest",
    "extract",
    "audit",
    "deleted",
)
_BUSINESS_DIMENSION_TOKENS = (
    "type",
    "category",
    "status",
    "reason",
    "channel",
    "segment",
    "service",
    "region",
    "zone",
    "site",
    "payment",
)
_BOOLEANISH_VALUES = {"y", "n", "yes", "no", "true", "false", "0", "1"}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_suggestions(
    discovery: DiscoveryResult,
    models: list[GeneratedModel] | None = None,
    contracts: list[ContractRule] | None = None,
    quality_results: list[ContractCheckResult] | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
    catalog=None,
    extra_relationships: list[Relationship] | None = None,
    business_insights: list[dict] | None = None,
    metadata: RetrievedMetadata | None = None,
) -> list[SuggestedQuestion]:
    """Generate suggested questions from all available metadata.

    All questions are derived from actual schema -- no hardcoded names.
    When a SemanticCatalog is provided, catalog-based suggestions are
    generated first (highest priority after mart).

    extra_relationships: supplemental FK relationships from confirmed PK/FK
    detection, merged with discovery.relationships for richer cross-table
    suggestions.

    Returns at most MAX_TOTAL_SUGGESTIONS questions in priority order.
    """
    all_models = models or []
    metadata = metadata or retrieve_metadata(discovery)
    profile_index = {(p.table_name, p.column_name): p for p in discovery.profiles}

    # Merge confirmed PK/FK relationships with discovered ones
    all_relationships = list(discovery.relationships)
    if extra_relationships:
        existing_pairs = {
            (r.from_table, r.from_column, r.to_table, r.to_column) for r in all_relationships
        }
        for rel in extra_relationships:
            key = (rel.from_table, rel.from_column, rel.to_table, rel.to_column)
            if key not in existing_pairs:
                all_relationships.append(rel)
                existing_pairs.add(key)

    # Log catalog status for observability
    if catalog is not None:
        entity_count = len(getattr(catalog, "entities", []))
        metric_count = len(getattr(catalog, "metrics", []))
        if entity_count == 0 and metric_count == 0:
            logger.warning(
                "Catalog is empty (0 entities, 0 metrics). "
                "Catalog-based suggestions will be skipped. "
                "Check enrichment pipeline and column classifications."
            )

    # Build schema graph for cross-table question generation
    graph = SchemaGraph(discovery, all_models)

    buckets: dict[str, list[SuggestedQuestion]] = {
        "business": _from_semantic_roles(
            discovery,
            all_models,
            con,
            metadata,
        ) + _from_business_insights(
            business_insights,
            con,
            discovery.tables,
            all_models,
            metadata,
        ),
        "catalog": _from_catalog(catalog, con, all_models) if catalog else [],
        "mart": _from_mart_models(all_models, con, metadata),
        "cross_table": _from_schema_graph(graph, all_relationships, all_models, con, metadata),
        "relationship": _from_relationships(
            discovery.tables, all_relationships, all_models, con, metadata
        ),
        "semantic": _from_table_structure(
            discovery.tables,
            profile_index,
            all_models,
            con,
            metadata,
        ),
        "quality": _from_quality_findings(
            contracts or [],
            quality_results or [],
            profile_index=profile_index,
            tables=discovery.tables,
        ),
    }

    result: list[SuggestedQuestion] = []
    seen: set[str] = set()
    candidates: list[SuggestedQuestion] = []
    for source in (
        "business",
        "catalog",
        "mart",
        "cross_table",
        "semantic",
        "relationship",
        "quality",
    ):
        for q in buckets[source]:
            key = " ".join(q.question.lower().split())
            if key in seen:
                continue
            seen.add(key)
            candidates.append(q)

    result = _select_diverse_questions(candidates, metadata)

    logger.info(
        "Generated %d suggestions: business=%d, catalog=%d, mart=%d, cross_table=%d, "
        "relationship=%d, semantic=%d, quality=%d",
        len(result),
        len(buckets["business"]),
        len(buckets["catalog"]),
        len(buckets["mart"]),
        len(buckets["cross_table"]),
        len(buckets["relationship"]),
        len(buckets["semantic"]),
        len(buckets["quality"]),
    )
    return result[:MAX_TOTAL_SUGGESTIONS]


def _select_diverse_questions(
    candidates: list[SuggestedQuestion],
    metadata: RetrievedMetadata | None = None,
) -> list[SuggestedQuestion]:
    source_limits = {
        "business": 5,
        "catalog": 3,
        "mart": 3,
        "cross_table": 3,
        "semantic": 3,
        "relationship": 2,
        "quality": 2,
    }
    shape_limits = {
        "trend": 2,
        "average": 2,
        "count": 2,
        "ranking": 4,
        "distribution": 2,
        "quality": 2,
        "other": 3,
    }
    source_priority = {
        "business": 0,
        "catalog": 1,
        "mart": 2,
        "cross_table": 3,
        "semantic": 4,
        "relationship": 5,
        "quality": 6,
    }

    selected: list[SuggestedQuestion] = []
    source_counts: dict[str, int] = {}
    shape_counts: dict[str, int] = {}
    table_counts: dict[str, int] = {}
    primary_tables = {
        (question.relevant_tables or [question.category])[0]
        for question in candidates
    }
    if len(primary_tables) <= 1:
        per_table_limit = 5
    elif len(primary_tables) <= 3:
        per_table_limit = 3
    else:
        per_table_limit = 2

    ranked = sorted(
        candidates,
        key=lambda q: (
            -_question_value_score(q, metadata),
            source_priority.get(q.source, 99),
            len(q.relevant_tables or []),
            len(q.question),
        ),
    )

    def can_add(question: SuggestedQuestion) -> bool:
        shape = _question_shape(question.question)
        if source_counts.get(question.source, 0) >= source_limits.get(question.source, 3):
            return False
        if shape_counts.get(shape, 0) >= shape_limits.get(shape, 3):
            return False
        if len(question.relevant_tables or []) >= 2:
            return True
        primary_table = (question.relevant_tables or [question.category])[0]
        return not table_counts.get(primary_table, 0) >= per_table_limit

    def add(question: SuggestedQuestion) -> None:
        selected.append(question)
        source_counts[question.source] = source_counts.get(question.source, 0) + 1
        shape = _question_shape(question.question)
        shape_counts[shape] = shape_counts.get(shape, 0) + 1
        primary_table = (question.relevant_tables or [question.category])[0]
        table_counts[primary_table] = table_counts.get(primary_table, 0) + 1

    lead_order = ("ranking", "trend", "other", "count", "distribution", "quality")
    for shape in lead_order:
        lead = next(
            (
                q
                for q in ranked
                if _question_shape(q.question) == shape and can_add(q)
            ),
            None,
        )
        if lead:
            add(lead)
        if len(selected) >= MAX_TOTAL_SUGGESTIONS:
            return selected

    for question in ranked:
        if len(selected) >= MAX_TOTAL_SUGGESTIONS:
            break
        if can_add(question):
            add(question)

    return selected


def _shape_allowed(
    question: SuggestedQuestion,
    shape_counts: dict[str, int],
) -> bool:
    shape = _question_shape(question.question)
    limits = {
        "trend": MAX_TREND_SUGGESTIONS,
        "average": MAX_AVERAGE_SUGGESTIONS,
        "count": MAX_COUNT_SUGGESTIONS,
    }
    limit = limits.get(shape)
    if limit is not None and shape_counts.get(shape, 0) >= limit:
        return False
    shape_counts[shape] = shape_counts.get(shape, 0) + 1
    return True


def _question_shape(question: str) -> str:
    q = " ".join(question.lower().split())
    if "changed over time" in q or q.startswith("how has "):
        return "trend"
    if q.startswith("how do ") and " compare" in q:
        return "comparison"
    if q.startswith("what is the average "):
        return "average"
    if q.startswith("how many "):
        return "count"
    if q.startswith("which ") and " highest " in q:
        return "ranking"
    if "distribution of" in q:
        return "distribution"
    if "drives " in q or "dominates " in q or "stand out" in q:
        return "ranking"
    if "missing values" in q or "duplicates" in q or "unexpected" in q:
        return "quality"
    return "other"


def _question_value_score(
    question: SuggestedQuestion,
    metadata: RetrievedMetadata | None = None,
) -> int:
    score = 0
    shape = _question_shape(question.question)
    shape_weight = {
        "ranking": 8,
        "trend": 5,
        "comparison": 6,
        "count": 4,
        "average": 3,
        "quality": 2,
        "distribution": -4,
        "other": 0,
    }
    score += shape_weight.get(shape, 0)
    if question.source in {"business", "cross_table", "catalog"}:
        score += 4
    elif question.source == "mart":
        score += 2
    if len(question.relevant_tables or []) >= 2:
        score += 2
    lower = question.question.lower()
    if "distribution of average" in lower or "distribution of avg " in lower:
        score -= 8
    if " summary" in lower:
        score -= 4
    if any(token in lower for token in ("highest", "changed over time", "how many")):
        score += 2
    if lower.startswith("how many "):
        score -= 2
    score += _decision_question_bonus(lower)
    score += _context_question_bonus(question.question, metadata)
    return score


def _decision_question_bonus(question: str) -> int:
    score = 0
    high_value_patterns = {
        "wait time": 8,
        "weekday and weekend": 7,
        "drives ": 7,
        "dominates ": 6,
        "longest ": 6,
        "routes have the longest": 7,
        "busiest": 6,
        "hour has the highest": 5,
        "highest": 3,
        "service": 2,
        "channel": 2,
        "payment": 1,
        "volume": 2,
    }
    for pattern, bonus in high_value_patterns.items():
        if pattern in question:
            score += bonus
    low_value_patterns = {
        "distribution of": -3,
        "what values stand out": -2,
    }
    for pattern, penalty in low_value_patterns.items():
        if pattern in question:
            score += penalty
    return score


def _from_business_insights(
    business_insights: list[dict] | None,
    con: duckdb.DuckDBPyConnection | None,
    tables: list[TableInfo],
    models: list[GeneratedModel],
    metadata: RetrievedMetadata | None,
) -> list[SuggestedQuestion]:
    suggestions: list[SuggestedQuestion] = []
    if not business_insights:
        return suggestions

    table_map = {table.name: table for table in tables}
    lookup_index = _build_lookup_index(tables, metadata)
    seen: set[str] = set()
    for insight in business_insights[:8]:
        table_name = insight.get("table")
        if not table_name or table_name not in table_map:
            continue
        table = table_map[table_name]
        ref = resolve_table_ref(table_name, con, models) if con is not None else table_name
        column = insight.get("column")
        group_by_column = insight.get("group_by_column")
        grain = insight.get("group_by_grain")
        chart_type = insight.get("chart_type")
        insight_id = str(insight.get("id", ""))

        question = None
        sql_hint = None
        if insight_id.startswith(("temporal_peak:", "metric_peak:")) and group_by_column:
            metric_expr = "COUNT(*)"
            metric_alias = "records"
            if insight.get("metric") == "period_total" and column:
                metric_expr = f'SUM("{column}")'
                metric_alias = f"total_{column}"
            period_expr = _time_bucket_expression(
                group_by_column,
                _column_dtype(table, group_by_column),
                grain,
            )
            question = (
                f"How has {_metric_question_label(_business_metric_label(insight), metadata)} "
                f"in {_table_label(table_name, metadata)} changed over time?"
            )
            sql_hint = (
                f"SELECT {period_expr} AS period, {metric_expr} AS {metric_alias} "
                f"FROM {ref} "
                f'WHERE "{group_by_column}" IS NOT NULL '
                f"GROUP BY 1 ORDER BY 1 LIMIT 100"
            )
        elif insight_id.startswith("metric_driver:") and column:
            group_expr, select_expr, join_sql, display_label = _dimension_projection(
                table,
                str(column),
                lookup_index,
                metadata,
            )
            question = (
                f"Which {display_label} drives {_business_metric_label(insight)} "
                f"in {_table_label(table_name, metadata)}?"
            )
            sql_hint = (
                f"SELECT {select_expr} AS dimension, "
                f'ROUND(SUM("{insight["metric"]}"), 2) AS total_value, '
                f"COUNT(*) AS records "
                f"FROM {ref} fact "
                f"{join_sql} "
                f'WHERE {group_expr} IS NOT NULL AND fact."{insight["metric"]}" IS NOT NULL '
                f"GROUP BY {group_expr} ORDER BY total_value DESC LIMIT 20"
            )
        elif insight_id.startswith("segment_concentration:") and column:
            group_expr, select_expr, join_sql, display_label = _dimension_projection(
                table,
                str(column),
                lookup_index,
                metadata,
            )
            question = (
                f"Which {display_label} segments dominate "
                f"{_table_label(table_name, metadata)}?"
            )
            sql_hint = (
                f"SELECT {select_expr} AS dimension, COUNT(*) AS records "
                f"FROM {ref} fact "
                f"{join_sql} "
                f"WHERE {group_expr} IS NOT NULL "
                f"GROUP BY {group_expr} ORDER BY records DESC LIMIT 20"
            )
        elif chart_type == "histogram" and column:
            question = (
                f"What is the distribution of {_humanize(column)} "
                f"in {_table_label(table_name, metadata)}?"
            )
            sql_hint = _distribution_sql(ref, str(column))

        if not question or not sql_hint:
            continue
        normalized = " ".join(question.lower().split())
        if normalized in seen:
            continue
        seen.add(normalized)
        suggestions.append(
            SuggestedQuestion(
                question=question,
                source="business",
                category="Business Signals",
                relevant_tables=[table_name],
                sql_hint=sql_hint,
            )
        )
    return suggestions


def _from_semantic_roles(
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None,
    metadata: RetrievedMetadata | None,
) -> list[SuggestedQuestion]:
    suggestions: list[SuggestedQuestion] = []
    semantic_schema = infer_semantic_schema(discovery, metadata.context if metadata else None)
    lookup_index = _build_lookup_index(discovery.tables, metadata)
    seen: set[str] = set()

    for table in discovery.tables:
        roles = roles_for_table(semantic_schema, table.name)
        if not roles:
            continue
        ref = resolve_table_ref(table.name, con, models) if con is not None else table.name
        table_label = _table_label(table.name, metadata)
        row_label = _row_subject_label(metadata, table_label)
        start = roles.get("lifecycle_start_ts") or roles.get("event_ts")
        end = roles.get("lifecycle_end_ts")
        request = roles.get("request_ts")
        origin = roles.get("origin_id") or roles.get("location_id")
        dest = roles.get("destination_id")
        service = roles.get("service_type")

        if start:
            start_expr = _timestamp_expression(table, start.column_name)
            suggestions.extend(
                _add_semantic_questions(
                    seen,
                    [
                        SuggestedQuestion(
                            question=(
                                f"Which hour has the highest {row_label} volume "
                                f"in {table_label}?"
                            ),
                            source="business",
                            category=_decision_category(metadata),
                            relevant_tables=[table.name],
                            sql_hint=(
                                f"SELECT EXTRACT(hour FROM {start_expr}) AS hour_of_day, "
                                f"COUNT(*) AS {row_label.replace(' ', '_')}_count "
                                f"FROM {ref} "
                                f"WHERE {start_expr} IS NOT NULL "
                                f"GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 24"
                            ),
                        ),
                    ],
                )
            )

        if start and end:
            start_expr = _timestamp_expression(table, start.column_name)
            duration_expr = _duration_minutes_expression(table, start.column_name, end.column_name)
            suggestions.extend(
                _add_semantic_questions(
                    seen,
                    [
                        SuggestedQuestion(
                            question=(
                                f"How do weekday and weekend "
                                f"{_row_subject_metric(metadata, 'duration')} "
                                f"compare in {table_label}?"
                            ),
                            source="business",
                            category=_decision_category(metadata),
                            relevant_tables=[table.name],
                            sql_hint=(
                                f"SELECT CASE "
                                f"WHEN EXTRACT(dow FROM {start_expr}) IN (0, 6) THEN 'Weekend' "
                                f"ELSE 'Weekday' END AS period_type, "
                                f"ROUND(AVG({duration_expr}), 2) AS avg_duration_min, "
                                f"COUNT(*) AS {row_label.replace(' ', '_')}_count "
                                f"FROM {ref} "
                                f"WHERE {start_expr} IS NOT NULL "
                                f"AND {duration_expr} BETWEEN 0 AND 1440 "
                                f"GROUP BY 1 ORDER BY avg_duration_min DESC"
                            ),
                        ),
                    ],
                )
            )
            if origin:
                group_expr, select_expr, join_sql, display_label = _dimension_projection(
                    table,
                    origin.column_name,
                    lookup_index,
                    metadata,
                )
                suggestions.extend(
                    _add_semantic_questions(
                        seen,
                        [
                            SuggestedQuestion(
                                question=(
                                    f"Which {display_label} has the longest "
                                    f"{_row_subject_metric(metadata, 'duration')} in {table_label}?"
                                ),
                                source="business",
                                category=_decision_category(metadata),
                                relevant_tables=[table.name],
                                sql_hint=(
                                    f"SELECT {select_expr} AS dimension, "
                                    f"ROUND(AVG({duration_expr}), 2) AS avg_duration_min, "
                                    f"COUNT(*) AS {row_label.replace(' ', '_')}_count "
                                    f"FROM {ref} fact "
                                    f"{join_sql} "
                                    f"WHERE {group_expr} IS NOT NULL "
                                    f"AND {duration_expr} BETWEEN 0 AND 1440 "
                                    f"GROUP BY {group_expr} ORDER BY avg_duration_min DESC LIMIT 20"
                                ),
                            ),
                        ],
                    )
                )
            if origin and dest:
                o_expr = f'fact."{origin.column_name}"'
                d_expr = f'fact."{dest.column_name}"'
                suggestions.extend(
                    _add_semantic_questions(
                        seen,
                        [
                            SuggestedQuestion(
                                question=(
                                    f"Which routes have the longest "
                                    f"{_row_subject_metric(metadata, 'duration')} in {table_label}?"
                                ),
                                source="business",
                                category=_decision_category(metadata),
                                relevant_tables=[table.name],
                                sql_hint=(
                                    f"SELECT CAST({o_expr} AS VARCHAR) || ' -> ' || "
                                    f"CAST({d_expr} AS VARCHAR) "
                                    f"AS route_pair, "
                                    f"ROUND(AVG({duration_expr}), 2) AS avg_duration_min, "
                                    f"COUNT(*) AS {row_label.replace(' ', '_')}_count "
                                    f"FROM {ref} fact "
                                    f"WHERE {o_expr} IS NOT NULL AND {d_expr} IS NOT NULL "
                                    f"AND {duration_expr} BETWEEN 0 AND 1440 "
                                    f"GROUP BY 1 ORDER BY avg_duration_min DESC LIMIT 20"
                                ),
                            ),
                        ],
                    )
                )

        if request and start and service:
            duration_expr = _duration_minutes_expression(
                table,
                request.column_name,
                start.column_name,
            )
            group_expr, select_expr, join_sql, display_label = _dimension_projection(
                table,
                service.column_name,
                lookup_index,
                metadata,
            )
            suggestions.extend(
                _add_semantic_questions(
                    seen,
                    [
                        SuggestedQuestion(
                            question=(
                                f"Which {display_label} has the highest "
                                f"{_row_subject_metric(metadata, 'wait time')} in {table_label}?"
                            ),
                            source="business",
                            category=_decision_category(metadata),
                            relevant_tables=[table.name],
                            sql_hint=(
                                f"SELECT {select_expr} AS dimension, "
                                    f"ROUND(AVG({duration_expr}), 2) AS avg_wait_min, "
                                    f"COUNT(*) AS {row_label.replace(' ', '_')}_count "
                                    f"FROM {ref} fact "
                                    f"{join_sql} "
                                    f"WHERE {group_expr} IS NOT NULL "
                                    f"AND {duration_expr} BETWEEN 0 AND 1440 "
                                    f"GROUP BY {group_expr} ORDER BY avg_wait_min DESC LIMIT 20"
                                ),
                            ),
                    ],
                )
            )
        elif start and service:
            start_expr = _timestamp_expression(table, start.column_name)
            group_expr, select_expr, join_sql, display_label = _dimension_projection(
                table,
                service.column_name,
                lookup_index,
                metadata,
            )
            suggestions.extend(
                _add_semantic_questions(
                    seen,
                    [
                        SuggestedQuestion(
                            question=(
                                f"Which {display_label} has the highest {row_label} volume "
                                f"in {table_label}?"
                            ),
                            source="business",
                            category=_decision_category(metadata),
                            relevant_tables=[table.name],
                            sql_hint=(
                                f"SELECT {select_expr} AS dimension, "
                                f"COUNT(*) AS {row_label.replace(' ', '_')}_count "
                                f"FROM {ref} fact "
                                f"{join_sql} "
                                f"WHERE {group_expr} IS NOT NULL AND {start_expr} IS NOT NULL "
                                f"GROUP BY {group_expr} ORDER BY 2 DESC LIMIT 20"
                            ),
                        ),
                    ],
                )
            )
    return suggestions


def _from_catalog(
    catalog,
    con: duckdb.DuckDBPyConnection | None = None,
    models: list[GeneratedModel] | None = None,
) -> list[SuggestedQuestion]:
    """Generate suggestions from semantic catalog metric x dimension cross-products.

    Every suggestion includes a valid sql_hint so clicking it works immediately.
    Table references are resolved to schema-qualified names (staging/marts).
    """
    suggestions: list[SuggestedQuestion] = []
    all_models = models or []

    def _ref(table_name: str) -> str:
        if con is not None:
            return resolve_table_ref(table_name, con, all_models)
        return f"staging.stg_{table_name}"

    for entity in catalog.entities:
        entity_metrics = [m for m in catalog.metrics if m.name in entity.metrics]
        entity_dims = [d for d in catalog.dimensions if d.name in entity.dimensions]

        for m in entity_metrics[:3]:
            if m.confidence < 0.5:
                continue
            t_ref = _ref(m.table)
            if m.agg_type == "count":
                suggestions.append(
                    SuggestedQuestion(
                        question=f"How many {entity.display_name.lower()}?",
                        source="catalog",
                        category="catalog",
                        relevant_tables=[entity.table],
                        sql_hint=f'SELECT {m.expression} AS "{m.display_name}" FROM {t_ref}',
                    )
                )

            for d in entity_dims[:4]:
                if d.confidence < 0.5:
                    continue
                sql = _build_catalog_sql(m, d, _ref)
                suggestions.append(
                    SuggestedQuestion(
                        question=f"{m.display_name} by {d.display_name}",
                        source="catalog",
                        category="catalog",
                        relevant_tables=[m.table, d.table] if m.table != d.table else [m.table],
                        sql_hint=sql,
                    )
                )
                if len(suggestions) >= 10:
                    return suggestions

    return suggestions


def _from_schema_graph(
    graph: SchemaGraph,
    relationships: list[Relationship],
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None,
    metadata: RetrievedMetadata | None,
) -> list[SuggestedQuestion]:
    """Generate cross-table analytical questions using SchemaGraph join paths.

    Finds directly related table pairs and generates questions combining
    metrics from one table with dimensions from another. Deeper join paths are
    left to NL-to-SQL planning; surfacing them as suggested questions is too
    noisy without stronger semantic confirmation.
    """
    suggestions: list[SuggestedQuestion] = []
    if len(graph.tables) < 2:
        return suggestions

    seen_combos: set[tuple[str, str, str]] = set()

    for fact_name, fact_node in graph.tables.items():
        if not fact_node.metrics:
            continue

        metric_col = _pick_cross_table_metric(fact_node)
        metric_label = _metric_question_label(metric_col, metadata) if metric_col else None

        for dim_name, dim_node in graph.tables.items():
            if dim_name == fact_name:
                continue
            if not dim_node.dimensions:
                continue

            # Find join path between fact and dim tables
            path = graph.find_join_path(fact_name, dim_name)
            if path is None:
                continue
            if len(path) > 1:
                continue

            dim_col_name = dim_node.dimensions[0]
            dim_label = _column_label(dim_col_name, metadata)
            fact_label = _table_label(fact_name, metadata)
            dim_table_label = _table_label(dim_name, metadata)

            combo_key = (fact_name, dim_name, dim_col_name)
            if combo_key in seen_combos:
                continue
            seen_combos.add(combo_key)

            # Build the SQL with proper joins
            fact_ref = (
                resolve_table_ref(fact_name, con, models) if con is not None else fact_name
            )
            join_clauses = []
            aliases = {fact_name: "t0"}

            for i, step in enumerate(path, 1):
                alias = f"t{i}"
                aliases[step.to_table] = alias
                step_ref = (
                    resolve_table_ref(step.to_table, con, models)
                    if con is not None
                    else step.to_table
                )
                from_alias = aliases.get(step.from_table, "t0")
                join_clauses.append(
                    f'JOIN {step_ref} {alias} ON {from_alias}."{step.from_column}" '
                    f'= {alias}."{step.to_column}"'
                )

            dim_alias = aliases.get(dim_name, "t0")
            fact_alias = aliases.get(fact_name, "t0")

            count_alias = f'{_humanize(fact_name).replace(" ", "_")}_count'
            if metric_col:
                sql = (
                    f'SELECT {dim_alias}."{dim_col_name}", '
                    f"COUNT(*) AS {count_alias}, "
                    f'ROUND(AVG({fact_alias}."{metric_col}"), 2) AS avg_{metric_col} '
                    f"FROM {fact_ref} t0 "
                    + " ".join(join_clauses)
                    + f' GROUP BY {dim_alias}."{dim_col_name}" '
                    f"ORDER BY avg_{metric_col} DESC LIMIT 20"
                )
                question = (
                    f"What is the average {metric_label} in {fact_label} "
                    f"by {dim_label} ({dim_table_label})?"
                )
            else:
                sql = (
                    f'SELECT {dim_alias}."{dim_col_name}", '
                    f"COUNT(*) AS {count_alias} "
                    f"FROM {fact_ref} t0 "
                    + " ".join(join_clauses)
                    + f' GROUP BY {dim_alias}."{dim_col_name}" '
                    f"ORDER BY {count_alias} DESC LIMIT 20"
                )
                question = (
                    f"How many {_row_subject_label(metadata, fact_label)} are there by "
                    f"{dim_label} ({dim_table_label})?"
                )

            suggestions.append(
                SuggestedQuestion(
                    question=question,
                    source="cross_table",
                    category="Cross-Table Analysis",
                    relevant_tables=[fact_name, dim_name],
                    sql_hint=sql,
                )
            )

            if len(suggestions) >= 6:
                return suggestions

    return suggestions


def _pick_cross_table_metric(fact_node) -> str | None:
    scored: list[tuple[int, str]] = []
    for metric in fact_node.metrics:
        score = _metric_question_score(metric)
        if score > 0:
            scored.append((score, metric))
    if not scored:
        return None
    scored.sort(key=lambda item: (-item[0], item[1]))
    return scored[0][1]


def _metric_question_score(column_name: str) -> int:
    lower = column_name.lower()
    if any(token in lower for token in ("age", "birth", "year_of_birth", "lat", "lon")):
        return -5
    score = 0
    strong_tokens = (
        "amount", "total", "cost", "price", "fare", "revenue", "sales", "value",
        "score", "severity", "duration", "elapsed", "distance", "miles", "rate",
        "percent", "ratio", "count", "qty", "quantity", "avg", "mean", "p90", "p95",
    )
    if any(token in lower for token in strong_tokens):
        score += 10
    if any(token in lower for token in ("current", "valid", "net", "gross")):
        score += 2
    if lower.endswith("_id") or lower.endswith("_code"):
        score -= 10
    return score


def _build_catalog_sql(metric, dimension, ref_fn=None) -> str:
    """Build SQL for a metric x dimension suggestion.

    ref_fn: callable that maps raw table name to schema-qualified reference.
    """
    m_alias = metric.display_name.lower().replace(" ", "_")
    d_alias = dimension.display_name.lower().replace(" ", "_")

    def _ref(name: str) -> str:
        if ref_fn:
            return ref_fn(name)
        return f'"{name}"'

    d_ref = _ref(dimension.table)
    d_col = f'{d_ref}."{dimension.column}"'

    select = f'{d_col} AS "{d_alias}", {metric.expression} AS "{m_alias}"'
    from_clause = _ref(metric.table)

    join = ""
    if dimension.table != metric.table and dimension.join_path:
        import re as _re

        match = _re.match(r"(\w+)\.(\w+)\s*->\s*(\w+)\.(\w+)", dimension.join_path)
        if match:
            from_t, from_c, to_t, to_c = match.groups()
            from_r = _ref(from_t)
            to_r = _ref(to_t)
            join_type = "LEFT JOIN" if dimension.join_nullable else "JOIN"
            join = f'\n{join_type} {to_r} ON {from_r}."{from_c}" = {to_r}."{to_c}"'

    order = f"{metric.expression} DESC"
    return f"SELECT {select}\nFROM {from_clause}{join}\nGROUP BY {d_col}\nORDER BY {order}"


# ---------------------------------------------------------------------------
# Mart-derived questions
# ---------------------------------------------------------------------------


def _from_mart_models(
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None,
    metadata: RetrievedMetadata | None,
) -> list[SuggestedQuestion]:
    """Generate analytical questions from mart model definitions.

    Uses the mart's name, description, and source_tables -- no hardcoded content.
    Only generates questions for marts that are materialized (status == "executed")
    or have been approved; the sql_hint targets `marts.{name}` which must exist.
    """
    questions: list[SuggestedQuestion] = []

    for model in models:
        if model.model_type != "mart":
            continue
        # Only suggest queries against marts we know are materialized
        if con is not None and not table_exists(con, "marts", model.name):
            continue

        label = _table_label(model.name, metadata)
        ref = f"marts.{model.name}"
        cols = _mart_columns(con, model.name) if con is not None else []
        row_count = _mart_row_count(con, model.name) if con is not None else None
        if row_count is not None and row_count <= 1:
            continue
        temporal = _pick_mart_temporal(cols)
        metric = _pick_mart_metric(cols)
        dimension = _pick_mart_dimension(cols, temporal, metric)

        if temporal and metric:
            temporal_dtype = next((dtype for name, dtype in cols if name == temporal), "")
            period_expr = _time_bucket_expression(temporal, temporal_dtype)
            question = (
                f"How has {_metric_question_label(metric, metadata)} "
                f"in {label} changed over time?"
            )
            sql_hint = (
                f"SELECT {period_expr} AS period, "
                f'ROUND(AVG("{metric}"), 2) AS avg_{metric}, '
                f"COUNT(*) AS records "
                f"FROM {ref} "
                f'WHERE "{temporal}" IS NOT NULL '
                f"GROUP BY 1 ORDER BY 1 LIMIT 100"
            )
        elif dimension and metric:
            group_expr, select_expr, join_sql, display_label = _dimension_projection(
                None,
                dimension,
                {},
                metadata,
            )
            question = (
                f"Which {display_label} has the highest "
                f"{_metric_question_label(metric, metadata)} in {label}?"
            )
            sql_hint = (
                f"SELECT {select_expr} AS dimension, "
                f"COUNT(*) AS records, "
                f'ROUND(AVG("{metric}"), 2) AS avg_{metric} '
                f"FROM {ref} fact "
                f"{join_sql} "
                f"GROUP BY {group_expr} ORDER BY avg_{metric} DESC LIMIT 20"
            )
        elif metric:
            if _is_aggregate_metric_name(metric) or _looks_like_summary_model(model.name, label):
                continue
            question = (
                f"What is the distribution of {_metric_question_label(metric, metadata)} "
                f"in {label}?"
            )
            sql_hint = _distribution_sql(ref, metric)
        else:
            question = _fallback_mart_question(model.name, label)
            sql_hint = f"SELECT * FROM {ref} LIMIT 50"

        questions.append(
            SuggestedQuestion(
                question=question,
                source="mart",
                category=label.title(),
                relevant_tables=model.source_tables,
                sql_hint=sql_hint,
            )
        )

    return questions


def _mart_columns(
    con: duckdb.DuckDBPyConnection,
    model_name: str,
) -> list[tuple[str, str]]:
    try:
        rows = con.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'marts' AND table_name = ?
            ORDER BY ordinal_position
            """,
            (model_name,),
        ).fetchall()
    except Exception:
        return []
    return [(str(row[0]), str(row[1])) for row in rows]


def _mart_row_count(
    con: duckdb.DuckDBPyConnection,
    model_name: str,
) -> int | None:
    try:
        row = con.execute(f"SELECT COUNT(*) FROM marts.{model_name}").fetchone()
    except Exception:
        return None
    return int(row[0]) if row else None


def _pick_mart_temporal(cols: list[tuple[str, str]]) -> str | None:
    for name, dtype in cols:
        lower = name.lower()
        if lower == "period" or any(
            token in lower for token in ("date", "time", "month", "year")
        ):
            return name
        if any(token in dtype.lower() for token in _TEMPORAL_DTYPES):
            return name
    return None


def _pick_mart_metric(cols: list[tuple[str, str]]) -> str | None:
    preferred = []
    fallback = []
    for name, dtype in cols:
        lower = name.lower()
        if _ID_NAME_RE.search(name) or lower in {"period"}:
            continue
        if not any(token in dtype.lower() for token in _NUMERIC_DTYPES):
            continue
        if any(token in lower for token in ("avg", "p90", "p95", "count", "total", "sum", "rate")):
            preferred.append(name)
        else:
            fallback.append(name)
    return (preferred or fallback or [None])[0]


def _is_aggregate_metric_name(name: str) -> bool:
    lower = name.lower()
    return bool(_AGGREGATE_METRIC_RE.match(lower))


def _looks_like_summary_model(*parts: str) -> bool:
    return any(_SUMMARY_NAME_RE.search(part) for part in parts if part)


def _pick_mart_dimension(
    cols: list[tuple[str, str]],
    temporal: str | None,
    metric: str | None,
) -> str | None:
    for name, dtype in cols:
        if name in {temporal, metric}:
            continue
        lower = name.lower()
        if "store_and_fwd" in lower or any(token in lower for token in ("flag", "indicator")):
            continue
        if _ID_NAME_RE.search(name) and not any(
            token in lower for token in ("zone", "location", "site")
        ):
            continue
        if any(token in dtype.lower() for token in _NUMERIC_DTYPES):
            continue
        return name
    return None


def _fallback_mart_question(model_name: str, label: str) -> str:
    lower = model_name.lower()
    if "_by_period" in lower or lower.endswith("by_period"):
        return f"How has {label.replace(' by period', '')} changed over time?"
    if "_by_" in lower:
        dim = lower.rsplit("_by_", 1)[-1]
        return f"Which {_humanize(dim)} stand out in {label}?"
    return f"What values stand out in {label}?"


# ---------------------------------------------------------------------------
# Relationship-derived questions
# ---------------------------------------------------------------------------


def _from_relationships(
    tables: list[TableInfo],
    relationships: list[Relationship],
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None,
    metadata: RetrievedMetadata | None,
) -> list[SuggestedQuestion]:
    """Generate cross-entity questions from detected foreign key relationships.

    For each relationship A.col -> B.col, generates a question about the
    distribution of A records per B entity using the actual column names.
    """
    questions: list[SuggestedQuestion] = []
    table_map = {t.name: t for t in tables}
    lookup_index = _build_lookup_index(tables, metadata)
    seen_pairs: set[frozenset[str]] = set()

    for rel in relationships:
        if rel.from_table not in table_map or rel.to_table not in table_map:
            continue

        pair = frozenset([rel.from_table, rel.to_table])
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        from_label = _table_label(rel.from_table, metadata)

        from_ref = (
            resolve_table_ref(rel.from_table, con, models) if con is not None else rel.from_table
        )
        to_ref = resolve_table_ref(rel.to_table, con, models) if con is not None else rel.to_table

        # Find a useful metric from the from_table to aggregate
        from_table_info = table_map[rel.from_table]
        metric_col = _pick_metric_col(from_table_info)
        to_table_info = table_map[rel.to_table]
        dim_candidates = _prefer_display_dim(
            [c.name for c in to_table_info.columns if c.name != rel.to_column],
            rel.to_table,
        )
        target_dim = dim_candidates[0] if dim_candidates else rel.to_column
        group_expr, select_expr, join_sql, display_label = _dimension_projection(
            to_table_info,
            target_dim,
            lookup_index,
            metadata,
            alias="t",
        )

        if metric_col:
            sql = (
                f"SELECT {select_expr} AS dimension, COUNT(*) AS {from_label}_count, "
                f'AVG(f."{metric_col}") AS avg_{metric_col} '
                f"FROM {from_ref} f "
                f'JOIN {to_ref} t ON f."{rel.from_column}" = t."{rel.to_column}" '
                f"{join_sql} "
                f"GROUP BY {group_expr} "
                f"ORDER BY {from_label}_count DESC LIMIT 20"
            )
        else:
            sql = (
                f"SELECT {select_expr} AS dimension, COUNT(*) AS {from_label}_count "
                f"FROM {from_ref} f "
                f'JOIN {to_ref} t ON f."{rel.from_column}" = t."{rel.to_column}" '
                f"{join_sql} "
                f"GROUP BY {group_expr} "
                f"ORDER BY {from_label}_count DESC LIMIT 20"
            )

        questions.append(
            SuggestedQuestion(
                question=(
                    f"How many {_row_subject_label(metadata, from_label)} "
                    f"are there per {display_label}?"
                ),
                source="relationship",
                category="Cross-Entity Analysis",
                relevant_tables=[rel.from_table, rel.to_table],
                sql_hint=sql,
            )
        )

    return questions


# ---------------------------------------------------------------------------
# Table-structure-derived questions (semantic)
# ---------------------------------------------------------------------------


def _from_table_structure(
    tables: list[TableInfo],
    profile_index: dict[tuple[str, str], ColumnProfile],
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None,
    metadata: RetrievedMetadata | None,
) -> list[SuggestedQuestion]:
    """Generate analytical questions by inspecting each table's actual column structure.

    For each table, identifies temporal, metric, and dimension columns and
    generates the most relevant question type:
      - temporal + metric  -> trend over time
      - dimension + metric -> breakdown / ranking
      - metric only        -> summary statistics
    """
    questions: list[SuggestedQuestion] = []
    lookup_index = _build_lookup_index(tables, metadata)

    for table in tables:
        ref = resolve_table_ref(table.name, con, models) if con is not None else table.name
        label = _table_label(table.name, metadata)

        temporal_cols = _get_temporal_cols(table)
        metric_cols = _get_metric_cols(table, profile_index)
        dim_cols = _prefer_display_dim(_get_dimension_cols(table, profile_index), table.name)

        if temporal_cols and metric_cols:
            t_col = temporal_cols[0]
            m_col = metric_cols[0]
            t_dtype = next((c.dtype for c in table.columns if c.name == t_col), "")
            period_expr = _time_bucket_expression(t_col, t_dtype)
            questions.append(
                SuggestedQuestion(
                    question=(
                        f"How has {_metric_question_label(m_col, metadata)} "
                        f"in {label} changed over time?"
                    ),
                    source="semantic",
                    category=label.title(),
                    relevant_tables=[table.name],
                    sql_hint=(
                        f"SELECT {period_expr} AS period, AVG(\"{m_col}\") AS avg_{m_col}, "
                        f"COUNT(*) AS records "
                        f"FROM {ref} "
                        f'WHERE "{t_col}" IS NOT NULL '
                        f"GROUP BY 1 ORDER BY 1 LIMIT 100"
                    ),
                )
            )

        if dim_cols and metric_cols:
            # Generate questions for up to 2 distinct dimensions (e.g. county
            # and state) so both geographic levels get coverage.
            dim_limit = min(len(dim_cols), 2)
            for d_col in dim_cols[:dim_limit]:
                m_col = metric_cols[0]
                group_expr, select_expr, join_sql, display_label = _dimension_projection(
                    table,
                    d_col,
                    lookup_index,
                    metadata,
                )
                questions.append(
                    SuggestedQuestion(
                        question=(
                            f"Which {display_label} has the highest "
                            f"{_metric_question_label(m_col, metadata)} in {label}?"
                        ),
                        source="semantic",
                        category=label.title(),
                        relevant_tables=[table.name],
                        sql_hint=(
                            f"SELECT {select_expr} AS dimension, "
                            f"COUNT(*) AS records, "
                            f'ROUND(AVG("{m_col}"), 2) AS avg_{m_col}, '
                            f'MAX("{m_col}") AS max_{m_col} '
                            f"FROM {ref} fact "
                            f"{join_sql} "
                            f"GROUP BY {group_expr} "
                            f"ORDER BY avg_{m_col} DESC LIMIT 20"
                        ),
                    )
                )

        if metric_cols and not temporal_cols and not dim_cols:
            m_col = metric_cols[0]
            questions.append(
                SuggestedQuestion(
                    question=(
                        f"What is the distribution of {_metric_question_label(m_col, metadata)} "
                        f"in {label}?"
                    ),
                    source="semantic",
                    category=label.title(),
                    relevant_tables=[table.name],
                    sql_hint=_distribution_sql(ref, m_col),
                )
            )

    return questions


# ---------------------------------------------------------------------------
# Quality-derived questions (lowest priority, heavily capped)
# ---------------------------------------------------------------------------


def _from_quality_findings(
    contracts: list[ContractRule],
    results: list[ContractCheckResult],
    profile_index: dict[tuple[str, str], ColumnProfile],
    tables: list[TableInfo],
) -> list[SuggestedQuestion]:
    """Generate data quality investigation questions from failed contract checks.

    Only surfaces questions that are analytically meaningful:
    - not_null: only for numeric metric columns with actual nulls
    - cardinality/unique: included but counted against the cap

    Hard cap: MAX_QUALITY_SUGGESTIONS. Quality never dominates the list.
    """
    table_map = {t.name: t for t in tables}
    failed_ids = {r.rule_id for r in results if not r.passed}
    questions: list[SuggestedQuestion] = []
    seen: set[tuple[str, str, str]] = set()

    for rule in contracts:
        if len(questions) >= MAX_QUALITY_SUGGESTIONS:
            break
        if rule.id not in failed_ids:
            continue

        col = rule.column_name or ""
        dedup_key = (rule.model_name, col, rule.rule_type)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        base_table = _humanize_model(rule.model_name)

        if rule.rule_type == "not_null" and col:
            if not _is_metric_col(base_table, col, profile_index, table_map):
                continue
            questions.append(
                SuggestedQuestion(
                    question=(f"Why are there missing values in {base_table} {col}?"),
                    source="quality",
                    category="Data Quality",
                    relevant_tables=[rule.model_name],
                    sql_hint=(f'SELECT * FROM {rule.model_name} WHERE "{col}" IS NULL LIMIT 20'),
                )
            )

        elif rule.rule_type == "cardinality" and col:
            questions.append(
                SuggestedQuestion(
                    question=(f"What unexpected {col} values appeared in {base_table}?"),
                    source="quality",
                    category="Data Quality",
                    relevant_tables=[rule.model_name],
                    sql_hint=(
                        f'SELECT "{col}", COUNT(*) AS cnt '
                        f"FROM {rule.model_name} "
                        f'GROUP BY "{col}" ORDER BY cnt DESC'
                    ),
                )
            )

        elif rule.rule_type == "unique" and col:
            questions.append(
                SuggestedQuestion(
                    question=(f"Which {col} values have duplicates in {base_table}?"),
                    source="quality",
                    category="Data Quality",
                    relevant_tables=[rule.model_name],
                    sql_hint=(
                        f'SELECT "{col}", COUNT(*) AS cnt '
                        f"FROM {rule.model_name} "
                        f'GROUP BY "{col}" HAVING cnt > 1 '
                        f"ORDER BY cnt DESC LIMIT 20"
                    ),
                )
            )

    return questions


# ---------------------------------------------------------------------------
# Column classification helpers
# ---------------------------------------------------------------------------


def _get_temporal_cols(table: TableInfo) -> list[str]:
    """Return temporal column names, preferring date/timestamp dtypes first."""
    raw = [
        c
        for c in table.columns
        if not c.is_primary_key
        and not _ID_NAME_RE.search(c.name)
        and (
            any(c.dtype.lower().startswith(t) for t in _TEMPORAL_DTYPES)
            or c.semantic_type == "temporal"
            or bool(_TEMPORAL_NAME_RE.search(c.name))
        )
    ]
    # Prefer actual date/timestamp dtypes over name-pattern matches (year, month)
    raw.sort(key=lambda c: 0 if any(c.dtype.lower().startswith(t) for t in _TEMPORAL_DTYPES) else 1)
    return [c.name for c in raw]


def _get_metric_cols(
    table: TableInfo,
    profile_index: dict[tuple[str, str], ColumnProfile],
) -> list[str]:
    scored: list[tuple[int, str]] = []
    for c in table.columns:
        profile = profile_index.get((table.name, c.name))
        if is_metric_column(c, profile):
            score = _metric_signal_score(c, profile)
            if score > 0:
                scored.append((score, c.name))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [name for _score, name in scored]


def _get_dimension_cols(
    table: TableInfo,
    profile_index: dict[tuple[str, str], ColumnProfile],
) -> list[str]:
    """Return low-cardinality columns suitable for GROUP BY."""
    scored: list[tuple[int, str]] = []
    for c in table.columns:
        profile = profile_index.get((table.name, c.name))
        if (
            is_dimension_column(c, profile)
            and not _is_low_signal_dimension_for_question(c, profile)
        ):
            score = _dimension_signal_score(c, profile, table.name)
            if score > 0:
                scored.append((score, c.name))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [name for _score, name in scored]


def _pick_metric_col(table: TableInfo) -> str | None:
    """Pick the first non-ID, non-code numeric column from a table."""
    for c in table.columns:
        if is_metric_column(c):
            return c.name
    return None


def _is_metric_col(
    table_name: str,
    column_name: str,
    profile_index: dict[tuple[str, str], ColumnProfile],
    table_map: dict[str, TableInfo],
) -> bool:
    """Return True if the column is a numeric metric."""
    table = table_map.get(table_name)
    if table:
        col_info = next(
            (c for c in table.columns if c.name == column_name),
            None,
        )
        if col_info:
            profile = profile_index.get((table_name, column_name))
            return is_metric_column(col_info, profile)
    # Fallback when table metadata is unavailable
    if _ID_NAME_RE.search(column_name) or _TEMPORAL_NAME_RE.search(column_name):
        return False
    profile = profile_index.get((table_name, column_name))
    if profile is None:
        return False
    return any(t in profile.dtype.lower() for t in _NUMERIC_DTYPES)


def _is_low_signal_dimension_for_question(
    column: ColumnInfo,
    profile: ColumnProfile | None,
) -> bool:
    lower = column.name.lower()
    distinct = profile.distinct_count if profile is not None else 0
    business_named = any(token in lower for token in _BUSINESS_DIMENSION_TOKENS)
    technical_named = any(token in lower for token in _LOW_SIGNAL_DIMENSION_TOKENS)

    if technical_named and distinct <= 3:
        return True
    if profile is None or not profile.top_values or business_named:
        return False
    values = {str(value).strip().lower() for value, _count in profile.top_values[:4]}
    return distinct <= 3 and values <= _BOOLEANISH_VALUES


def _metric_signal_score(
    column: ColumnInfo,
    profile: ColumnProfile | None,
) -> int:
    score = _metric_question_score(column.name)
    if score <= 0:
        return score
    if profile is None:
        return score
    if profile.null_rate >= 0.98:
        return -10
    if profile.distinct_count <= 1:
        return -10
    if profile.distinct_count <= 3:
        score -= 4
    elif profile.distinct_count >= 20:
        score += 3
    elif profile.distinct_count >= 5:
        score += 1
    if profile.stddev is not None and profile.stddev > 0:
        score += 2
    if profile.mean is not None and profile.stddev == 0:
        score -= 3
    return score


def _dimension_signal_score(
    column: ColumnInfo,
    profile: ColumnProfile | None,
    table_name: str,
) -> int:
    lower = column.name.lower()
    table_words = set(table_name.lower().replace("_", " ").split()) if table_name else set()
    col_words = set(lower.replace("_", " ").split())
    score = 0

    if col_words & table_words:
        score += 4
    if any(token in lower for token in ("_name", "name_", "label", "description", "title")):
        score += 4
    elif any(token in lower for token in ("_code", "code_", "_num", "_id", "_key")):
        score -= 4
    if any(token in lower for token in _BUSINESS_DIMENSION_TOKENS):
        score += 3

    if profile is None:
        return score + 1
    if profile.null_rate >= 0.98 or profile.distinct_count <= 1:
        return -10
    distinct = profile.distinct_count
    if 2 <= distinct <= 20:
        score += 4
    elif distinct <= 50:
        score += 3
    elif distinct <= 200:
        score += 1
    else:
        score -= 2
    if profile.top_values:
        total = sum(count for _value, count in profile.top_values[:5])
        if total > 0:
            top_share = profile.top_values[0][1] / total
            if top_share >= 0.95:
                score -= 5
            elif top_share >= 0.85:
                score -= 2
    return score


# ---------------------------------------------------------------------------
# String helpers
# ---------------------------------------------------------------------------


def _prefer_display_dim(dim_cols: list[str], table_name: str = "") -> list[str]:
    """Sort dimension columns: table-name match > names > plain > codes.

    Suggestions read better with human-readable columns (state_name)
    than with code columns (state_code) or identifiers (state_id).

    Columns whose name appears in the table name rank highest -- if the table
    is ``aqi_by_county``, the ``county`` column is the most natural dimension.
    """
    table_words = set(table_name.lower().replace("_", " ").split()) if table_name else set()

    def _rank(col: str) -> tuple[int, int]:
        lower = col.lower()
        col_words = set(lower.replace("_", " ").split())

        # Primary: boost columns whose name overlaps with the table name
        table_affinity = 0 if (col_words & table_words) else 1

        # Secondary: human-readable names > plain > codes
        if any(s in lower for s in ("_name", "name_", "label", "description")):
            display = 0  # Best: human-readable names
        elif any(s in lower for s in ("_code", "code_", "_num", "_id", "_key")):
            display = 2  # Worst: codes/IDs
        else:
            display = 1  # Middle: plain column names

        return (table_affinity, display)

    return sorted(dim_cols, key=_rank)


def _time_bucket_expression(column_name: str, dtype: str, grain: str | None = None) -> str:
    quoted = f'"{column_name}"'
    lower = column_name.lower()
    dtype_lower = dtype.lower()
    if lower == "year" or lower.endswith("_year"):
        return f"CAST({quoted} AS VARCHAR)"
    if lower == "month" or lower.endswith("_month"):
        return f"CAST({quoted} AS VARCHAR)"
    if grain == "hour":
        return f"strftime(CAST({quoted} AS TIMESTAMP), '%Y-%m-%d %H:00')"
    if grain == "month":
        return f"strftime(CAST({quoted} AS TIMESTAMP), '%Y-%m')"
    if grain == "year":
        return f"strftime(CAST({quoted} AS TIMESTAMP), '%Y')"
    if any(token in dtype_lower for token in _TEMPORAL_DTYPES) or bool(
        _TEMPORAL_NAME_RE.search(column_name)
    ):
        return f"CAST({quoted} AS DATE)"
    return quoted


def _column_dtype(table: TableInfo, column_name: str) -> str:
    column = next((col for col in table.columns if col.name == column_name), None)
    return column.dtype if column else ""


def _business_metric_label(insight: dict) -> str:
    metric = str(insight.get("metric") or "")
    column = str(insight.get("column") or "")
    if metric == "record_volume":
        return f"{_humanize(insight['table'])} volume"
    if metric == "period_total" and column:
        return _humanize(column)
    if metric == "segment_share" and column:
        return _humanize(column)
    if column:
        return _humanize(column)
    return _humanize(str(insight.get("table") or "activity"))


def _glossary_label(name: str, metadata: RetrievedMetadata | None) -> str | None:
    if metadata is None:
        return None
    terms = (name.lower(), _humanize(name).lower(), name.lower().replace("_", " "))
    for term in terms:
        description = metadata.glossary.get(term)
        if not description:
            continue
        first = re.split(r"[.;(]", description, maxsplit=1)[0].strip()
        words = first.split()
        if 1 < len(words) <= 6:
            return first.lower()
    return None


def _table_label(name: str, metadata: RetrievedMetadata | None) -> str:
    return _glossary_label(name, metadata) or _humanize(name)


def _metric_question_label(name: str, metadata: RetrievedMetadata | None = None) -> str:
    glossary = _glossary_label(name, metadata)
    if glossary:
        return glossary
    lower = name.lower()
    if lower.startswith("avg_"):
        return f"average {_humanize(name[4:])}"
    if lower.startswith("mean_"):
        return f"average {_humanize(name[5:])}"
    if lower.startswith("median_"):
        return f"median {_humanize(name[7:])}"
    if re.match(r"^p\d+_", lower):
        prefix, rest = lower.split("_", 1)
        return f"{prefix} {_humanize(rest)}"
    if lower.startswith("total_"):
        return f"total {_humanize(name[6:])}"
    if lower.endswith("_count") and len(name) > 6:
        return f"{_humanize(name[:-6])} count"
    return _humanize(name)


def _column_label(name: str, metadata: RetrievedMetadata | None) -> str:
    return _glossary_label(name, metadata) or _humanize(name)


def _row_subject_label(metadata: RetrievedMetadata | None, fallback: str) -> str:
    phrase = metadata.context.row_represents if metadata and metadata.context else None
    if not phrase:
        return fallback
    phrase = phrase.strip().lower()
    if not phrase:
        return fallback
    return phrase if phrase.endswith("s") else f"{phrase}s"


def _context_question_bonus(
    question: str,
    metadata: RetrievedMetadata | None,
) -> int:
    if metadata is None or metadata.context is None or not metadata.context.decisions:
        return 0
    decisions = metadata.context.decisions.lower()
    q = question.lower()
    score = 0
    if (
        any(token in decisions for token in ("operation", "dispatch", "capacity", "service"))
        and any(
            token in q
            for token in ("changed over time", "how many", "duration", "wait", "hour")
        )
    ):
        score += 3
    if (
        any(token in decisions for token in ("revenue", "pricing", "sales", "finance"))
        and any(token in q for token in ("amount", "fare", "price", "revenue", "tip"))
    ):
        score += 3
    if (
        any(token in decisions for token in ("compliance", "quality", "audit", "risk"))
        and any(token in q for token in ("missing values", "unexpected", "duplicates", "status"))
    ):
        score += 3
    return score


def _decision_category(metadata: RetrievedMetadata | None) -> str:
    decisions = (
        metadata.context.decisions.lower()
        if metadata and metadata.context and metadata.context.decisions
        else ""
    )
    if any(token in decisions for token in ("revenue", "pricing", "sales", "finance")):
        return "Revenue Signals"
    if any(token in decisions for token in ("compliance", "quality", "audit", "risk")):
        return "Compliance Signals"
    if any(token in decisions for token in ("operation", "dispatch", "capacity", "service")):
        return "Operational Signals"
    return "Decision Signals"


def _row_subject_metric(metadata: RetrievedMetadata | None, fallback: str) -> str:
    phrase = metadata.context.row_represents if metadata and metadata.context else None
    if not phrase:
        return fallback
    phrase = phrase.strip().lower()
    if not phrase:
        return fallback
    return f"{phrase} {fallback}"


def _add_semantic_questions(
    seen: set[str],
    questions: list[SuggestedQuestion],
) -> list[SuggestedQuestion]:
    added: list[SuggestedQuestion] = []
    for question in questions:
        key = " ".join(question.question.lower().split())
        if key in seen:
            continue
        seen.add(key)
        added.append(question)
    return added


def _humanize(name: str) -> str:
    """Convert snake_case or prefixed model names to readable label."""
    name = name.split(".")[-1]  # drop schema prefix
    for prefix in ("mart_", "stg_"):
        if name.startswith(prefix):
            name = name[len(prefix) :]
    # Strip trailing numeric suffixes that look like source identifiers
    name = re.sub(r"_\d+$", "", name)
    return name.replace("_", " ")


def _humanize_model(model_name: str) -> str:
    """Convert staging.stg_readings -> readings."""
    return _humanize(model_name)


def _build_lookup_index(
    tables: list[TableInfo],
    metadata: RetrievedMetadata | None = None,
) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    if metadata is not None:
        for table_name, lookup in metadata.lookup_tables.items():
            index.setdefault(
                lookup["id_column"].lower(),
                {
                    "table_name": table_name,
                    "id_column": lookup["id_column"],
                    "label_column": lookup["label_column"],
                },
            )
    for table in tables:
        id_cols = [col.name for col in table.columns if _ID_NAME_RE.search(col.name)]
        label_cols = [
            col.name
            for col in table.columns
            if any(
                token in col.name.lower()
                for token in (
                    "name",
                    "label",
                    "description",
                    "zone",
                    "borough",
                    "region",
                    "title",
                )
            )
        ]
        if not id_cols or not label_cols or table.row_count > 100_000:
            continue
        for id_col in id_cols:
            index.setdefault(
                id_col.lower(),
                {
                    "table_name": table.name,
                    "id_column": id_col,
                    "label_column": label_cols[0],
                },
            )
    return index


def _sql_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _enum_case_expression(
    column_name: str,
    raw_expr: str,
    metadata: RetrievedMetadata | None = None,
) -> str | None:
    column_key = column_name.lower()
    metadata_mapping = metadata.enum_mappings.get(column_key) if metadata else None
    if metadata_mapping:
        cases = []
        for value, label in metadata_mapping.items():
            cases.append(
                f"WHEN CAST({raw_expr} AS VARCHAR) = {_sql_string_literal(value)} "
                f"THEN {_sql_string_literal(label)}"
            )
        return (
            "CASE "
            + " ".join(cases)
            + f" ELSE CONCAT('Unknown (', CAST({raw_expr} AS VARCHAR), ')') END"
        )

    mapping = _ENUM_LABEL_REGISTRY.get(column_key)
    if not mapping:
        return None
    cases = []
    for value, label in mapping.items():
        value_sql = _sql_string_literal(value) if isinstance(value, str) else str(value)
        cases.append(f"WHEN {raw_expr} = {value_sql} THEN {_sql_string_literal(label)}")
    return (
        "CASE "
        + " ".join(cases)
        + f" ELSE CONCAT('Unknown (', CAST({raw_expr} AS VARCHAR), ')') END"
    )


def _dimension_projection(
    table: TableInfo | None,
    column_name: str,
    lookup_index: dict[str, dict[str, str]],
    metadata: RetrievedMetadata | None = None,
    alias: str = "fact",
) -> tuple[str, str, str, str]:
    raw_expr = f'{alias}."{column_name}"'
    enum_expr = _enum_case_expression(column_name, raw_expr, metadata)
    if enum_expr:
        return (
            enum_expr,
            enum_expr,
            "",
            _ENUM_DIMENSION_LABELS.get(column_name.lower(), _column_label(column_name, metadata)),
        )

    lookup = lookup_index.get(column_name.lower())
    if lookup and table is not None and lookup["table_name"] != table.name:
        join_alias = f"{alias}_lu"
        join_sql = (
            f'LEFT JOIN "{lookup["table_name"]}" {join_alias} '
            f'ON {raw_expr} = {join_alias}."{lookup["id_column"]}"'
        )
        label_expr = (
            f"COALESCE(CAST({join_alias}.\"{lookup['label_column']}\" AS VARCHAR), "
            f"CAST({raw_expr} AS VARCHAR))"
        )
        return (
            label_expr,
            label_expr,
            join_sql,
            _column_label(lookup["label_column"], metadata),
        )

    return raw_expr, raw_expr, "", _column_label(column_name, metadata)


def _timestamp_expression(table: TableInfo, column_name: str) -> str:
    dtype = _column_dtype(table, column_name).lower()
    quoted = f'"{column_name}"'
    if "timestamp" in dtype:
        return quoted
    return f"CAST({quoted} AS TIMESTAMP)"


def _duration_minutes_expression(
    table: TableInfo,
    start_column: str,
    end_column: str,
) -> str:
    start_expr = _timestamp_expression(table, start_column)
    end_expr = _timestamp_expression(table, end_column)
    return f"date_diff('second', {start_expr}, {end_expr}) / 60.0"


def _distribution_sql(ref: str, metric: str) -> str:
    value_expr = f'CAST("{metric}" AS DOUBLE)'
    return f"""
WITH stats AS (
    SELECT
        MIN({value_expr}) AS min_value,
        MAX({value_expr}) AS max_value
    FROM {ref}
    WHERE "{metric}" IS NOT NULL
),
bucketed AS (
    SELECT
        CASE
            WHEN stats.max_value = stats.min_value THEN 0
            ELSE LEAST(
                9,
                CAST(
                    FLOOR(
                        (
                            ({value_expr} - stats.min_value)
                            / NULLIF(stats.max_value - stats.min_value, 0)
                        ) * 10
                    ) AS INTEGER
                )
            )
        END AS bucket_idx,
        stats.min_value,
        stats.max_value
    FROM {ref}
    CROSS JOIN stats
    WHERE "{metric}" IS NOT NULL
)
SELECT
    CASE
        WHEN min_value = max_value THEN printf('%.2f', min_value)
        ELSE printf(
            '%.2f - %.2f',
            min_value + bucket_idx * ((max_value - min_value) / 10.0),
            min_value + (bucket_idx + 1) * ((max_value - min_value) / 10.0)
        )
    END AS bucket,
    COUNT(*) AS records
FROM bucketed
GROUP BY bucket_idx, min_value, max_value
ORDER BY bucket_idx
""".strip()
