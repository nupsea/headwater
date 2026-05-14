"""Insights API -- aggregated KPIs and data quality metrics for the UI."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime

from fastapi import APIRouter, HTTPException, Request

from headwater.analyzer.metadata_retrieval import (
    RetrievedMetadata,
    build_lookup_index,
    lookup_for_column,
    retrieve_metadata,
)
from headwater.analyzer.semantic_schema import infer_semantic_schema, roles_for_table
from headwater.api.project_scope import scoped_pipeline
from headwater.api.routes.project import _compute_maturity, _compute_progress
from headwater.core.models import DatasetContext
from headwater.explorer.readability import (
    enum_case_expression,
    enum_dimension_label,
    enum_mapping_for_column,
    is_low_signal_dimension,
    is_opaque_business_value,
)
from headwater.explorer.statistical import (
    detect_insights_with_diagnostics,
    insight_type_priority_weights,
)
from headwater.explorer.utils import resolve_table_ref

router = APIRouter()
logger = logging.getLogger(__name__)

_RAW_ROUTE_RE = re.compile(r"route is (?P<origin>.+?) -> (?P<dest>.+?):", re.IGNORECASE)
_RAW_GEO_RE = re.compile(r"^(?P<label>.+?) has the longest high-volume ", re.IGNORECASE)


@router.get("/insights")
async def get_insights(request: Request, project_id: str | None = None):
    """Compute aggregated data insights from the discovery and quality pipeline."""
    pipeline = scoped_pipeline(request, project_id)
    discovery = pipeline["discovery"]
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet.")
    logger.info(
        "Computing insights: %d tables, %d profiles, %d relationships",
        len(discovery.tables),
        len(discovery.profiles),
        len(discovery.relationships),
    )

    profiles = discovery.profiles
    tables = discovery.tables
    relationships = discovery.relationships
    contracts = pipeline["contracts"]
    quality_report = pipeline["quality_report"]
    exec_results = pipeline["execution_results"]
    staging_models = pipeline["staging_models"]
    mart_models = pipeline["mart_models"]

    # --- Overall metrics ---
    total_rows = sum(t.row_count for t in tables)
    total_columns = sum(len(t.columns) for t in tables)
    total_cells = sum(t.row_count * len(t.columns) for t in tables)

    # --- Completeness ---
    total_nulls = sum(p.null_count for p in profiles)
    table_rows = {t.name: t.row_count for t in tables}
    profiled_cells = sum(table_rows.get(p.table_name, 0) for p in profiles)
    completeness_pct = (
        ((profiled_cells - total_nulls) / profiled_cells * 100) if profiled_cells > 0 else 100.0
    )

    # --- Per-table health ---
    table_health = []
    for t in tables:
        t_profiles = [p for p in profiles if p.table_name == t.name]
        if not t_profiles:
            table_health.append(
                {
                    "name": t.name,
                    "row_count": t.row_count,
                    "column_count": len(t.columns),
                    "domain": t.domain,
                    "description": t.description,
                    "completeness": 100.0,
                    "avg_null_rate": 0.0,
                    "pk_columns": [c.name for c in t.columns if c.is_primary_key],
                    "fk_columns": [],
                    "has_relationships": False,
                }
            )
            continue

        avg_null = sum(p.null_rate for p in t_profiles) / len(t_profiles)
        completeness = (1 - avg_null) * 100

        fk_cols = [
            {"column": r.from_column, "references": f"{r.to_table}.{r.to_column}"}
            for r in relationships
            if r.from_table == t.name
        ]
        has_rels = any(r.from_table == t.name or r.to_table == t.name for r in relationships)

        table_health.append(
            {
                "name": t.name,
                "row_count": t.row_count,
                "column_count": len(t.columns),
                "domain": t.domain,
                "description": t.description,
                "completeness": round(completeness, 1),
                "avg_null_rate": round(avg_null * 100, 1),
                "pk_columns": [c.name for c in t.columns if c.is_primary_key],
                "fk_columns": fk_cols,
                "has_relationships": has_rels,
            }
        )

    # --- Column issues (sorted by severity) ---
    column_issues = []
    for p in profiles:
        issues = []
        if p.null_rate > 0.05:
            issues.append(
                {
                    "type": "high_null_rate",
                    "severity": "error" if p.null_rate > 0.2 else "warning",
                    "message": f"{p.null_rate * 100:.1f}% null values",
                    "detail": f"{p.null_count} of {table_rows.get(p.table_name, 0)} rows",
                }
            )
        if p.uniqueness_ratio == 1.0 and p.distinct_count > 1 and not p.column_name.endswith("_id"):
            issues.append(
                {
                    "type": "unexpected_uniqueness",
                    "severity": "info",
                    "message": "100% unique values -- possible natural key",
                    "detail": f"{p.distinct_count} distinct values",
                }
            )
        if p.distinct_count == 1 and p.null_rate == 0:
            issues.append(
                {
                    "type": "constant_column",
                    "severity": "warning",
                    "message": "Single constant value -- consider removing",
                    "detail": "No variation in this column",
                }
            )
        if p.null_rate == 0 and p.distinct_count == 0:
            issues.append(
                {
                    "type": "empty_column",
                    "severity": "error",
                    "message": "Column appears empty",
                    "detail": "No non-null values found",
                }
            )
        if issues:
            column_issues.append(
                {
                    "table": p.table_name,
                    "column": p.column_name,
                    "dtype": p.dtype,
                    "issues": issues,
                }
            )

    sev_order = {"error": 0, "warning": 1, "info": 2}
    column_issues.sort(key=lambda x: min(sev_order.get(i["severity"], 3) for i in x["issues"]))

    # --- Null rate analysis ---
    null_analysis = []
    for p in profiles:
        if p.null_rate > 0:
            null_analysis.append(
                {
                    "table": p.table_name,
                    "column": p.column_name,
                    "null_rate": round(p.null_rate * 100, 2),
                    "null_count": p.null_count,
                    "total_rows": table_rows.get(p.table_name, 0),
                }
            )
    null_analysis.sort(key=lambda x: x["null_rate"], reverse=True)

    # --- Uniqueness analysis ---
    uniqueness_analysis = []
    for p in profiles:
        uniqueness_analysis.append(
            {
                "table": p.table_name,
                "column": p.column_name,
                "uniqueness_ratio": round(p.uniqueness_ratio * 100, 1),
                "distinct_count": p.distinct_count,
                "is_pk_candidate": p.uniqueness_ratio == 1.0 and p.distinct_count > 1,
            }
        )

    # --- Pattern detection summary ---
    patterns_found = []
    for p in profiles:
        if p.detected_pattern:
            patterns_found.append(
                {
                    "table": p.table_name,
                    "column": p.column_name,
                    "pattern": p.detected_pattern,
                }
            )

    # --- Relationship map ---
    rel_map = []
    for r in relationships:
        rel_map.append(
            {
                "from_table": r.from_table,
                "from_column": r.from_column,
                "to_table": r.to_table,
                "to_column": r.to_column,
                "type": r.type,
                "confidence": round(r.confidence * 100),
                "integrity": round(r.referential_integrity * 100),
            }
        )

    # --- Domain summary ---
    domains = {}
    for t in tables:
        d = t.domain or "Unclassified"
        if d not in domains:
            domains[d] = {"tables": [], "total_rows": 0}
        domains[d]["tables"].append(t.name)
        domains[d]["total_rows"] += t.row_count

    # --- Data profile (concrete, defensible metrics) ---
    data_profile = _compute_data_profile(
        tables, profiles, relationships, completeness_pct, quality_report
    )
    store = request.app.state.metadata_store
    context_row = store.get_dataset_context(discovery.source.name) if store else None
    context = DatasetContext(**context_row) if context_row else None
    metadata = retrieve_metadata(discovery, context)
    top_insights = _compute_top_insights(
        request.app.state.duckdb_con,
        tables,
        profiles,
        metadata,
    )
    semantic_highlights = compute_semantic_highlights(
        request.app.state.duckdb_con,
        discovery,
        context,
        staging_models + mart_models,
    )

    # --- Workflow state ---
    workflow = _compute_workflow_state(
        tables,
        profiles,
        relationships,
        contracts,
        staging_models,
        mart_models,
        exec_results,
        quality_report,
        column_issues,
    )

    # --- Advisory actions (what to do next) ---
    advisory_actions = _compute_advisory_actions(
        tables,
        profiles,
        relationships,
        contracts,
        staging_models,
        mart_models,
        exec_results,
        quality_report,
        column_issues,
    )

    # --- Quality summary ---
    quality_summary = None
    if quality_report:
        quality_summary = {
            "total": quality_report.total_contracts,
            "passed": quality_report.passed,
            "failed": quality_report.failed,
            "pass_rate": (
                round(quality_report.passed / quality_report.total_contracts * 100, 1)
                if quality_report.total_contracts > 0
                else 0
            ),
        }

    # --- Model suggestions ---
    model_suggestions = _compute_model_suggestions(tables, profiles, relationships, pipeline)

    # --- Catalog health (v2) ---
    try:
        catalog_health = _compute_catalog_health(request, discovery, pipeline)
    except Exception:
        logger.exception("Failed to compute catalog health")
        catalog_health = {
            "metrics_total": 0,
            "metrics_confirmed": 0,
            "dimensions_total": 0,
            "dimensions_confirmed": 0,
            "entities_total": 0,
            "catalog_confidence": 0.0,
            "catalog_coverage": 0.0,
            "maturity": "raw",
            "maturity_score": 0.0,
        }

    return {
        "data_profile": data_profile,
        "top_insights": top_insights,
        "semantic_highlights": semantic_highlights,
        "workflow": workflow,
        "advisory_actions": advisory_actions,
        "overview": {
            "total_tables": len(tables),
            "total_columns": total_columns,
            "total_rows": total_rows,
            "total_cells": total_cells,
            "total_relationships": len(relationships),
            "completeness_pct": round(completeness_pct, 1),
            "total_profiles": len(profiles),
            "total_contracts": len(contracts),
        },
        "domains": domains,
        "table_health": table_health,
        "column_issues": column_issues,
        "null_analysis": null_analysis,
        "uniqueness_analysis": uniqueness_analysis,
        "patterns_found": patterns_found,
        "relationship_map": rel_map,
        "quality_summary": quality_summary,
        "model_suggestions": model_suggestions,
        "catalog_health": catalog_health,
    }


# ---------------------------------------------------------------------------
# Data Profile -- concrete numbers, each with a plain-English definition
# ---------------------------------------------------------------------------


def _compute_data_profile(tables, profiles, relationships, completeness_pct, quality_report):
    """Return concrete, defensible metrics -- no composite scores."""

    # Completeness: % of non-null cells across all profiled columns
    completeness = round(min(completeness_pct, 100.0), 1)

    # PK coverage: how many tables have at least one identified primary key
    tables_with_pk = sum(1 for t in tables if any(c.is_primary_key for c in t.columns))
    pk_coverage = {
        "tables_with_pk": tables_with_pk,
        "total_tables": len(tables),
        "description": "Tables with an identified primary key",
    }

    # FK integrity: average referential integrity across all detected relationships
    if relationships:
        avg_integrity = sum(r.referential_integrity for r in relationships) / len(relationships)
        fk_integrity = {
            "avg_integrity_pct": round(avg_integrity * 100, 1),
            "total_relationships": len(relationships),
            "description": "Average % of foreign key values that resolve to a parent record",
        }
    else:
        fk_integrity = {
            "avg_integrity_pct": None,
            "total_relationships": 0,
            "description": "No foreign key relationships detected",
        }

    # Quality pass rate: how many auto-generated quality contracts pass
    if quality_report and quality_report.total_contracts > 0:
        quality = {
            "passed": quality_report.passed,
            "total": quality_report.total_contracts,
            "pass_rate_pct": round(quality_report.passed / quality_report.total_contracts * 100, 1),
            "description": "Auto-generated quality contracts that pass",
        }
    else:
        quality = {
            "passed": 0,
            "total": 0,
            "pass_rate_pct": None,
            "description": "Quality contracts not yet evaluated",
        }

    # Columns with issues: high nulls, constants, etc.
    high_null_cols = sum(1 for p in profiles if p.null_rate > 0.05)
    constant_cols = sum(1 for p in profiles if p.distinct_count == 1 and p.null_rate == 0)

    return {
        "completeness_pct": completeness,
        "pk_coverage": pk_coverage,
        "fk_integrity": fk_integrity,
        "quality": quality,
        "high_null_columns": high_null_cols,
        "constant_columns": constant_cols,
        "total_columns_profiled": len(profiles),
    }


# ---------------------------------------------------------------------------
# Top Insights -- business-readable patterns from actual data values
# ---------------------------------------------------------------------------


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _chart_rows(items, *, label_key: str, value_key: str, limit: int = 6):
    rows = []
    for item in items[:limit]:
        rows.append(
            {
                "label": str(item[label_key]),
                "value": round(float(item[value_key]), 2),
            }
        )
    return rows


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table_ref(table) -> str:
    if table.schema_name:
        return f"{_quote_ident(table.schema_name)}.{_quote_ident(table.name)}"
    return _quote_ident(table.name)


def _humanize_name(name: str) -> str:
    return name.replace("stg_", "").replace("_", " ").strip().title()


def _column_profile(profiles, table_name: str, column_name: str):
    return next(
        (p for p in profiles if p.table_name == table_name and p.column_name == column_name),
        None,
    )


def _is_temporal_column(column, profile) -> bool:
    name = column.name.lower()
    dtype = column.dtype.lower()
    return (
        column.role == "temporal"
        or column.semantic_type == "temporal"
        or "date" in dtype
        or "time" in dtype
        or name in {"year", "month", "quarter"}
        or name.endswith("_year")
        or name.endswith("_date")
        or name.endswith("_at")
        or (profile and profile.detected_pattern == "iso_date")
    )


def _is_metric_column(column, profile) -> bool:
    name = column.name.lower()
    dtype = column.dtype.lower()
    if column.is_primary_key or name == "id" or name.endswith("_id") or name.endswith("_code"):
        return False
    if column.role == "metric" or column.semantic_type == "metric":
        return True
    if not any(token in dtype for token in ("int", "float", "double", "decimal", "numeric")):
        return False
    return bool(profile and profile.distinct_count > 5 and profile.uniqueness_ratio < 0.9)


def _is_dimension_column(column, profile) -> bool:
    name = column.name.lower()
    if column.is_primary_key or name == "id" or name.endswith("_id"):
        return False
    if profile and profile.distinct_count <= 1:
        return False
    if column.role == "dimension" or column.semantic_type in {"dimension", "geographic"}:
        return True
    if not profile or not profile.top_values:
        return False
    return 2 <= profile.distinct_count <= 40


def _is_low_signal_dimension(column, profile) -> bool:
    return is_low_signal_dimension(column.name, profile)


def _period_expression(column) -> str | None:
    quoted = _quote_ident(column.name)
    name = column.name.lower()
    dtype = column.dtype.lower()
    if name in {"year"} or name.endswith("_year"):
        return f"CAST({quoted} AS VARCHAR)"
    if "date" in dtype or "time" in dtype or "date" in name or name.endswith("_at"):
        return f"strftime(CAST({quoted} AS TIMESTAMP), '%Y')"
    return None


def _time_series_spec(con, ref: str, column) -> tuple[str, str, str] | None:
    quoted = _quote_ident(column.name)
    name = column.name.lower()
    dtype = column.dtype.lower()
    if name == "year" or name.endswith("_year"):
        return f"CAST({quoted} AS VARCHAR)", "year", column.name
    if name == "month" or name.endswith("_month"):
        return f"CAST({quoted} AS VARCHAR)", "month", column.name
    if not ("date" in dtype or "time" in dtype or "date" in name or name.endswith("_at")):
        return None

    rows = _query_rows(
        con,
        f"""
        SELECT MIN(CAST({quoted} AS TIMESTAMP)) AS min_ts,
               MAX(CAST({quoted} AS TIMESTAMP)) AS max_ts
        FROM {ref}
        WHERE {quoted} IS NOT NULL
        """,
    )
    if not rows or rows[0][0] is None or rows[0][1] is None:
        return None

    min_ts, max_ts = rows[0]
    if isinstance(min_ts, date) and not isinstance(min_ts, datetime):
        min_ts = datetime.combine(min_ts, datetime.min.time())
    if isinstance(max_ts, date) and not isinstance(max_ts, datetime):
        max_ts = datetime.combine(max_ts, datetime.min.time())
    span_days = max(1.0, (max_ts - min_ts).total_seconds() / 86400.0)

    if span_days <= 3:
        return (
            f"strftime(CAST({quoted} AS TIMESTAMP), '%Y-%m-%d %H:00')",
            "hour",
            column.name,
        )
    if span_days <= 120:
        return f"strftime(CAST({quoted} AS TIMESTAMP), '%Y-%m-%d')", "day", column.name
    if span_days <= 900:
        return f"strftime(CAST({quoted} AS TIMESTAMP), '%Y-%m')", "month", column.name
    return f"strftime(CAST({quoted} AS TIMESTAMP), '%Y')", "year", column.name


def _time_preposition(grain: str) -> str:
    if grain == "hour":
        return "at"
    if grain == "day":
        return "on"
    return "in"


def _query_rows(con, sql: str, params: list | None = None):
    try:
        return con.execute(sql, params or []).fetchall()
    except Exception:
        logger.debug("Business insight query failed: %s", sql, exc_info=True)
        return []


def _compute_temporal_peak_insights(con, table, temporal_cols, metric_cols) -> list[dict]:
    insights = []
    ref = _table_ref(table)
    table_label = _humanize_name(table.name)

    for temporal in temporal_cols[:2]:
        period_spec = _time_series_spec(con, ref, temporal)
        if not period_spec:
            continue
        period_expr, period_grain, group_by_column = period_spec

        rows = _query_rows(
            con,
            f"""
            SELECT {period_expr} AS period, COUNT(*) AS value
            FROM {ref}
            WHERE {_quote_ident(temporal.name)} IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """,
        )
        chart = [
            {"label": str(period), "value": float(value)}
            for period, value in rows
            if period is not None and value is not None
        ]
        if len(chart) >= 2:
            peak = max(chart, key=lambda row: row["value"])
            avg = sum(row["value"] for row in chart) / len(chart)
            lift = _safe_ratio(peak["value"], avg)
            if lift >= 1.15 or len(chart) <= 4:
                insights.append(
                    {
                        "id": f"temporal_peak:{table.name}:{temporal.name}",
                        "category": "Did You Know",
                        "severity": "info",
                        "title": (
                            f"{table_label} peaked "
                            f"{_time_preposition(period_grain)} {peak['label']}"
                        ),
                        "detail": (
                            f"{peak['label']} had {peak['value']:,.0f} records, "
                            f"{(lift - 1) * 100:.0f}% above the typical {period_grain}."
                        ),
                        "table": table.name,
                        "column": temporal.name,
                        "group_by_column": group_by_column,
                        "group_by_grain": period_grain,
                        "metric": "record_volume",
                        "value": round(lift, 2),
                        "unit": "x",
                        "chart_type": "line",
                        "chart": chart,
                        "score": lift,
                    }
                )

        for metric in metric_cols[:3]:
            metric_label = _humanize_name(metric.name)
            rows = _query_rows(
                con,
                f"""
                SELECT {period_expr} AS period, SUM({_quote_ident(metric.name)}) AS value
                FROM {ref}
                WHERE {_quote_ident(temporal.name)} IS NOT NULL
                  AND {_quote_ident(metric.name)} IS NOT NULL
                GROUP BY 1
                ORDER BY 1
                """,
            )
            chart = [
                {"label": str(period), "value": float(value)}
                for period, value in rows
                if period is not None and value is not None
            ]
            if len(chart) < 2:
                continue
            peak = max(chart, key=lambda row: row["value"])
            avg = sum(row["value"] for row in chart) / len(chart)
            lift = _safe_ratio(peak["value"], avg)
            if lift >= 1.2:
                insights.append(
                    {
                        "id": f"metric_peak:{table.name}:{temporal.name}:{metric.name}",
                        "category": "Did You Know",
                        "severity": "warning" if lift >= 1.5 else "info",
                        "title": (
                            f"{metric_label} peaked "
                            f"{_time_preposition(period_grain)} {peak['label']}"
                        ),
                        "detail": (
                            f"{table_label} recorded {peak['value']:,.0f} total "
                            f"{metric_label.lower()} in {peak['label']}, "
                            f"{(lift - 1) * 100:.0f}% above a typical {period_grain}."
                        ),
                        "table": table.name,
                        "column": metric.name,
                        "group_by_column": group_by_column,
                        "group_by_grain": period_grain,
                        "metric": "period_total",
                        "value": round(lift, 2),
                        "unit": "x",
                        "chart_type": "line",
                        "chart": chart,
                        "score": lift,
                    }
                )

    return insights


def _is_code_like_column(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(("_id", "_code", "_key", "_num")) or any(
        token in lower for token in ("base_num", "vendor_id", "locationid")
    )


def _is_opaque_value(value: object) -> bool:
    return is_opaque_business_value(value)


def _resolve_dimension_projection(
    table,
    dimension,
    lookup_index: dict[str, dict[str, str]],
    metadata: RetrievedMetadata | None = None,
) -> tuple[str | None, str | None, str | None]:
    enum_expr = enum_case_expression(dimension.name, f'fact."{dimension.name}"', metadata)
    if enum_expr:
        return (
            None,
            enum_expr,
            enum_dimension_label(dimension.name, _humanize_name(dimension.name)),
        )
    if not _is_code_like_column(dimension.name):
        return None, dimension.name, _humanize_name(dimension.name)

    lookup = lookup_for_column(table.name, dimension.name, lookup_index)
    if lookup and lookup["table_name"] != table.name:
        alias = "lu"
        lookup_ref = _quote_ident(lookup["table_name"])
        if lookup.get("schema_name"):
            lookup_ref = f'{_quote_ident(lookup["schema_name"])}.{lookup_ref}'
        join_sql = (
            f"LEFT JOIN {lookup_ref} {alias} "
            f'ON fact."{dimension.name}" = {alias}."{lookup["id_column"]}"'
        )
        return (
            join_sql,
            f'{alias}."{lookup["label_column"]}"',
            _humanize_name(lookup["label_column"]),
        )

    return None, None, None


def _compute_segment_insights(
    con,
    table,
    dimension_cols,
    metric_cols,
    lookup_index: dict[str, dict[str, str]],
    metadata: RetrievedMetadata | None = None,
) -> list[dict]:
    insights = []
    ref = _table_ref(table)
    table_label = _humanize_name(table.name)

    for dimension in dimension_cols[:4]:
        join_sql, dimension_expr, dim_label = _resolve_dimension_projection(
            table,
            dimension,
            lookup_index,
            metadata,
        )
        if dimension_expr is None or dim_label is None:
            continue
        rows = _query_rows(
            con,
            f"""
            SELECT CAST({dimension_expr} AS VARCHAR) AS segment, COUNT(*) AS value
            FROM {ref} fact
            {join_sql or ""}
            WHERE {dimension_expr} IS NOT NULL
            GROUP BY 1
            ORDER BY value DESC
            LIMIT 8
            """,
        )
        chart = [
            {"label": str(segment), "value": float(value)}
            for segment, value in rows
            if segment is not None and value is not None
        ]
        total = sum(row["value"] for row in chart)
        if chart and total > 0:
            leader = chart[0]
            share = _safe_ratio(leader["value"], total)
            if _is_opaque_value(leader["label"]):
                continue
            if share >= 0.25:
                insights.append(
                    {
                        "id": f"segment_concentration:{table.name}:{dimension.name}",
                        "category": "Did You Know",
                        "severity": "info",
                        "title": (
                            f"{leader['label']} leads {table_label.lower()} "
                            f"by {dim_label.lower()}"
                        ),
                        "detail": (
                            f"{leader['label']} represents {share * 100:.1f}% of the top "
                            f"{dim_label.lower()} groups in {table_label.lower()}."
                        ),
                        "table": table.name,
                        "column": dimension.name,
                        "group_by_column": dimension.name,
                        "metric": "segment_share",
                        "value": round(share * 100, 1),
                        "unit": "%",
                        "chart_type": "pie",
                        "chart": chart,
                        "score": share,
                    }
                )

        for metric in metric_cols[:3]:
            metric_label = _humanize_name(metric.name)
            rows = _query_rows(
                con,
                f"""
                SELECT CAST({dimension_expr} AS VARCHAR) AS segment,
                       SUM(fact.{_quote_ident(metric.name)}) AS value
                FROM {ref} fact
                {join_sql or ""}
                WHERE {dimension_expr} IS NOT NULL
                  AND fact.{_quote_ident(metric.name)} IS NOT NULL
                GROUP BY 1
                ORDER BY value DESC
                LIMIT 8
                """,
            )
            chart = [
                {"label": str(segment), "value": float(value)}
                for segment, value in rows
                if segment is not None and value is not None
            ]
            total = sum(row["value"] for row in chart)
            if not chart or total <= 0:
                continue
            leader = chart[0]
            share = _safe_ratio(leader["value"], total)
            if _is_opaque_value(leader["label"]):
                continue
            if share >= 0.3:
                insights.append(
                    {
                        "id": f"metric_driver:{table.name}:{dimension.name}:{metric.name}",
                        "category": "Did You Know",
                        "severity": "warning" if share >= 0.5 else "info",
                        "title": f"{leader['label']} drives {metric_label.lower()}",
                        "detail": (
                            f"{leader['label']} accounts for {share * 100:.1f}% of "
                            f"top-group {metric_label.lower()} in {table_label.lower()}."
                        ),
                        "table": table.name,
                        "column": dimension.name,
                        "group_by_column": dimension.name,
                        "metric": metric.name,
                        "value": round(share * 100, 1),
                        "unit": "%",
                        "chart_type": "bar",
                        "chart": chart,
                        "score": share,
                    }
                )

    return insights


def _compute_distribution_insights(con, table, metric_cols, column_profiles) -> list[dict]:
    insights = []
    ref = _table_ref(table)
    table_label = _humanize_name(table.name)

    for metric in metric_cols[:4]:
        profile = column_profiles.get(metric.name)
        if not profile or profile.min_value is None or profile.max_value is None:
            continue
        min_value = float(profile.min_value)
        max_value = float(profile.max_value)
        if min_value == max_value:
            continue

        metric_label = _humanize_name(metric.name)
        width = (max_value - min_value) / 5
        bins = []
        case_parts = []
        for index in range(5):
            low = min_value + width * index
            high = max_value if index == 4 else min_value + width * (index + 1)
            label = f"{low:,.0f}-{high:,.0f}"
            bins.append(label)
            comparator = "<=" if index == 4 else "<"
            case_parts.append(
                f"WHEN {_quote_ident(metric.name)} {comparator} {high} THEN '{label}'"
            )

        rows = _query_rows(
            con,
            f"""
            SELECT bucket, COUNT(*) AS value
            FROM (
              SELECT CASE {' '.join(case_parts)} END AS bucket
              FROM {ref}
              WHERE {_quote_ident(metric.name)} IS NOT NULL
            )
            WHERE bucket IS NOT NULL
            GROUP BY bucket
            """,
        )
        counts = {str(bucket): float(value) for bucket, value in rows if bucket is not None}
        chart = [{"label": label, "value": counts.get(label, 0)} for label in bins]
        total = sum(row["value"] for row in chart)
        if total <= 0:
            continue

        leader = max(chart, key=lambda row: row["value"])
        share = _safe_ratio(leader["value"], total)
        if share < 0.35:
            continue

        insights.append(
            {
                "id": f"value_distribution:{table.name}:{metric.name}",
                "category": "Did You Know",
                "severity": "info",
                "title": f"{metric_label} clusters around {leader['label']}",
                "detail": (
                    f"{share * 100:.1f}% of {table_label.lower()} records fall in the "
                    f"{leader['label']} {metric_label.lower()} range."
                ),
                "table": table.name,
                "column": metric.name,
                "group_by_column": metric.name,
                "metric": "value_distribution",
                "value": round(share * 100, 1),
                "unit": "%",
                "chart_type": "histogram",
                "chart": chart,
                "score": share,
            }
        )

    return insights


def _select_diverse_insights(insights: list[dict], limit: int = 10) -> list[dict]:
    """Keep the final set mixed across tables and visual patterns."""
    selected: list[dict] = []
    selected_ids: set[str] = set()
    table_counts: dict[str, int] = {}
    chart_counts: dict[str, int] = {}
    max_by_chart = {"line": 4}
    max_by_table = 3
    initial_line_cap = 2
    preferred_chart_order = ("bar", "pie", "histogram")

    def can_add(insight: dict) -> bool:
        chart_type = insight["chart_type"]
        table = insight["table"]
        if insight["id"] in selected_ids:
            return False
        if chart_counts.get(chart_type, 0) >= max_by_chart.get(chart_type, limit):
            return False
        return table_counts.get(table, 0) < max_by_table

    def add(insight: dict) -> None:
        selected.append(insight)
        selected_ids.add(insight["id"])
        chart_type = insight["chart_type"]
        table = insight["table"]
        chart_counts[chart_type] = chart_counts.get(chart_type, 0) + 1
        table_counts[table] = table_counts.get(table, 0) + 1

    ranked = sorted(insights, key=lambda item: item.get("score", item["value"]), reverse=True)

    lead_trend = next((insight for insight in ranked if insight["chart_type"] == "line"), None)
    if lead_trend:
        add(lead_trend)

    for chart_type in preferred_chart_order:
        best = next(
            (
                insight
                for insight in ranked
                if insight["chart_type"] == chart_type and can_add(insight)
            ),
            None,
        )
        if best:
            add(best)
        if len(selected) >= limit:
            break

    while len(selected) < min(5, limit):
        next_line = next(
            (
                insight
                for insight in ranked
                if insight["chart_type"] == "line"
                and chart_counts.get("line", 0) < initial_line_cap
                and can_add(insight)
            ),
            None,
        )
        if not next_line:
            break
        add(next_line)

    for insight in ranked:
        if len(selected) >= limit:
            break
        if can_add(insight):
            add(insight)

    if len(selected) < limit:
        for insight in ranked:
            if len(selected) >= limit:
                break
            if (
                insight["id"] not in selected_ids
                and chart_counts.get(insight["chart_type"], 0)
                < max_by_chart.get(insight["chart_type"], limit)
            ):
                add(insight)

    cleaned = []
    for insight in selected[:limit]:
        cleaned.append({key: value for key, value in insight.items() if key != "score"})
    return cleaned


def _rank_dimension_column(column) -> tuple[int, int]:
    lower = column.name.lower()
    if any(token in lower for token in ("_name", "name_", "label", "description")):
        return (0, 0)
    if any(token in lower for token in ("borough", "zone", "site", "region", "category", "type")):
        return (0, 1)
    if _is_code_like_column(lower):
        return (1, 2)
    return (0, 2)


def _is_business_dimension_column(
    table,
    column,
    profile,
    lookup_index,
    metadata: RetrievedMetadata | None = None,
) -> bool:
    if _is_dimension_column(column, profile):
        return True
    if column.role not in {"dimension", "geographic"} and column.semantic_type not in {
        "dimension",
        "geographic",
    }:
        return False
    if enum_mapping_for_column(column.name, metadata):
        return True
    lookup = lookup_for_column(table.name, column.name, lookup_index)
    return bool(lookup and lookup["table_name"] != table.name)


def _compute_top_insights(
    con,
    tables,
    profiles,
    metadata: RetrievedMetadata | None = None,
) -> list[dict]:
    """Find CXO-readable patterns from actual records.

    The output avoids schema-quality commentary and favors business outcome
    statements: when activity peaked, which segment dominates, and what groups
    drive measurable totals.
    """
    insights: list[dict] = []
    lookup_index = build_lookup_index(tables, metadata)
    for table in sorted(tables, key=lambda t: t.row_count, reverse=True):
        column_profiles = {
            c.name: _column_profile(profiles, table.name, c.name)
            for c in table.columns
        }
        temporal_cols = [
            c for c in table.columns
            if _is_temporal_column(c, column_profiles.get(c.name))
        ]
        metric_cols = [
            c for c in table.columns
            if _is_metric_column(c, column_profiles.get(c.name))
        ]
        dimension_cols = [
            c for c in table.columns
            if (
                _is_business_dimension_column(
                    table,
                    c,
                    column_profiles.get(c.name),
                    lookup_index,
                    metadata,
                )
                and not _is_low_signal_dimension(c, column_profiles.get(c.name))
            )
        ]
        dimension_cols = sorted(dimension_cols, key=_rank_dimension_column)

        insights.extend(_compute_temporal_peak_insights(con, table, temporal_cols, metric_cols))
        insights.extend(
            _compute_segment_insights(
                con,
                table,
                dimension_cols,
                metric_cols,
                lookup_index,
                metadata,
            )
        )
        insights.extend(_compute_distribution_insights(con, table, metric_cols, column_profiles))

    return _select_diverse_insights(insights)


def compute_top_insights(
    con,
    tables,
    profiles,
    metadata: RetrievedMetadata | None = None,
) -> list[dict]:
    """Public helper for business-readable insights reused by Explore."""
    return _compute_top_insights(con, tables, profiles, metadata)


_SEMANTIC_HIGHLIGHT_ORDER = [
    "data_quality",
    "duration_distribution",
    "wait_time_pattern",
    "geographic_hotspot",
    "peak_period",
    "route_pair",
    "volume_distribution",
    "congestion_proxy",
]


def _decision_lens(context: DatasetContext | None, insight) -> str:
    text = " ".join(
        value
        for value in (
            context.decisions if context else None,
            context.quality_caveats if context else None,
        )
        if value
    ).lower()
    if insight.insight_type == "data_quality":
        return "Data Quality"
    if any(token in insight.metric.lower() for token in ("amount", "fare", "revenue", "price")):
        return "Revenue"
    if any(token in text for token in ("compliance", "audit", "regulation", "risk")):
        return "Compliance"
    if any(token in text for token in ("pricing", "margin", "revenue", "sales")):
        return "Revenue"
    return "Operations"


def _replace_record_label(text: str, row_represents: str | None) -> str:
    if not row_represents:
        return text
    phrase = row_represents.strip().lower()
    if not phrase:
        return text
    plural = phrase if phrase.endswith("s") else f"{phrase}s"
    return text.replace(" records", f" {plural}")


def _semantic_highlight_title(description: str) -> str:
    base = description.strip().rstrip(".")
    for separator in (":", ". "):
        if separator in base:
            return base.split(separator, 1)[0].strip()
    return base


def _lookup_label_for_value(
    con,
    lookup: dict[str, str] | None,
    raw_value: str,
    models,
    cache: dict[tuple[str, str, str, str], str | None],
) -> str | None:
    if lookup is None or not raw_value:
        return None
    ref = resolve_table_ref(lookup["table_name"], con, models)
    cache_key = (
        ref,
        lookup["id_column"],
        lookup["label_column"],
        raw_value,
    )
    if cache_key in cache:
        return cache[cache_key]
    try:
        row = con.execute(
            f"""
            SELECT CAST({_quote_ident(lookup["label_column"])} AS VARCHAR)
            FROM {ref}
            WHERE CAST({_quote_ident(lookup["id_column"])} AS VARCHAR) = ?
              AND {_quote_ident(lookup["label_column"])} IS NOT NULL
            LIMIT 1
            """,
            [raw_value],
        ).fetchone()
    except Exception:
        cache[cache_key] = None
        return None
    label = str(row[0]).strip() if row and row[0] is not None else None
    cache[cache_key] = label or None
    return cache[cache_key]


def _semantic_highlight_detail(
    con,
    insight,
    detail: str,
    roles: dict[str, object],
    lookup_index: dict[str, dict[str, str]],
    models,
    lookup_cache: dict[tuple[str, str, str, str], str | None],
) -> str | None:
    if insight.insight_type == "geographic_hotspot":
        match = _RAW_GEO_RE.match(detail)
        if not match:
            return detail
        raw_label = match.group("label").strip()
        if not _is_opaque_value(raw_label):
            return detail
        origin = roles.get("origin_id") or roles.get("location_id")
        if origin is None:
            return None
        lookup = lookup_for_column(insight.table_name, origin.column_name, lookup_index)
        resolved = _lookup_label_for_value(con, lookup, raw_label, models, lookup_cache)
        if not resolved or _is_opaque_value(resolved):
            return None
        return detail.replace(raw_label, resolved, 1)

    if insight.insight_type == "route_pair":
        match = _RAW_ROUTE_RE.search(detail)
        if not match:
            return detail
        raw_origin = match.group("origin").strip()
        raw_dest = match.group("dest").strip()
        origin = roles.get("origin_id") or roles.get("location_id")
        dest = roles.get("destination_id")
        if origin is None or dest is None:
            return None if _is_opaque_value(raw_origin) or _is_opaque_value(raw_dest) else detail
        origin_lookup = lookup_for_column(insight.table_name, origin.column_name, lookup_index)
        dest_lookup = lookup_for_column(insight.table_name, dest.column_name, lookup_index)
        resolved_origin = raw_origin
        resolved_dest = raw_dest
        if _is_opaque_value(raw_origin):
            resolved_origin = (
                _lookup_label_for_value(con, origin_lookup, raw_origin, models, lookup_cache)
                or raw_origin
            )
        if _is_opaque_value(raw_dest):
            resolved_dest = (
                _lookup_label_for_value(con, dest_lookup, raw_dest, models, lookup_cache)
                or raw_dest
            )
        if _is_opaque_value(resolved_origin) or _is_opaque_value(resolved_dest):
            return None
        return detail.replace(
            f"{raw_origin} -> {raw_dest}",
            f"{resolved_origin} -> {resolved_dest}",
            1,
        )

    return detail


def _semantic_highlight_score(insight) -> float:
    severity_weight = {"critical": 3.0, "warning": 2.0, "info": 1.0}
    type_weight = insight_type_priority_weights()
    score = (
        type_weight.get(insight.insight_type, 1)
        * severity_weight.get(insight.severity, 1.0)
        * max(abs(insight.magnitude), 1.0)
        * max((insight.support_count or 0) ** 0.25, 1.0)
    )
    if insight.metric == "wait_min" or "wait time" in insight.description.lower():
        score *= 1.5
    return score


def compute_semantic_highlights(
    con,
    discovery,
    context: DatasetContext | None,
    models,
    limit: int = 5,
) -> list[dict]:
    """Convert semantic-family insights into business-facing findings."""
    metadata = retrieve_metadata(discovery, context)
    semantic_schema = infer_semantic_schema(discovery, context)
    semantic_roles = {
        table.name: roles_for_table(semantic_schema, table.name)
        for table in discovery.tables
    }
    lookup_index = build_lookup_index(discovery.tables, metadata, discovery.relationships)
    lookup_cache: dict[tuple[str, str, str, str], str | None] = {}
    result = detect_insights_with_diagnostics(
        con,
        schema="staging",
        discovery=discovery,
        dataset_context=context,
        models=models,
    )
    highlight_types = set(_SEMANTIC_HIGHLIGHT_ORDER) | {"coverage_period"}
    candidates = [
        insight
        for insight in result.insights
        if insight.insight_type in highlight_types
        and insight.insight_type != "coverage_period"
    ]
    selected: list[dict] = []
    seen_tables: dict[str, int] = {}
    max_per_table = 5 if len({insight.table_name for insight in candidates}) <= 1 else 2
    ranked_candidates = sorted(candidates, key=_semantic_highlight_score, reverse=True)
    seen_types: set[str] = set()

    def append_highlight(insight) -> bool:
        if seen_tables.get(insight.table_name, 0) >= max_per_table:
            return False
        detail = _replace_record_label(
            insight.description,
            context.row_represents if context else None,
        )
        detail = _semantic_highlight_detail(
            con,
            insight,
            detail,
            semantic_roles.get(insight.table_name, {}),
            lookup_index,
            models,
            lookup_cache,
        )
        if detail is None:
            return False
        lens = _decision_lens(context, insight)
        if context and context.decisions:
            detail = f"{detail} Relevant for {lens.lower()} decisions."
        selected.append(
            {
                "id": f"semantic:{insight.table_name}:{insight.insight_type}:{insight.metric}",
                "title": _semantic_highlight_title(detail),
                "detail": detail,
                "table": insight.table_name,
                "metric": insight.metric,
                "insight_type": insight.insight_type,
                "severity": insight.severity,
                "confidence_level": insight.confidence_level,
                "support_count": insight.support_count,
                "decision_lens": lens,
                "metadata_signals": {
                    "has_context": metadata.context is not None,
                    "glossary_terms": len(metadata.glossary),
                    "lookup_tables": len(metadata.lookup_tables),
                },
            }
        )
        seen_tables[insight.table_name] = seen_tables.get(insight.table_name, 0) + 1
        seen_types.add(insight.insight_type)
        return True

    for insight_type in _SEMANTIC_HIGHLIGHT_ORDER:
        candidate = next(
            (item for item in ranked_candidates if item.insight_type == insight_type),
            None,
        )
        if candidate is not None:
            append_highlight(candidate)
        if len(selected) >= limit:
            return selected[:limit]

    for insight in ranked_candidates:
        if seen_tables.get(insight.table_name, 0) >= max_per_table:
            continue
        if (
            insight.insight_type in {"volume_distribution", "peak_period"}
            and insight.insight_type in seen_types
        ):
            continue
        append_highlight(insight)
        if len(selected) >= limit:
            break
    if not any(item["metric"] == "wait_min" for item in selected):
        wait_candidate = next(
            (
                insight
                for insight in ranked_candidates
                if insight.metric == "wait_min" or "wait time" in insight.description.lower()
            ),
            None,
        )
        if wait_candidate is not None:
            detail = _replace_record_label(
                wait_candidate.description,
                context.row_represents if context else None,
            )
            detail = _semantic_highlight_detail(
                con,
                wait_candidate,
                detail,
                semantic_roles.get(wait_candidate.table_name, {}),
                lookup_index,
                models,
                lookup_cache,
            )
            if detail is None:
                return selected[:limit]
            lens = _decision_lens(context, wait_candidate)
            if context and context.decisions:
                detail = f"{detail} Relevant for {lens.lower()} decisions."
            wait_item = {
                "id": (
                    f"semantic:{wait_candidate.table_name}:"
                    f"{wait_candidate.insight_type}:{wait_candidate.metric}"
                ),
                "title": _semantic_highlight_title(detail),
                "detail": detail,
                "table": wait_candidate.table_name,
                "metric": wait_candidate.metric,
                "insight_type": wait_candidate.insight_type,
                "severity": wait_candidate.severity,
                "confidence_level": wait_candidate.confidence_level,
                "support_count": wait_candidate.support_count,
                "decision_lens": lens,
                "metadata_signals": {
                    "has_context": metadata.context is not None,
                    "glossary_terms": len(metadata.glossary),
                    "lookup_tables": len(metadata.lookup_tables),
                },
            }
            if len(selected) >= limit:
                selected[-1] = wait_item
            else:
                selected.append(wait_item)
    return selected


# ---------------------------------------------------------------------------
# Workflow state -- where is this dataset in the journey?
# ---------------------------------------------------------------------------

_PHASES = [
    {"key": "discovery", "label": "Discovered"},
    {"key": "profiling", "label": "Profiled"},
    {"key": "review", "label": "Schema Reviewed"},
    {"key": "modeling", "label": "Modeled"},
    {"key": "quality", "label": "Quality Baselined"},
]


def _compute_workflow_state(
    tables,
    profiles,
    relationships,
    contracts,
    staging_models,
    mart_models,
    exec_results,
    quality_report,
    column_issues,
):
    """Determine which workflow phase the dataset is in."""
    phases = []

    # Phase 1: Discovery -- did we find tables?
    discovered = len(tables) > 0
    phases.append(
        {
            "key": "discovery",
            "label": "Discovered",
            "status": "complete" if discovered else "pending",
            "detail": f"{len(tables)} tables found" if discovered else "Not started",
        }
    )

    # Phase 2: Profiling -- did we profile columns?
    profiled = len(profiles) > 0
    phases.append(
        {
            "key": "profiling",
            "label": "Profiled",
            "status": "complete" if profiled else "pending",
            "detail": (
                f"{len(profiles)} columns, {len(relationships)} relationships"
                if profiled
                else "Not started"
            ),
        }
    )

    # Phase 3: Schema Review -- are there unresolved issues?
    # Issues that need human attention: tables without PKs, high-null columns, isolated tables
    tables_no_pk = [t.name for t in tables if not any(c.is_primary_key for c in t.columns)]
    connected = set()
    for r in relationships:
        connected.add(r.from_table)
        connected.add(r.to_table)
    error_issues = [
        ci for ci in column_issues if any(i["severity"] == "error" for i in ci["issues"])
    ]

    review_blockers = len(tables_no_pk) + len(error_issues)
    if not profiled:
        review_status = "pending"
        review_detail = "Profiling must complete first"
    elif review_blockers == 0:
        review_status = "complete"
        review_detail = "No blocking schema issues"
    else:
        review_status = "active"
        parts = []
        if tables_no_pk:
            parts.append(f"{len(tables_no_pk)} tables missing PKs")
        if error_issues:
            parts.append(f"{len(error_issues)} columns with critical issues")
        review_detail = ", ".join(parts)

    phases.append(
        {
            "key": "review",
            "label": "Schema Reviewed",
            "status": review_status,
            "detail": review_detail,
        }
    )

    # Phase 4: Modeling -- are models generated and reviewed?
    total_models = len(staging_models) + len(mart_models)
    pending_marts = [m for m in mart_models if m.status == "proposed"]
    executed = [r for r in exec_results if r.success]

    if total_models == 0:
        model_status = "pending"
        model_detail = "No models generated yet"
    elif pending_marts:
        model_status = "active"
        model_detail = f"{len(pending_marts)} mart model(s) awaiting review"
    elif len(executed) > 0:
        model_status = "complete"
        model_detail = f"{len(executed)} models materialized"
    else:
        model_status = "active"
        model_detail = f"{total_models} models generated, none executed"

    phases.append(
        {
            "key": "modeling",
            "label": "Modeled",
            "status": model_status,
            "detail": model_detail,
        }
    )

    # Phase 5: Quality -- are contracts evaluated?
    if not quality_report:
        quality_status = "pending"
        quality_detail = "Quality checks not yet run"
    elif quality_report.failed > 0:
        quality_status = "active"
        quality_detail = (
            f"{quality_report.failed} of {quality_report.total_contracts} contracts failed"
        )
    else:
        quality_status = "complete"
        quality_detail = f"All {quality_report.total_contracts} contracts pass"

    phases.append(
        {
            "key": "quality",
            "label": "Quality Baselined",
            "status": quality_status,
            "detail": quality_detail,
        }
    )

    # Current phase = first non-complete phase, or last phase if all complete
    current = "quality"
    for p in phases:
        if p["status"] != "complete":
            current = p["key"]
            break

    return {
        "phases": phases,
        "current_phase": current,
    }


# ---------------------------------------------------------------------------
# Advisory actions -- "what should I do next?"
# ---------------------------------------------------------------------------


def _compute_advisory_actions(
    tables,
    profiles,
    relationships,
    contracts,
    staging_models,
    mart_models,
    exec_results,
    quality_report,
    column_issues,
):
    """Generate prioritized, actionable recommendations grouped by phase."""
    actions = []

    # --- Schema Review actions ---

    # Tables without primary keys
    tables_no_pk = [t.name for t in tables if not any(c.is_primary_key for c in t.columns)]
    if tables_no_pk:
        actions.append(
            {
                "phase": "review",
                "priority": "blocking",
                "title": f"Confirm primary keys for {len(tables_no_pk)} table(s)",
                "detail": (
                    f"{', '.join(tables_no_pk[:4])}"
                    + (f" and {len(tables_no_pk) - 4} more" if len(tables_no_pk) > 4 else "")
                    + " -- without PKs, deduplication and joins may be unreliable"
                ),
                "link": "/discovery",
            }
        )

    # Isolated tables
    connected = set()
    for r in relationships:
        connected.add(r.from_table)
        connected.add(r.to_table)
    isolated = [t.name for t in tables if t.name not in connected]
    if isolated:
        actions.append(
            {
                "phase": "review",
                "priority": "recommended",
                "title": f"Verify {len(isolated)} isolated table(s)",
                "detail": (
                    f"{', '.join(isolated[:4])}"
                    + (f" and {len(isolated) - 4} more" if len(isolated) > 4 else "")
                    + " -- no foreign key relationships detected. "
                    "Confirm these are standalone or identify missing links"
                ),
                "link": "/discovery",
            }
        )

    # --- Data Cleanup actions ---

    # High null columns
    high_null = [p for p in profiles if p.null_rate > 0.05]
    if high_null:
        error_level = [p for p in high_null if p.null_rate > 0.2]
        worst = max(high_null, key=lambda p: p.null_rate)
        if error_level:
            actions.append(
                {
                    "phase": "cleanup",
                    "priority": "blocking",
                    "title": f"{len(error_level)} column(s) are >20% null",
                    "detail": (
                        f"Worst: {worst.table_name}.{worst.column_name} "
                        f"at {worst.null_rate * 100:.0f}% -- decide whether to drop, "
                        "fill with defaults, or flag as legitimately sparse"
                    ),
                    "link": "/quality",
                }
            )
        elif len(high_null) > 0:
            actions.append(
                {
                    "phase": "cleanup",
                    "priority": "recommended",
                    "title": f"Review {len(high_null)} column(s) with >5% nulls",
                    "detail": (
                        f"Worst: {worst.table_name}.{worst.column_name} "
                        f"at {worst.null_rate * 100:.0f}% -- decide which need defaults "
                        "vs. which are legitimately optional"
                    ),
                    "link": "/quality",
                }
            )

    # Low FK integrity
    weak_fks = [r for r in relationships if r.referential_integrity < 0.95]
    if weak_fks:
        worst_fk = min(weak_fks, key=lambda r: r.referential_integrity)
        actions.append(
            {
                "phase": "cleanup",
                "priority": "recommended",
                "title": f"{len(weak_fks)} relationship(s) have weak referential integrity",
                "detail": (
                    f"Lowest: {worst_fk.from_table}.{worst_fk.from_column} -> "
                    f"{worst_fk.to_table}.{worst_fk.to_column} at "
                    f"{worst_fk.referential_integrity * 100:.0f}% -- "
                    "JOINs in mart models may silently drop rows"
                ),
                "link": "/quality",
            }
        )

    # --- Modeling actions ---

    pending_marts = [m for m in mart_models if m.status == "proposed"]
    if pending_marts:
        actions.append(
            {
                "phase": "modeling",
                "priority": "blocking",
                "title": f"Review {len(pending_marts)} mart model(s)",
                "detail": (
                    f"{', '.join(m.name for m in pending_marts[:3])}"
                    + (f" and {len(pending_marts) - 3} more" if len(pending_marts) > 3 else "")
                    + " -- mart models encode business logic and need human approval"
                ),
                "link": "/models",
            }
        )

    # Tables not covered by any mart
    if mart_models:
        mart_sources = set()
        for m in mart_models:
            mart_sources.update(m.source_tables)
        uncovered = [t.name for t in tables if t.name not in mart_sources]
        if uncovered:
            actions.append(
                {
                    "phase": "modeling",
                    "priority": "informational",
                    "title": f"{len(uncovered)} table(s) not referenced in any mart model",
                    "detail": (
                        f"{', '.join(uncovered[:4])}"
                        + (f" and {len(uncovered) - 4} more" if len(uncovered) > 4 else "")
                        + " -- consider whether analytical models should include them"
                    ),
                    "link": "/models",
                }
            )

    # --- Quality actions ---

    if quality_report and quality_report.failed > 0:
        actions.append(
            {
                "phase": "quality",
                "priority": "blocking",
                "title": f"{quality_report.failed} quality contract(s) failed",
                "detail": (
                    f"{quality_report.passed} of {quality_report.total_contracts} pass -- "
                    "review failures to decide whether data needs fixing or the contract "
                    "expectations should be adjusted"
                ),
                "link": "/quality",
            }
        )

    if contracts and quality_report:
        observing = [c for c in contracts if c.status == "observing"]
        if observing:
            actions.append(
                {
                    "phase": "quality",
                    "priority": "informational",
                    "title": f"{len(observing)} contract(s) in observation mode",
                    "detail": (
                        "Contracts are tracking violations without enforcing -- "
                        "review results to decide which to promote to enforcement"
                    ),
                    "link": "/quality",
                }
            )

    # --- Success signals (important -- show progress, not just problems) ---

    null_free = []
    for t in tables:
        t_profiles = [p for p in profiles if p.table_name == t.name]
        if t_profiles and all(p.null_rate == 0 for p in t_profiles):
            null_free.append(t.name)
    if null_free:
        actions.append(
            {
                "phase": "review",
                "priority": "success",
                "title": f"{len(null_free)} table(s) are 100% complete",
                "detail": ", ".join(null_free[:5]),
                "link": "/discovery",
            }
        )

    if quality_report and quality_report.failed == 0 and quality_report.total_contracts > 0:
        actions.append(
            {
                "phase": "quality",
                "priority": "success",
                "title": f"All {quality_report.total_contracts} quality contracts pass",
                "detail": "Quality baseline established -- data meets all auto-generated expectations",  # noqa: E501
                "link": "/quality",
            }
        )

    if exec_results:
        ok = sum(1 for r in exec_results if r.success)
        if ok == len(exec_results) and ok > 0:
            total_time = sum(r.execution_time_ms for r in exec_results)
            actions.append(
                {
                    "phase": "modeling",
                    "priority": "success",
                    "title": f"All {ok} models materialized successfully",
                    "detail": f"Total execution time: {total_time:.0f}ms",
                    "link": "/models",
                }
            )

    # Sort: blocking first, then recommended, then informational, success last
    priority_order = {"blocking": 0, "recommended": 1, "informational": 2, "success": 3}
    actions.sort(key=lambda a: priority_order.get(a["priority"], 9))

    return actions


# ---------------------------------------------------------------------------
# Model suggestions (unchanged)
# ---------------------------------------------------------------------------


def _compute_model_suggestions(tables, profiles, relationships, pipeline):
    """Generate suggestions for model improvements."""
    suggestions = []
    mart_models = pipeline.get("mart_models", [])

    # Check for tables not covered by any mart
    mart_sources = set()
    for m in mart_models:
        mart_sources.update(m.source_tables)
    uncovered = [t.name for t in tables if t.name not in mart_sources]
    if uncovered:
        suggestions.append(
            {
                "type": "coverage",
                "title": f"{len(uncovered)} source table(s) not used in any mart model",
                "detail": (
                    f"Tables {', '.join(uncovered)} are not directly referenced. "
                    "Consider whether analytical models should include them."
                ),
            }
        )

    # Suggest dedup for tables with low uniqueness on ID columns
    for p in profiles:
        if p.column_name.endswith("_id") and p.uniqueness_ratio < 1.0 and p.uniqueness_ratio > 0:
            suggestions.append(
                {
                    "type": "dedup",
                    "title": f"{p.table_name}.{p.column_name} is not fully unique",
                    "detail": (
                        f"Uniqueness: {p.uniqueness_ratio * 100:.1f}%. "
                        "Staging model should include deduplication logic."
                    ),
                }
            )

    # Suggest relationship validation for low-integrity FKs
    for r in relationships:
        if r.referential_integrity < 0.95:
            suggestions.append(
                {
                    "type": "integrity",
                    "title": (
                        f"Weak referential integrity: "
                        f"{r.from_table}.{r.from_column} -> {r.to_table}.{r.to_column}"
                    ),
                    "detail": (
                        f"Only {r.referential_integrity * 100:.0f}% of FK values found in PK. "
                        "JOINs in mart models may drop rows."
                    ),
                }
            )

    # Suggest quality observations for proposed marts
    for m in mart_models:
        if m.status == "proposed" and m.questions:
            suggestions.append(
                {
                    "type": "review",
                    "title": f"{m.name} has {len(m.questions)} open question(s)",
                    "detail": m.questions[0],
                }
            )

    return suggestions


# ---------------------------------------------------------------------------
# Catalog health (v2)
# ---------------------------------------------------------------------------


def _compute_catalog_health(request: Request, discovery, pipeline: dict | None = None) -> dict:
    """Return catalog health metrics for the insights dashboard."""
    store = request.app.state.metadata_store
    pipeline = pipeline or request.app.state.pipeline
    source = getattr(discovery, "source", None)
    if source is None:
        logger.warning("Discovery has no source attribute -- cannot compute catalog health")
        raise ValueError("Discovery missing source")
    source_name = source.name
    logger.info("Computing catalog health for source '%s'", source_name)

    metrics = store.get_catalog_metrics(source_name)
    dimensions = store.get_catalog_dimensions(source_name)
    entities = store.get_catalog_entities(source_name)

    # Progress and maturity
    progress = _compute_progress(discovery, pipeline, store, pipeline.get("project") or source_name)
    maturity, maturity_score = _compute_maturity(progress)

    # Project info
    project = pipeline.get("project") or store.get_project(source_name)
    catalog_confidence = project.get("catalog_confidence", 0.0) if project else 0.0

    return {
        "metrics_total": len(metrics),
        "metrics_confirmed": sum(1 for m in metrics if m.get("status") == "confirmed"),
        "dimensions_total": len(dimensions),
        "dimensions_confirmed": sum(1 for d in dimensions if d.get("status") == "confirmed"),
        "entities_total": len(entities),
        "catalog_confidence": catalog_confidence,
        "catalog_coverage": progress["catalog_coverage"],
        "maturity": maturity,
        "maturity_score": maturity_score,
    }
