"""Statistical insights -- detects significant patterns in materialized data.

Uses Polars for time-series windowing and scipy.stats for significance testing.
Scans mart tables with temporal + metric columns to surface anomalies,
change-points, and correlations automatically.

Wave E2 improvements:
- Benjamini-Hochberg FDR correction for multiple comparisons
- Normality testing with MAD fallback for non-Gaussian data
- Seasonal decomposition before anomaly detection
- Binary segmentation change-point detection (replaces naive midpoint split)
- Correlation detrending to avoid spurious trend-driven correlations
- IQR-based winsorization for outlier robustness
- Severity calibration with magnitude thresholds
"""

from __future__ import annotations

import json
import logging
import math
import statistics
import warnings
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import yaml
from scipy import stats

from headwater.analyzer.metadata_retrieval import RetrievedMetadata
from headwater.analyzer.semantic_schema import (
    infer_semantic_schema,
    quote_ident,
    roles_for_table,
)
from headwater.core.models import (
    DatasetContext,
    DiscoveryResult,
    GeneratedModel,
    InsightDetectionResult,
    InsightFamilyDiagnostic,
    SemanticColumnRole,
    StatisticalInsight,
    TableInfo,
)

logger = logging.getLogger(__name__)

# Minimum rows needed for meaningful statistical analysis
_MIN_ROWS = 10
_MIN_TEMPORAL_POINTS = 7
_ZSCORE_THRESHOLD = 2.0  # Flag values beyond 2 standard deviations
_P_VALUE_THRESHOLD = 0.05  # 95% confidence
_MAX_POLARS_LOAD_ROWS = 1_000_000

_INSIGHT_TYPE_TO_FAMILY = {
    "coverage_period": "temporal_coverage",
    "volume_distribution": "temporal_volume",
    "peak_period": "duration_peak_window",
    "duration_distribution": "duration_distribution",
    "wait_time_pattern": "lead_time_pattern",
    "geographic_hotspot": "location_distribution",
    "route_pair": "path_distribution",
    "congestion_proxy": "distance_efficiency",
    "data_quality": "data_quality",
}
_DEFAULT_FAMILY_SPEC = {
    "version": 1,
    "families": [
        {
            "key": "temporal_coverage",
            "required_roles": ["event_ts"],
            "priority": 5,
        },
        {
            "key": "temporal_volume",
            "required_roles": ["event_ts"],
            "priority": 8,
        },
        {
            "key": "duration_distribution",
            "required_roles": ["lifecycle_start_ts", "lifecycle_end_ts"],
            "priority": 7,
        },
        {
            "key": "data_quality",
            "required_roles": [],
            "priority": 9,
        },
    ],
}
_GENERIC_INSIGHT_TYPE_PRIORITY = {
    "period_comparison": 3.0,
    "change_point": 2.0,
    "correlation": 2.0,
    "temporal_anomaly": 1.0,
    "distribution_shift": 1.0,
}


def detect_insights(
    con: duckdb.DuckDBPyConnection,
    schema: str = "marts",
    discovery: DiscoveryResult | None = None,
    dataset_context: DatasetContext | None = None,
    models: list[GeneratedModel] | None = None,
    project_id: str | None = None,
    metadata: RetrievedMetadata | None = None,
) -> list[StatisticalInsight]:
    """Scan all materialized tables in a schema for statistical patterns.

    Automatically identifies temporal + metric column pairs, then runs:
    - Temporal anomaly detection (rolling z-scores with normality check)
    - Change-point detection (binary segmentation with BIC)
    - Cross-metric correlation (with detrending)

    Applies Benjamini-Hochberg FDR correction before returning.
    """
    return detect_insights_with_diagnostics(
        con,
        schema=schema,
        discovery=discovery,
        dataset_context=dataset_context,
        models=models,
        project_id=project_id,
        metadata=metadata,
    ).insights


def detect_insights_with_diagnostics(
    con: duckdb.DuckDBPyConnection,
    schema: str = "marts",
    discovery: DiscoveryResult | None = None,
    dataset_context: DatasetContext | None = None,
    models: list[GeneratedModel] | None = None,
    project_id: str | None = None,
    metadata: RetrievedMetadata | None = None,
) -> InsightDetectionResult:
    """Detect insights and return per-table/family execution diagnostics."""
    insights: list[StatisticalInsight] = []
    diagnostics: list[InsightFamilyDiagnostic] = []
    scoped_models = _models_for_discovery(models, discovery)
    tables = _filter_tables_for_models(_list_tables(con, schema), schema, scoped_models)

    if discovery is not None:
        family_spec = _load_family_spec(
            source_name=discovery.source.name,
            project_id=project_id,
            metadata=metadata,
        )
        semantic_schema = infer_semantic_schema(
            discovery,
            dataset_context,
            project_id=project_id,
            metadata=metadata,
        )
        for table_name in tables:
            source_table = _source_table_for_physical_table(table_name, discovery, scoped_models)
            if source_table is None:
                diagnostics.append(
                    InsightFamilyDiagnostic(
                        schema_name=schema,
                        physical_table=table_name,
                        family="semantic_schema",
                        status="skipped",
                        reason="No matching discovered source table for physical table.",
                    )
                )
                continue
            result = _detect_family_insights(
                con,
                schema,
                table_name,
                source_table.name,
                _roles_for_physical_table(
                    con,
                    schema,
                    table_name,
                    source_table.name,
                    roles_for_table(semantic_schema, source_table.name),
                ),
                family_spec,
            )
            insights.extend(result.insights)
            diagnostics.extend(result.diagnostics)

        if insights:
            return InsightDetectionResult(
                insights=_rank_family_insights(insights, family_spec),
                diagnostics=diagnostics,
            )

    for table_name in tables:
        try:
            df = _load_table(con, schema, table_name)
            if df is None or df.height < _MIN_ROWS:
                diagnostics.append(
                    InsightFamilyDiagnostic(
                        schema_name=schema,
                        physical_table=table_name,
                        table_name=table_name,
                        family="generic_statistical",
                        status="skipped",
                        reason="Table is too large for fallback load or has too few rows.",
                    )
                )
                continue

            temporal_cols = _find_temporal_columns(df)
            metric_cols = _find_metric_columns(df)
            before = len(insights)

            if temporal_cols and metric_cols:
                for t_col in temporal_cols:
                    for m_col in metric_cols:
                        insights.extend(_detect_temporal_anomalies(df, table_name, t_col, m_col))
                        insights.extend(
                            _detect_change_points_for_column(df, table_name, t_col, m_col)
                        )

            if len(metric_cols) >= 2:
                insights.extend(_detect_correlations(df, table_name, metric_cols))

            generated = len(insights) - before
            diagnostics.append(
                InsightFamilyDiagnostic(
                    schema_name=schema,
                    physical_table=table_name,
                    table_name=table_name,
                    family="generic_statistical",
                    status="generated" if generated else "skipped",
                    required_roles=["temporal_column", "metric_column"],
                    found_roles=[
                        *(["temporal_column"] if temporal_cols else []),
                        *(["metric_column"] if metric_cols else []),
                    ],
                    generated_count=generated,
                    reason=None if generated else "No temporal/metric pattern met thresholds.",
                )
            )

        except Exception as e:
            logger.warning("Statistical analysis failed for %s.%s: %s", schema, table_name, e)
            diagnostics.append(
                InsightFamilyDiagnostic(
                    schema_name=schema,
                    physical_table=table_name,
                    table_name=table_name,
                    family="generic_statistical",
                    status="failed",
                    reason=str(e),
                )
            )

    # Apply FDR correction to control false positives from multiple comparisons
    insights = _apply_fdr_correction(insights)

    return InsightDetectionResult(insights=insights, diagnostics=diagnostics)


# ---------------------------------------------------------------------------
# FDR Correction (E2.1)
# ---------------------------------------------------------------------------

def _apply_fdr_correction(
    insights: list[StatisticalInsight],
    alpha: float = 0.05,
) -> list[StatisticalInsight]:
    """Filter insights using Benjamini-Hochberg False Discovery Rate control."""
    if not insights:
        return []

    # Separate p-value insights from non-p-value insights
    with_p = [(i, i.p_value) for i in insights if i.p_value is not None]
    without_p = [i for i in insights if i.p_value is None]

    if not with_p:
        return insights

    # Sort by p-value ascending
    with_p.sort(key=lambda x: x[1])
    m = len(with_p)
    corrected = []
    for rank, (insight, p) in enumerate(with_p):
        bh_threshold = alpha * (rank + 1) / m
        if p <= bh_threshold:
            corrected.append(insight)
        else:
            break  # All subsequent p-values are larger

    return corrected + without_p


# ---------------------------------------------------------------------------
# Semantic Insight Families
# ---------------------------------------------------------------------------

def _detect_family_insights(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    physical_table: str,
    source_table: str,
    roles: dict[str, SemanticColumnRole],
    family_spec: dict,
) -> InsightDetectionResult:
    """Run semantic insight families with DuckDB-side aggregations."""
    found_roles = sorted(roles)
    if not roles:
        return InsightDetectionResult(
            diagnostics=[
                InsightFamilyDiagnostic(
                    schema_name=schema,
                    physical_table=physical_table,
                    table_name=source_table,
                    family="semantic_schema",
                    status="skipped",
                    found_roles=[],
                    reason="No semantic roles inferred for this table.",
                )
            ]
        )

    table_ref = f"{quote_ident(schema)}.{quote_ident(physical_table)}"
    configured_families = {
        str(family.get("key")): family
        for family in family_spec.get("families", [])
        if isinstance(family, dict) and family.get("key")
    }

    insights: list[StatisticalInsight] = []
    diagnostics: list[InsightFamilyDiagnostic] = []
    start = roles.get("lifecycle_start_ts") or roles.get("event_ts")
    end = roles.get("lifecycle_end_ts")
    request = roles.get("request_ts")
    origin = roles.get("origin_id") or roles.get("location_id")
    dest = roles.get("destination_id")
    distance = roles.get("distance")
    service = roles.get("service_type")

    try:
        canonical_ref, derived = _create_canonical_view(
            con,
            table_ref,
            schema,
            physical_table,
            start=start,
            end=end,
            request=request,
            distance=distance,
            origin=origin,
            dest=dest,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "Semantic canonical view failed for %s.%s: %s",
            schema,
            physical_table,
            exc,
        )
        return InsightDetectionResult(
            diagnostics=[
                InsightFamilyDiagnostic(
                    schema_name=schema,
                    physical_table=physical_table,
                    table_name=source_table,
                    family="semantic_schema",
                    status="failed",
                    found_roles=found_roles,
                    reason=str(exc),
                )
            ]
        )
    table_ref = canonical_ref
    start_expr = derived.get("start_ts")
    duration_expr = derived.get("duration_min")
    wait_expr = derived.get("wait_min")

    def run_family(
        family: str,
        required_roles: list[str],
        can_run: bool,
        fn,
        skip_reason: str,
    ) -> None:
        if not can_run:
            diagnostics.append(
                InsightFamilyDiagnostic(
                    schema_name=schema,
                    physical_table=physical_table,
                    table_name=source_table,
                    family=family,
                    status="skipped",
                    required_roles=required_roles,
                    found_roles=found_roles,
                    reason=skip_reason,
                )
            )
            return
        try:
            before = len(insights)
            family_insights = fn()
            insights.extend(family_insights)
            generated = len(insights) - before
            diagnostics.append(
                InsightFamilyDiagnostic(
                    schema_name=schema,
                    physical_table=physical_table,
                    table_name=source_table,
                    family=family,
                    status="generated" if generated else "skipped",
                    required_roles=required_roles,
                    found_roles=found_roles,
                    generated_count=generated,
                    reason=None if generated else "No rows met family support thresholds.",
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Semantic insight family failed for %s.%s family=%s: %s",
                schema,
                physical_table,
                family,
                exc,
            )
            diagnostics.append(
                InsightFamilyDiagnostic(
                    schema_name=schema,
                    physical_table=physical_table,
                    table_name=source_table,
                    family=family,
                    status="failed",
                    required_roles=required_roles,
                    found_roles=found_roles,
                    reason=str(exc),
                )
            )

    family_handlers = {
        "temporal_coverage": (
            bool(start and start_expr),
            lambda: _coverage_family(con, table_ref, source_table, start, start_expr),
            "Requires a temporal role that can be cast to TIMESTAMP.",
        ),
        "temporal_volume": (
            bool(start and start_expr),
            lambda: _volume_family(con, table_ref, source_table, start, start_expr, duration_expr),
            "Requires a temporal role that can be cast to TIMESTAMP.",
        ),
        "duration_peak_window": (
            bool(start_expr and duration_expr),
            lambda: _peak_family(con, table_ref, source_table, start_expr, duration_expr),
            "Requires temporal start and lifecycle end roles to derive duration.",
        ),
        "duration_distribution": (
            bool(duration_expr),
            lambda: _duration_family(con, table_ref, source_table, start_expr, duration_expr),
            "Requires lifecycle start and end roles to derive duration.",
        ),
        "location_distribution": (
            bool(origin and duration_expr),
            lambda: _geo_family(con, table_ref, source_table, origin, duration_expr),
            "Requires origin/location and duration roles.",
        ),
        "path_distribution": (
            bool(origin and dest and duration_expr),
            lambda: _route_family(con, table_ref, source_table, origin, dest, duration_expr),
            "Requires origin, destination, and duration roles.",
        ),
        "distance_efficiency": (
            bool(distance and duration_expr),
            lambda: _congestion_family(con, table_ref, source_table, distance, duration_expr),
            "Requires distance and duration roles.",
        ),
        "data_quality": (
            True,
            lambda: _quality_family(
                con,
                table_ref,
                source_table,
                origin,
                dest,
                distance,
                duration_expr,
                service,
            ),
            "Quality family is always eligible.",
        ),
        "lead_time_pattern": (
            bool(start_expr and wait_expr),
            lambda: _wait_family(con, table_ref, source_table, start_expr, wait_expr, service),
            "Requires request and event/lifecycle start roles.",
        ),
    }

    for family_name, config in configured_families.items():
        handler = family_handlers.get(family_name)
        if handler is None:
            continue
        required_roles = [str(role) for role in config.get("required_roles", [])]
        can_run, fn, skip_reason = handler
        run_family(family_name, required_roles, can_run, fn, skip_reason)

    return InsightDetectionResult(insights=insights, diagnostics=diagnostics)


def _coverage_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    start: SemanticColumnRole,
    start_expr: str,
) -> list[StatisticalInsight]:
    row = con.execute(
        f"""
        SELECT MIN({start_expr}) AS min_ts, MAX({start_expr}) AS max_ts, COUNT(*) AS trips
        FROM {table_ref}
        WHERE {start_expr} IS NOT NULL
        """
    ).fetchone()
    if not row or row[0] is None or row[1] is None:
        return []
    days = _days_between(row[0], row[1])
    return [
        StatisticalInsight(
            metric=start.column_name,
            table_name=source_table,
            insight_type="coverage_period",
            description=(
                f"{_humanize(source_table)} covers {_format_date(row[0])} to "
                f"{_format_date(row[1])} across {int(row[2]):,} "
                f"{_record_label(source_table)}; avoid claims "
                "outside this observed period."
            ),
            magnitude=float(days),
            confidence_level="99%",
            comparison_baseline="observed timestamp range",
            severity="info",
            support_count=int(row[2]),
        )
    ]


def _create_canonical_view(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    schema: str,
    physical_table: str,
    *,
    start: SemanticColumnRole | None,
    end: SemanticColumnRole | None,
    request: SemanticColumnRole | None,
    distance: SemanticColumnRole | None,
    origin: SemanticColumnRole | None,
    dest: SemanticColumnRole | None,
) -> tuple[str, dict[str, str]]:
    """Define canonical derived fields once in DuckDB and reuse them downstream."""
    expressions: dict[str, str] = {}
    start_ts_expr = _temporal_expr(start)
    end_ts_expr = _temporal_expr(end)
    request_ts_expr = _temporal_expr(request)
    if start_ts_expr:
        expressions["start_ts"] = start_ts_expr
    if end_ts_expr:
        expressions["end_ts"] = end_ts_expr
    if request_ts_expr:
        expressions["request_ts"] = request_ts_expr

    raw_duration = _duration_expr(start_ts_expr, end_ts_expr)
    if raw_duration:
        expressions["duration_min"] = raw_duration
    raw_wait = _wait_expr(request_ts_expr, start_ts_expr)
    if raw_wait:
        expressions["wait_min"] = raw_wait
    if raw_duration and distance:
        dist = quote_ident(distance.column_name)
        expressions["speed_per_hour"] = (
            f"CASE WHEN ({raw_duration}) > 0 THEN {dist} / (({raw_duration}) / 60.0) END"
        )
    if origin and dest:
        expressions["route_pair"] = (
            f"CAST({quote_ident(origin.column_name)} AS VARCHAR) || ' -> ' || "
            f"CAST({quote_ident(dest.column_name)} AS VARCHAR)"
        )
    if not expressions:
        return table_ref, {}

    view_name = quote_ident(f"__hw_insights_{schema}_{_safe_identifier(physical_table)}")
    select_exprs = [
        f"{expr} AS {quote_ident(f'__hw_{name}')}" for name, expr in expressions.items()
    ]
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT *, {", ".join(select_exprs)}
        FROM {table_ref}
        """
    )
    derived = {name: quote_ident(f"__hw_{name}") for name in expressions}
    return view_name, derived


def _volume_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    start: SemanticColumnRole,
    start_expr: str,
    duration_expr: str | None,
) -> list[StatisticalInsight]:
    rows = con.execute(
        f"""
        SELECT CAST(EXTRACT(hour FROM {start_expr}) AS INTEGER) AS hour_bucket, COUNT(*) AS n
        FROM {table_ref}
        WHERE {start_expr} IS NOT NULL
        GROUP BY 1
        ORDER BY n DESC
        LIMIT 3
        """
    ).fetchall()
    if not rows:
        return []
    top_hour, top_n = rows[0]
    desc = (
        f"{_format_hour(top_hour)} is the busiest {_event_label(start.column_name)} "
        f"with {int(top_n):,} {_record_label(source_table)}."
    )
    magnitude = float(top_n)
    if duration_expr:
        peak_avg_expr = (
            f"AVG(CASE WHEN EXTRACT(hour FROM {start_expr}) BETWEEN 15 AND 18 "
            f"THEN {duration_expr} END)"
        )
        off_avg_expr = (
            f"AVG(CASE WHEN EXTRACT(hour FROM {start_expr}) NOT BETWEEN 15 AND 18 "
            f"THEN {duration_expr} END)"
        )
        peak = con.execute(
            f"""
            SELECT
                {peak_avg_expr} AS peak_avg,
                {off_avg_expr} AS off_avg
            FROM {table_ref}
            WHERE {start_expr} IS NOT NULL AND {duration_expr} BETWEEN 0 AND 1440
            """
        ).fetchone()
        if peak and peak[0] is not None and peak[1] is not None:
            diff = float(peak[0]) - float(peak[1])
            direction = "longer" if diff >= 0 else "shorter"
            desc += (
                f" The 3 PM-6 PM period averages {abs(diff):.2f} minutes "
                f"{direction} than other hours."
            )
            magnitude = abs(diff)
    return [
        StatisticalInsight(
            metric="record_count",
            table_name=source_table,
            insight_type="volume_distribution",
            description=desc,
            magnitude=round(magnitude, 2),
            confidence_level="99%",
            comparison_baseline="hour-of-day volume",
            severity="info",
            support_count=int(top_n),
        )
    ]


def _peak_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    start_expr: str,
    duration_expr: str,
) -> list[StatisticalInsight]:
    weekday_filter = f"EXTRACT(dow FROM {start_expr}) BETWEEN 1 AND 5"
    weekend_filter = f"EXTRACT(dow FROM {start_expr}) IN (0, 6)"
    row = con.execute(
        f"""
        SELECT
            COUNT(CASE WHEN {weekday_filter} THEN 1 END) AS weekday_n,
            AVG(CASE WHEN {weekday_filter} THEN {duration_expr} END) AS weekday_avg,
            quantile_cont(
                CASE WHEN {weekday_filter} THEN {duration_expr} END, 0.9
            ) AS weekday_p90,
            COUNT(CASE WHEN {weekend_filter} THEN 1 END) AS weekend_n,
            AVG(CASE WHEN {weekend_filter} THEN {duration_expr} END) AS weekend_avg,
            quantile_cont(
                CASE WHEN {weekend_filter} THEN {duration_expr} END, 0.9
            ) AS weekend_p90
        FROM {table_ref}
        WHERE {start_expr} IS NOT NULL AND {duration_expr} BETWEEN 0 AND 1440
        """
    ).fetchone()
    if not row or row[1] is None or row[4] is None:
        return []
    diff = float(row[1]) - float(row[4])
    if abs(diff) < 0.5:
        return []
    return [
        StatisticalInsight(
            metric="duration_min",
            table_name=source_table,
            insight_type="peak_period",
            description=(
                f"Weekday {_record_label(source_table)} average {abs(diff):.2f} minutes "
                f"{'longer' if diff >= 0 else 'shorter'} than weekend "
                f"{_record_label(source_table)} "
                f"(p90 {float(row[2]):.1f} vs {float(row[5]):.1f} minutes)."
            ),
            magnitude=round(abs(diff), 2),
            confidence_level="99%",
            comparison_baseline="weekday vs weekend",
            severity="info" if abs(diff) < 5 else "warning",
            support_count=int(row[0] or 0) + int(row[3] or 0),
        )
    ]


def _duration_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    start_expr: str | None,
    duration_expr: str,
) -> list[StatisticalInsight]:
    group_expr = "1"
    group_label = "overall"
    if start_expr:
        group_expr = f"CAST(EXTRACT(hour FROM {start_expr}) AS INTEGER)"
        group_label = "hour"
    row = con.execute(
        f"""
        WITH grouped AS (
            SELECT {group_expr} AS bucket,
                   COUNT(*) AS n,
                   AVG({duration_expr}) AS avg_duration,
                   quantile_cont({duration_expr}, 0.5) AS p50_duration,
                   quantile_cont({duration_expr}, 0.9) AS p90_duration,
                   quantile_cont({duration_expr}, 0.95) AS p95_duration
            FROM {table_ref}
            WHERE {duration_expr} BETWEEN 0 AND 1440
            GROUP BY 1
            HAVING COUNT(*) >= 30
        )
        SELECT bucket, n, avg_duration, p50_duration, p90_duration, p95_duration
        FROM grouped
        ORDER BY p90_duration DESC, n DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return []
    bucket = _format_hour(row[0]) if group_label == "hour" else "overall"
    return [
        StatisticalInsight(
            metric="duration_min",
            table_name=source_table,
            insight_type="duration_distribution",
            description=(
                f"The slowest {group_label} is {bucket}: avg {float(row[2]):.1f} min, "
                f"p50 {float(row[3]):.1f}, p90 {float(row[4]):.1f}, p95 {float(row[5]):.1f} "
                f"across {int(row[1]):,} {_record_label(source_table)}."
            ),
            magnitude=round(float(row[4]), 2),
            confidence_level="99%",
            comparison_baseline=f"p90 duration by {group_label}",
            severity="info",
            support_count=int(row[1]),
        )
    ]


def _geo_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    origin: SemanticColumnRole,
    duration_expr: str,
) -> list[StatisticalInsight]:
    origin_col = quote_ident(origin.column_name)
    row = con.execute(
        f"""
        WITH grouped AS (
            SELECT {origin_col} AS origin_value,
                   COUNT(*) AS n,
                   AVG({duration_expr}) AS avg_duration,
                   quantile_cont({duration_expr}, 0.9) AS p90_duration
            FROM {table_ref}
            WHERE {origin_col} IS NOT NULL AND {duration_expr} BETWEEN 0 AND 1440
            GROUP BY 1
            HAVING COUNT(*) >= 30
        )
        SELECT origin_value, n, avg_duration, p90_duration
        FROM grouped
        ORDER BY p90_duration DESC, n DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return []
    label = _format_dimension_value(row[0])
    return [
        StatisticalInsight(
            metric="duration_min",
            table_name=source_table,
            insight_type="geographic_hotspot",
            description=(
                f"{label} has the longest high-volume {_humanize(origin.column_name)} durations: "
                f"avg {float(row[2]):.1f} min, p90 {float(row[3]):.1f} min across "
                f"{int(row[1]):,} {_record_label(source_table)}."
            ),
            magnitude=round(float(row[3]), 2),
            confidence_level="99%",
            comparison_baseline=f"p90 duration by {origin.column_name}",
            severity="warning",
            support_count=int(row[1]),
        )
    ]


def _route_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    origin: SemanticColumnRole,
    dest: SemanticColumnRole,
    duration_expr: str,
) -> list[StatisticalInsight]:
    o_col = quote_ident(origin.column_name)
    d_col = quote_ident(dest.column_name)
    row = con.execute(
        f"""
        WITH grouped AS (
            SELECT {o_col} AS origin_value, {d_col} AS destination_value,
                   COUNT(*) AS n,
                   AVG({duration_expr}) AS avg_duration,
                   quantile_cont({duration_expr}, 0.9) AS p90_duration
            FROM {table_ref}
            WHERE {o_col} IS NOT NULL AND {d_col} IS NOT NULL AND {duration_expr} BETWEEN 0 AND 1440
            GROUP BY 1, 2
            HAVING COUNT(*) >= 30
        )
        SELECT origin_value, destination_value, n, avg_duration, p90_duration
        FROM grouped
        ORDER BY p90_duration DESC, n DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return []
    route = f"{_format_dimension_value(row[0])} -> {_format_dimension_value(row[1])}"
    return [
        StatisticalInsight(
            metric="duration_min",
            table_name=source_table,
            insight_type="route_pair",
            description=(
                f"The slowest high-volume route is {route}: avg {float(row[3]):.1f} min, "
                f"p90 {float(row[4]):.1f} min across {int(row[2]):,} "
                f"{_record_label(source_table)}."
            ),
            magnitude=round(float(row[4]), 2),
            confidence_level="99%",
            comparison_baseline="p90 duration by origin/destination pair",
            severity="warning",
            support_count=int(row[2]),
        )
    ]


def _congestion_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    distance: SemanticColumnRole,
    duration_expr: str,
) -> list[StatisticalInsight]:
    dist = quote_ident(distance.column_name)
    speed_expr = f"CASE WHEN {duration_expr} > 0 THEN {dist} / ({duration_expr} / 60.0) END"
    row = con.execute(
        f"""
        SELECT COUNT(*) AS n,
               quantile_cont({speed_expr}, 0.1) AS p10_speed,
               AVG({speed_expr}) AS avg_speed
        FROM {table_ref}
        WHERE {dist} > 0 AND {duration_expr} BETWEEN 1 AND 1440 AND {speed_expr} BETWEEN 0 AND 200
        """
    ).fetchone()
    if not row or row[1] is None:
        return []
    return [
        StatisticalInsight(
            metric="speed_per_hour",
            table_name=source_table,
            insight_type="congestion_proxy",
            description=(
                f"Low-speed records flag potential congestion: p10 speed is "
                f"{float(row[1]):.1f} distance-units/hour and average speed is "
                f"{float(row[2]):.1f} across {int(row[0]):,} valid "
                f"{_record_label(source_table)}."
            ),
            magnitude=round(float(row[1]), 2),
            confidence_level="95%",
            comparison_baseline="distance divided by lifecycle duration",
            severity="info",
            support_count=int(row[0]),
        )
    ]


def _quality_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    origin: SemanticColumnRole | None,
    dest: SemanticColumnRole | None,
    distance: SemanticColumnRole | None,
    duration_expr: str | None,
    service: SemanticColumnRole | None,
) -> list[StatisticalInsight]:
    insights: list[StatisticalInsight] = []
    service_col = quote_ident(service.column_name) if service else None
    cols = [role for role in (origin, dest) if role]
    for role in cols:
        col = quote_ident(role.column_name)
        if service_col:
            row = con.execute(
                f"""
                WITH grouped AS (
                    SELECT CAST({service_col} AS VARCHAR) AS service_value,
                           COUNT(*) AS n,
                           SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS nulls
                    FROM {table_ref}
                    WHERE {service_col} IS NOT NULL
                    GROUP BY 1
                    HAVING COUNT(*) >= 30
                )
                SELECT service_value, n, nulls
                FROM grouped
                WHERE CAST(nulls AS DOUBLE) / CAST(n AS DOUBLE) >= 0.2
                ORDER BY CAST(nulls AS DOUBLE) / CAST(n AS DOUBLE) DESC, n DESC
                LIMIT 1
                """
            ).fetchone()
            if row and row[1]:
                rate = float(row[2]) / float(row[1]) * 100
                insights.append(
                    StatisticalInsight(
                        metric=role.column_name,
                        table_name=source_table,
                        insight_type="data_quality",
                        description=(
                            f"{_service_value_label(row[0])} location analysis is unreliable "
                            f"unless null {_location_role_label(role.column_name)} are handled: "
                            f"{rate:.1f}% are missing."
                        ),
                        magnitude=round(rate, 2),
                        confidence_level="99%",
                        comparison_baseline="service-level null-rate threshold 20%",
                        severity="critical" if rate >= 50 else "warning",
                        support_count=int(row[1]),
                    )
                )
                continue
        row = con.execute(
            "SELECT COUNT(*) AS n, "
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS nulls "
            f"FROM {table_ref}"
        ).fetchone()
        if row and row[0] and float(row[1]) / float(row[0]) >= 0.2:
            rate = float(row[1]) / float(row[0]) * 100
            service_label = _service_label(source_table, None)
            insights.append(
                StatisticalInsight(
                    metric=role.column_name,
                    table_name=source_table,
                    insight_type="data_quality",
                    description=(
                        f"{service_label} location analysis is unreliable unless null "
                        f"{role.column_name} values are handled: {rate:.1f}% are missing."
                    ),
                    magnitude=round(rate, 2),
                    confidence_level="99%",
                    comparison_baseline="null-rate threshold 20%",
                    severity="critical" if rate >= 50 else "warning",
                    support_count=int(row[0]),
                )
            )
    if distance:
        col = quote_ident(distance.column_name)
        row = con.execute(
            "SELECT COUNT(*) AS n, "
            f"SUM(CASE WHEN {col} <= 0 THEN 1 ELSE 0 END) AS bad "
            f"FROM {table_ref}"
        ).fetchone()
        if row and row[0] and float(row[1]) / float(row[0]) >= 0.05:
            rate = float(row[1]) / float(row[0]) * 100
            insights.append(
                StatisticalInsight(
                    metric=distance.column_name,
                    table_name=source_table,
                    insight_type="data_quality",
                    description=(
                        f"{rate:.1f}% of records have non-positive "
                        f"{distance.column_name}; distance-based insights should filter them."
                    ),
                    magnitude=round(rate, 2),
                    confidence_level="99%",
                    comparison_baseline="non-positive distance rate",
                    severity="warning",
                    support_count=int(row[0]),
                )
            )
    if duration_expr:
        row = con.execute(
            "SELECT COUNT(*) AS n, "
            f"SUM(CASE WHEN {duration_expr} <= 0 OR {duration_expr} > 1440 "
            f"THEN 1 ELSE 0 END) AS bad FROM {table_ref}"
        ).fetchone()
        if row and row[0] and float(row[1]) / float(row[0]) >= 0.01:
            rate = float(row[1]) / float(row[0]) * 100
            insights.append(
                StatisticalInsight(
                    metric="duration_min",
                    table_name=source_table,
                    insight_type="data_quality",
                    description=(
                        f"{rate:.1f}% of records have impossible or extreme "
                        "lifecycle duration; duration insights should filter them."
                    ),
                    magnitude=round(rate, 2),
                    confidence_level="99%",
                    comparison_baseline="duration outside 0-1440 minutes",
                    severity="warning",
                    support_count=int(row[0]),
                )
            )
    return insights


def _wait_family(
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    source_table: str,
    start_expr: str,
    wait_expr: str,
    service: SemanticColumnRole | None,
) -> list[StatisticalInsight]:
    service_col = quote_ident(service.column_name) if service else None
    if service_col:
        rows = con.execute(
            f"""
            WITH grouped AS (
                SELECT CAST({service_col} AS VARCHAR) AS service_value,
                       CAST(EXTRACT(hour FROM {start_expr}) AS INTEGER) AS hour_bucket,
                       COUNT(*) AS n,
                       quantile_cont({wait_expr}, 0.9) AS p90_wait
                FROM {table_ref}
                WHERE {service_col} IS NOT NULL
                  AND {start_expr} IS NOT NULL
                  AND {wait_expr} BETWEEN 0 AND 240
                GROUP BY 1, 2
                HAVING COUNT(*) >= 30
            ),
            ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY service_value
                           ORDER BY p90_wait DESC, n DESC
                       ) AS service_hour_rank
                FROM grouped
            ),
            chosen_service AS (
                SELECT service_value,
                       MAX(p90_wait) AS top_p90,
                       SUM(CASE WHEN service_hour_rank <= 2 THEN n ELSE 0 END) AS support_n
                FROM ranked
                GROUP BY 1
                ORDER BY top_p90 DESC, support_n DESC
                LIMIT 1
            )
            SELECT r.service_value, r.hour_bucket, r.n, r.p90_wait
            FROM ranked r
            INNER JOIN chosen_service c
                ON r.service_value = c.service_value
            WHERE r.service_hour_rank <= 2
            ORDER BY r.p90_wait DESC, r.n DESC
            """
        ).fetchall()
        if rows:
            hours = " and ".join(_format_hour(row[1]) for row in rows)
            support = sum(int(row[2]) for row in rows)
            return [
                StatisticalInsight(
                    metric="wait_min",
                    table_name=source_table,
                    insight_type="wait_time_pattern",
                    description=(
                        f"{_service_value_label(rows[0][0])} wait time is highest around "
                        f"{hours}, with p90 wait near {float(rows[0][3]):.1f} minutes."
                    ),
                    magnitude=round(float(rows[0][3]), 2),
                    confidence_level="99%",
                    comparison_baseline="service-level p90 wait by hour",
                    severity="info",
                    support_count=support,
                )
            ]
    rows = con.execute(
        f"""
        SELECT CAST(EXTRACT(hour FROM {start_expr}) AS INTEGER) AS hour_bucket,
               COUNT(*) AS n,
               quantile_cont({wait_expr}, 0.9) AS p90_wait
        FROM {table_ref}
        WHERE {start_expr} IS NOT NULL AND {wait_expr} BETWEEN 0 AND 240
        GROUP BY 1
        HAVING COUNT(*) >= 30
        ORDER BY p90_wait DESC, n DESC
        LIMIT 2
        """
    ).fetchall()
    if not rows:
        return []
    hours = " and ".join(_format_hour(row[0]) for row in rows)
    support = sum(int(row[1]) for row in rows)
    return [
        StatisticalInsight(
            metric="wait_min",
            table_name=source_table,
            insight_type="wait_time_pattern",
            description=(
                f"Wait time is highest around {hours}, with p90 wait near "
                f"{float(rows[0][2]):.1f} minutes."
            ),
            magnitude=round(float(rows[0][2]), 2),
            confidence_level="99%",
            comparison_baseline="p90 wait by hour",
            severity="info",
            support_count=support,
        )
    ]


def insight_type_priority_weights(family_spec: dict | None = None) -> dict[str, float]:
    """Return ranking priorities by insight type, backed by the family catalog."""
    priorities = dict(_GENERIC_INSIGHT_TYPE_PRIORITY)
    family_spec = family_spec or _load_family_spec()
    configured = {
        str(family.get("key")): float(family.get("priority", 1.0))
        for family in family_spec.get("families", [])
        if isinstance(family, dict) and family.get("key")
    }
    for insight_type, family in _INSIGHT_TYPE_TO_FAMILY.items():
        priorities[insight_type] = configured.get(family, priorities.get(insight_type, 1.0))
    return priorities


def _rank_family_insights(
    insights: list[StatisticalInsight],
    family_spec: dict | None = None,
) -> list[StatisticalInsight]:
    severity_weight = {"critical": 3.0, "warning": 2.0, "info": 1.0}
    type_weight = insight_type_priority_weights(family_spec)
    return sorted(
        insights,
        key=lambda i: (
            -(
                abs(i.magnitude)
                * math.log10((i.support_count or 0) + 10)
                * severity_weight.get(i.severity, 1.0)
                * max(type_weight.get(i.insight_type, 1.0), 1.0)
            ),
            i.table_name,
            i.metric,
        ),
    )


# ---------------------------------------------------------------------------
# Normality Testing (E2.2)
# ---------------------------------------------------------------------------

def _check_normality(values: list[float], sample_size: int = 200) -> bool:
    """Test if data is approximately normal using Jarque-Bera test."""
    sample = values[:sample_size] if len(values) > sample_size else values
    if len(sample) < 20:
        return True  # Not enough data to test; assume normal
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            _, p = stats.jarque_bera(sample)
        return p > 0.05  # Fail to reject normality
    except Exception:
        return True  # On error, fall back to normal assumption


def _compute_mad_zscore(value: float, values: list[float]) -> float:
    """Compute Modified Z-score using Median Absolute Deviation (MAD).

    Robust alternative to standard z-score for non-normal distributions.
    When MAD is 0 (highly concentrated data), falls back to mean-based deviation.
    """
    median = statistics.median(values)
    mad = statistics.median([abs(v - median) for v in values])
    if mad == 0:
        # MAD=0 means most values are identical. Use mean absolute deviation instead.
        mean_ad = statistics.mean([abs(v - median) for v in values])
        if mean_ad == 0:
            # All values identical; any different value is anomalous
            if value != median:
                return 10.0 if value > median else -10.0
            return 0.0
        return 0.6745 * (value - median) / mean_ad
    # 0.6745 is the 0.75th quantile of the standard normal distribution
    return 0.6745 * (value - median) / mad


# ---------------------------------------------------------------------------
# Seasonal Adjustment (E2.3)
# ---------------------------------------------------------------------------

def _detect_period(values: list[float], max_period: int = 60) -> int | None:
    """Auto-detect periodicity using autocorrelation peaks."""
    n = len(values)
    if n < 24:
        return None

    mean = statistics.mean(values)
    var = statistics.variance(values)
    if var < 1e-10:
        return None

    # Compute autocorrelation for candidate periods
    max_lag = min(max_period, n // 2)
    autocorrs = []
    for lag in range(1, max_lag + 1):
        corr = sum((values[i] - mean) * (values[i + lag] - mean) for i in range(n - lag))
        corr /= (n - lag) * var
        autocorrs.append((lag, corr))

    # Find the first significant peak (above 0.3)
    for i in range(1, len(autocorrs) - 1):
        lag, corr = autocorrs[i]
        if corr > 0.3:
            prev_corr = autocorrs[i - 1][1]
            next_corr = autocorrs[i + 1][1]
            if corr >= prev_corr and corr >= next_corr:
                return lag

    return None


def _deseasonalize(values: list[float], period: int | None = None) -> tuple[list[float], bool]:
    """Remove seasonal component if detected. Returns (residuals, is_seasonal)."""
    if period is None:
        period = _detect_period(values)
        if period is None:
            return values, False

    if len(values) < 2 * period:
        return values, False

    # Compute seasonal index per period position
    n = len(values)
    seasonal_index = [0.0] * period
    counts = [0] * period
    for i, v in enumerate(values):
        pos = i % period
        seasonal_index[pos] += v
        counts[pos] += 1
    seasonal_index = [s / c if c > 0 else 0 for s, c in zip(seasonal_index, counts, strict=True)]
    grand_mean = sum(seasonal_index) / period

    # Residuals = observed - seasonal + grand_mean
    residuals = [values[i] - seasonal_index[i % period] + grand_mean for i in range(n)]
    return residuals, True


# ---------------------------------------------------------------------------
# Winsorization (E2.6)
# ---------------------------------------------------------------------------

def _winsorize(values: list[float], percentile: float = 0.01) -> list[float]:
    """Clip extreme values to the 1st/99th percentile."""
    if len(values) < 10:
        return values
    sorted_v = sorted(values)
    low_idx = max(0, int(len(sorted_v) * percentile))
    high_idx = min(len(sorted_v) - 1, int(len(sorted_v) * (1 - percentile)))
    low = sorted_v[low_idx]
    high = sorted_v[high_idx]
    return [max(low, min(high, v)) for v in values]


# ---------------------------------------------------------------------------
# Severity Calibration (E2.7)
# ---------------------------------------------------------------------------

def _calibrate_severity(p_value: float | None, magnitude_pct: float) -> str | None:
    """Determine severity using both statistical significance AND practical magnitude.

    Returns None if the insight should not be reported (magnitude too small).
    """
    abs_mag = abs(magnitude_pct)

    # Critical: large magnitude AND highly significant
    if abs_mag > 50 and p_value is not None and p_value < 0.001:
        return "critical"
    # Warning: moderate magnitude AND significant
    if abs_mag > 20 and p_value is not None and p_value < 0.01:
        return "warning"
    # Info: noticeable magnitude AND significant
    if abs_mag > 5 and p_value is not None and p_value < 0.05:
        return "info"
    # Below thresholds: not worth reporting
    return None


# ---------------------------------------------------------------------------
# Table Discovery
# ---------------------------------------------------------------------------

def _list_tables(con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    """List all tables in a schema."""
    try:
        result = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [schema],
        ).fetchall()
        return [row[0] for row in result]
    except Exception:
        return []


def _load_table(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> pl.DataFrame | None:
    """Load a table into a Polars DataFrame via Arrow.

    Casts Decimal columns to Float64 so scipy/numpy can process them.
    """
    try:
        row_count = _table_row_count(con, schema, table)
        if row_count is not None and row_count > _MAX_POLARS_LOAD_ROWS:
            logger.info(
                "Skipping Polars full-table load for %s.%s (%d rows)",
                schema,
                table,
                row_count,
            )
            return None
        arrow = con.execute(f"SELECT * FROM {schema}.{table}").arrow()
        df = pl.from_arrow(arrow)
        # Cast Decimal columns to Float64 for scipy compatibility
        decimal_cols = [
            c for c in df.columns
            if df[c].dtype == pl.Decimal or str(df[c].dtype).startswith("Decimal")
        ]
        if decimal_cols:
            df = df.with_columns([pl.col(c).cast(pl.Float64) for c in decimal_cols])
        return df
    except Exception as e:
        logger.debug("Could not load %s.%s: %s", schema, table, e)
        return None


def _table_row_count(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
) -> int | None:
    try:
        value = con.execute(
            f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)}"
        ).fetchone()[0]
        return int(value)
    except Exception:
        return None


def _find_temporal_columns(df: pl.DataFrame) -> list[str]:
    """Identify date/datetime columns suitable for time-series analysis."""
    temporal = []
    for col_name in df.columns:
        dtype = df[col_name].dtype
        if dtype in (pl.Date, pl.Datetime, pl.Datetime("ms"), pl.Datetime("us"), pl.Datetime("ns")):
            temporal.append(col_name)
    return temporal


def _find_metric_columns(df: pl.DataFrame) -> list[str]:
    """Identify numeric columns suitable for statistical analysis."""
    metrics = []
    for col_name in df.columns:
        dtype = df[col_name].dtype
        if dtype.is_numeric():
            # Skip ID-like columns and counts that are always 1
            if col_name.endswith("_id") or col_name == "id":
                continue
            non_null = df[col_name].drop_nulls()
            if non_null.len() >= _MIN_ROWS and non_null.std() is not None:
                std = non_null.std()
                if std is not None and std > 0:
                    metrics.append(col_name)
    return metrics


# ---------------------------------------------------------------------------
# Temporal Anomaly Detection (E2.2 + E2.3 + E2.6)
# ---------------------------------------------------------------------------

def _detect_temporal_anomalies(
    df: pl.DataFrame,
    table_name: str,
    temporal_col: str,
    metric_col: str,
) -> list[StatisticalInsight]:
    """Detect anomalous time periods using rolling z-scores.

    Improvements over baseline:
    - Winsorizes data before computing rolling statistics (E2.6)
    - Deseasonalizes if periodic pattern detected (E2.3)
    - Uses MAD-based z-scores for non-normal data (E2.2)
    - Calibrates severity by magnitude (E2.7)
    """
    insights: list[StatisticalInsight] = []

    try:
        # Aggregate by temporal column (in case of multiple rows per date)
        agg = (
            df.select([pl.col(temporal_col), pl.col(metric_col)])
            .drop_nulls()
            .group_by(temporal_col)
            .agg(pl.col(metric_col).mean().alias("value"))
            .sort(temporal_col)
        )

        if agg.height < _MIN_TEMPORAL_POINTS:
            return insights

        raw_values = agg["value"].to_list()
        dates = agg[temporal_col].to_list()

        # Deseasonalize if periodic pattern detected
        values, is_seasonal = _deseasonalize(raw_values)
        seasonal_note = ""
        if is_seasonal:
            seasonal_note = " (after removing seasonal pattern)"

        # Check normality to decide z-score method
        is_normal = _check_normality(values)

        # Winsorize for rolling statistics robustness
        winsorized = _winsorize(values)

        # Use a rolling window of ~30% of the data, minimum 5 points
        window = max(5, len(values) // 3)

        if is_normal:
            # Standard rolling z-score on winsorized data
            df_roll = pl.DataFrame({"value": winsorized})
            rolling_mean = df_roll.select(
                pl.col("value").rolling_mean(window_size=window).alias("rmean")
            )["rmean"].to_list()
            rolling_std = df_roll.select(
                pl.col("value").rolling_std(window_size=window).alias("rstd")
            )["rstd"].to_list()

            for i in range(window, len(values)):
                if rolling_std[i] is None or rolling_std[i] == 0 or rolling_mean[i] is None:
                    continue

                z = (values[i] - rolling_mean[i]) / rolling_std[i]
                if abs(z) >= _ZSCORE_THRESHOLD:
                    _emit_anomaly(
                        insights, table_name, metric_col, values, raw_values,
                        dates, i, z, window, seasonal_note, is_normal=True,
                    )
        else:
            # MAD-based detection for non-normal data
            for i in range(window, len(values)):
                window_vals = values[max(0, i - window):i]
                if len(window_vals) < 5:
                    continue
                z = _compute_mad_zscore(values[i], window_vals)
                if abs(z) >= _ZSCORE_THRESHOLD:
                    _emit_anomaly(
                        insights, table_name, metric_col, values, raw_values,
                        dates, i, z, window, seasonal_note + " (MAD-based)", is_normal=False,
                    )

    except Exception as e:
        logger.debug("Temporal anomaly detection failed for %s.%s: %s", table_name, metric_col, e)

    return insights


def _emit_anomaly(
    insights: list[StatisticalInsight],
    table_name: str,
    metric_col: str,
    values: list[float],
    raw_values: list[float],
    dates: list,
    idx: int,
    z: float,
    window: int,
    note: str,
    is_normal: bool,
) -> None:
    """Create an anomaly insight if severity/magnitude thresholds are met."""
    # Compute deviation from local mean
    window_vals = values[max(0, idx - window):idx]
    local_mean = statistics.mean(window_vals) if window_vals else 0
    if abs(local_mean) < 1e-10:
        return

    deviation_pct = ((values[idx] - local_mean) / abs(local_mean)) * 100
    p_value = round(2 * (1 - stats.norm.cdf(abs(z))), 6)

    severity = _calibrate_severity(p_value, deviation_pct)
    if severity is None:
        return

    direction = "above" if z > 0 else "below"
    date_str = _format_date(dates[idx])

    insights.append(
        StatisticalInsight(
            metric=metric_col,
            table_name=table_name,
            insight_type="temporal_anomaly",
            description=(
                f"{metric_col} was {abs(deviation_pct):.0f}% {direction} "
                f"the rolling average on {date_str}{note} "
                f"(z-score: {z:.1f})"
            ),
            magnitude=round(deviation_pct, 1),
            z_score=round(z, 2),
            p_value=p_value,
            confidence_level=_z_to_confidence(z),
            time_period=date_str,
            comparison_baseline=f"{window}-point rolling average",
            severity=severity,
            support_count=len(values),
        )
    )


# ---------------------------------------------------------------------------
# Change-Point Detection (E2.4)
# ---------------------------------------------------------------------------

def _detect_change_points(values: list[float], min_segment: int = 10) -> list[int]:
    """Find change points using binary segmentation with BIC penalty."""
    if len(values) < 2 * min_segment:
        return []

    def segment_cost(start: int, end: int) -> float:
        segment = values[start:end]
        if len(segment) < 2:
            return 0.0
        var = statistics.variance(segment) if len(segment) > 1 else 1e-10
        return len(segment) * math.log(max(var, 1e-10))

    def binary_segmentation(start: int, end: int, depth: int = 0) -> list[int]:
        if end - start < 2 * min_segment or depth > 5:
            return []
        total_cost = segment_cost(start, end)
        best_cp, best_gain = -1, 0.0
        for cp in range(start + min_segment, end - min_segment):
            split_cost = segment_cost(start, cp) + segment_cost(cp, end)
            gain = total_cost - split_cost
            if gain > best_gain:
                best_gain = gain
                best_cp = cp
        # BIC penalty: log(n) * num_params
        penalty = math.log(end - start) * 2
        if best_gain > penalty and best_cp > 0:
            left_cps = binary_segmentation(start, best_cp, depth + 1)
            right_cps = binary_segmentation(best_cp, end, depth + 1)
            return left_cps + [best_cp] + right_cps
        return []

    return binary_segmentation(0, len(values))


def _detect_change_points_for_column(
    df: pl.DataFrame,
    table_name: str,
    temporal_col: str,
    metric_col: str,
) -> list[StatisticalInsight]:
    """Detect structural changes in a time series using binary segmentation."""
    insights: list[StatisticalInsight] = []

    try:
        agg = (
            df.select([pl.col(temporal_col), pl.col(metric_col)])
            .drop_nulls()
            .group_by(temporal_col)
            .agg(pl.col(metric_col).mean().alias("value"))
            .sort(temporal_col)
        )

        if agg.height < _MIN_ROWS * 2:
            return insights

        values = agg["value"].to_list()
        dates = agg[temporal_col].to_list()

        change_points = _detect_change_points(values)

        for cp in change_points:
            before = values[max(0, cp - 20):cp]
            after = values[cp:min(len(values), cp + 20)]

            if len(before) < 5 or len(after) < 5:
                continue

            before_mean = statistics.mean(before)
            after_mean = statistics.mean(after)

            if abs(before_mean) < 1e-10:
                continue

            change_pct = ((after_mean - before_mean) / abs(before_mean)) * 100

            # Welch's t-test for significance
            t_stat, p_value = stats.ttest_ind(before, after, equal_var=False)

            severity = _calibrate_severity(p_value, change_pct)
            if severity is None:
                continue

            direction = "increased" if change_pct > 0 else "decreased"
            cp_date = _format_date(dates[cp])

            insights.append(
                StatisticalInsight(
                    metric=metric_col,
                    table_name=table_name,
                    insight_type="change_point",
                    description=(
                        f"{metric_col} {direction} by {abs(change_pct):.1f}% "
                        f"around {cp_date} (from {before_mean:.1f} to {after_mean:.1f}, "
                        f"p={p_value:.4f})"
                    ),
                    magnitude=round(change_pct, 1),
                    z_score=round(t_stat, 2),
                    p_value=round(p_value, 6),
                    confidence_level=_p_to_confidence(p_value),
                    time_period=cp_date,
                    comparison_baseline=f"Before {cp_date}",
                    severity=severity,
                    support_count=len(before) + len(after),
                )
            )

    except Exception as e:
        logger.debug("Change-point detection failed for %s.%s: %s", table_name, metric_col, e)

    return insights


# ---------------------------------------------------------------------------
# Correlation with Detrending (E2.5)
# ---------------------------------------------------------------------------

def _detrend(values: list[float]) -> tuple[list[float], bool]:
    """Remove linear trend if significant. Returns (residuals, was_detrended)."""
    n = len(values)
    if n < 10:
        return values, False

    x = list(range(n))
    slope, intercept, r_val, p_val, _ = stats.linregress(x, values)

    if p_val < 0.05:
        residuals = [v - (slope * i + intercept) for i, v in enumerate(values)]
        return residuals, True

    return values, False


def _detect_correlations(
    df: pl.DataFrame,
    table_name: str,
    metric_cols: list[str],
) -> list[StatisticalInsight]:
    """Detect statistically significant correlations between metric pairs.

    Detrends columns with significant trends before computing correlation
    to avoid spurious correlations from common trends.
    """
    insights: list[StatisticalInsight] = []
    seen: set[tuple[str, str]] = set()

    for i, col_a in enumerate(metric_cols):
        for col_b in metric_cols[i + 1:]:
            pair = (min(col_a, col_b), max(col_a, col_b))
            if pair in seen:
                continue
            seen.add(pair)

            try:
                paired = df.select([pl.col(col_a), pl.col(col_b)]).drop_nulls()
                if paired.height < _MIN_ROWS:
                    continue

                a_vals = [float(v) for v in paired[col_a].to_list()]
                b_vals = [float(v) for v in paired[col_b].to_list()]

                # Compute raw correlation first
                r_raw, p_raw = stats.pearsonr(a_vals, b_vals)

                # Check if both columns have significant trends
                a_detrended, a_had_trend = _detrend(a_vals)
                b_detrended, b_had_trend = _detrend(b_vals)

                detrended = a_had_trend and b_had_trend
                if detrended:
                    # After detrending, check if residuals have variance
                    a_var = statistics.variance(a_detrended) if len(a_detrended) > 1 else 0
                    b_var = statistics.variance(b_detrended) if len(b_detrended) > 1 else 0
                    if a_var < 1e-10 or b_var < 1e-10:
                        # Detrending removed all variance -- the correlation IS the trend.
                        # Report raw correlation with detrended=True flag.
                        r, p_value = r_raw, p_raw
                    else:
                        r, p_value = stats.pearsonr(a_detrended, b_detrended)
                    threshold = 0.7  # Higher threshold for detrended correlations
                else:
                    r, p_value = r_raw, p_raw
                    threshold = 0.6

                if abs(r) >= threshold and p_value < _P_VALUE_THRESHOLD:
                    strength = "strong" if abs(r) >= 0.8 else "moderate"
                    direction = "positive" if r > 0 else "negative"

                    desc = (
                        f"{strength.title()} {direction} correlation between "
                        f"{col_a} and {col_b} (r={r:.2f}, p={p_value:.4f})."
                    )
                    if detrended:
                        desc += f" Detrended from raw r={r_raw:.2f}."
                    desc += (
                        f" As {col_a} {'increases' if r > 0 else 'decreases'}, "
                        f"{col_b} tends to {'increase' if r > 0 else 'decrease'}."
                    )

                    insights.append(
                        StatisticalInsight(
                            metric=f"{col_a} vs {col_b}",
                            table_name=table_name,
                            insight_type="correlation",
                            description=desc,
                            magnitude=round(r * 100, 1),
                            p_value=round(p_value, 6),
                            confidence_level=_p_to_confidence(p_value),
                            detrended=detrended,
                            severity="info",
                            support_count=paired.height,
                        )
                    )

            except Exception as e:
                logger.debug("Correlation failed for %s vs %s: %s", col_a, col_b, e)

    return insights


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metadata_root() -> Path:
    return Path(__file__).resolve().parents[2] / "metadata"


def _slugify_metadata_key(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized.strip("-")


def _parse_family_spec(path: Path) -> dict | None:
    try:
        parsed = yaml.safe_load(path.read_text()) or {}
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    try:
        parsed = json.loads(path.read_text())
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return None
    return None


def _merge_family_specs(base: dict, override: dict) -> dict:
    if override.get("replace_defaults"):
        return {
            "version": override.get("version", base.get("version", 1)),
            "families": list(override.get("families", [])),
        }
    merged = {
        "version": override.get("version", base.get("version", 1)),
        "families": [],
    }
    by_key = {
        str(family.get("key")): dict(family)
        for family in base.get("families", [])
        if isinstance(family, dict) and family.get("key")
    }
    for family in override.get("families", []):
        if not isinstance(family, dict) or not family.get("key"):
            continue
        key = str(family["key"])
        by_key[key] = {**by_key.get(key, {}), **family}
    merged["families"] = list(by_key.values())
    return merged


def _candidate_family_spec_paths(source_name: str | None, project_id: str | None) -> list[Path]:
    metadata_root = _metadata_root()
    candidates: list[Path] = []
    for name in [project_id, source_name]:
        if not name:
            continue
        for candidate in [str(name), _slugify_metadata_key(str(name))]:
            path = metadata_root / candidate / "insight_families.yaml"
            if path not in candidates:
                candidates.append(path)
    return candidates


def _load_family_spec(
    source_name: str | None = None,
    project_id: str | None = None,
    metadata: RetrievedMetadata | None = None,
) -> dict:
    spec = {
        "version": _DEFAULT_FAMILY_SPEC["version"],
        "families": [dict(family) for family in _DEFAULT_FAMILY_SPEC["families"]],
    }
    for path in _candidate_family_spec_paths(source_name, project_id):
        if not path.exists():
            continue
        parsed = _parse_family_spec(path)
        if parsed:
            spec = _merge_family_specs(spec, parsed)
            break
    context_spec = _family_spec_from_context(metadata)
    if context_spec:
        spec = _merge_family_specs(spec, context_spec)
    return spec


def _family_spec_from_context(metadata: RetrievedMetadata | None) -> dict | None:
    if metadata is None or not metadata.insight_families:
        return None
    return {
        "version": 1,
        "families": [dict(family) for family in metadata.insight_families],
    }


def _source_table_for_physical_table(
    physical_table: str,
    discovery: DiscoveryResult,
    models: list[GeneratedModel] | None = None,
) -> TableInfo | None:
    tables_by_name = {table.name: table for table in discovery.tables}
    tables_by_lower = {table.name.lower(): table for table in discovery.tables}
    candidates = [physical_table]
    if physical_table.startswith("stg_"):
        candidates.append(physical_table[4:])
    for candidate in candidates:
        if candidate in tables_by_name:
            return tables_by_name[candidate]
    lowered = physical_table.lower()
    if lowered in tables_by_lower:
        return tables_by_lower[lowered]
    if lowered.startswith("stg_") and lowered[4:] in tables_by_lower:
        return tables_by_lower[lowered[4:]]

    for model in models or []:
        if model.name != physical_table and model.name.lower() != lowered:
            continue
        for source in model.source_tables:
            for candidate in _table_name_candidates(source):
                if candidate in tables_by_name:
                    return tables_by_name[candidate]
                if candidate.lower() in tables_by_lower:
                    return tables_by_lower[candidate.lower()]
    return None


def _filter_tables_for_models(
    tables: list[str],
    schema: str,
    models: list[GeneratedModel] | None,
) -> list[str]:
    if models is None:
        return tables
    expected_type = "staging" if schema == "staging" else "mart" if schema == "marts" else None
    relevant = {
        model.name
        for model in models
        if expected_type is None or model.model_type == expected_type
    }
    if not relevant:
        return []
    return [table for table in tables if table in relevant]


def _models_for_discovery(
    models: list[GeneratedModel] | None,
    discovery: DiscoveryResult | None,
) -> list[GeneratedModel] | None:
    if models is None or discovery is None:
        return models
    table_names = {table.name for table in discovery.tables}
    table_names_lower = {table.lower() for table in table_names}
    scoped: list[GeneratedModel] = []
    for model in models:
        if model.source_tables:
            if any(
                candidate in table_names_lower
                for source in model.source_tables
                for candidate in _table_name_candidates(source)
            ):
                scoped.append(model)
            continue
        lowered = model.name.lower()
        if (
            model.name in table_names
            or lowered in table_names_lower
            or (lowered.startswith("stg_") and lowered[4:] in table_names_lower)
        ):
            scoped.append(model)
    return scoped


def _table_name_candidates(value: str) -> set[str]:
    lowered = value.lower()
    candidates = {value, lowered}
    if "." in lowered:
        tail = lowered.rsplit(".", 1)[-1]
        candidates.add(tail)
    else:
        tail = lowered
    if tail.startswith("stg_"):
        candidates.add(tail[4:])
    return candidates


def _roles_for_physical_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    physical_table: str,
    source_table: str,
    roles: dict[str, SemanticColumnRole],
) -> dict[str, SemanticColumnRole]:
    """Keep only semantic roles whose columns exist on the materialized table."""
    column_names = _physical_column_names(con, schema, physical_table)
    if not column_names:
        return roles

    by_lower = {name.lower(): name for name in column_names}
    mapped: dict[str, SemanticColumnRole] = {}
    for canonical_role, role in roles.items():
        actual = by_lower.get(role.column_name.lower())
        if actual is None:
            continue
        mapped[canonical_role] = role.model_copy(
            update={"table_name": source_table, "column_name": actual}
        )

    if "event_ts" not in mapped and "lifecycle_start_ts" not in mapped:
        for candidate in ("period", "date", "event_date", "timestamp"):
            actual = by_lower.get(candidate)
            if actual is None:
                continue
            mapped["event_ts"] = SemanticColumnRole(
                table_name=source_table,
                column_name=actual,
                canonical_role="event_ts",
                confidence=0.7,
                source="name_registry",
                reason="Materialized table period column",
            )
            break

    return mapped


def _physical_column_names(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    physical_table: str,
) -> list[str]:
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            (schema, physical_table),
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not inspect columns for %s.%s: %s", schema, physical_table, exc)
        return []
    return [str(row[0]) for row in rows]


def _safe_identifier(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value)


def _temporal_expr(role: SemanticColumnRole | None) -> str | None:
    if not role:
        return None
    col = quote_ident(role.column_name)
    name = role.column_name.lower()
    if name == "year" or name.endswith("_year"):
        year_expr = f"try_cast({col} AS INTEGER)"
        return (
            f"CASE WHEN {year_expr} BETWEEN 1 AND 9999 "
            f"THEN make_timestamp({year_expr}, 1, 1, 0, 0, 0) END"
        )
    return f"try_cast({col} AS TIMESTAMP)"


def _duration_expr(
    start_expr: str | None,
    end_expr: str | None,
) -> str | None:
    if not start_expr or not end_expr:
        return None
    return f"date_diff('second', {start_expr}, {end_expr}) / 60.0"


def _wait_expr(
    request_expr: str | None,
    start_expr: str | None,
) -> str | None:
    if not request_expr or not start_expr:
        return None
    return f"date_diff('second', {request_expr}, {start_expr}) / 60.0"


def _days_between(start, end) -> int:
    try:
        delta = end - start
        return max(1, int(getattr(delta, "days", 0)) + 1)
    except Exception:
        return 1


def _format_hour(hour: object) -> str:
    try:
        h = int(hour)
    except Exception:
        return str(hour)
    suffix = "AM" if h < 12 else "PM"
    display = h % 12
    if display == 0:
        display = 12
    return f"{display} {suffix}"


def _humanize(value: str) -> str:
    text = value.replace("_", " ").replace("-", " ").strip()
    return " ".join(text.split())


def _event_label(column_name: str) -> str:
    label = _humanize(column_name).lower()
    if "pickup" in label:
        return "pickup hour"
    if "start" in label:
        return "start hour"
    if "created" in label:
        return "created hour"
    return "event hour"


def _format_dimension_value(value: object) -> str:
    if value is None:
        return "missing"
    return _humanize(str(value))


def _record_label(source_table: str) -> str:
    lowered = source_table.lower()
    if "trip" in lowered:
        return "trips"
    if any(token in lowered for token in ("event", "incident", "complaint", "inspection")):
        return "records"
    return "records"


def _service_label(source_table: str, service: SemanticColumnRole | None) -> str:
    name = source_table.lower()
    if "hvfhv" in name or "fhvhv" in name:
        return "HVFHV"
    if "fhv" in name:
        return "FHV"
    if "yellow" in name:
        return "Yellow service"
    if "green" in name:
        return "Green service"
    if service:
        return _humanize(service.column_name)
    return _humanize(source_table)


def _service_value_label(value: object) -> str:
    if value is None:
        return "Service"
    text = str(value).strip()
    compact = text.lower().replace(" ", "").replace("-", "").replace("_", "")
    if "hvfhv" in compact or "fhvhv" in compact:
        return "HVFHV"
    if compact == "fhv":
        return "FHV"
    if "yellow" in compact:
        return "Yellow service"
    if "green" in compact:
        return "Green service"
    return _humanize(text)


def _location_role_label(column_name: str) -> str:
    lowered = column_name.lower()
    if "pickup" in lowered or lowered.startswith("pu_") or "pu_location" in lowered:
        return "pickup location values"
    if "dropoff" in lowered or lowered.startswith("do_") or "do_location" in lowered:
        return "dropoff location values"
    if "origin" in lowered:
        return "origin location values"
    if "destination" in lowered or "dest" in lowered:
        return "destination location values"
    return f"{_humanize(column_name).lower()} values"


def _format_date(val: object) -> str:
    """Format a date/datetime value to a readable string."""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    return str(val)


def _z_to_confidence(z: float) -> str:
    """Map a z-score to a human-readable confidence level."""
    az = abs(z)
    if az >= 3.29:
        return "99.9%"
    if az >= 2.576:
        return "99%"
    if az >= 1.96:
        return "95%"
    if az >= 1.645:
        return "90%"
    return "<90%"


def _p_to_confidence(p: float) -> str:
    """Map a p-value to a human-readable confidence level."""
    if p < 0.001:
        return "99.9%"
    if p < 0.01:
        return "99%"
    if p < 0.05:
        return "95%"
    if p < 0.1:
        return "90%"
    return "<90%"
