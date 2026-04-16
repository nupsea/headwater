"""Statistical insights -- detects significant patterns in materialized data.

Uses Polars for time-series windowing and scipy.stats for significance testing.
Scans mart tables with temporal + metric columns to surface anomalies,
period-over-period changes, and correlations automatically.
"""

from __future__ import annotations

import logging
from datetime import datetime

import duckdb
import polars as pl
from scipy import stats

from headwater.core.models import StatisticalInsight

logger = logging.getLogger(__name__)

# Minimum rows needed for meaningful statistical analysis
_MIN_ROWS = 10
_MIN_TEMPORAL_POINTS = 7
_ZSCORE_THRESHOLD = 2.0  # Flag values beyond 2 standard deviations
_P_VALUE_THRESHOLD = 0.05  # 95% confidence


def detect_insights(
    con: duckdb.DuckDBPyConnection,
    schema: str = "marts",
) -> list[StatisticalInsight]:
    """Scan all materialized tables in a schema for statistical patterns.

    Automatically identifies temporal + metric column pairs, then runs:
    - Temporal anomaly detection (rolling z-scores)
    - Period-over-period significance testing
    - Cross-metric correlation
    """
    insights: list[StatisticalInsight] = []

    tables = _list_tables(con, schema)
    for table_name in tables:
        try:
            df = _load_table(con, schema, table_name)
            if df is None or df.height < _MIN_ROWS:
                continue

            temporal_cols = _find_temporal_columns(df)
            metric_cols = _find_metric_columns(df)

            if temporal_cols and metric_cols:
                for t_col in temporal_cols:
                    for m_col in metric_cols:
                        insights.extend(_detect_temporal_anomalies(df, table_name, t_col, m_col))
                        insights.extend(_detect_period_shifts(df, table_name, t_col, m_col))

            if len(metric_cols) >= 2:
                insights.extend(_detect_correlations(df, table_name, metric_cols))

        except Exception as e:
            logger.warning("Statistical analysis failed for %s.%s: %s", schema, table_name, e)

    return insights


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
    """Load a table into a Polars DataFrame via Arrow."""
    try:
        arrow = con.execute(f"SELECT * FROM {schema}.{table}").arrow()
        return pl.from_arrow(arrow)
    except Exception as e:
        logger.debug("Could not load %s.%s: %s", schema, table, e)
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
        if dtype in (pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64):
            # Skip ID-like columns and counts that are always 1
            if col_name.endswith("_id") or col_name == "id":
                continue
            non_null = df[col_name].drop_nulls()
            if non_null.len() >= _MIN_ROWS and non_null.std() is not None:
                std = non_null.std()
                if std is not None and std > 0:
                    metrics.append(col_name)
    return metrics


def _detect_temporal_anomalies(
    df: pl.DataFrame,
    table_name: str,
    temporal_col: str,
    metric_col: str,
) -> list[StatisticalInsight]:
    """Detect anomalous time periods using rolling z-scores.

    Aggregates the metric by the temporal column, computes a rolling mean/std,
    and flags periods where the z-score exceeds the threshold.
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

        values = agg["value"].to_list()
        dates = agg[temporal_col].to_list()

        # Use a rolling window of ~30% of the data, minimum 5 points
        window = max(5, agg.height // 3)

        # Compute rolling statistics
        rolling_mean = agg.select(pl.col("value").rolling_mean(window_size=window).alias("rmean"))[
            "rmean"
        ].to_list()
        rolling_std = agg.select(pl.col("value").rolling_std(window_size=window).alias("rstd"))[
            "rstd"
        ].to_list()

        # Find anomalies (skip the warm-up window)
        for i in range(window, len(values)):
            if rolling_std[i] is None or rolling_std[i] == 0 or rolling_mean[i] is None:
                continue

            z = (values[i] - rolling_mean[i]) / rolling_std[i]

            if abs(z) >= _ZSCORE_THRESHOLD:
                deviation_pct = ((values[i] - rolling_mean[i]) / abs(rolling_mean[i])) * 100
                direction = "above" if z > 0 else "below"
                date_str = _format_date(dates[i])

                # Check if this is part of a consecutive anomalous stretch
                streak_start = i
                while streak_start > window:
                    prev_z = (
                        (values[streak_start - 1] - rolling_mean[streak_start - 1])
                        / rolling_std[streak_start - 1]
                        if rolling_std[streak_start - 1] and rolling_std[streak_start - 1] > 0
                        else 0
                    )
                    if abs(prev_z) < _ZSCORE_THRESHOLD or (z > 0) != (prev_z > 0):
                        break
                    streak_start -= 1

                # Only report once per streak (at the end)
                if i < len(values) - 1:
                    next_z = (
                        (values[i + 1] - rolling_mean[i + 1]) / rolling_std[i + 1]
                        if (
                            i + 1 < len(rolling_std)
                            and rolling_std[i + 1]
                            and rolling_std[i + 1] > 0
                        )
                        else 0
                    )
                    if abs(next_z) >= _ZSCORE_THRESHOLD and (z > 0) == (next_z > 0):
                        continue  # Not end of streak yet

                time_period = (
                    f"{_format_date(dates[streak_start])} to {date_str}"
                    if streak_start != i
                    else date_str
                )

                severity = "critical" if abs(z) >= 3.0 else "warning" if abs(z) >= 2.5 else "info"

                insights.append(
                    StatisticalInsight(
                        metric=metric_col,
                        table_name=table_name,
                        insight_type="temporal_anomaly",
                        description=(
                            f"{metric_col} was {abs(deviation_pct):.0f}% {direction} "
                            f"the rolling average on {time_period} "
                            f"(z-score: {z:.1f})"
                        ),
                        magnitude=round(deviation_pct, 1),
                        z_score=round(z, 2),
                        p_value=round(2 * (1 - stats.norm.cdf(abs(z))), 6),
                        confidence_level=_z_to_confidence(z),
                        time_period=time_period,
                        comparison_baseline=f"{window}-point rolling average",
                        severity=severity,
                    )
                )

    except Exception as e:
        logger.debug("Temporal anomaly detection failed for %s.%s: %s", table_name, metric_col, e)

    return insights


def _detect_period_shifts(
    df: pl.DataFrame,
    table_name: str,
    temporal_col: str,
    metric_col: str,
) -> list[StatisticalInsight]:
    """Compare metric distributions across time halves using a t-test.

    Splits the time series into early and recent halves, tests whether
    the means are significantly different.
    """
    insights: list[StatisticalInsight] = []

    try:
        agg = df.select([pl.col(temporal_col), pl.col(metric_col)]).drop_nulls().sort(temporal_col)

        if agg.height < _MIN_ROWS * 2:
            return insights

        mid = agg.height // 2
        early = agg[metric_col][:mid].to_list()
        recent = agg[metric_col][mid:].to_list()

        # Filter out None values
        early = [v for v in early if v is not None]
        recent = [v for v in recent if v is not None]

        if len(early) < _MIN_ROWS or len(recent) < _MIN_ROWS:
            return insights

        t_stat, p_value = stats.ttest_ind(early, recent, equal_var=False)

        if p_value < _P_VALUE_THRESHOLD:
            early_mean = sum(early) / len(early)
            recent_mean = sum(recent) / len(recent)

            if abs(early_mean) < 1e-10:
                return insights

            change_pct = ((recent_mean - early_mean) / abs(early_mean)) * 100
            direction = "increased" if change_pct > 0 else "decreased"

            dates = agg[temporal_col].to_list()
            mid_date = _format_date(dates[mid])

            confidence = _p_to_confidence(p_value)
            severity = "critical" if p_value < 0.001 else "warning" if p_value < 0.01 else "info"

            insights.append(
                StatisticalInsight(
                    metric=metric_col,
                    table_name=table_name,
                    insight_type="period_comparison",
                    description=(
                        f"{metric_col} {direction} by {abs(change_pct):.1f}% "
                        f"in the recent half of the data (after {mid_date}), "
                        f"statistically significant at {confidence} confidence "
                        f"(p={p_value:.4f})"
                    ),
                    magnitude=round(change_pct, 1),
                    z_score=round(t_stat, 2),
                    p_value=round(p_value, 6),
                    confidence_level=confidence,
                    time_period=f"After {mid_date}",
                    comparison_baseline=f"Before {mid_date}",
                    severity=severity,
                )
            )

    except Exception as e:
        logger.debug("Period shift detection failed for %s.%s: %s", table_name, metric_col, e)

    return insights


def _detect_correlations(
    df: pl.DataFrame,
    table_name: str,
    metric_cols: list[str],
) -> list[StatisticalInsight]:
    """Detect statistically significant correlations between metric pairs."""
    insights: list[StatisticalInsight] = []

    seen: set[tuple[str, str]] = set()

    for i, col_a in enumerate(metric_cols):
        for col_b in metric_cols[i + 1 :]:
            pair = (min(col_a, col_b), max(col_a, col_b))
            if pair in seen:
                continue
            seen.add(pair)

            try:
                # Extract paired non-null values
                paired = df.select([pl.col(col_a), pl.col(col_b)]).drop_nulls()
                if paired.height < _MIN_ROWS:
                    continue

                a_vals = paired[col_a].to_list()
                b_vals = paired[col_b].to_list()

                r, p_value = stats.pearsonr(a_vals, b_vals)

                # Only report strong, significant correlations
                if abs(r) >= 0.6 and p_value < _P_VALUE_THRESHOLD:
                    strength = "strong" if abs(r) >= 0.8 else "moderate"
                    direction = "positive" if r > 0 else "negative"

                    insights.append(
                        StatisticalInsight(
                            metric=f"{col_a} vs {col_b}",
                            table_name=table_name,
                            insight_type="correlation",
                            description=(
                                f"{strength.title()} {direction} correlation between "
                                f"{col_a} and {col_b} (r={r:.2f}, p={p_value:.4f}). "
                                f"As {col_a} {'increases' if r > 0 else 'decreases'}, "
                                f"{col_b} tends to {'increase' if r > 0 else 'decrease'}."
                            ),
                            magnitude=round(r * 100, 1),
                            p_value=round(p_value, 6),
                            confidence_level=_p_to_confidence(p_value),
                            severity="info",
                        )
                    )

            except Exception as e:
                logger.debug("Correlation failed for %s vs %s: %s", col_a, col_b, e)

    return insights


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
