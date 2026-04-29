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

import logging
import math
import statistics
import warnings
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
    - Temporal anomaly detection (rolling z-scores with normality check)
    - Change-point detection (binary segmentation with BIC)
    - Cross-metric correlation (with detrending)

    Applies Benjamini-Hochberg FDR correction before returning.
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
                        insights.extend(
                            _detect_change_points_for_column(df, table_name, t_col, m_col)
                        )

            if len(metric_cols) >= 2:
                insights.extend(_detect_correlations(df, table_name, metric_cols))

        except Exception as e:
            logger.warning("Statistical analysis failed for %s.%s: %s", schema, table_name, e)

    # Apply FDR correction to control false positives from multiple comparisons
    insights = _apply_fdr_correction(insights)

    return insights


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
                        )
                    )

            except Exception as e:
                logger.debug("Correlation failed for %s vs %s: %s", col_a, col_b, e)

    return insights


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
