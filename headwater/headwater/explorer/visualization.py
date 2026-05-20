"""Visualization recommender -- suggests chart types from query results.

Analyzes result shape (columns, row count, data types) to recommend the most
appropriate visualization. No rendering -- the UI layer handles display.
"""

from __future__ import annotations

import re
from typing import Any

from headwater.analyzer.metadata_retrieval import RetrievedMetadata
from headwater.core.models import VisualizationSpec

# Column names that indicate temporal data
_TEMPORAL_PATTERNS = re.compile(
    r"(date|month|year|quarter|week|day|time|period|timestamp|_at$)", re.IGNORECASE
)

# Column names that indicate categorical/dimension data
_DIMENSION_PATTERNS = re.compile(
    r"(dimension|name|type|category|status|zone|site|region|level|priority|severity|segment|group|label|code|_type$|_id$|^id$)",
    re.IGNORECASE,
)
_PIE_QUESTION_PATTERNS = re.compile(
    r"(distribution of|share of|split by|breakdown of|composition of|how many .+ by)",
    re.IGNORECASE,
)
_BUCKET_PATTERNS = re.compile(r"(bucket|bin|range|histogram)", re.IGNORECASE)
_SHARE_METRIC_PATTERNS = re.compile(
    r"(count|records|share|pct|percent|percentage|volume|total)",
    re.IGNORECASE,
)
_RANKING_QUESTION_PATTERNS = re.compile(
    r"(highest|lowest|top|bottom|most|least|rank|stand out|dominates?)",
    re.IGNORECASE,
)
_HEATMAP_QUESTION_PATTERNS = re.compile(
    r"(vary by .+ and .+|across .+ and .+|by .+ and .+|combination of)",
    re.IGNORECASE,
)


def recommend_visualization(
    columns: list[str],
    data: list[dict[str, Any]],
    question: str = "",
    metadata: RetrievedMetadata | None = None,
) -> VisualizationSpec:
    """Recommend a chart type based on the result shape and content."""
    if not data or not columns:
        return VisualizationSpec(
            chart_type="table", title="Query Results", description="No data returned"
        )

    row_count = len(data)
    col_types = _classify_columns(columns, data)
    hinted = _visualization_from_hints(columns, col_types, question, metadata)
    if hinted is not None:
        return hinted

    temporal_cols = [c for c, t in col_types.items() if t == "temporal"]
    metric_cols = [c for c, t in col_types.items() if t == "metric"]
    dimension_cols = [c for c, t in col_types.items() if t == "dimension"]

    # Single value -> KPI card
    if row_count == 1 and len(metric_cols) == 1 and not dimension_cols and not temporal_cols:
        return VisualizationSpec(
            chart_type="kpi",
            title=_humanize_column(metric_cols[0]),
            y_axis=metric_cols[0],
            description=f"Single metric value: {metric_cols[0]}",
        )

    # Single row, multiple metrics -> KPI card with multiple values
    if row_count == 1 and len(metric_cols) > 1:
        return VisualizationSpec(
            chart_type="kpi",
            title=_title_from_question(question) or "Key Metrics",
            description=f"{len(metric_cols)} metrics",
        )

    categorical_axes = temporal_cols + dimension_cols

    # Two axes + metric with a matrix-style question -> heatmap
    if (
        len(categorical_axes) >= 2
        and len(metric_cols) == 1
        and _should_use_heatmap(question, row_count)
    ):
        return VisualizationSpec(
            chart_type="heatmap",
            title=(
                _title_from_question(question)
                or f"{metric_cols[0]} by {categorical_axes[0]} and {categorical_axes[1]}"
            ),
            x_axis=categorical_axes[0],
            y_axis=categorical_axes[1],
            description=f"Heatmap of {metric_cols[0]}",
        )

    # Temporal + metric -> line chart
    if temporal_cols and metric_cols:
        group = dimension_cols[0] if dimension_cols else None
        return VisualizationSpec(
            chart_type="line",
            title=_title_from_question(question) or f"{metric_cols[0]} over time",
            x_axis=temporal_cols[0],
            y_axis=metric_cols[0],
            group_by=group,
            description=f"Time series: {metric_cols[0]} by {temporal_cols[0]}",
        )

    # Small categorical part-of-whole -> pie chart
    if (
        dimension_cols
        and len(metric_cols) == 1
        and len(dimension_cols) == 1
        and _should_use_pie_chart(
            columns,
            data,
            dimension_cols[0],
            metric_cols[0],
            question,
        )
    ):
        return VisualizationSpec(
            chart_type="pie",
            title=(
                _title_from_question(question)
                or f"{metric_cols[0]} share by {dimension_cols[0]}"
            ),
            x_axis=dimension_cols[0],
            y_axis=metric_cols[0],
            description=f"Part-to-whole: {metric_cols[0]} share across {dimension_cols[0]}",
        )

    # Dimension + metric (few categories) -> bar chart
    if dimension_cols and metric_cols and row_count <= 30:
        return VisualizationSpec(
            chart_type="bar",
            title=_title_from_question(question) or f"{metric_cols[0]} by {dimension_cols[0]}",
            x_axis=dimension_cols[0],
            y_axis=metric_cols[0],
            group_by=dimension_cols[1] if len(dimension_cols) > 1 else None,
            description=f"Comparison: {metric_cols[0]} across {dimension_cols[0]}",
        )

    # Two metrics -> scatter plot
    if len(metric_cols) >= 2 and not temporal_cols:
        return VisualizationSpec(
            chart_type="scatter",
            title=(_title_from_question(question) or f"{metric_cols[0]} vs {metric_cols[1]}"),
            x_axis=metric_cols[0],
            y_axis=metric_cols[1],
            group_by=dimension_cols[0] if dimension_cols else None,
            description=f"Relationship between {metric_cols[0]} and {metric_cols[1]}",
        )

    # Two dimensions + metric -> heatmap
    if len(dimension_cols) >= 2 and metric_cols and row_count > 5:
        return VisualizationSpec(
            chart_type="heatmap",
            title=(
                _title_from_question(question)
                or f"{metric_cols[0]} by {dimension_cols[0]} and {dimension_cols[1]}"
            ),
            x_axis=dimension_cols[0],
            y_axis=dimension_cols[1],
            description=f"Heatmap of {metric_cols[0]}",
        )

    # Fallback -> table
    return VisualizationSpec(
        chart_type="table",
        title=_title_from_question(question) or "Query Results",
        description=f"{row_count} rows, {len(columns)} columns",
    )


def _classify_columns(
    columns: list[str],
    data: list[dict[str, Any]],
) -> dict[str, str]:
    """Classify each column as temporal, metric, or dimension."""
    result: dict[str, str] = {}
    sample = data[:50]  # Sample first 50 rows

    for col in columns:
        values = [row.get(col) for row in sample if row.get(col) is not None]
        if not values:
            if _TEMPORAL_PATTERNS.search(col):
                result[col] = "temporal"
            else:
                result[col] = "dimension"
            continue

        # Trust actual value shape first. Numeric columns like avg_trip_time or
        # wait_time are metrics even though the name contains "time".
        if all(isinstance(v, (int, float)) for v in values):
            if _DIMENSION_PATTERNS.search(col):
                result[col] = "dimension"
            else:
                result[col] = "metric"
        elif all(isinstance(v, str) for v in values):
            if _TEMPORAL_PATTERNS.search(col) or _looks_like_date(values[:5]):
                result[col] = "temporal"
            else:
                result[col] = "dimension"
        else:
            result[col] = "dimension"

    return result


def _visualization_from_hints(
    columns: list[str],
    col_types: dict[str, str],
    question: str,
    metadata: RetrievedMetadata | None,
) -> VisualizationSpec | None:
    if metadata is None:
        return None
    available = set(columns)
    lowered_question = question.lower()
    for hint in metadata.visualization_hints:
        chart_type = hint.get("chart_type")
        if chart_type not in {"kpi", "bar", "line", "pie", "scatter", "table", "heatmap"}:
            continue
        required_columns = [str(col) for col in hint.get("columns") or []]
        if required_columns and not set(required_columns).issubset(available):
            continue
        question_terms = _hint_terms(hint, "question_terms", "question_keywords")
        if question_terms and not any(term in lowered_question for term in question_terms):
            continue
        x_axis = _hint_axis(hint.get("x_axis"), columns)
        y_axis = _hint_axis(hint.get("y_axis"), columns)
        group_by = _hint_axis(hint.get("group_by"), columns)
        if x_axis is None:
            x_axis = _first_column_of_type(columns, col_types, "temporal")
            if x_axis is None:
                x_axis = _first_column_of_type(columns, col_types, "dimension")
        if y_axis is None:
            y_axis = _first_column_of_type(columns, col_types, "metric")
        title = str(hint.get("title") or _title_from_question(question) or "Query Results")
        description = str(hint.get("description") or f"Context hint: {chart_type}")
        return VisualizationSpec(
            chart_type=chart_type,
            title=title,
            x_axis=x_axis,
            y_axis=y_axis,
            group_by=group_by,
            description=description,
        )
    return None


def _hint_terms(hint: dict, *keys: str) -> list[str]:
    terms: list[str] = []
    for key in keys:
        value = hint.get(key)
        if isinstance(value, str):
            terms.extend(part.strip().lower() for part in re.split(r"[,;]", value))
        elif isinstance(value, list):
            terms.extend(str(part).strip().lower() for part in value)
    return [term for term in terms if term]


def _hint_axis(value: Any, columns: list[str]) -> str | None:
    if not isinstance(value, str) or value not in columns:
        return None
    return value


def _first_column_of_type(
    columns: list[str],
    col_types: dict[str, str],
    column_type: str,
) -> str | None:
    return next((column for column in columns if col_types.get(column) == column_type), None)


def _should_use_pie_chart(
    columns: list[str],
    data: list[dict[str, Any]],
    dimension_col: str,
    metric_col: str,
    question: str,
) -> bool:
    if len(data) < 2 or len(data) > 8:
        return False
    if len(columns) > 3:
        return False
    lower_dimension = dimension_col.lower()
    lower_metric = metric_col.lower()
    lower_question = question.lower()
    if _RANKING_QUESTION_PATTERNS.search(lower_question):
        return False
    if _BUCKET_PATTERNS.search(lower_dimension):
        return False
    if _looks_like_range_dimension(data, dimension_col):
        return False
    metric_supports_share = _SHARE_METRIC_PATTERNS.search(lower_metric) is not None
    question_supports_share = _PIE_QUESTION_PATTERNS.search(lower_question) is not None
    if not metric_supports_share:
        return False
    if not question_supports_share and " by " not in lower_question:
        return False
    total = 0.0
    for row in data:
        value = row.get(metric_col)
        if not isinstance(value, (int, float)):
            return False
        if value < 0:
            return False
        total += float(value)
    return total > 0


def _should_use_heatmap(question: str, row_count: int) -> bool:
    if row_count < 6:
        return False
    return _HEATMAP_QUESTION_PATTERNS.search(question.lower()) is not None


def _looks_like_range_dimension(data: list[dict[str, Any]], column: str) -> bool:
    values = [str(row.get(column, "")).strip() for row in data[:8]]
    return all(
        value
        and (
            re.match(r"^-?\d+(?:\.\d+)?\s*-\s*-?\d+(?:\.\d+)?$", value) is not None
            or re.match(r"^\d+(?:\.\d+)?$", value) is not None
        )
        for value in values
    )


def _looks_like_date(values: list[Any]) -> bool:
    """Check if string values look like ISO dates."""
    date_pattern = re.compile(r"^\d{4}-\d{2}(-\d{2})?")
    return all(isinstance(v, str) and date_pattern.match(v) for v in values if v)


def _humanize_column(name: str) -> str:
    """Convert a column name to a human-readable title."""
    return name.replace("_", " ").replace("-", " ").title()


def _title_from_question(question: str) -> str:
    """Extract a chart title from the user's question."""
    if not question:
        return ""
    # Clean up the question for use as a title
    q = question.strip().rstrip("?").strip()
    if len(q) > 60:
        q = q[:57] + "..."
    return q
