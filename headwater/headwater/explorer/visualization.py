"""Visualization recommender -- suggests chart types from query results.

Analyzes result shape (columns, row count, data types) to recommend the most
appropriate visualization. No rendering -- the UI layer handles display.
"""

from __future__ import annotations

import re
from typing import Any

from headwater.core.models import VisualizationSpec

# Column names that indicate temporal data
_TEMPORAL_PATTERNS = re.compile(
    r"(date|month|year|quarter|week|day|time|period|timestamp|_at$)", re.IGNORECASE
)

# Column names that indicate categorical/dimension data
_DIMENSION_PATTERNS = re.compile(
    r"(name|type|category|status|zone|site|region|level|priority|severity|_type$)", re.IGNORECASE
)


def recommend_visualization(
    columns: list[str],
    data: list[dict[str, Any]],
    question: str = "",
) -> VisualizationSpec:
    """Recommend a chart type based on the result shape and content."""
    if not data or not columns:
        return VisualizationSpec(
            chart_type="table", title="Query Results", description="No data returned"
        )

    row_count = len(data)
    col_types = _classify_columns(columns, data)

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
        # Check by name pattern first
        if _TEMPORAL_PATTERNS.search(col):
            result[col] = "temporal"
            continue

        # Check by value type
        values = [row.get(col) for row in sample if row.get(col) is not None]
        if not values:
            result[col] = "dimension"
            continue

        # If all values are numeric, it's a metric
        if all(isinstance(v, (int, float)) for v in values):
            # Unless it looks like a dimension by name
            if _DIMENSION_PATTERNS.search(col):
                result[col] = "dimension"
            else:
                result[col] = "metric"
        elif all(isinstance(v, str) for v in values):
            # Check if string values look like dates
            if _looks_like_date(values[:5]):
                result[col] = "temporal"
            else:
                result[col] = "dimension"
        else:
            result[col] = "dimension"

    return result


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
