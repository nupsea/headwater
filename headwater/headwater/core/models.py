"""Pydantic domain models -- the contract between all Headwater layers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Source configuration
# ---------------------------------------------------------------------------


class SourceConfig(BaseModel):
    """Describes a data source to discover."""

    name: str
    type: Literal["json", "csv", "parquet", "postgres", "mysql"]
    path: str | None = None  # For file-based sources
    uri: str | None = None  # For database sources
    mode: Literal["generate", "observe"] = "generate"


# ---------------------------------------------------------------------------
# Schema / Discovery
# ---------------------------------------------------------------------------


class ColumnInfo(BaseModel):
    """Metadata for a single column."""

    name: str
    dtype: str  # Normalised type string (int64, float64, varchar, bool, timestamp, json, list)
    nullable: bool = True
    is_primary_key: bool = False
    description: str | None = None  # Filled by analyzer
    semantic_type: str | None = None  # pii, metric, dimension, id, foreign_key, etc.
    locked: bool = False  # True = description approved by human; skip re-enrichment


class TableInfo(BaseModel):
    """Metadata for a single table."""

    name: str
    schema_name: str | None = None
    row_count: int = 0
    columns: list[ColumnInfo] = Field(default_factory=list)
    description: str | None = None  # Filled by analyzer
    domain: str | None = None  # Filled by analyzer
    tags: list[str] = Field(default_factory=list)
    locked: bool = False  # True = description approved by human; skip re-enrichment


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------


class ColumnProfile(BaseModel):
    """Statistical profile for a single column."""

    table_name: str
    column_name: str
    dtype: str

    # Universal
    null_count: int = 0
    null_rate: float = 0.0
    distinct_count: int = 0
    uniqueness_ratio: float = 0.0

    # Numeric
    min_value: float | None = None
    max_value: float | None = None
    mean: float | None = None
    median: float | None = None
    stddev: float | None = None
    p25: float | None = None
    p75: float | None = None
    p95: float | None = None

    # String
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None
    top_values: list[tuple[str, int]] | None = None  # (value, count) pairs

    # Temporal
    min_date: str | None = None
    max_date: str | None = None

    # Pattern detection
    detected_pattern: str | None = None  # email, uuid, phone, iso_date, url, etc.


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------


class Relationship(BaseModel):
    """A detected relationship between two columns."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    type: Literal["one_to_many", "many_to_one", "many_to_many", "one_to_one"]
    confidence: float = Field(ge=0.0, le=1.0)
    referential_integrity: float = Field(ge=0.0, le=1.0)  # % of FK values found in PK
    source: Literal["declared", "inferred_name", "inferred_value"]


# ---------------------------------------------------------------------------
# Models (generated SQL)
# ---------------------------------------------------------------------------


class GeneratedModel(BaseModel):
    """A generated SQL model (staging or mart)."""

    name: str
    model_type: Literal["staging", "mart"]
    sql: str
    description: str
    source_tables: list[str] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)
    status: Literal["proposed", "approved", "rejected", "executed"] = "proposed"
    assumptions: list[str] = Field(default_factory=list)  # For marts
    questions: list[str] = Field(default_factory=list)  # Clarifying questions for reviewer


# ---------------------------------------------------------------------------
# Quality contracts
# ---------------------------------------------------------------------------


class ContractRule(BaseModel):
    """A data quality contract rule."""

    id: str | None = None
    model_name: str
    column_name: str | None = None  # None = table-level rule
    rule_type: Literal[
        "not_null", "unique", "range", "cardinality", "row_count", "referential", "custom"
    ]
    expression: str  # SQL expression to evaluate
    severity: Literal["error", "warning", "info"] = "warning"
    description: str = ""
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    status: Literal["proposed", "observing", "enforced", "disabled"] = "proposed"


# ---------------------------------------------------------------------------
# Execution results
# ---------------------------------------------------------------------------


class ExecutionResult(BaseModel):
    """Result of executing a single model."""

    model_name: str
    success: bool
    row_count: int | None = None
    execution_time_ms: float = 0.0
    error: str | None = None


class ContractCheckResult(BaseModel):
    """Result of evaluating a single contract rule."""

    rule_id: str
    model_name: str
    passed: bool
    observed_value: Any = None
    message: str = ""


class QualityReport(BaseModel):
    """Aggregated quality report after execution."""

    total_contracts: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    results: list[ContractCheckResult] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Discovery result (the big aggregate)
# ---------------------------------------------------------------------------


class DiscoveryResult(BaseModel):
    """Complete result of a discovery run."""

    source: SourceConfig
    tables: list[TableInfo] = Field(default_factory=list)
    profiles: list[ColumnProfile] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    domains: dict[str, list[str]] = Field(default_factory=dict)  # domain -> [table_names]
    discovered_at: datetime = Field(default_factory=datetime.now)


# ---------------------------------------------------------------------------
# Explorer -- NL questions, statistical insights, visualization
# ---------------------------------------------------------------------------


class SuggestedQuestion(BaseModel):
    """A natural language question the system can answer from materialized models."""

    question: str
    source: Literal["mart", "relationship", "quality", "semantic", "statistical"]
    category: str  # e.g. "Air Quality", "Inspections", "Trends"
    relevant_tables: list[str] = Field(default_factory=list)
    sql_hint: str | None = None  # Optional pre-generated SQL


class StatisticalInsight(BaseModel):
    """A statistically significant pattern detected in materialized data."""

    metric: str  # Column or expression measured
    table_name: str
    insight_type: Literal[
        "temporal_anomaly", "period_comparison", "correlation", "distribution_shift"
    ]
    description: str  # Plain-English explanation
    magnitude: float  # % deviation or correlation coefficient
    z_score: float | None = None
    p_value: float | None = None
    confidence_level: str | None = None  # "90%", "95%", "99%"
    time_period: str | None = None  # e.g. "2024-12-20 to 2025-01-03"
    comparison_baseline: str | None = None  # e.g. "90-day rolling average"
    severity: Literal["info", "warning", "critical"] = "info"


class VisualizationSpec(BaseModel):
    """Recommendation for how to visualize a query result."""

    chart_type: Literal["kpi", "bar", "line", "scatter", "table", "heatmap"]
    title: str
    x_axis: str | None = None
    y_axis: str | None = None
    group_by: str | None = None
    description: str = ""


class ExplorationResult(BaseModel):
    """Result of a natural language query against the analytical data."""

    question: str
    sql: str
    data: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    visualization: VisualizationSpec | None = None
    error: str | None = None
    warnings: list[str] = Field(default_factory=list)  # Grounding / confidence warnings
    repaired: bool = False  # True if LLM auto-repaired a failed query
    repair_history: list[dict[str, str]] = Field(default_factory=list)  # [{sql, error}]
