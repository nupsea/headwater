"""Mart model generator -- domain-agnostic archetype-based pattern matching.

Mart models encode business logic and require individual human review. Never batch-approved.

US-501: Replace hard-coded _MART_DEFINITIONS with PatternMatcher that detects archetypes
from discovered column semantic types and table relationships.

US-503: Quality gate -- only yield a candidate if it meets minimum evidence thresholds.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

from headwater.core.classification import is_metric_column
from headwater.core.models import ColumnInfo, DiscoveryResult, GeneratedModel

logger = logging.getLogger(__name__)

# Semantic types that indicate metric columns
_METRIC_SEMANTIC_TYPES = {"metric"}

# Semantic types that indicate temporal columns
_TEMPORAL_SEMANTIC_TYPES = {"temporal"}

# Column name patterns that suggest temporal values (fallback if semantic_type not set)
_TEMPORAL_PATTERNS = ("date", "time", "at", "period", "month", "year", "week", "day")

# Minimum evidence thresholds (US-503) -- defaults used when settings are not provided.
# These can be overridden via HeadwaterSettings (env: HEADWATER_MART_MIN_*).
_MIN_RELATIONSHIPS_FOR_ENTITY_SUMMARY = 2
_MIN_METRIC_COLUMNS_FOR_ENTITY_SUMMARY = 1
_MIN_ROWS_FOR_ENTITY_SUMMARY = 100
_MIN_ROWS_FOR_AGGREGATION = 100


def _get_thresholds() -> tuple[int, int, int, int]:
    """Return (min_rels, min_metric_cols, min_rows_entity, min_rows_agg) from settings."""
    try:
        from headwater.core.config import get_settings
        s = get_settings()
        return (
            s.mart_min_relationships,
            s.mart_min_metric_columns,
            s.mart_min_rows,
            s.mart_min_rows,
        )
    except Exception:
        return (
            _MIN_RELATIONSHIPS_FOR_ENTITY_SUMMARY,
            _MIN_METRIC_COLUMNS_FOR_ENTITY_SUMMARY,
            _MIN_ROWS_FOR_ENTITY_SUMMARY,
            _MIN_ROWS_FOR_AGGREGATION,
        )


@dataclass
class MartCandidate:
    """Proposed mart model before SQL rendering."""

    archetype: Literal["period_comparison", "entity_summary", "aggregation"]
    candidate_tables: list[str]
    candidate_columns: list[str]
    proposed_name: str
    assumptions: list[str] = field(default_factory=list)
    questions: list[str] = field(default_factory=list)


class PatternMatcher:
    """Detects mart archetypes from a DiscoveryResult.

    Archetypes:
    - period_comparison: source has a temporal column -> period-over-period metrics
    - entity_summary: source has metric column + FK to dimension -> aggregation by dimension
    - aggregation: source has multiple numeric columns, no FK context -> simple aggregation

    Quality gate (US-503): candidates below minimum evidence thresholds are discarded.
    """

    def match(self, discovery: DiscoveryResult) -> list[MartCandidate]:
        """Return all matched MartCandidates for the discovery result."""
        candidates: list[MartCandidate] = []
        min_rels, min_metrics, min_rows_entity, min_rows_agg = _get_thresholds()

        # Build FK set: (from_table, to_table) pairs
        fk_pairs = {
            (r.from_table, r.to_table)
            for r in discovery.relationships
        }
        fk_pairs_reverse = {
            (r.to_table, r.from_table)
            for r in discovery.relationships
        }
        all_fk_pairs = fk_pairs | fk_pairs_reverse

        # Count relationships per table
        rel_count: dict[str, int] = {}
        for r in discovery.relationships:
            rel_count[r.from_table] = rel_count.get(r.from_table, 0) + 1
            rel_count[r.to_table] = rel_count.get(r.to_table, 0) + 1

        for table in discovery.tables:
            temporal_cols = self._get_temporal_cols(table.columns)
            metric_cols = self._get_metric_cols(table.columns)
            dimension_tables = self._get_dimension_tables(
                table.name, all_fk_pairs, discovery
            )
            table_rels = rel_count.get(table.name, 0)

            # Archetype 1: period_comparison
            if temporal_cols:
                candidate = self._build_period_comparison(table, temporal_cols, metric_cols)
                candidates.append(candidate)
                logger.debug(
                    "period_comparison candidate: %s (temporal=%s)",
                    table.name, temporal_cols,
                )

            # Archetype 2: entity_summary (quality gate applies)
            if metric_cols and dimension_tables:
                passes_gate = (
                    table_rels >= min_rels
                    or len(metric_cols) >= min_metrics
                ) and table.row_count >= min_rows_entity
                if passes_gate:
                    candidate = self._build_entity_summary(
                        table, metric_cols, dimension_tables
                    )
                    candidates.append(candidate)
                    logger.debug(
                        "entity_summary candidate: %s (metrics=%s, dims=%s)",
                        table.name, metric_cols, dimension_tables,
                    )
                else:
                    logger.debug(
                        "Skipped entity_summary candidate for %s: "
                        "insufficient evidence (rels=%d, rows=%d)",
                        table.name, table_rels, table.row_count,
                    )

            # Archetype 3: aggregation (numeric columns, no FK context, enough rows)
            if (
                len(metric_cols) >= 2
                and not dimension_tables
                and table.row_count >= min_rows_agg
            ):
                candidate = self._build_aggregation(table, metric_cols)
                candidates.append(candidate)
                logger.debug(
                    "aggregation candidate: %s (metrics=%s)",
                    table.name, metric_cols,
                )

        return candidates

    def _get_temporal_cols(self, columns: list[ColumnInfo]) -> list[str]:
        result = []
        for col in columns:
            is_temporal = col.semantic_type in _TEMPORAL_SEMANTIC_TYPES or (
                col.semantic_type is None
                and any(p in col.name.lower() for p in _TEMPORAL_PATTERNS)
            )
            if is_temporal:
                result.append(col.name)
        return result

    def _get_metric_cols(self, columns: list[ColumnInfo]) -> list[str]:
        return [col.name for col in columns if is_metric_column(col)]

    def _get_dimension_tables(
        self,
        table_name: str,
        fk_pairs: set[tuple[str, str]],
        discovery: DiscoveryResult,
    ) -> list[str]:
        """Return tables that this table is related to (potential dimension tables)."""
        result = []
        for r in discovery.relationships:
            if r.from_table == table_name:
                result.append(r.to_table)
            elif r.to_table == table_name:
                result.append(r.from_table)
        return list(set(result))

    def _build_period_comparison(
        self,
        table,  # TableInfo
        temporal_cols: list[str],
        metric_cols: list[str],
    ) -> MartCandidate:
        time_col = temporal_cols[0]
        return MartCandidate(
            archetype="period_comparison",
            candidate_tables=[table.name],
            candidate_columns=temporal_cols + metric_cols,
            proposed_name=f"mart_{table.name}_by_period",
            assumptions=[
                f"Groups by {time_col} (truncated to day/week/month -- review granularity)",
                "Period-over-period comparison uses LAG() -- requires consistent time intervals",
                "Only non-null values are aggregated",
            ],
            questions=[
                f"What time granularity should '{time_col}' be truncated to? (day/week/month)",
                "Should period comparison be week-over-week or month-over-month?",
                "Are there time zones to consider for the temporal column?",
            ],
        )

    def _build_entity_summary(
        self,
        table,  # TableInfo
        metric_cols: list[str],
        dimension_tables: list[str],
    ) -> MartCandidate:
        dim_table = dimension_tables[0]
        return MartCandidate(
            archetype="entity_summary",
            candidate_tables=[table.name, dim_table],
            candidate_columns=metric_cols,
            proposed_name=f"mart_{table.name}_by_{dim_table}",
            assumptions=[
                f"Joins {table.name} to {dim_table} via detected foreign key",
                f"Aggregates metrics: {', '.join(metric_cols[:5])}",
                "Groups by all dimension columns from the joined table",
            ],
            questions=[
                f"Is the join between {table.name} and {dim_table} the correct grain?",
                "Should any metrics be summed vs averaged?",
                "Are there additional dimension tables that should be joined?",
            ],
        )

    def _build_aggregation(
        self,
        table,  # TableInfo
        metric_cols: list[str],
    ) -> MartCandidate:
        return MartCandidate(
            archetype="aggregation",
            candidate_tables=[table.name],
            candidate_columns=metric_cols,
            proposed_name=f"mart_{table.name}_summary",
            assumptions=[
                f"Aggregates all numeric columns: {', '.join(metric_cols[:5])}",
                "No foreign key context detected -- single-table aggregation",
                "NULL values excluded from aggregate calculations",
            ],
            questions=[
                "Should any of these numeric columns be treated as dimensions (GROUP BY) "
                "rather than metrics?",
                "Are there categorical columns to break this aggregation out by?",
                "What is the expected grain of this summary?",
            ],
        )


def _render_period_comparison(
    candidate: MartCandidate,
    target_schema: str,
) -> GeneratedModel:
    """Render a period_comparison candidate to a GeneratedModel with SQL."""
    table_name = candidate.candidate_tables[0]
    time_cols = [c for c in candidate.candidate_columns if any(
        p in c.lower() for p in _TEMPORAL_PATTERNS
    )]
    metric_cols = [c for c in candidate.candidate_columns if c not in time_cols]
    time_col = time_cols[0] if time_cols else "created_at"

    metric_lines = "\n".join(
        f'    AVG("{c}") AS avg_{c},'
        for c in metric_cols[:5]
    )
    if not metric_lines:
        metric_lines = "    COUNT(*) AS record_count,"

    sql = f"""-- Mart: {candidate.proposed_name}
-- Archetype: period_comparison
-- REQUIRES REVIEW: Temporal aggregation and period comparison logic.

CREATE OR REPLACE TABLE {target_schema}.{candidate.proposed_name} AS
WITH by_period AS (
    SELECT
        DATE_TRUNC('month', CAST("{time_col}" AS DATE)) AS period,
{metric_lines}
        COUNT(*) AS row_count
    FROM staging.stg_{table_name}
    GROUP BY 1
)
SELECT
    period,
    row_count,
{metric_lines.replace("AVG(", "    ").replace(" AS avg_", " AS current_avg_")},
    LAG(row_count) OVER (ORDER BY period) AS prev_period_row_count
FROM by_period
ORDER BY period"""

    return GeneratedModel(
        name=candidate.proposed_name,
        model_type="mart",
        sql=sql.strip(),
        description=(
            f"Period-over-period comparison for {table_name}. "
            f"Groups by {time_col} and computes aggregate metrics across time periods."
        ),
        source_tables=candidate.candidate_tables,
        depends_on=[f"stg_{t}" for t in candidate.candidate_tables],
        status="proposed",
        assumptions=candidate.assumptions,
        questions=candidate.questions,
    )


def _render_entity_summary(
    candidate: MartCandidate,
    target_schema: str,
) -> GeneratedModel:
    """Render an entity_summary candidate to a GeneratedModel with SQL."""
    fact_table = candidate.candidate_tables[0]
    dim_table = candidate.candidate_tables[1] if len(candidate.candidate_tables) > 1 else None
    metric_cols = candidate.candidate_columns

    metric_lines = "\n".join(
        f'    AVG(f."{c}") AS avg_{c},\n    SUM(f."{c}") AS total_{c},'
        for c in metric_cols[:3]
    )
    if not metric_lines:
        metric_lines = "    COUNT(*) AS record_count,"

    dim_key = dim_table[:-1] if dim_table.endswith("s") else dim_table
    join_clause = (
        f"JOIN staging.stg_{dim_table} d ON f.{dim_key}_id = d.{dim_key}_id"
        if dim_table else ""
    )

    sql = f"""-- Mart: {candidate.proposed_name}
-- Archetype: entity_summary
-- REQUIRES REVIEW: Join logic and metric aggregation assumptions.

CREATE OR REPLACE TABLE {target_schema}.{candidate.proposed_name} AS
SELECT
    d.*,
{metric_lines}
    COUNT(*) AS fact_count
FROM staging.stg_{fact_table} f
{join_clause}
GROUP BY ALL"""

    return GeneratedModel(
        name=candidate.proposed_name,
        model_type="mart",
        sql=sql.strip(),
        description=(
            f"Summary of {fact_table} grouped by {dim_table or 'dimension'}. "
            f"Aggregates metrics: {', '.join(metric_cols[:3])}."
        ),
        source_tables=candidate.candidate_tables,
        depends_on=[f"stg_{t}" for t in candidate.candidate_tables],
        status="proposed",
        assumptions=candidate.assumptions,
        questions=candidate.questions,
    )


def _render_aggregation(
    candidate: MartCandidate,
    target_schema: str,
) -> GeneratedModel:
    """Render an aggregation candidate to a GeneratedModel with SQL."""
    table_name = candidate.candidate_tables[0]
    metric_cols = candidate.candidate_columns

    metric_lines = "\n".join(
        f'    AVG("{c}") AS avg_{c},\n    MIN("{c}") AS min_{c},\n    MAX("{c}") AS max_{c},'
        for c in metric_cols[:5]
    )

    sql = f"""-- Mart: {candidate.proposed_name}
-- Archetype: aggregation
-- REQUIRES REVIEW: Verify which columns are metrics vs dimensions.

CREATE OR REPLACE TABLE {target_schema}.{candidate.proposed_name} AS
SELECT
{metric_lines}
    COUNT(*) AS total_rows
FROM staging.stg_{table_name}"""

    return GeneratedModel(
        name=candidate.proposed_name,
        model_type="mart",
        sql=sql.strip(),
        description=f"{_humanize_name(table_name)} summary",
        source_tables=candidate.candidate_tables,
        depends_on=[f"stg_{t}" for t in candidate.candidate_tables],
        status="proposed",
        assumptions=candidate.assumptions,
        questions=candidate.questions,
    )


def _humanize_name(name: str) -> str:
    """Convert snake_case to Title Case."""
    return name.replace("_", " ").title()


_ARCHETYPE_RENDERERS = {
    "period_comparison": _render_period_comparison,
    "entity_summary": _render_entity_summary,
    "aggregation": _render_aggregation,
}


def generate_mart_models(
    discovery: DiscoveryResult,
    target_schema: str = "marts",
) -> list[GeneratedModel]:
    """Generate mart SQL models using archetype-based pattern matching.

    Replaces the hard-coded _MART_DEFINITIONS with a PatternMatcher that detects
    archetypes from discovered column semantic types and table relationships.

    Each mart is proposed with assumptions and clarifying questions.
    Status is always 'proposed' -- never auto-approved.
    """
    matcher = PatternMatcher()
    candidates = matcher.match(discovery)

    models: list[GeneratedModel] = []
    seen_names: set[str] = set()

    for candidate in candidates:
        if candidate.proposed_name in seen_names:
            continue  # Deduplicate
        seen_names.add(candidate.proposed_name)

        renderer = _ARCHETYPE_RENDERERS.get(candidate.archetype)
        if renderer is None:
            logger.warning("No renderer for archetype %s", candidate.archetype)
            continue

        model = renderer(candidate, target_schema)
        models.append(model)

    return models
