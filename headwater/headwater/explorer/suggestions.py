"""Suggestion engine -- auto-generates BI-oriented questions from metadata.

Generates analytical questions that a data professional would actually ask,
derived entirely from the discovered schema: column names, dtypes, semantic types,
detected relationships, and mart model definitions.

No hardcoded table names, column names, or domain knowledge -- everything is
inferred from what the data actually contains.

Priority order (highest to lowest):
  mart > relationship > semantic > quality

Quality questions are intentionally de-prioritized and capped:
  - Only for numeric metric columns with actual nulls present
  - Hard cap of MAX_QUALITY_SUGGESTIONS regardless of how many contracts exist

Total output is capped at MAX_TOTAL_SUGGESTIONS, deduplicated.
"""

from __future__ import annotations

import re

import duckdb

from headwater.core.models import (
    ColumnProfile,
    ContractCheckResult,
    ContractRule,
    DiscoveryResult,
    GeneratedModel,
    Relationship,
    SuggestedQuestion,
    TableInfo,
)
from headwater.explorer.utils import resolve_table_ref, table_exists

MAX_TOTAL_SUGGESTIONS = 15
MAX_QUALITY_SUGGESTIONS = 3

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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_suggestions(
    discovery: DiscoveryResult,
    models: list[GeneratedModel] | None = None,
    contracts: list[ContractRule] | None = None,
    quality_results: list[ContractCheckResult] | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> list[SuggestedQuestion]:
    """Generate suggested questions from all available metadata.

    All questions are derived from actual schema -- no hardcoded names.
    Returns at most MAX_TOTAL_SUGGESTIONS questions in priority order.
    """
    all_models = models or []
    profile_index = {(p.table_name, p.column_name): p for p in discovery.profiles}

    buckets: dict[str, list[SuggestedQuestion]] = {
        "mart": _from_mart_models(all_models, con),
        "relationship": _from_relationships(
            discovery.tables, discovery.relationships, all_models, con
        ),
        "semantic": _from_table_structure(discovery.tables, profile_index, all_models, con),
        "quality": _from_quality_findings(
            contracts or [],
            quality_results or [],
            profile_index=profile_index,
            tables=discovery.tables,
        ),
    }

    result: list[SuggestedQuestion] = []
    seen: set[str] = set()

    for source in ("mart", "relationship", "semantic", "quality"):
        for q in buckets[source]:
            key = " ".join(q.question.lower().split())
            if key not in seen:
                seen.add(key)
                result.append(q)

    return result[:MAX_TOTAL_SUGGESTIONS]


# ---------------------------------------------------------------------------
# Mart-derived questions
# ---------------------------------------------------------------------------


def _from_mart_models(
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None,
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

        label = _humanize(model.name)
        ref = f"marts.{model.name}"

        # One clean question per mart -- no description leakage
        questions.append(
            SuggestedQuestion(
                question=f"What are the key metrics in {label}?",
                source="mart",
                category=label.title(),
                relevant_tables=model.source_tables,
                sql_hint=f"SELECT * FROM {ref} LIMIT 50",
            )
        )

    return questions


# ---------------------------------------------------------------------------
# Relationship-derived questions
# ---------------------------------------------------------------------------


def _from_relationships(
    tables: list[TableInfo],
    relationships: list[Relationship],
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None,
) -> list[SuggestedQuestion]:
    """Generate cross-entity questions from detected foreign key relationships.

    For each relationship A.col -> B.col, generates a question about the
    distribution of A records per B entity using the actual column names.
    """
    questions: list[SuggestedQuestion] = []
    table_map = {t.name: t for t in tables}
    seen_pairs: set[frozenset[str]] = set()

    for rel in relationships:
        if rel.from_table not in table_map or rel.to_table not in table_map:
            continue

        pair = frozenset([rel.from_table, rel.to_table])
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        from_label = _humanize(rel.from_table)
        to_label = _humanize(rel.to_table)

        from_ref = (
            resolve_table_ref(rel.from_table, con, models)
            if con is not None
            else rel.from_table
        )
        to_ref = (
            resolve_table_ref(rel.to_table, con, models)
            if con is not None
            else rel.to_table
        )

        # Find a useful metric from the from_table to aggregate
        from_table_info = table_map[rel.from_table]
        metric_col = _pick_metric_col(from_table_info)

        if metric_col:
            sql = (
                f'SELECT t."{rel.to_column}", COUNT(*) AS {from_label}_count, '
                f'AVG(f."{metric_col}") AS avg_{metric_col} '
                f"FROM {from_ref} f "
                f'JOIN {to_ref} t ON f."{rel.from_column}" = t."{rel.to_column}" '
                f'GROUP BY t."{rel.to_column}" '
                f"ORDER BY {from_label}_count DESC LIMIT 20"
            )
        else:
            sql = (
                f'SELECT t."{rel.to_column}", COUNT(*) AS {from_label}_count '
                f"FROM {from_ref} f "
                f'JOIN {to_ref} t ON f."{rel.from_column}" = t."{rel.to_column}" '
                f'GROUP BY t."{rel.to_column}" '
                f"ORDER BY {from_label}_count DESC LIMIT 20"
            )

        questions.append(
            SuggestedQuestion(
                question=f"How many {from_label} records are there per {to_label}?",
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
) -> list[SuggestedQuestion]:
    """Generate analytical questions by inspecting each table's actual column structure.

    For each table, identifies temporal, metric, and dimension columns and
    generates the most relevant question type:
      - temporal + metric  -> trend over time
      - dimension + metric -> breakdown / ranking
      - metric only        -> summary statistics
    """
    questions: list[SuggestedQuestion] = []

    for table in tables:
        ref = (
            resolve_table_ref(table.name, con, models)
            if con is not None
            else table.name
        )
        label = _humanize(table.name)

        temporal_cols = _get_temporal_cols(table)
        metric_cols = _get_metric_cols(table, profile_index)
        dim_cols = _get_dimension_cols(table, profile_index)

        if temporal_cols and metric_cols:
            t_col = temporal_cols[0]
            m_col = metric_cols[0]
            questions.append(
                SuggestedQuestion(
                    question=f"How has {_humanize(m_col)} in {label} changed over time?",
                    source="semantic",
                    category=label.title(),
                    relevant_tables=[table.name],
                    sql_hint=(
                        f'SELECT "{t_col}", AVG("{m_col}") AS avg_{m_col}, '
                        f'COUNT(*) AS records '
                        f"FROM {ref} "
                        f'GROUP BY "{t_col}" ORDER BY "{t_col}" LIMIT 100'
                    ),
                )
            )

        if dim_cols and metric_cols:
            d_col = dim_cols[0]
            m_col = metric_cols[0]
            questions.append(
                SuggestedQuestion(
                    question=(
                        f"Which {_humanize(d_col)} has the highest {_humanize(m_col)} "
                        f"in {label}?"
                    ),
                    source="semantic",
                    category=label.title(),
                    relevant_tables=[table.name],
                    sql_hint=(
                        f'SELECT "{d_col}", '
                        f'COUNT(*) AS records, '
                        f'ROUND(AVG("{m_col}"), 2) AS avg_{m_col}, '
                        f'MAX("{m_col}") AS max_{m_col} '
                        f"FROM {ref} "
                        f'GROUP BY "{d_col}" '
                        f"ORDER BY avg_{m_col} DESC LIMIT 20"
                    ),
                )
            )

        if metric_cols and not temporal_cols and not dim_cols:
            m_col = metric_cols[0]
            questions.append(
                SuggestedQuestion(
                    question=f"What is the distribution of {_humanize(m_col)} in {label}?",
                    source="semantic",
                    category=label.title(),
                    relevant_tables=[table.name],
                    sql_hint=(
                        f'SELECT MIN("{m_col}") AS min, MAX("{m_col}") AS max, '
                        f'ROUND(AVG("{m_col}"), 2) AS mean, COUNT(*) AS records '
                        f"FROM {ref}"
                    ),
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
                    question=(
                        f"Why are there missing values in "
                        f"{base_table} {col}?"
                    ),
                    source="quality",
                    category="Data Quality",
                    relevant_tables=[rule.model_name],
                    sql_hint=(
                        f"SELECT * FROM {rule.model_name} "
                        f'WHERE "{col}" IS NULL LIMIT 20'
                    ),
                )
            )

        elif rule.rule_type == "cardinality" and col:
            questions.append(
                SuggestedQuestion(
                    question=(
                        f"What unexpected {col} values appeared in "
                        f"{base_table}?"
                    ),
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
                    question=(
                        f"Which {col} values have duplicates in {base_table}?"
                    ),
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
    return [
        c.name
        for c in table.columns
        if not c.is_primary_key
        and not _ID_NAME_RE.search(c.name)
        and (
            any(c.dtype.lower().startswith(t) for t in _TEMPORAL_DTYPES)
            or c.semantic_type == "temporal"
            or bool(_TEMPORAL_NAME_RE.search(c.name))
        )
    ]


def _get_metric_cols(
    table: TableInfo,
    profile_index: dict[tuple[str, str], ColumnProfile],
) -> list[str]:
    cols = []
    for c in table.columns:
        if c.is_primary_key or _ID_NAME_RE.search(c.name):
            continue
        if c.semantic_type in ("id", "foreign_key", "primary_key", "dimension", "temporal"):
            continue
        if c.semantic_type in ("metric", "measure", "kpi"):
            cols.append(c.name)
            continue
        profile = profile_index.get((table.name, c.name))
        dtype = (profile.dtype if profile else c.dtype).lower()
        if any(t in dtype for t in _NUMERIC_DTYPES):
            cols.append(c.name)
    return cols


def _get_dimension_cols(
    table: TableInfo,
    profile_index: dict[tuple[str, str], ColumnProfile],
) -> list[str]:
    """Return low-cardinality columns suitable for GROUP BY.

    Includes varchar/text columns and numeric "code" columns with low
    cardinality (e.g. state_code, huc codes) that act as categorical dimensions.
    """
    cols = []
    for c in table.columns:
        if c.is_primary_key or _ID_NAME_RE.search(c.name):
            continue
        if c.semantic_type in ("id", "foreign_key", "primary_key"):
            continue
        profile = profile_index.get((table.name, c.name))
        dtype = (profile.dtype if profile else c.dtype).lower()

        is_text = "varchar" in dtype or "char" in dtype or "text" in dtype
        is_numeric_code = (
            any(t in dtype for t in _NUMERIC_DTYPES)
            and c.semantic_type == "dimension"
        )

        if not is_text and not is_numeric_code:
            continue
        # Only good grouping columns if distinct count is reasonably low
        if profile is not None and profile.distinct_count > 200:
            continue
        cols.append(c.name)
    return cols


def _pick_metric_col(table: TableInfo) -> str | None:
    """Pick the first non-ID, non-code numeric column from a table."""
    for c in table.columns:
        if c.is_primary_key or _ID_NAME_RE.search(c.name):
            continue
        if c.semantic_type in ("id", "foreign_key", "primary_key", "dimension", "temporal"):
            continue
        dtype = c.dtype.lower()
        if any(t in dtype for t in _NUMERIC_DTYPES):
            return c.name
    return None


def _is_metric_col(
    table_name: str,
    column_name: str,
    profile_index: dict[tuple[str, str], ColumnProfile],
    table_map: dict[str, TableInfo],
) -> bool:
    """Return True if the column is a numeric metric with actual nulls."""
    col_lower = column_name.lower()
    if _ID_NAME_RE.search(col_lower):
        return False
    if bool(_TEMPORAL_NAME_RE.search(col_lower)):
        return False
    if col_lower in ("year", "month", "day", "date"):
        return False

    # Check semantic type
    table = table_map.get(table_name)
    if table:
        col_info = next((c for c in table.columns if c.name == column_name), None)
        if col_info and col_info.semantic_type in (
            "id", "foreign_key", "primary_key", "temporal", "dimension",
        ):
            return False

    # Use profile dtype to confirm it's numeric -- string/varchar columns are not metrics
    profile = profile_index.get((table_name, column_name))
    return profile is None or any(t in profile.dtype.lower() for t in _NUMERIC_DTYPES)


# ---------------------------------------------------------------------------
# String helpers
# ---------------------------------------------------------------------------


def _humanize(name: str) -> str:
    """Convert snake_case or prefixed model names to readable label."""
    name = name.split(".")[-1]  # drop schema prefix
    for prefix in ("mart_", "stg_"):
        if name.startswith(prefix):
            name = name[len(prefix):]
    return name.replace("_", " ")


def _humanize_model(model_name: str) -> str:
    """Convert staging.stg_readings -> readings."""
    return _humanize(model_name)
