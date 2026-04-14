"""NL-to-SQL engine -- translates natural language questions to SQL queries.

Uses LLM with metadata context for intelligent SQL generation.
Falls back to pre-generated SQL hints from suggested questions when no LLM is available.
Always validates that generated SQL is read-only before execution.

Auto-repair: when a query fails execution, the engine sends the error back to the LLM
with the original SQL and schema context, asking it to fix the query. This loop runs
up to MAX_REPAIR_ATTEMPTS times before returning the error to the user.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

import duckdb

from headwater.analyzer.llm import LLMProvider, NoLLMProvider
from headwater.core.classification import (
    is_dimension_column as _shared_is_dimension,
)
from headwater.core.classification import (
    is_metric_column as _shared_is_metric,
)
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    ExplorationResult,
    GeneratedModel,
    Relationship,
    SuggestedQuestion,
    TableInfo,
)
from headwater.explorer.query_planner import QueryPlanner
from headwater.explorer.schema_graph import SchemaGraph
from headwater.explorer.utils import resolve_table_ref, table_exists
from headwater.explorer.visualization import recommend_visualization

logger = logging.getLogger(__name__)

MAX_REPAIR_ATTEMPTS = 3

_FORBIDDEN_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXEC)\b",
    re.IGNORECASE,
)

_SYSTEM_PROMPT = """You are a SQL query generator for a DuckDB analytical database.
You receive table metadata (schemas, column descriptions, relationships) and a natural
language question. Generate a single SELECT query that answers the question.

Rules:
- Generate ONLY a SELECT statement. Never INSERT, UPDATE, DELETE, DROP, or ALTER.
- Use the exact table and column names from the metadata.
- Tables in the staging schema use the prefix "staging." (e.g., staging.stg_readings).
- Tables in the marts schema use the prefix "marts." (e.g., marts.mart_air_quality_daily).
- Prefer mart tables when they contain the needed data (they are pre-aggregated).
- Use DuckDB SQL syntax.
- Return ONLY the SQL query, no explanation or markdown fences.
- Keep results manageable: use LIMIT 100 unless the question implies aggregation.
- Use double quotes around column names that contain special characters."""

_REPAIR_SYSTEM_PROMPT = """You are a SQL repair assistant for a DuckDB analytical database.
A query was generated but failed to execute. You are given the original question,
the failed SQL, and the database error. Fix the SQL so it runs correctly.

Rules:
- Generate ONLY the corrected SELECT statement. No explanation.
- The error message tells you exactly what went wrong -- fix that specific issue.
- Common fixes: wrong column name, wrong table name, missing JOIN, type mismatch,
  missing schema prefix, incorrect function syntax.
- Use the schema metadata to find the correct column/table names.
- Return ONLY the fixed SQL, no markdown fences or commentary."""


def ask(
    question: str,
    con: duckdb.DuckDBPyConnection,
    discovery: DiscoveryResult,
    models: list[GeneratedModel] | None = None,
    suggestions: list[SuggestedQuestion] | None = None,
    provider: LLMProvider | None = None,
    reviewed_tables: set[str] | None = None,
) -> ExplorationResult:
    """Translate a natural language question to SQL, execute it, and return results.

    When *reviewed_tables* is provided, only those tables are available for
    query planning. This gates the explorer on the data dictionary review.

    Strategy:
    1. Check if the question matches a suggested question with a pre-built SQL hint.
    2. Try the QueryPlanner (schema-graph-based entity resolution + SQL generation).
    3. Fall back to legacy heuristic SQL generation.
    4. If LLM is available, generate SQL from metadata context.
    5. Validate the SQL is read-only.
    6. Execute against DuckDB.
    7. If execution fails and LLM is available, auto-repair the query (up to 3 attempts).
    8. Return results with visualization recommendation.
    """
    # Gate: if reviewed_tables is provided but empty, block exploration
    if reviewed_tables is not None and len(reviewed_tables) == 0:
        return ExplorationResult(
            question=question,
            sql="",
            error=(
                "No tables have been reviewed yet. "
                "Visit the Data Dictionary to review table metadata before exploring."
            ),
        )

    has_llm = provider is not None and not isinstance(provider, NoLLMProvider)
    context = _build_context(discovery, models or []) if has_llm else ""

    # Try matching a suggested question first
    sql = _match_suggestion(question, suggestions or [])

    # Try QueryPlanner (schema-graph-based, handles cross-table joins)
    if sql is None:
        sql = _planned_sql(
            question, discovery, models or [], con=con,
            reviewed_tables=reviewed_tables,
        )

    # Fall back to legacy heuristic SQL generation
    if sql is None:
        sql = _heuristic_sql(question, discovery, models or [], con=con)

    # If still no match and LLM is available, generate SQL
    if sql is None and has_llm:
        sql = asyncio.run(_generate_sql(question, context, provider))

    if sql is None:
        return ExplorationResult(
            question=question,
            sql="",
            error=(
                "Could not generate SQL for this question. "
                "Try rephrasing with table or column names from your data."
            ),
        )

    # Validate read-only
    if not _is_read_only(sql):
        return ExplorationResult(
            question=question,
            sql=sql,
            error="Generated SQL contains write operations and was blocked for safety.",
        )

    # Grounding check: verify question terms exist in schema + generated SQL
    warnings = _check_grounding(
        question, discovery, models or [], sql, suggestions or []
    )

    # Execute (with auto-repair if LLM is available)
    result = _execute_query(question, sql, con)

    if result.error and has_llm:
        result = _repair_loop(question, sql, result.error, con, context, provider)

    result.warnings = warnings
    return result


# ---------------------------------------------------------------------------
# Auto-repair loop
# ---------------------------------------------------------------------------


def _repair_loop(
    question: str,
    original_sql: str,
    original_error: str,
    con: duckdb.DuckDBPyConnection,
    context: str,
    provider: LLMProvider,
) -> ExplorationResult:
    """Attempt to repair a failed SQL query using the LLM.

    Sends the error + original SQL + schema context to the LLM and asks it
    to fix the query. Retries up to MAX_REPAIR_ATTEMPTS times.
    """
    repair_history: list[dict[str, str]] = [
        {"sql": original_sql, "error": original_error}
    ]

    current_sql = original_sql
    current_error = original_error

    for attempt in range(MAX_REPAIR_ATTEMPTS):
        logger.info(
            "Auto-repair attempt %d/%d for: %s",
            attempt + 1,
            MAX_REPAIR_ATTEMPTS,
            question,
        )

        fixed_sql = asyncio.run(
            _generate_repair(question, current_sql, current_error, context, provider)
        )

        if fixed_sql is None:
            logger.warning("LLM returned no repair for attempt %d", attempt + 1)
            break

        if not _is_read_only(fixed_sql):
            logger.warning("Repaired SQL is not read-only, stopping repair")
            break

        # Try executing the repaired query
        result = _execute_query(question, fixed_sql, con)

        if result.error is None:
            # Repair succeeded
            result.repaired = True
            result.repair_history = repair_history
            logger.info(
                "Auto-repair succeeded on attempt %d for: %s",
                attempt + 1,
                question,
            )
            return result

        # Repair didn't work, record and try again
        repair_history.append({"sql": fixed_sql, "error": result.error})
        current_sql = fixed_sql
        current_error = result.error

    # All repair attempts exhausted
    return ExplorationResult(
        question=question,
        sql=original_sql,
        error=(
            f"Query failed and auto-repair was unsuccessful after "
            f"{len(repair_history)} attempt(s). "
            f"Last error: {current_error}"
        ),
        repair_history=repair_history,
    )


async def _generate_repair(
    question: str,
    failed_sql: str,
    error: str,
    context: str,
    provider: LLMProvider,
) -> str | None:
    """Ask the LLM to fix a failed SQL query."""
    prompt = f"""{context}

=== Original Question ===
{question}

=== Failed SQL ===
{failed_sql}

=== Database Error ===
{error}

Fix the SQL query so it executes successfully. Return ONLY the corrected SQL."""

    try:
        result = await provider.analyze(prompt, system=_REPAIR_SYSTEM_PROMPT)
        if isinstance(result, dict) and "sql" in result:
            return result["sql"]
        return None
    except Exception as e:
        logger.warning("LLM repair failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Suggestion matching
# ---------------------------------------------------------------------------


def _match_suggestion(
    question: str,
    suggestions: list[SuggestedQuestion],
    con: duckdb.DuckDBPyConnection | None = None,
) -> str | None:
    """Find a suggested question that matches and has a SQL hint."""
    q_lower = question.lower().strip().rstrip("?")

    for s in suggestions:
        if s.sql_hint and _questions_similar(q_lower, s.question.lower().strip().rstrip("?")):
            return s.sql_hint

    return None


_STOP_WORDS = {
    "what", "is", "the", "a", "an", "are", "how", "do", "does", "which",
    "where", "when", "who", "in", "on", "by", "for", "to", "of", "and",
    "or", "from", "with", "that", "this", "there", "have", "has", "was",
    "were", "be", "been", "being", "my", "your", "their", "its",
    "we", "our", "us", "i", "me", "more", "less", "than", "not", "no",
    "one", "ones", "other", "each", "every", "any", "some", "all",
    "give", "tell", "can", "could", "would", "should", "please", "just",
}


def _questions_similar(a: str, b: str) -> bool:
    """Check if two questions are similar enough to match.

    Intentionally strict: exact match or substring containment only.
    Fuzzy word-overlap matching is dangerous -- changing one key term
    (e.g. "by severity" -> "by inspection") changes the entire meaning
    but barely moves the overlap ratio, leading to silently wrong SQL.
    """
    if a == b:
        return True
    return bool(a in b or b in a)


# ---------------------------------------------------------------------------
# Schema-graph-based query planner (primary heuristic path)
# ---------------------------------------------------------------------------


def _planned_sql(
    question: str,
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None = None,
    reviewed_tables: set[str] | None = None,
) -> str | None:
    """Use the QueryPlanner to generate SQL from a natural language question.

    Builds a SchemaGraph from the discovery metadata, then uses the planner
    to resolve entities, plan joins, and generate SQL.  Returns None if the
    planner's confidence is too low.

    When *reviewed_tables* is provided, only those tables are indexed in the
    schema graph -- the planner physically cannot reference unreviewed tables.
    """
    graph = SchemaGraph(discovery, models, reviewed_tables=reviewed_tables)
    planner = QueryPlanner(graph, con=con, models=models)
    return planner.plan_sql(question)


# ---------------------------------------------------------------------------
# Legacy heuristic SQL builder -- fallback when planner declines
# ---------------------------------------------------------------------------

# Question patterns that indicate what kind of query to build
_TREND_WORDS = {
    "trend", "trends", "trending", "trended",
    "increasing", "decreasing", "growing", "growth",
    "over", "time", "changing",
}
_COUNT_WORDS = {"how", "many", "count", "total", "number"}
_BREAKDOWN_WORDS = {"by", "across", "per", "breakdown", "break", "distribution", "distributed"}
_TOP_WORDS = {"top", "most", "highest", "worst", "best", "largest", "lowest", "least", "smallest"}

def _resolve_table_ref(
    table_name: str,
    con: duckdb.DuckDBPyConnection,
    models: list[GeneratedModel],
) -> str:
    return resolve_table_ref(table_name, con, models)


def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return table_exists(con, schema, table)


def _heuristic_sql(
    question: str,
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None = None,
) -> str | None:
    """Build SQL from metadata heuristics when no suggestion matches.

    Only handles single-table queries where intent is clear. Returns None
    (rather than wrong SQL) when the question implies JOINs, references
    entities from multiple tables, or can't be mapped confidently.
    """
    q_lower = question.lower()
    q_words = set(re.sub(r"[^a-z0-9 ]", "", q_lower).split())
    content_words = q_words - _STOP_WORDS - _ANALYTICAL_WORDS

    # Step 1: Find the primary table
    table = _match_table(q_words, discovery, models)
    if table is None:
        return None

    # Step 2: Check if the question references another table.
    # Try direct FK, then indirect (shared FK target), then fall back to single-table.
    secondary = _find_referenced_table(content_words, table, discovery)
    join_rel = None
    indirect_join = None
    if secondary is not None:
        join_rel = _find_join_relationship(table, secondary, discovery)
        if join_rel is None:
            indirect_join = _find_indirect_join(table, secondary, discovery)

    # Build profile index for column classification
    profile_index: dict[tuple[str, str], ColumnProfile] = {
        (p.table_name, p.column_name): p for p in discovery.profiles
    }

    # Classify columns by role.
    # Prefer actual date/timestamp columns for trends (higher granularity) over
    # name-only matches like "year" or "month" which produce few groups.
    _temporal_raw = [
        c for c in table.columns
        if c.dtype in ("timestamp", "date")
        or c.semantic_type == "temporal"
        or re.search(r"(date|time|month|year|day|week|quarter|_at$)", c.name, re.IGNORECASE)
    ]
    temporal_cols = sorted(
        _temporal_raw,
        key=lambda c: (0 if c.dtype in ("timestamp", "date") else 1),
    )
    metric_cols = [
        c for c in table.columns
        if _shared_is_metric(c, profile_index.get((table.name, c.name)))
    ]
    dimension_cols = [
        c for c in table.columns
        if _shared_is_dimension(c, profile_index.get((table.name, c.name)))
    ]

    if con is not None:
        table_ref = _resolve_table_ref(table.name, con, models)
    else:
        # No connection — prefer executed mart from models list if one covers this table
        mart_ref = next(
            (
                f"marts.{m.name}"
                for m in models
                if m.model_type == "mart"
                and table.name in m.source_tables
                and m.status == "executed"
            ),
            None,
        )
        table_ref = mart_ref if mart_ref is not None else f"staging.stg_{table.name}"

    # Step 2b: If we have a FK relationship, build a JOIN query
    if join_rel is not None and secondary is not None:
        return _build_join_sql(
            table, secondary, join_rel, q_words, con, models,
        )

    # Step 2c: Indirect join through a shared intermediate table
    if indirect_join is not None and secondary is not None:
        mid_table, rel_a, rel_b = indirect_join
        return _build_indirect_join_sql(
            table, secondary, mid_table, rel_a, rel_b, q_words, con, models,
        )

    # If secondary was found but no join path exists, fall through to
    # single-table analysis on the primary table (better than giving up).

    # Step 3: Detect the question intent and build SQL (single-table)
    is_trend = bool(q_words & _TREND_WORDS) and temporal_cols
    is_count = bool(q_words & _COUNT_WORDS)
    is_top = bool(q_words & _TOP_WORDS)
    has_breakdown_word = bool(q_words & _BREAKDOWN_WORDS)

    # For column matching, remove table-name words (and their stems) so
    # "complaints" / "reading" from the table name doesn't shadow the user's
    # actual column reference like "county".
    table_name_words = set(table.name.lower().replace("_", " ").split())
    table_name_stems: set[str] = set()
    for tw in table_name_words:
        table_name_stems.update(_stem(tw))
    col_match_words = {
        w for w in content_words
        if w not in table_name_words and not (_stem(w) & table_name_stems)
    }

    # Try to find a specific dimension/metric mentioned in the question
    target_dim = _match_column(col_match_words, dimension_cols)
    target_metric = _match_column(col_match_words, metric_cols)

    # Track whether the user's specific words (minus table name) matched anything.
    # Used later to decide if a blind fallback is appropriate.
    first_pass_matched = target_dim is not None or target_metric is not None

    # If col_match_words yielded nothing, fall back to full content_words
    if target_dim is None and target_metric is None:
        target_dim = _match_column(content_words, dimension_cols)
        target_metric = _match_column(content_words, metric_cols)

    # Guard: if the user referenced specific columns (e.g. "county") but the
    # first pass found nothing, the second-pass match is likely a false positive
    # from table-name word overlap (e.g. "complaints" -> "complaint_type_311").
    # Return None rather than building a query with the wrong column.
    # Exclude words that matched a secondary table (those are table references,
    # not orphaned column references).
    unmatched_words = set(col_match_words)
    if secondary is not None:
        sec_name_words = set(secondary.name.lower().replace("_", " ").split())
        sec_stems: set[str] = set()
        for sw in sec_name_words:
            sec_stems.update(_stem(sw))
        unmatched_words = {
            w for w in unmatched_words
            if w not in sec_name_words and not (_stem(w) & sec_stems)
        }
    if bool(unmatched_words) and not first_pass_matched:
        return None

    # -- Trend query: metric over time, optionally grouped by a dimension
    if is_trend and temporal_cols:
        time_col = temporal_cols[0]
        metric = target_metric or (metric_cols[0] if metric_cols else None)
        dim = target_dim

        # Build time expression based on dtype:
        # - timestamp -> truncate to month for meaningful grouping
        # - date -> use as-is
        # - varchar/int (e.g. "year", "month") -> use the raw column
        if time_col.dtype == "timestamp":
            time_expr = f"DATE_TRUNC('month', \"{time_col.name}\")"
        elif time_col.dtype == "date":
            time_expr = f'CAST("{time_col.name}" AS DATE)'
        else:
            # Name-pattern temporal (year, month, etc.) -- use raw value
            time_expr = f'"{time_col.name}"'

        parts = [f"SELECT {time_expr} AS period"]
        if dim:
            parts[0] += f', "{dim.name}"'
        if metric:
            parts[0] += f', ROUND(AVG("{metric.name}"), 2) AS avg_{metric.name}'
            parts[0] += ", COUNT(*) AS total"
        else:
            parts[0] += ", COUNT(*) AS total"

        parts.append(f"FROM {table_ref}")
        group = "GROUP BY period"
        if dim:
            group += f', "{dim.name}"'
        parts.append(group)
        parts.append("ORDER BY period")
        return " ".join(parts)

    # -- Breakdown query: metric grouped by a dimension
    if target_dim and (has_breakdown_word or target_metric):
        metric = target_metric or (metric_cols[0] if metric_cols else None)
        parts = [f'SELECT "{target_dim.name}"']
        if metric:
            parts[0] += f', ROUND(AVG("{metric.name}"), 2) AS avg_{metric.name}'
        parts[0] += ", COUNT(*) AS total"
        parts.append(f"FROM {table_ref}")
        parts.append(f'GROUP BY "{target_dim.name}"')
        order = "ORDER BY total DESC"
        if metric:
            order = f"ORDER BY avg_{metric.name} DESC"
        parts.append(order)
        return " ".join(parts)

    # -- Top/ranking query
    if is_top and (metric_cols or dimension_cols):
        metric = target_metric or (metric_cols[0] if metric_cols else None)
        dim = target_dim or (dimension_cols[0] if dimension_cols else None)
        if dim and metric:
            return (
                f'SELECT "{dim.name}", ROUND(AVG("{metric.name}"), 2) AS avg_{metric.name}, '
                f"COUNT(*) AS total "
                f"FROM {table_ref} "
                f'GROUP BY "{dim.name}" '
                f"ORDER BY avg_{metric.name} DESC LIMIT 15"
            )
        if dim:
            return (
                f'SELECT "{dim.name}", COUNT(*) AS total '
                f"FROM {table_ref} "
                f'GROUP BY "{dim.name}" '
                f"ORDER BY total DESC LIMIT 15"
            )

    # -- Count query
    if is_count:
        parts = ["SELECT COUNT(*) AS total"]
        if target_dim:
            parts[0] = f'SELECT "{target_dim.name}", COUNT(*) AS total'
        parts.append(f"FROM {table_ref}")
        if target_dim:
            parts.append(f'GROUP BY "{target_dim.name}" ORDER BY total DESC')
        return " ".join(parts)

    # -- Fallback: if we matched a table, show a useful summary
    if dimension_cols and metric_cols:
        dim = target_dim or dimension_cols[0]
        metric = target_metric or metric_cols[0]
        return (
            f'SELECT "{dim.name}", '
            f'ROUND(AVG("{metric.name}"), 2) AS avg_{metric.name}, '
            f"COUNT(*) AS total "
            f"FROM {table_ref} "
            f'GROUP BY "{dim.name}" '
            f"ORDER BY total DESC LIMIT 20"
        )

    if dimension_cols:
        dim = target_dim or dimension_cols[0]
        return (
            f'SELECT "{dim.name}", COUNT(*) AS total '
            f"FROM {table_ref} "
            f'GROUP BY "{dim.name}" ORDER BY total DESC LIMIT 20'
        )

    # Can't build anything meaningful
    return None


def _build_join_sql(
    primary: TableInfo,
    secondary: TableInfo,
    rel: Relationship,
    q_words: set[str],
    con: duckdb.DuckDBPyConnection | None,
    models: list[GeneratedModel],
) -> str:
    """Build a JOIN query between two FK-related tables.

    Joins the primary table to the secondary and produces a GROUP BY
    aggregation using the best available dimension from the secondary table
    and a metric or COUNT from the primary table.
    """
    # Resolve schema-qualified references
    if con is not None:
        p_ref = resolve_table_ref(primary.name, con, models)
        s_ref = resolve_table_ref(secondary.name, con, models)
    else:
        p_ref = primary.name
        s_ref = secondary.name

    # Determine join columns and alias direction
    # The FK goes from_table.from_column -> to_table.to_column
    if rel.from_table == primary.name:
        p_join_col, s_join_col = rel.from_column, rel.to_column
    else:
        p_join_col, s_join_col = rel.to_column, rel.from_column

    # Pick a good dimension from the secondary table for GROUP BY
    sec_dim = next(
        (c for c in secondary.columns if _shared_is_dimension(c)), None,
    )

    # Pick a metric from primary for aggregation
    pri_metric = next(
        (c for c in primary.columns if _shared_is_metric(c)), None,
    )

    # Build the SELECT
    group_col = f's."{sec_dim.name}"' if sec_dim else f's."{s_join_col}"'
    group_label = sec_dim.name if sec_dim else s_join_col

    select_parts = [f'{group_col} AS "{group_label}"', "COUNT(*) AS total"]
    if pri_metric:
        select_parts.append(
            f'ROUND(AVG(p."{pri_metric.name}"), 2) AS avg_{pri_metric.name}'
        )

    return (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {p_ref} p "
        f'JOIN {s_ref} s ON p."{p_join_col}" = s."{s_join_col}" '
        f'GROUP BY {group_col} '
        f"ORDER BY total DESC LIMIT 20"
    )


def _find_referenced_table(
    content_words: set[str],
    primary_table: TableInfo,
    discovery: DiscoveryResult,
) -> TableInfo | None:
    """If the question mentions another table, return it. Otherwise None."""
    primary_vocab: set[str] = set()
    primary_vocab.update(primary_table.name.lower().replace("_", " ").split())
    if primary_table.domain:
        primary_vocab.update(primary_table.domain.lower().split())
    if primary_table.description:
        primary_vocab.update(primary_table.description.lower().split())
    for col in primary_table.columns:
        primary_vocab.update(col.name.lower().replace("_", " ").split())

    for other_table in discovery.tables:
        if other_table.name == primary_table.name:
            continue
        other_words = set(other_table.name.lower().replace("_", " ").split())
        if other_table.domain:
            other_words.update(other_table.domain.lower().split())
        for col in other_table.columns:
            col_parts = set(col.name.lower().replace("_", " ").split())
            other_words.update(
                w for w in col_parts
                if w not in ("id", "name", "type", "status", "date")
            )
        unique_other = other_words - primary_vocab
        for qw in content_words:
            qw_stems = _stem(qw)
            for ow in unique_other:
                if _stem(ow) & qw_stems:
                    return other_table
    return None


def _find_join_relationship(
    table_a: TableInfo,
    table_b: TableInfo,
    discovery: DiscoveryResult,
) -> Relationship | None:
    """Find a FK relationship between two tables (either direction)."""
    for rel in discovery.relationships:
        if (
            (rel.from_table == table_a.name and rel.to_table == table_b.name)
            or (rel.from_table == table_b.name and rel.to_table == table_a.name)
        ):
            return rel
    return None


def _find_indirect_join(
    table_a: TableInfo,
    table_b: TableInfo,
    discovery: DiscoveryResult,
) -> tuple[TableInfo, Relationship, Relationship] | None:
    """Find an indirect FK path: table_a -> mid -> table_b (one hop).

    Returns (mid_table, rel_a_to_mid, rel_b_to_mid) or None.
    """
    table_lookup = {t.name: t for t in discovery.tables}

    # Build adjacency: for each table, which relationships does it participate in?
    rels_by_table: dict[str, list[Relationship]] = {}
    for rel in discovery.relationships:
        rels_by_table.setdefault(rel.from_table, []).append(rel)
        rels_by_table.setdefault(rel.to_table, []).append(rel)

    # Find tables that both table_a and table_b relate to
    a_neighbours: dict[str, Relationship] = {}
    for rel in rels_by_table.get(table_a.name, []):
        other = rel.to_table if rel.from_table == table_a.name else rel.from_table
        a_neighbours[other] = rel

    for rel in rels_by_table.get(table_b.name, []):
        other = rel.to_table if rel.from_table == table_b.name else rel.from_table
        if other in a_neighbours and other != table_a.name and other != table_b.name:
            mid = table_lookup.get(other)
            if mid is not None:
                return (mid, a_neighbours[other], rel)

    return None


def _build_indirect_join_sql(
    primary: TableInfo,
    secondary: TableInfo,
    mid_table: TableInfo,
    rel_a: Relationship,
    rel_b: Relationship,
    q_words: set[str],
    con: duckdb.DuckDBPyConnection | None,
    models: list[GeneratedModel],
) -> str:
    """Build a two-hop JOIN: primary -> mid_table -> secondary.

    Produces a GROUP BY aggregation using a dimension from the secondary table.
    """
    if con is not None:
        p_ref = resolve_table_ref(primary.name, con, models)
        m_ref = resolve_table_ref(mid_table.name, con, models)
        s_ref = resolve_table_ref(secondary.name, con, models)
    else:
        p_ref = primary.name
        m_ref = mid_table.name
        s_ref = secondary.name

    # Determine join columns for primary -> mid
    if rel_a.from_table == primary.name:
        p_join_col, m_join_col_a = rel_a.from_column, rel_a.to_column
    else:
        p_join_col, m_join_col_a = rel_a.to_column, rel_a.from_column

    # Determine join columns for secondary -> mid
    if rel_b.from_table == secondary.name:
        s_join_col, m_join_col_b = rel_b.from_column, rel_b.to_column
    else:
        s_join_col, m_join_col_b = rel_b.to_column, rel_b.from_column

    # Pick a dimension from the secondary table
    sec_dim = next(
        (c for c in secondary.columns if _shared_is_dimension(c)), None,
    )

    # Pick a metric from primary
    pri_metric = next(
        (c for c in primary.columns if _shared_is_metric(c)), None,
    )

    group_col = f's."{sec_dim.name}"' if sec_dim else f's."{s_join_col}"'
    group_label = sec_dim.name if sec_dim else s_join_col

    select_parts = [f'{group_col} AS "{group_label}"', "COUNT(*) AS total"]
    if pri_metric:
        select_parts.append(
            f'ROUND(AVG(p."{pri_metric.name}"), 2) AS avg_{pri_metric.name}'
        )

    return (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {p_ref} p "
        f'JOIN {m_ref} m ON p."{p_join_col}" = m."{m_join_col_a}" '
        f'JOIN {s_ref} s ON m."{m_join_col_b}" = s."{s_join_col}" '
        f"GROUP BY {group_col} "
        f"ORDER BY total DESC LIMIT 20"
    )


def _match_table(
    q_words: set[str],
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
) -> TableInfo | None:
    """Find the table most relevant to the question words."""
    best_table: TableInfo | None = None
    best_score = 0

    for table in discovery.tables:
        score = 0
        table_words = set(table.name.lower().replace("_", " ").split())

        # Direct table name match (strongest signal)
        for tw in table_words:
            stems = _stem(tw)
            for qw in q_words:
                qw_stems = _stem(qw)
                if stems & qw_stems:
                    score += 10
                    break

        # Domain match
        if table.domain:
            domain_words = set(table.domain.lower().split())
            score += len(q_words & domain_words) * 3

        # Description match
        if table.description:
            desc_words = set(table.description.lower().split())
            score += len(q_words & desc_words) * 2

        # Column name match (strong signal -- referencing a column implies the table)
        for col in table.columns:
            col_words = set(col.name.lower().replace("_", " ").split())
            for cw in col_words:
                if cw in q_words or any(_stem(cw) & _stem(qw) for qw in q_words):
                    score += 3
                    break  # One match per column is enough

        if score > best_score:
            best_score = score
            best_table = table

    # Require a minimum match strength
    if best_score < 5:
        return None

    return best_table


def _match_column(
    q_words: set[str],
    columns: list[ColumnInfo],
) -> ColumnInfo | None:
    """Find the column whose name best matches the question words.

    Scores each column and returns the best match. Exact word matches score
    higher than stem matches to avoid false positives like "complaint_number"
    matching "complaints" when the user asked for "county".
    """
    best_col: ColumnInfo | None = None
    best_score = 0

    for col in columns:
        col_words = set(col.name.lower().replace("_", " ").split())
        score = 0
        for cw in col_words:
            for qw in q_words:
                if cw == qw:
                    score += 10  # exact match
                elif _stem(cw) & _stem(qw):
                    score += 3   # stem match
        if score > best_score:
            best_score = score
            best_col = col

    return best_col if best_score > 0 else None


# ---------------------------------------------------------------------------
# Context building
# ---------------------------------------------------------------------------


def _build_context(
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None = None,
) -> str:
    """Build a metadata context prompt for the LLM. Never includes raw data.

    Uses SchemaGraph to enrich context with column roles and cardinality.
    Uses _resolve_table_ref so the LLM is told about tables that actually exist,
    not tables that were planned but may not have been materialized.
    """
    graph = SchemaGraph(discovery, models)
    lines: list[str] = []
    lines.append("=== Available Tables ===\n")

    for table in discovery.tables:
        if con is not None:
            ref = _resolve_table_ref(table.name, con, models)
        else:
            ref = f"staging.stg_{table.name}"
        lines.append(f"\nTable: {ref} ({table.row_count} rows)")
        if table.description:
            lines.append(f"  Description: {table.description}")

        tnode = graph.tables.get(table.name)
        for col in table.columns:
            desc = f' -- {col.description}' if col.description else ''
            sem = f' [{col.semantic_type}]' if col.semantic_type else ''
            # Add column role from schema graph for richer LLM context
            role_hint = ''
            if tnode and col.name in tnode.columns:
                cnode = tnode.columns[col.name]
                role_hint = f' (role: {cnode.role})'
                if cnode.profile and cnode.profile.distinct_count:
                    role_hint += f' cardinality={cnode.profile.distinct_count}'
            lines.append(f"  - {col.name} ({col.dtype}){sem}{role_hint}{desc}")

    # Mart tables (only executed ones)
    executed_marts = [m for m in models if m.model_type == "mart" and m.status == "executed"]
    if executed_marts:
        lines.append("\n-- Mart tables (marts schema, analytical models):")
        for mart in executed_marts:
            if con is None or _table_exists(con, "marts", mart.name):
                lines.append(f"\nTable: marts.{mart.name}")
                lines.append(f"  Description: {mart.description}")
                lines.append(f"  Source tables: {', '.join(mart.source_tables)}")
                if mart.assumptions:
                    lines.append(f"  Assumptions: {'; '.join(mart.assumptions[:2])}")

    # Relationships — use resolved refs so the LLM gets correct table names
    if discovery.relationships:
        lines.append("\n=== Relationships ===")
        for r in discovery.relationships:
            if con is not None:
                from_ref = _resolve_table_ref(r.from_table, con, models)
                to_ref = _resolve_table_ref(r.to_table, con, models)
            else:
                from_ref = f"staging.stg_{r.from_table}"
                to_ref = f"staging.stg_{r.to_table}"
            lines.append(f"  {from_ref}.{r.from_column} -> {to_ref}.{r.to_column} ({r.type})")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Grounding check -- verify question terms exist in the schema
# ---------------------------------------------------------------------------

# Words that describe analytical operations, not data entities
_ANALYTICAL_WORDS = {
    # Aggregation / metrics
    "average", "avg", "mean", "sum", "total", "count", "max", "min",
    "median", "rate", "rates", "ratio", "percent", "percentage", "pct",
    # Trends / comparison
    "trend", "trends", "trending", "trended",
    "compare", "comparison", "comparing", "compared",
    "increase", "increasing", "decrease", "decreasing",
    "change", "changes", "changed",
    # Distribution / analysis
    "distribution", "distributed", "breakdown", "break", "down",
    "across", "between", "relative", "among",
    # Time
    "over", "time", "daily", "weekly", "monthly", "yearly", "per",
    "during", "since", "before", "after", "recent", "recently",
    # Ranking / filtering
    "top", "bottom", "highest", "lowest", "most", "least", "many",
    "much", "often", "common", "commonly", "frequent", "frequently",
    "consistently", "likely", "unlikely",
    # Actions
    "show", "list", "get", "find", "display", "report",
    # Quantities
    "number", "numbers", "levels", "level",
    # Analysis types
    "correlation", "correlate", "correlated", "impact", "impacts",
    "pattern", "patterns", "associated", "association",
    # BI verbs / adjectives
    "meeting", "meet", "face", "facing", "experience", "experiencing",
    "wait", "waiting", "unresolved", "resolved",
    "fail", "failing", "failed", "pass", "passing", "passed",
    # Superlatives / qualifiers
    "worst", "best", "longest", "shortest", "largest", "smallest",
    "lower", "higher", "greater", "fewer", "worse", "better",
    "active", "inactive", "routine",
    # Domain-agnostic BI terms
    "allocated", "allocation", "driven", "operational",
    "exceed", "exceeds", "exceeding", "threshold", "thresholds",
    "capita", "per-capita",
    "discovery", "discovered", "finding", "findings",
    "sla", "slas", "kpi", "kpis",
    "unhealthy", "healthy",
    "reports", "reported", "reporting",
    # Geographic scale terms (not entity names -- those are data terms)
    "region", "regions", "area", "areas",
}


def _build_vocabulary(
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
) -> set[str]:
    """Build a set of all known terms from schema metadata.

    Includes table names, column names, descriptions, domains, semantic types,
    profile top-values, and model names -- all lowercased and split into words.
    """
    vocab: set[str] = set()

    for table in discovery.tables:
        # Table name words: "air_quality_readings" -> {"air", "quality", "readings"}
        vocab.update(table.name.lower().replace("_", " ").split())
        if table.description:
            vocab.update(table.description.lower().split())
        if table.domain:
            vocab.update(table.domain.lower().split())
        for col in table.columns:
            vocab.update(col.name.lower().replace("_", " ").split())
            if col.description:
                vocab.update(col.description.lower().split())
            if col.semantic_type:
                vocab.update(col.semantic_type.lower().replace("_", " ").split())

    # Profile top-values (known categorical values in the data)
    for profile in discovery.profiles:
        if profile.top_values:
            for val, _count in profile.top_values:
                vocab.update(val.lower().split())

    # Model names and descriptions
    for model in models:
        vocab.update(model.name.lower().replace("_", " ").split())
        if model.description:
            vocab.update(model.description.lower().split())

    # Relationship columns
    for rel in discovery.relationships:
        vocab.update(rel.from_table.lower().replace("_", " ").split())
        vocab.update(rel.to_table.lower().replace("_", " ").split())

    return vocab


def _stem(word: str) -> set[str]:
    """Generate plausible stems for a word to match against vocabulary.

    Not a full stemmer -- just handles common English suffixes that appear
    in BI questions vs schema names (e.g. "categories" -> "category",
    "communities" -> "community", "readings" -> "reading").
    """
    stems = {word}

    # -ies -> -y (categories -> category, communities -> community)
    if word.endswith("ies") and len(word) > 4:
        stems.add(word[:-3] + "y")

    # -es -> base (processes -> process, indexes -> index)
    if word.endswith("es") and len(word) > 3:
        stems.add(word[:-2])

    # -s -> base (zones -> zone, readings -> reading)
    if word.endswith("s") and not word.endswith("ss") and len(word) > 3:
        stems.add(word[:-1])

    # -ing -> base (trending -> trend, monitoring -> monitor)
    if word.endswith("ing") and len(word) > 5:
        stems.add(word[:-3])
        stems.add(word[:-3] + "e")  # monitoring -> monitore? No, but: investigating -> investigate

    # -ed -> base (hospitalized -> hospitalize, resolved -> resolve)
    if word.endswith("ed") and len(word) > 4:
        stems.add(word[:-2])
        stems.add(word[:-1])  # -ed where base ends in e (e.g. resolved -> resolve: -d)
        if word.endswith("ied"):
            stems.add(word[:-3] + "y")  # carried -> carry

    # -ly -> base (consistently -> consistent)
    if word.endswith("ly") and len(word) > 4:
        stems.add(word[:-2])

    # -ment -> base (acknowledgment -> acknowledge)
    if word.endswith("ment") and len(word) > 6:
        stems.add(word[:-4])

    return stems


def _word_in_vocab(word: str, vocab: set[str]) -> bool:
    """Check if a word or any of its stems exist in the vocabulary."""
    stems = _stem(word)
    if any(s in vocab for s in stems):
        return True

    # Also check if any vocab word starts with or contains this stem
    # (handles partial matches like "inspect" matching "inspections")
    for s in stems:
        if len(s) >= 4 and any(v.startswith(s) or s.startswith(v) for v in vocab):
            return True

    return False


def _check_grounding(
    question: str,
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
    sql: str = "",
    suggestions: list[SuggestedQuestion] | None = None,
) -> list[str]:
    """Check that the question's key terms exist in the schema vocabulary.

    Vocabulary comes from four sources:
    1. Schema metadata (table/column names, descriptions, domains, top-values)
    2. Model names and descriptions
    3. The generated SQL itself (column aliases, table references)
    4. Curated suggestion text (system-generated questions are grounded by definition)

    Returns a list of warnings. An empty list means the question is fully grounded.
    """
    vocab = _build_vocabulary(discovery, models)

    # Curated suggestion questions are grounded by definition -- every word
    # in a system-generated question is a valid domain term
    for s in suggestions or []:
        vocab.update(re.sub(r"[^a-z0-9 ]", "", s.question.lower()).split())

    # Also extract identifiers from the SQL as grounding evidence
    if sql:
        sql_words = set(re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", sql.lower()))
        for w in sql_words:
            vocab.update(w.replace("_", " ").split())

    # Extract content words from the question
    # Normalize: strip punctuation, collapse "PM2.5" -> "pm25" etc.
    raw_words = question.lower().strip().rstrip("?").split()
    q_words: set[str] = set()
    for w in raw_words:
        # Primary form: letters + digits, punctuation removed ("PM2.5" -> "pm25")
        cleaned = re.sub(r"[^a-z0-9]", "", w)
        if cleaned:
            q_words.add(cleaned)
    content_words = q_words - _STOP_WORDS - _ANALYTICAL_WORDS

    if not content_words:
        return []

    ungrounded = []
    for word in content_words:
        if _word_in_vocab(word, vocab):
            continue
        ungrounded.append(word)

    if not ungrounded:
        return []

    warnings: list[str] = []
    terms = ", ".join(f'"{w}"' for w in sorted(ungrounded))
    warnings.append(
        f"Unrecognized terms not found in your data: {terms}. "
        f"The generated query may not accurately reflect your question."
    )

    # If more than half the content words are ungrounded, strong warning
    if len(ungrounded) > len(content_words) / 2:
        warnings.append(
            "Most key terms in this question could not be matched to any "
            "table, column, or known value. Results are likely unreliable."
        )

    return warnings


# ---------------------------------------------------------------------------
# SQL generation and execution
# ---------------------------------------------------------------------------


async def _generate_sql(
    question: str,
    context: str,
    provider: LLMProvider,
) -> str | None:
    """Use the LLM to generate SQL from a natural language question."""
    prompt = f"""{context}

=== Question ===
{question}

Generate a DuckDB SELECT query that answers this question. Return ONLY the SQL, no explanation."""

    try:
        result = await provider.analyze(prompt, system=_SYSTEM_PROMPT)
        # The provider returns a dict, but for SQL we need raw text
        # Fall back to trying the provider's raw response
        if isinstance(result, dict) and "sql" in result:
            return result["sql"]
        return None
    except Exception as e:
        logger.warning("LLM SQL generation failed: %s", e)
        return None


def _is_read_only(sql: str) -> bool:
    """Validate that SQL contains only read operations."""
    stripped = sql.strip().rstrip(";").strip()
    if not stripped.upper().startswith("SELECT") and not stripped.upper().startswith("WITH"):
        return False
    return not bool(_FORBIDDEN_PATTERNS.search(sql))


def _execute_query(
    question: str,
    sql: str,
    con: duckdb.DuckDBPyConnection,
) -> ExplorationResult:
    """Execute a validated SQL query and return structured results."""
    try:
        result = con.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        data: list[dict[str, Any]] = []
        for row in rows[:500]:  # Cap at 500 rows
            data.append(dict(zip(columns, [_serialize_value(v) for v in row], strict=False)))

        viz = recommend_visualization(columns, data, question)

        return ExplorationResult(
            question=question,
            sql=sql,
            data=data,
            row_count=len(rows),
            visualization=viz,
        )
    except Exception as e:
        return ExplorationResult(
            question=question,
            sql=sql,
            error=f"Query execution failed: {e}",
        )


def _serialize_value(val: Any) -> Any:
    """Convert a value to a JSON-serializable type."""
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)
