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
from headwater.core.models import (
    ColumnInfo,
    DiscoveryResult,
    ExplorationResult,
    GeneratedModel,
    SuggestedQuestion,
    TableInfo,
)
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
) -> ExplorationResult:
    """Translate a natural language question to SQL, execute it, and return results.

    Strategy:
    1. Check if the question matches a suggested question with a pre-built SQL hint.
    2. If LLM is available, generate SQL from metadata context.
    3. Validate the SQL is read-only.
    4. Execute against DuckDB.
    5. If execution fails and LLM is available, auto-repair the query (up to 3 attempts).
    6. Return results with visualization recommendation.
    """
    has_llm = provider is not None and not isinstance(provider, NoLLMProvider)
    context = _build_context(discovery, models or []) if has_llm else ""

    # Try matching a suggested question first
    sql = _match_suggestion(question, suggestions or [])

    # If no match, try heuristic SQL generation from metadata
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
# Heuristic SQL builder -- constructs queries from metadata, no LLM needed
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

    # Step 2: Bail out if the question references other tables
    # (implies a JOIN we can't reliably construct)
    if _references_other_tables(content_words, table, discovery):
        return None

    # Classify columns by role
    temporal_cols = [
        c for c in table.columns
        if c.dtype in ("timestamp", "date")
        or c.semantic_type == "temporal"
        or re.search(r"(date|time|month|year|day|week|quarter|_at$)", c.name, re.IGNORECASE)
    ]
    metric_cols = [
        c for c in table.columns
        if c.dtype in ("int64", "float64")
        and c.semantic_type not in ("id", "foreign_key", "dimension", "temporal")
        and not c.name.endswith("_id")
        and not c.is_primary_key
        and not re.search(r"(code$|flag$|indicator$|_key$|_fk$|_pk$)", c.name, re.IGNORECASE)
    ]
    dimension_cols = [
        c for c in table.columns
        if c.dtype == "varchar"
        and c.semantic_type not in ("id", "foreign_key")
        and not c.name.endswith("_id")
        and not c.is_primary_key
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

    # Step 3: Detect the question intent and build SQL
    is_trend = bool(q_words & _TREND_WORDS) and temporal_cols
    is_count = bool(q_words & _COUNT_WORDS)
    is_top = bool(q_words & _TOP_WORDS)
    has_breakdown_word = bool(q_words & _BREAKDOWN_WORDS)

    # Try to find a specific dimension mentioned in the question
    target_dim = _match_column(q_words, dimension_cols)
    target_metric = _match_column(q_words, metric_cols)

    # -- Trend query: metric over time, optionally grouped by a dimension
    if is_trend and temporal_cols:
        time_col = temporal_cols[0]
        metric = target_metric or (metric_cols[0] if metric_cols else None)
        dim = target_dim

        time_expr = f'CAST("{time_col.name}" AS DATE)'
        if time_col.dtype == "timestamp":
            time_expr = f"DATE_TRUNC('month', \"{time_col.name}\")"

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


def _references_other_tables(
    content_words: set[str],
    primary_table: TableInfo,
    discovery: DiscoveryResult,
) -> bool:
    """Check if the question references entities from tables other than the primary.

    If someone asks about "inspections by neighborhood", "neighborhood" maps to
    the zones table -- that requires a JOIN we can't reliably build. Return True
    to signal the heuristic should bail out.
    """
    primary_vocab: set[str] = set()
    # Build vocabulary for the primary table (name + columns + domain + description)
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
            # Only include meaningful column name parts (skip "id", "name", etc.)
            other_words.update(
                w for w in col_parts
                if w not in ("id", "name", "type", "status", "date")
            )

        # Remove words that overlap with the primary table's vocabulary
        unique_other = other_words - primary_vocab

        # Check if any content word from the question stems to a unique other-table word
        for qw in content_words:
            qw_stems = _stem(qw)
            for ow in unique_other:
                if _stem(ow) & qw_stems:
                    return True

    return False


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
    """Find the column whose name best matches the question words."""
    for col in columns:
        col_words = set(col.name.lower().replace("_", " ").split())
        for cw in col_words:
            cw_stems = _stem(cw)
            for qw in q_words:
                if _stem(qw) & cw_stems:
                    return col
    return None


# ---------------------------------------------------------------------------
# Context building
# ---------------------------------------------------------------------------


def _build_context(
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
    con: duckdb.DuckDBPyConnection | None = None,
) -> str:
    """Build a metadata context prompt for the LLM. Never includes raw data.

    Uses _resolve_table_ref so the LLM is told about tables that actually exist,
    not tables that were planned but may not have been materialized.
    """
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
        for col in table.columns:
            desc = f' -- {col.description}' if col.description else ''
            sem = f' [{col.semantic_type}]' if col.semantic_type else ''
            lines.append(f"  - {col.name} ({col.dtype}){sem}{desc}")

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
