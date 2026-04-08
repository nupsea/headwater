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
    DiscoveryResult,
    ExplorationResult,
    GeneratedModel,
    SuggestedQuestion,
)
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

    # If no match and LLM is available, generate SQL
    if sql is None and has_llm:
        sql = asyncio.run(_generate_sql(question, context, provider))

    if sql is None:
        return ExplorationResult(
            question=question,
            sql="",
            error=(
                "Could not generate SQL for this question. "
                "Try selecting one of the suggested questions, or enable LLM mode."
            ),
        )

    # Validate read-only
    if not _is_read_only(sql):
        return ExplorationResult(
            question=question,
            sql=sql,
            error="Generated SQL contains write operations and was blocked for safety.",
        )

    # Execute (with auto-repair if LLM is available)
    result = _execute_query(question, sql, con)

    if result.error and has_llm:
        result = _repair_loop(question, sql, result.error, con, context, provider)

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
}


def _questions_similar(a: str, b: str) -> bool:
    """Check if two questions are similar enough to match."""
    # Exact match
    if a == b:
        return True
    # One contains the other
    if a in b or b in a:
        return True
    # Compare content words (exclude stop words) with >60% overlap
    words_a = set(a.split()) - _STOP_WORDS
    words_b = set(b.split()) - _STOP_WORDS
    if not words_a or not words_b:
        return False
    overlap = len(words_a & words_b)
    return overlap / max(len(words_a), len(words_b)) > 0.6


# ---------------------------------------------------------------------------
# Context building
# ---------------------------------------------------------------------------


def _build_context(
    discovery: DiscoveryResult,
    models: list[GeneratedModel],
) -> str:
    """Build a metadata context prompt for the LLM. Never includes raw data."""
    lines: list[str] = []

    # Staging tables
    lines.append("=== Available Tables ===\n")
    lines.append("-- Staging tables (staging schema, cleaned source data):")
    for table in discovery.tables:
        lines.append(f"\nTable: staging.stg_{table.name} ({table.row_count} rows)")
        if table.description:
            lines.append(f"  Description: {table.description}")
        for col in table.columns:
            desc = f' -- {col.description}' if col.description else ''
            sem = f' [{col.semantic_type}]' if col.semantic_type else ''
            lines.append(f"  - {col.name} ({col.dtype}){sem}{desc}")

    # Mart tables
    executed_marts = [m for m in models if m.model_type == "mart" and m.status == "executed"]
    if executed_marts:
        lines.append("\n-- Mart tables (marts schema, analytical models):")
        for mart in executed_marts:
            lines.append(f"\nTable: marts.{mart.name}")
            lines.append(f"  Description: {mart.description}")
            lines.append(f"  Source tables: {', '.join(mart.source_tables)}")
            if mart.assumptions:
                lines.append(f"  Assumptions: {'; '.join(mart.assumptions[:2])}")

    # Relationships
    if discovery.relationships:
        lines.append("\n=== Relationships ===")
        for r in discovery.relationships:
            lines.append(
                f"  staging.stg_{r.from_table}.{r.from_column} -> "
                f"staging.stg_{r.to_table}.{r.to_column} ({r.type})"
            )

    return "\n".join(lines)


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
