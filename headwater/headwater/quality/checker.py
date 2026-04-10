"""Quality checker -- evaluates contract rules against materialized tables."""

from __future__ import annotations

import logging
import re

import duckdb

from headwater.core.exceptions import ContractExpressionError
from headwater.core.models import ContractCheckResult, ContractRule

logger = logging.getLogger(__name__)


def check_contract(
    con: duckdb.DuckDBPyConnection,
    rule: ContractRule,
) -> ContractCheckResult:
    """Evaluate a single contract rule against its target table."""
    try:
        if rule.rule_type == "not_null":
            return _check_not_null(con, rule)
        elif rule.rule_type == "unique":
            return _check_unique(con, rule)
        elif rule.rule_type == "range":
            return _check_range(con, rule)
        elif rule.rule_type == "cardinality":
            return _check_cardinality(con, rule)
        elif rule.rule_type == "row_count":
            return _check_row_count(con, rule)
        else:
            return _check_generic(con, rule)
    except Exception as e:
        logger.warning("Contract check failed for %s.%s: %s", rule.model_name, rule.column_name, e)
        return ContractCheckResult(
            rule_id=rule.id or "",
            model_name=rule.model_name,
            passed=False,
            message=f"Check error: {e}",
        )


def check_contracts(
    con: duckdb.DuckDBPyConnection,
    rules: list[ContractRule],
    *,
    only_active: bool = True,
) -> list[ContractCheckResult]:
    """Evaluate all contract rules. By default only checks observing/enforced rules."""
    results: list[ContractCheckResult] = []
    for rule in rules:
        if only_active and rule.status not in ("observing", "enforced"):
            continue
        results.append(check_contract(con, rule))
    return results


def _check_not_null(
    con: duckdb.DuckDBPyConnection,
    rule: ContractRule,
) -> ContractCheckResult:
    """Check that a column has no null values."""
    sql = (
        f"SELECT COUNT(*) FROM {rule.model_name} "
        f'WHERE "{rule.column_name}" IS NULL'
    )
    null_count = con.execute(sql).fetchone()[0]
    return ContractCheckResult(
        rule_id=rule.id or "",
        model_name=rule.model_name,
        passed=null_count == 0,
        observed_value=null_count,
        message=f"{null_count} null values found" if null_count > 0 else "No nulls",
    )


def _check_unique(
    con: duckdb.DuckDBPyConnection,
    rule: ContractRule,
) -> ContractCheckResult:
    """Check that a column has all unique values."""
    sql = (
        f"SELECT COUNT(*) - COUNT(DISTINCT \"{rule.column_name}\") "
        f"FROM {rule.model_name}"
    )
    duplicate_count = con.execute(sql).fetchone()[0]
    return ContractCheckResult(
        rule_id=rule.id or "",
        model_name=rule.model_name,
        passed=duplicate_count == 0,
        observed_value=duplicate_count,
        message=(
            f"{duplicate_count} duplicate values" if duplicate_count > 0
            else "All values unique"
        ),
    )


def _check_range(
    con: duckdb.DuckDBPyConnection,
    rule: ContractRule,
) -> ContractCheckResult:
    """Check that all values fall within the expected range."""
    sql = (
        f"SELECT COUNT(*) FROM {rule.model_name} "
        f"WHERE NOT ({rule.expression})"
    )
    out_of_range = con.execute(sql).fetchone()[0]
    return ContractCheckResult(
        rule_id=rule.id or "",
        model_name=rule.model_name,
        passed=out_of_range == 0,
        observed_value=out_of_range,
        message=(
            f"{out_of_range} values out of range" if out_of_range > 0
            else "All values in range"
        ),
    )


def _check_cardinality(
    con: duckdb.DuckDBPyConnection,
    rule: ContractRule,
) -> ContractCheckResult:
    """Check that all values are in the allowed set."""
    sql = (
        f"SELECT COUNT(*) FROM {rule.model_name} "
        f'WHERE "{rule.column_name}" IS NOT NULL AND NOT ({rule.expression})'
    )
    unexpected = con.execute(sql).fetchone()[0]
    return ContractCheckResult(
        rule_id=rule.id or "",
        model_name=rule.model_name,
        passed=unexpected == 0,
        observed_value=unexpected,
        message=(
            f"{unexpected} unexpected values" if unexpected > 0
            else "All values in allowed set"
        ),
    )


def _check_row_count(
    con: duckdb.DuckDBPyConnection,
    rule: ContractRule,
) -> ContractCheckResult:
    """Check that the table has at least the expected row count."""
    sql = f"SELECT COUNT(*) FROM {rule.model_name}"
    row_count = con.execute(sql).fetchone()[0]
    passed = _evaluate_count_expression(rule.expression, row_count)
    return ContractCheckResult(
        rule_id=rule.id or "",
        model_name=rule.model_name,
        passed=passed,
        observed_value=row_count,
        message=f"Row count: {row_count}",
    )


# Supported: COUNT(*) >= N, <= N, > N, < N, == N, BETWEEN N AND M
_COUNT_EXPR_PATTERNS = [
    (re.compile(r"^COUNT\(\*\)\s*>=\s*(\d+)$"), "gte"),
    (re.compile(r"^COUNT\(\*\)\s*<=\s*(\d+)$"), "lte"),
    (re.compile(r"^COUNT\(\*\)\s*>\s*(\d+)$"), "gt"),
    (re.compile(r"^COUNT\(\*\)\s*<\s*(\d+)$"), "lt"),
    (re.compile(r"^COUNT\(\*\)\s*==\s*(\d+)$"), "eq"),
    (re.compile(r"^COUNT\(\*\)\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)$", re.IGNORECASE), "between"),
]


def _evaluate_count_expression(expression: str, row_count: int) -> bool:
    """Safely evaluate a COUNT(*) expression without using eval().

    Supported formats:
      COUNT(*) >= N
      COUNT(*) <= N
      COUNT(*) > N
      COUNT(*) < N
      COUNT(*) == N
      COUNT(*) BETWEEN N AND M

    Raises ContractExpressionError for unsupported formats.
    """
    expr = expression.strip()
    for pattern, op in _COUNT_EXPR_PATTERNS:
        m = pattern.match(expr)
        if m:
            if op == "between":
                lo, hi = int(m.group(1)), int(m.group(2))
                return lo <= row_count <= hi
            n = int(m.group(1))
            if op == "gte":
                return row_count >= n
            if op == "lte":
                return row_count <= n
            if op == "gt":
                return row_count > n
            if op == "lt":
                return row_count < n
            if op == "eq":
                return row_count == n
    raise ContractExpressionError(
        f"Unsupported row_count expression: {expression!r}. "
        "Supported: COUNT(*) >=/<=/>/</== N, COUNT(*) BETWEEN N AND M"
    )


def _check_generic(
    con: duckdb.DuckDBPyConnection,
    rule: ContractRule,
) -> ContractCheckResult:
    """Fallback: evaluate the expression as a WHERE clause."""
    sql = (
        f"SELECT COUNT(*) FROM {rule.model_name} "
        f"WHERE NOT ({rule.expression})"
    )
    violations = con.execute(sql).fetchone()[0]
    return ContractCheckResult(
        rule_id=rule.id or "",
        model_name=rule.model_name,
        passed=violations == 0,
        observed_value=violations,
        message=f"{violations} violations" if violations > 0 else "All checks passed",
    )
