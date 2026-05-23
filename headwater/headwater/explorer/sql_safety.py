"""SQL safety guardrails for natural-language exploration."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class SqlSafetyResult:
    allowed: bool
    reason: str | None = None


_FORBIDDEN_PATTERNS = re.compile(
    r"\b("
    r"INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|GRANT|REVOKE|EXEC|"
    r"CALL|COPY|EXPORT|IMPORT|ATTACH|DETACH|INSTALL|LOAD|PRAGMA|SET|VACUUM"
    r")\b",
    re.IGNORECASE,
)
_LEADING_KEYWORD_RE = re.compile(r"^\s*([a-zA-Z]+)\b")


def validate_explore_sql(sql: str) -> SqlSafetyResult:
    """Allow one read-only analytical SELECT statement for exploration."""
    stripped = _strip_sql_comments(sql).strip()
    if not stripped:
        return SqlSafetyResult(False, "SQL is empty.")

    if _has_multiple_statements(stripped):
        return SqlSafetyResult(False, "Only one SQL statement is allowed.")

    keyword = _leading_keyword(stripped)
    if keyword not in {"select", "with"}:
        return SqlSafetyResult(False, "Only SELECT and WITH queries are allowed.")

    if _FORBIDDEN_PATTERNS.search(stripped):
        return SqlSafetyResult(False, "SQL contains a blocked operation.")

    return SqlSafetyResult(True)


def _strip_sql_comments(sql: str) -> str:
    without_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n\r]*", " ", without_block)


def _has_multiple_statements(sql: str) -> bool:
    trimmed = sql.strip()
    if trimmed.endswith(";"):
        trimmed = trimmed[:-1]
    return ";" in trimmed


def _leading_keyword(sql: str) -> str | None:
    match = _LEADING_KEYWORD_RE.match(sql)
    return match.group(1).lower() if match else None
