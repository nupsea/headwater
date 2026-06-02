"""Shared, domain-agnostic vocabulary for classifying columns.

Single source of truth for what counts as a numeric/temporal dtype and a measure
role, plus small helpers over a question's declared column roles.  Centralised so
a stakeholder-driven change — a new numeric dtype, or treating a new role as a
measure — is a one-line edit here rather than a hunt across ``h2_answer``,
``h2_resolve``, ``h2_semantics`` and ``h2_project_*``.

Contains NO dataset-specific values — only generic dtype and role names.
"""

from __future__ import annotations

from typing import Any

# Dtypes usable directly as a numeric measure.
NUMERIC_DTYPES = frozenset(
    {
        "int", "int8", "int16", "int32", "int64", "integer", "bigint", "smallint",
        "float", "float32", "float64", "double", "decimal", "numeric", "real",
    }
)

# Dtypes that represent a point in time.
TIMESTAMP_DTYPES = frozenset(
    {
        "timestamp", "date", "datetime", "timestamptz",
        "timestamp with time zone", "timestamp without time zone",
    }
)

# Semantic roles that mark a column as an aggregatable measure.
MEASURE_ROLES = frozenset({"measure", "duration", "quantity", "metric", "amount"})


def is_numeric_dtype(dtype: str | None) -> bool:
    """True when ``dtype`` can be aggregated as a number without a derivation."""
    return bool(dtype) and dtype.strip().lower() in NUMERIC_DTYPES


def is_measure_role(role: str | None) -> bool:
    """True when ``role`` marks an aggregatable measure column."""
    return bool(role) and role.strip().lower() in MEASURE_ROLES


def measure_column_ref(question: dict[str, Any] | None) -> str | None:
    """The ``table.column`` a question measures, from its declared column roles.

    Returns the first column whose declared role is a measure, or ``None``.
    """
    if not question:
        return None
    roles = (question.get("question") or {}).get("col_roles") or {}
    for ref, role in roles.items():
        if is_measure_role(str(role)):
            return ref
    return None
