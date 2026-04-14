"""Shared column classification constants and helpers.

Every module that decides whether a column is a metric, dimension, or should
be excluded from aggregation imports from here -- ensuring consistent behavior
across suggestions, NL-to-SQL, mart generation, and heuristic enrichment.
"""

from __future__ import annotations

import re

from headwater.core.models import ColumnInfo, ColumnProfile

# -- Semantic types that are NEVER metrics -----------------------------------
NON_METRIC_SEMANTIC_TYPES: frozenset[str] = frozenset(
    {
        "id",
        "foreign_key",
        "primary_key",
        "dimension",
        "temporal",
        "geographic",
        "pii",
        "text",
    }
)

# -- Semantic types that are explicitly metrics ------------------------------
METRIC_SEMANTIC_TYPES: frozenset[str] = frozenset(
    {
        "metric",
        "measure",
        "kpi",
    }
)

# -- Numeric dtype substrings ------------------------------------------------
NUMERIC_DTYPES: tuple[str, ...] = (
    "int",
    "float",
    "double",
    "decimal",
    "numeric",
    "real",
    "bigint",
    "hugeint",
)

# -- Name patterns for IDs/codes (never useful as metrics) -------------------
ID_NAME_RE: re.Pattern = re.compile(
    r"(_id$|_key$|_fk$|_pk$|^id$|^key$|^uuid$)",
    re.IGNORECASE,
)

# -- Name patterns for non-metric numeric columns ---------------------------
NON_METRIC_NAME_RE: re.Pattern = re.compile(
    r"(code$|flag$|indicator$|^lat$|^lon$|latitude|longitude|_number$|^number$)",
    re.IGNORECASE,
)


def is_metric_column(
    col: ColumnInfo,
    profile: ColumnProfile | None = None,
) -> bool:
    """Return True if a column is a plausible numeric metric for aggregation.

    Centralizes the logic used by suggestions.py, nl_to_sql.py, and marts.py.
    """
    if col.is_primary_key or col.name.endswith("_id"):
        return False
    if ID_NAME_RE.search(col.name):
        return False
    if NON_METRIC_NAME_RE.search(col.name):
        return False
    if col.semantic_type in NON_METRIC_SEMANTIC_TYPES:
        return False
    # Regardless of semantic type, a metric must have a numeric dtype.
    # A VARCHAR column tagged "metric" by a name pattern (e.g. units_of_measure)
    # cannot be aggregated with AVG/SUM.
    dtype = (profile.dtype if profile else col.dtype).lower()
    is_numeric = any(t in dtype for t in NUMERIC_DTYPES)
    if not is_numeric:
        return False
    if col.semantic_type in METRIC_SEMANTIC_TYPES:
        return True
    return True  # numeric, not excluded by any rule above


def is_dimension_column(
    col: ColumnInfo,
    profile: ColumnProfile | None = None,
    max_cardinality: int = 200,
) -> bool:
    """Return True if a column is suitable for GROUP BY.

    Includes:
    - varchar/text columns with reasonable cardinality
    - numeric columns with semantic_type == "dimension"
    - numeric columns with low cardinality (from profile)

    Excludes IDs, foreign keys, geographic, and high-cardinality columns.
    """
    if col.is_primary_key or col.name.endswith("_id"):
        return False
    if ID_NAME_RE.search(col.name):
        return False
    if col.semantic_type in ("id", "foreign_key", "primary_key"):
        return False
    if col.semantic_type == "geographic":
        return False

    dtype = (profile.dtype if profile else col.dtype).lower()
    is_text = "varchar" in dtype or "char" in dtype or "text" in dtype
    is_numeric = any(t in dtype for t in NUMERIC_DTYPES)

    # Explicit dimension semantic type -- trust it regardless of dtype
    if col.semantic_type == "dimension":
        return not (profile is not None and profile.distinct_count > max_cardinality)

    # Text columns with reasonable cardinality
    if is_text:
        return not (profile is not None and profile.distinct_count > max_cardinality)

    # Numeric column with low cardinality from profile -> dimension
    if is_numeric and profile is not None and profile.distinct_count <= max_cardinality:
        return col.semantic_type not in METRIC_SEMANTIC_TYPES

    return False
