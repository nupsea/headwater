"""PK/FK auto-detection from data profiling results."""

from __future__ import annotations

import re

from pydantic import BaseModel


class PKCandidate(BaseModel):
    """A primary key candidate with confidence scoring."""

    table: str
    column: str
    uniqueness_ratio: float
    null_rate: float
    confidence: float
    reasons: list[str]


class FKCandidate(BaseModel):
    """A foreign key candidate with confidence scoring."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    value_overlap: float
    confidence: float
    reasons: list[str]


_ID_SUFFIX = re.compile(r"(?:_id|_key|_code)$", re.IGNORECASE)
_ID_EXACT = re.compile(r"^id$", re.IGNORECASE)
_INTEGER_TYPES = {"INTEGER", "BIGINT", "INT", "SMALLINT", "INT64", "INT32"}
_KEY_SUFFIXES = ("_id", "_key", "_code")


def _get_stat(stats: dict, key: str, default=None):
    """Safely get a stat value, handling both dict and nested access."""
    if isinstance(stats, dict):
        return stats.get(key, default)
    return default


def _strip_key_suffix(column_name: str) -> str | None:
    """Return the likely entity stem for key-like column names."""
    lower = column_name.lower()
    for suffix in _KEY_SUFFIXES:
        if lower.endswith(suffix):
            return column_name[: -len(suffix)]
    return None


def suggest_primary_keys(
    table_name: str,
    profiles: list[dict],
) -> list[PKCandidate]:
    """Detect PK candidates from column profiles.

    Each profile dict has: column_name, dtype, stats (dict with
    null_count, distinct_count, row_count, min, max, etc.)
    """
    candidates: list[PKCandidate] = []

    for prof in profiles:
        col_name = prof["column_name"]
        dtype = prof.get("dtype", "").upper()
        stats = prof.get("stats", {})
        if not isinstance(stats, dict):
            continue

        row_count = _get_stat(stats, "row_count", 0)
        if not row_count or row_count == 0:
            continue

        distinct_count = _get_stat(stats, "distinct_count", 0)
        null_count = _get_stat(stats, "null_count", 0)

        uniqueness = distinct_count / row_count if row_count else 0.0
        null_rate = null_count / row_count if row_count else 0.0

        if uniqueness < 0.95 or null_rate > 0.05:
            continue

        confidence = 0.0
        reasons: list[str] = []

        # Uniqueness scoring
        if uniqueness == 1.0:
            confidence += 0.5
            reasons.append("100% unique values")
        else:
            confidence += 0.3
            reasons.append(f"{uniqueness:.1%} unique values")

        # Null rate scoring
        if null_rate == 0.0:
            confidence += 0.2
            reasons.append("No null values")
        elif null_rate < 0.01:
            confidence += 0.1
            reasons.append(f"Low null rate ({null_rate:.2%})")

        # Name pattern scoring
        if _ID_EXACT.match(col_name):
            confidence += 0.2
            reasons.append("Column named 'id'")
        elif _ID_SUFFIX.search(col_name):
            confidence += 0.15
            reasons.append("Name matches key pattern (_id/_key/_code)")

        # Type scoring
        if dtype in _INTEGER_TYPES:
            confidence += 0.05
            reasons.append("Integer type")

        confidence = min(confidence, 1.0)

        candidates.append(
            PKCandidate(
                table=table_name,
                column=col_name,
                uniqueness_ratio=uniqueness,
                null_rate=null_rate,
                confidence=round(confidence, 3),
                reasons=reasons,
            )
        )

    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates


def suggest_foreign_keys(
    tables_profiles: dict[str, list[dict]],
    pk_columns: dict[str, list[str]] | None = None,
) -> list[FKCandidate]:
    """Detect FK candidates by name matching and cardinality analysis.

    tables_profiles: table_name -> list of column profiles
    pk_columns: table_name -> known PK column names (optional)
    """
    candidates: list[FKCandidate] = []
    table_names = set(tables_profiles.keys())
    pk_columns = pk_columns or {}

    # Build lookup: table_name -> set of column names
    table_cols: dict[str, set[str]] = {}
    for tbl, profs in tables_profiles.items():
        table_cols[tbl] = {p["column_name"] for p in profs}

    # Build lookup for cardinality
    col_stats: dict[tuple[str, str], dict] = {}
    for tbl, profs in tables_profiles.items():
        for p in profs:
            col_stats[(tbl, p["column_name"])] = p.get("stats", {})

    for from_table, profs in tables_profiles.items():
        for prof in profs:
            from_col = prof["column_name"]

            # Only consider columns with _id suffix as FK candidates
            if not _ID_SUFFIX.search(from_col) and not _ID_EXACT.match(from_col):
                continue

            # Skip if this is already a known PK of its own table
            if from_col in pk_columns.get(from_table, []):
                continue

            # Try to match to a target table
            matches = _find_fk_targets(from_table, from_col, table_names, table_cols, pk_columns)

            for to_table, to_col in matches:
                confidence = 0.0
                reasons: list[str] = []

                # Name match confidence
                confidence += 0.5
                reasons.append(f"Name match: {from_table}.{from_col} -> {to_table}.{to_col}")

                # Known PK boost
                if to_col in pk_columns.get(to_table, []):
                    confidence += 0.15
                    reasons.append("Target is a confirmed PK")

                # Cardinality analysis
                from_stats = col_stats.get((from_table, from_col), {})
                to_stats = col_stats.get((to_table, to_col), {})
                from_distinct = _get_stat(from_stats, "distinct_count", 0)
                to_distinct = _get_stat(to_stats, "distinct_count", 0)
                from_rows = _get_stat(from_stats, "row_count", 0)

                if from_rows and from_distinct and to_distinct:
                    # FK should have fewer distinct values than rows (many-to-one)
                    if from_distinct < from_rows:
                        confidence += 0.1
                        reasons.append("Many-to-one cardinality pattern")

                    # FK distinct values should be <= target distinct values
                    if to_distinct > 0 and from_distinct <= to_distinct:
                        confidence += 0.1
                        reasons.append(f"Cardinality compatible ({from_distinct} <= {to_distinct})")

                # Value overlap estimation (heuristic based on cardinality)
                value_overlap = 0.0
                if to_distinct and from_distinct:
                    value_overlap = min(from_distinct / to_distinct, 1.0)

                confidence = min(confidence, 1.0)

                candidates.append(
                    FKCandidate(
                        from_table=from_table,
                        from_column=from_col,
                        to_table=to_table,
                        to_column=to_col,
                        value_overlap=round(value_overlap, 3),
                        confidence=round(confidence, 3),
                        reasons=reasons,
                    )
                )

    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates


def _find_fk_targets(
    from_table: str,
    from_col: str,
    table_names: set[str],
    table_cols: dict[str, set[str]],
    pk_columns: dict[str, list[str]],
) -> list[tuple[str, str]]:
    """Find candidate target (table, column) pairs for a FK column."""
    matches: list[tuple[str, str]] = []

    # Strip key suffix to get potential table name.
    base_name = _strip_key_suffix(from_col)
    if not base_name:
        return matches

    # Try exact table name match and plural forms
    candidate_tables = set()
    for tbl in table_names:
        if tbl == from_table:
            continue
        tbl_lower = tbl.lower()
        base_lower = base_name.lower()
        if tbl_lower in (base_lower, base_lower + "s", base_lower + "es"):
            candidate_tables.add(tbl)

    for target_table in candidate_tables:
        cols = table_cols.get(target_table, set())
        # Prefer matching against known PKs
        target_pks = pk_columns.get(target_table, [])
        if target_pks:
            for pk in target_pks:
                matches.append((target_table, pk))
        elif from_col in cols:
            matches.append((target_table, from_col))
        elif "id" in cols:
            matches.append((target_table, "id"))

    return matches


class CompositePKCandidate(BaseModel):
    """A composite primary key candidate verified against actual data."""

    table: str
    columns: list[str]
    uniqueness_ratio: float
    null_rate: float
    confidence: float
    reasons: list[str]


# Columns likely to participate in composite keys: identifiers, dimensions,
# temporal, foreign keys -- NOT metrics, text, or geographic.
_COMPOSITE_KEY_ROLES = {"dimension", "identifier", "temporal", "id", "foreign_key"}

# Semantic types that suggest a column is a key participant
_COMPOSITE_KEY_SEMANTIC_TYPES = {
    "dimension", "temporal", "id", "foreign_key", "primary_key",
}

# Name patterns that suggest key participation (generic, not dataset-specific)
_KEY_PARTICIPANT_RE = re.compile(
    r"(_id$|^id$|_key$|_code$|_type$|^year$|^month$|^date$|"
    r"^name$|_name$|^state$|^county$|^country$|^region$|^city$|"
    r"^category$|^department$|^zone$|^district$|^class$|^group$)",
    re.IGNORECASE,
)
_MEASURE_NAME_RE = re.compile(
    r"(days?$|_days?$|^count$|_count$|count_|total|amount|score|rate|ratio|percent|percentile|"
    r"median|mean|avg|average|min_|max_|_min$|_max$|aqi|good|moderate|"
    r"unhealthy|hazardous)",
    re.IGNORECASE,
)
_NATURAL_KEY_NAME_RE = re.compile(
    r"(^state$|^county$|^country$|^region$|^city$|^district$|^year$|^month$|"
    r"^date$|_code$|_key$|_id$|^id$)",
    re.IGNORECASE,
)


def detect_composite_keys(
    con,
    table_name: str,
    schema_name: str,
    profiles: list[dict],
    columns: list | None = None,
    max_key_width: int = 4,
) -> list[CompositePKCandidate]:
    """Detect composite PK candidates by testing column combinations against data.

    Strategy:
    1. Identify candidate columns: low-to-moderate cardinality, non-null,
       dimension/identifier/temporal role, or key-like name patterns.
    2. Try combinations of 2..max_key_width columns.
    3. Verify uniqueness via SQL: COUNT(DISTINCT (cols)) == COUNT(*).
    4. Return verified candidates sorted by confidence.

    Args:
        con: DuckDB connection for SQL verification.
        table_name: Table to analyze.
        schema_name: Schema containing the table.
        profiles: Column profile dicts (column_name, dtype, stats).
        columns: Optional list of ColumnInfo objects for role/semantic_type hints.
        max_key_width: Maximum number of columns in a composite key (default 4).
    """
    if not profiles:
        return []

    # Get row count
    row_count = 0
    for prof in profiles:
        stats = prof.get("stats", {})
        if isinstance(stats, dict):
            rc = _get_stat(stats, "row_count", 0)
            if rc and rc > row_count:
                row_count = rc
    if row_count == 0:
        return []

    # Build column info index if provided
    col_info_map = {}
    if columns:
        for c in columns:
            col_info_map[c.name] = c
    profile_order = {prof["column_name"]: i for i, prof in enumerate(profiles)}

    # Step 1: Identify candidate columns for composite key participation
    key_candidates: list[tuple[str, int, float]] = []  # (col_name, distinct, uniqueness)
    for prof in profiles:
        col_name = prof["column_name"]
        stats = prof.get("stats", {})
        if not isinstance(stats, dict):
            continue

        distinct = _get_stat(stats, "distinct_count", 0)
        null_count = _get_stat(stats, "null_count", 0)
        null_rate = null_count / row_count if row_count else 0
        uniqueness = distinct / row_count if row_count else 0

        # Skip columns with high nulls -- PKs must be non-null
        if null_rate > 0.01:
            continue

        # Skip columns that are already 95%+ unique (single-column PK candidates)
        if uniqueness >= 0.95:
            continue

        # Skip columns with very low cardinality (< 2 distinct values = constant)
        if distinct < 2:
            continue

        # Determine if this column should participate in composite key detection
        is_candidate = False

        # Check role/semantic_type from enrichment
        col = col_info_map.get(col_name)
        if (
            (col and (col.role == "metric" or col.semantic_type == "metric"))
            or _MEASURE_NAME_RE.search(col_name)
        ):
            continue

        if col:
            if col.role in _COMPOSITE_KEY_ROLES:
                is_candidate = True
            if col.semantic_type in _COMPOSITE_KEY_SEMANTIC_TYPES:
                is_candidate = True

        # Check name pattern (generic patterns, not dataset-specific)
        if _KEY_PARTICIPANT_RE.search(col_name):
            is_candidate = True

        # Low-cardinality non-metric columns are natural key candidates
        # (e.g. 50 states, 100 counties, 20 years)
        if distinct <= 500 and uniqueness < 0.5:
            dtype = prof.get("dtype", "").lower()
            is_text = "varchar" in dtype or "char" in dtype or "text" in dtype
            is_id_like = _ID_SUFFIX.search(col_name) or _ID_EXACT.match(col_name)
            if is_text or is_id_like:
                is_candidate = True
            # Integer columns with low cardinality that aren't metrics
            if col and col.role != "metric" and col.semantic_type != "metric":
                is_candidate = True

        if is_candidate:
            key_candidates.append((col_name, distinct, uniqueness))

    if len(key_candidates) < 2:
        return []

    # Sort by cardinality descending -- higher cardinality columns contribute more
    # to uniqueness, so try them first
    key_candidates.sort(key=lambda x: x[1], reverse=True)

    # Limit to top 8 candidates to avoid combinatorial explosion
    key_candidates = key_candidates[:8]

    # Step 2: Try combinations, starting with smallest (2 columns)
    from itertools import combinations

    qualified = f'"{schema_name}"."{table_name}"'
    candidates: list[CompositePKCandidate] = []

    for width in range(2, min(max_key_width + 1, len(key_candidates) + 1)):
        for combo in combinations(key_candidates, width):
            col_names = sorted((c[0] for c in combo), key=lambda c: profile_order.get(c, 0))

            # Heuristic pre-check: product of distinct counts should be >= row_count
            # This avoids expensive SQL for clearly non-unique combinations
            product = 1
            for _, dist, _ in combo:
                product *= dist
                if product >= row_count:
                    break
            if product < row_count:
                continue

            # Step 3: SQL verification
            try:
                cols_sql = ", ".join(f'"{c}"' for c in col_names)
                sql = (
                    f"SELECT COUNT(*) AS total, "
                    f"COUNT(DISTINCT ({cols_sql})) AS distinct_count "
                    f"FROM {qualified}"
                )
                result = con.execute(sql).fetchone()
                total, distinct_combo = result[0], result[1]
                if total == 0:
                    continue

                combo_uniqueness = distinct_combo / total
                if combo_uniqueness < 0.99:
                    continue

                # This combination is (nearly) unique -- it's a composite PK
                confidence = 0.0
                reasons: list[str] = []

                if combo_uniqueness == 1.0:
                    confidence += 0.5
                    reasons.append("100% unique combination")
                else:
                    confidence += 0.35
                    reasons.append(f"{combo_uniqueness:.2%} unique combination")

                # Bonus for fewer columns (simpler key = better)
                if width == 2:
                    confidence += 0.2
                    reasons.append("Minimal 2-column key")
                elif width == 3:
                    confidence += 0.1
                    reasons.append("3-column key")

                # Bonus for name patterns suggesting natural key
                key_pattern_count = sum(
                    1 for c in col_names if _KEY_PARTICIPANT_RE.search(c)
                )
                if key_pattern_count == len(col_names):
                    confidence += 0.15
                    reasons.append("All columns have key-like names")
                elif key_pattern_count > 0:
                    confidence += 0.05

                natural_key_count = sum(
                    1 for c in col_names if _NATURAL_KEY_NAME_RE.search(c)
                )
                if natural_key_count == len(col_names):
                    confidence += 0.15
                    reasons.append("All columns look like natural key attributes")
                elif natural_key_count > 0:
                    confidence += 0.05

                # Bonus for no nulls in any column
                all_no_null = all(
                    _get_stat(
                        next(
                            (p.get("stats", {}) for p in profiles if p["column_name"] == c),
                            {},
                        ),
                        "null_count", 0,
                    ) == 0
                    for c in col_names
                )
                if all_no_null:
                    confidence += 0.1
                    reasons.append("No nulls in any key column")

                confidence = min(confidence, 1.0)

                candidates.append(
                    CompositePKCandidate(
                        table=table_name,
                        columns=col_names,
                        uniqueness_ratio=round(combo_uniqueness, 4),
                        null_rate=0.0,
                        confidence=round(confidence, 3),
                        reasons=reasons,
                    )
                )

            except Exception:
                continue

        # If we found a good candidate at this width, skip wider combinations
        if any(c.confidence >= 0.7 for c in candidates):
            break

    candidates.sort(key=lambda c: (-c.confidence, len(c.columns)))
    return candidates
