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


def _get_stat(stats: dict, key: str, default=None):
    """Safely get a stat value, handling both dict and nested access."""
    if isinstance(stats, dict):
        return stats.get(key, default)
    return default


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

    # Strip _id suffix to get potential table name
    base_name = re.sub(r"_id$", "", from_col, flags=re.IGNORECASE)
    if not base_name or base_name == from_col:
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


def detect_composite_keys(
    table_name: str,
    profiles: list[dict],
) -> list[PKCandidate]:
    """Detect composite PK candidates from column pairs.

    Heuristic: pairs of _id columns with moderate cardinality.
    """
    candidates: list[PKCandidate] = []

    id_cols = []
    for prof in profiles:
        col_name = prof["column_name"]
        stats = prof.get("stats", {})
        if not isinstance(stats, dict):
            continue
        row_count = _get_stat(stats, "row_count", 0)
        if not row_count:
            continue
        distinct = _get_stat(stats, "distinct_count", 0)
        uniqueness = distinct / row_count if row_count else 0

        if _ID_SUFFIX.search(col_name) and 0.01 < uniqueness < 1.0:
            id_cols.append((col_name, distinct, row_count, uniqueness))

    # Try pairs
    for i, (col_a, dist_a, rows, _) in enumerate(id_cols):
        for col_b, dist_b, _, _ in id_cols[i + 1 :]:
            # If product of distinct counts >= row_count, combination might be unique
            if dist_a * dist_b >= rows:
                confidence = 0.5
                reasons = [
                    f"Pair ({col_a}, {col_b}) may form composite key",
                    f"Combined cardinality: {dist_a} x {dist_b} = {dist_a * dist_b} >= {rows} rows",
                ]
                candidates.append(
                    PKCandidate(
                        table=table_name,
                        column=f"{col_a}+{col_b}",
                        uniqueness_ratio=min(dist_a * dist_b / rows, 1.0),
                        null_rate=0.0,
                        confidence=round(confidence, 3),
                        reasons=reasons,
                    )
                )

    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates
