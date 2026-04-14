"""Foreign key relationship detection via name heuristics and value validation."""

from __future__ import annotations

import duckdb

from headwater.core.models import Relationship, TableInfo

# Common FK suffixes to strip when matching table names
_ID_SUFFIX = "_id"


def detect_relationships(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    tables: list[TableInfo],
) -> list[Relationship]:
    """Detect foreign key relationships between tables.

    Three passes:
    1. Name heuristics: columns ending in _id that match another table's PK
    2. Value validation: check referential integrity via SQL
    3. Cardinality inference: determine relationship type
    """
    # Build PK lookup: table_name -> pk_column_name
    pk_map: dict[str, str] = {}
    for t in tables:
        for c in t.columns:
            if c.is_primary_key:
                pk_map[t.name] = c.name
                break

    candidates = _find_candidates(tables, pk_map)
    relationships: list[Relationship] = []

    for fk_table, fk_col, pk_table, pk_col in candidates:
        integrity = _check_referential_integrity(con, schema, fk_table, fk_col, pk_table, pk_col)
        if integrity < 0.5:
            continue  # Too low -- probably not a real FK

        rel_type = _infer_cardinality(con, schema, fk_table, fk_col)
        confidence = 0.9 if integrity > 0.9 else 0.7

        relationships.append(
            Relationship(
                from_table=fk_table,
                from_column=fk_col,
                to_table=pk_table,
                to_column=pk_col,
                type=rel_type,
                confidence=round(confidence, 2),
                referential_integrity=round(integrity, 4),
                source="inferred_name" if integrity < 1.0 else "inferred_value",
            )
        )

    return relationships


def _find_candidates(
    tables: list[TableInfo], pk_map: dict[str, str]
) -> list[tuple[str, str, str, str]]:
    """Find FK candidate pairs via name heuristics.

    Returns list of (fk_table, fk_column, pk_table, pk_column).
    """
    candidates: list[tuple[str, str, str, str]] = []
    table_names = {t.name for t in tables}

    for table in tables:
        for col in table.columns:
            if col.is_primary_key:
                continue
            if not col.name.endswith(_ID_SUFFIX):
                continue

            # Extract the reference target from column name
            prefix = col.name[: -len(_ID_SUFFIX)]

            # Try matching against table names
            # e.g. zone_id -> zones, site_id -> sites
            for candidate_table in [prefix + "s", prefix + "es", prefix]:
                if candidate_table in table_names and candidate_table != table.name:
                    pk_col = pk_map.get(candidate_table)
                    if pk_col:
                        candidates.append((table.name, col.name, candidate_table, pk_col))
                        break

            # Also check if column name exactly matches a PK in another table
            for pk_table, pk_col_name in pk_map.items():
                if pk_table == table.name:
                    continue
                already = (table.name, col.name, pk_table, pk_col_name)
                if col.name == pk_col_name and already not in candidates:
                    candidates.append((table.name, col.name, pk_table, pk_col_name))

    return candidates


def _check_referential_integrity(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    fk_table: str,
    fk_col: str,
    pk_table: str,
    pk_col: str,
) -> float:
    """Check what fraction of FK values exist in the PK table."""
    try:
        result = con.execute(
            f"""
            SELECT
                COUNT(DISTINCT fk."{fk_col}") as total_fk,
                COUNT(DISTINCT CASE
                    WHEN pk."{pk_col}" IS NOT NULL THEN fk."{fk_col}"
                END) as matched
            FROM "{schema}"."{fk_table}" fk
            LEFT JOIN "{schema}"."{pk_table}" pk ON fk."{fk_col}" = pk."{pk_col}"
            WHERE fk."{fk_col}" IS NOT NULL
            """
        ).fetchone()
        if result is None or result[0] == 0:
            return 0.0
        return result[1] / result[0]
    except Exception:
        return 0.0


def _infer_cardinality(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    fk_table: str,
    fk_col: str,
) -> str:
    """Infer whether the FK side is one-to-many or many-to-many."""
    try:
        result = con.execute(
            f"""
            SELECT COUNT(*) as total, COUNT(DISTINCT "{fk_col}") as distinct_vals
            FROM "{schema}"."{fk_table}"
            WHERE "{fk_col}" IS NOT NULL
            """
        ).fetchone()
        if result is None:
            return "many_to_one"
        total, distinct = result
        if total == distinct:
            return "one_to_one"
        return "many_to_one"
    except Exception:
        return "many_to_one"
