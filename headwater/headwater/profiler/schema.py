"""Schema extraction from DuckDB information_schema."""

from __future__ import annotations

import duckdb

from headwater.core.models import ColumnInfo, TableInfo

# Map DuckDB types to our normalised type strings
_TYPE_MAP: dict[str, str] = {
    "BIGINT": "int64",
    "INTEGER": "int64",
    "SMALLINT": "int64",
    "TINYINT": "int64",
    "HUGEINT": "int64",
    "UBIGINT": "int64",
    "UINTEGER": "int64",
    "USMALLINT": "int64",
    "UTINYINT": "int64",
    "DOUBLE": "float64",
    "FLOAT": "float64",
    "REAL": "float64",
    "DECIMAL": "float64",
    "VARCHAR": "varchar",
    "TEXT": "varchar",
    "BLOB": "varchar",
    "BOOLEAN": "bool",
    "BOOL": "bool",
    "DATE": "date",
    "TIME": "time",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp",
    "INTERVAL": "varchar",
    "JSON": "json",
}


def _normalise_type(duckdb_type: str) -> str:
    """Normalise a DuckDB type string to our standard set."""
    upper = duckdb_type.upper()
    # Exact match
    if upper in _TYPE_MAP:
        return _TYPE_MAP[upper]
    # Prefix match for parameterised types (e.g. DECIMAL(18,2), VARCHAR(255))
    for prefix, norm in _TYPE_MAP.items():
        if upper.startswith(prefix):
            return norm
    # Struct/list/array types
    if upper.startswith("STRUCT"):
        return "json"
    if upper.startswith("MAP"):
        return "json"
    if "[]" in upper or upper.startswith("LIST"):
        return "list"
    return "varchar"  # fallback


def extract_schema(
    con: duckdb.DuckDBPyConnection, schema: str
) -> list[TableInfo]:
    """Extract table and column metadata from DuckDB information_schema.

    Returns a list of TableInfo with ColumnInfo populated (no descriptions yet).
    """
    # Get tables
    table_rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = ? ORDER BY table_name",
        [schema],
    ).fetchall()

    tables: list[TableInfo] = []
    for (table_name,) in table_rows:
        # Row count
        row_count = con.execute(
            f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
        ).fetchone()[0]

        # Columns
        col_rows = con.execute(
            "SELECT column_name, data_type, is_nullable, ordinal_position "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema, table_name],
        ).fetchall()

        columns: list[ColumnInfo] = []
        for col_name, dtype, is_nullable, _ordinal in col_rows:
            normalised = _normalise_type(dtype)
            # Heuristic PK detection: columns named *_id that end with the table singular
            is_pk = _is_likely_pk(col_name, table_name)
            columns.append(
                ColumnInfo(
                    name=col_name,
                    dtype=normalised,
                    nullable=is_nullable == "YES",
                    is_primary_key=is_pk,
                )
            )

        tables.append(
            TableInfo(
                name=table_name,
                schema_name=schema,
                row_count=row_count,
                columns=columns,
            )
        )

    return tables


def _is_likely_pk(col_name: str, table_name: str) -> bool:
    """Heuristic: is this column likely the primary key?

    Matches patterns like:
      - zone_id in table zones
      - site_id in table sites
      - reading_id in table readings
      - id in any table
    """
    if col_name == "id":
        return True
    if col_name.endswith("_id"):
        prefix = col_name[:-3]  # strip _id
        # Check if prefix is singular of table name
        if table_name.endswith("s") and table_name[:-1] == prefix:
            return True
        if table_name.endswith("es") and table_name[:-2] == prefix:
            return True
        if table_name == prefix:
            return True
    return False
