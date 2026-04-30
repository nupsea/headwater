"""DuckDB connector -- reads an existing DuckDB database without mutating it."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from headwater.connectors.capabilities import (
    DUCKDB_GENERATE_CAPABILITIES,
    ConnectorCapabilities,
)
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig

_EXCLUDED_SCHEMAS = {"information_schema", "pg_catalog"}


class DuckDBConnector:
    """Connects to a local DuckDB file in read-only mode."""

    def __init__(self) -> None:
        self._path: Path | None = None
        self._con: duckdb.DuckDBPyConnection | None = None

    def connect(self, config: SourceConfig) -> None:
        if config.path is None:
            raise ConnectorError("DuckDBConnector requires a path")
        path = Path(config.path)
        if not path.exists() or not path.is_file():
            raise ConnectorError(f"DuckDB file not found: {path}")
        self._path = path
        try:
            self._con = duckdb.connect(str(path), read_only=True)
        except Exception as exc:
            raise ConnectorError(f"Failed to open DuckDB database read-only: {exc}") from exc

    def capabilities(self) -> ConnectorCapabilities:
        return DUCKDB_GENERATE_CAPABILITIES

    def list_schemas(self) -> list[str]:
        self._assert_connected()
        rows = self._con.execute(  # type: ignore[union-attr]
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
        ).fetchall()
        return [row[0] for row in rows if row[0] not in _EXCLUDED_SCHEMAS]

    def list_tables(self) -> list[str]:
        self._assert_connected()
        rows = self._con.execute(  # type: ignore[union-attr]
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
        ).fetchall()
        return [
            _format_table_name(schema, table)
            for schema, table in rows
            if schema not in _EXCLUDED_SCHEMAS
        ]

    def list_columns(self, table_name: str) -> list[dict]:
        self._assert_connected()
        schema, table = _split_table(table_name)
        rows = self._con.execute(  # type: ignore[union-attr]
            """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
        return [
            {
                "name": name,
                "data_type": data_type,
                "is_nullable": is_nullable == "YES",
                "ordinal_position": ordinal,
            }
            for name, data_type, is_nullable, ordinal in rows
        ]

    def profile(self, table_name: str) -> dict:
        self._assert_connected()
        schema, table = _split_table(table_name)
        qualified = _qualified(schema, table)
        columns = self.list_columns(table_name)
        if not columns:
            return {}

        selects = ["COUNT(*) AS _total_rows"]
        for column in columns:
            name = column["name"]
            quoted = _quote_identifier(name)
            alias = _safe_alias(name)
            selects.extend(
                [
                    f"COUNT({quoted}) AS _nn_{alias}",
                    f"MIN({quoted}::VARCHAR) AS _min_{alias}",
                    f"MAX({quoted}::VARCHAR) AS _max_{alias}",
                    f"COUNT(DISTINCT {quoted}) AS _dist_{alias}",
                ]
            )
        row = self._con.execute(  # type: ignore[union-attr]
            f"SELECT {', '.join(selects)} FROM {qualified}"
        ).fetchone()
        if row is None:
            return {}

        names = [desc[0] for desc in self._con.description]  # type: ignore[union-attr]
        values = dict(zip(names, row, strict=True))
        row_count = values["_total_rows"]
        stats: dict[str, dict] = {}
        for column in columns:
            name = column["name"]
            alias = _safe_alias(name)
            non_null = values.get(f"_nn_{alias}", 0) or 0
            stats[name] = {
                "row_count": row_count,
                "count": row_count,
                "non_null": non_null,
                "null_count": row_count - non_null,
                "min": values.get(f"_min_{alias}"),
                "max": values.get(f"_max_{alias}"),
                "distinct_count": values.get(f"_dist_{alias}", 0),
            }
        return stats

    def sample(self, table_name: str, n: int = 10_000) -> pa.Table:
        self._assert_connected()
        schema, table = _split_table(table_name)
        return self._con.execute(  # type: ignore[union-attr]
            f"SELECT * FROM {_qualified(schema, table)} LIMIT ?",
            [n],
        ).arrow().read_all()

    def execute_readonly(self, sql: str) -> pa.Table:
        self._assert_connected()
        statement = sql.strip().lower()
        if not statement.startswith(("select", "with")):
            raise ConnectorError("DuckDBConnector only allows read-only SELECT statements")
        return self._con.execute(sql).arrow().read_all()  # type: ignore[union-attr]

    def load_to_duckdb(self, con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
        if self._path is None:
            raise ConnectorError("Not connected -- call connect() first")
        tables = self.list_tables()
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
        alias = "__headwater_duckdb_source"
        con.execute(f"ATTACH '{_sql_string(str(self._path))}' AS {alias} (READ_ONLY)")
        try:
            loaded = []
            for table_name in tables:
                source_schema, source_table = _split_table(table_name)
                target_table = table_name.replace(".", "_")
                con.execute(
                    f"CREATE OR REPLACE TABLE {_quote_identifier(schema)}."
                    f"{_quote_identifier(target_table)} AS "
                    f"SELECT * FROM {alias}.{_quote_identifier(source_schema)}."
                    f"{_quote_identifier(source_table)}"
                )
                loaded.append(target_table)
            return loaded
        finally:
            con.execute(f"DETACH {alias}")

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def _assert_connected(self) -> None:
        if self._con is None:
            raise ConnectorError("Not connected -- call connect() first")


def _split_table(table_name: str) -> tuple[str, str]:
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        return schema, table
    return "main", table_name


def _format_table_name(schema: str, table: str) -> str:
    return table if schema == "main" else f"{schema}.{table}"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _safe_alias(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value)


def _sql_string(value: str) -> str:
    return value.replace("'", "''")
