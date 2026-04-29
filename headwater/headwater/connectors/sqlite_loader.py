"""SQLite connector -- reads an existing SQLite database without mutating it."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pyarrow as pa

from headwater.connectors.capabilities import (
    SQLITE_GENERATE_CAPABILITIES,
    ConnectorCapabilities,
)
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig


class SQLiteConnector:
    """Connects to a local SQLite file in read-only mode."""

    def __init__(self) -> None:
        self._path: Path | None = None
        self._con: sqlite3.Connection | None = None

    def connect(self, config: SourceConfig) -> None:
        if config.path is None:
            raise ConnectorError("SQLiteConnector requires a path")
        path = Path(config.path)
        if not path.exists() or not path.is_file():
            raise ConnectorError(f"SQLite file not found: {path}")
        self._path = path
        try:
            self._con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            self._con.row_factory = sqlite3.Row
        except sqlite3.Error as exc:
            raise ConnectorError(f"Failed to open SQLite database read-only: {exc}") from exc

    def capabilities(self) -> ConnectorCapabilities:
        return SQLITE_GENERATE_CAPABILITIES

    def list_tables(self) -> list[str]:
        self._assert_connected()
        rows = self._con.execute(  # type: ignore[union-attr]
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        return [row["name"] for row in rows]

    def list_columns(self, table_name: str) -> list[dict]:
        self._assert_connected()
        rows = self._con.execute(  # type: ignore[union-attr]
            f"PRAGMA table_info({_quote_identifier(table_name)})"
        ).fetchall()
        return [
            {
                "name": row["name"],
                "data_type": row["type"] or "TEXT",
                "is_nullable": not bool(row["notnull"]),
                "ordinal_position": int(row["cid"]) + 1,
                "is_primary_key": bool(row["pk"]),
            }
            for row in rows
        ]

    def profile(self, table_name: str) -> dict:
        self._assert_connected()
        columns = self.list_columns(table_name)
        if not columns:
            return {}
        quoted_table = _quote_identifier(table_name)
        selects = ["COUNT(*) AS _total_rows"]
        for column in columns:
            name = column["name"]
            quoted = _quote_identifier(name)
            alias = _safe_alias(name)
            selects.extend(
                [
                    f"COUNT({quoted}) AS _nn_{alias}",
                    f"MIN(CAST({quoted} AS TEXT)) AS _min_{alias}",
                    f"MAX(CAST({quoted} AS TEXT)) AS _max_{alias}",
                    f"COUNT(DISTINCT {quoted}) AS _dist_{alias}",
                ]
            )
        row = self._con.execute(  # type: ignore[union-attr]
            f"SELECT {', '.join(selects)} FROM {quoted_table}"
        ).fetchone()
        if row is None:
            return {}
        row_count = row["_total_rows"]
        stats: dict[str, dict] = {}
        for column in columns:
            name = column["name"]
            alias = _safe_alias(name)
            non_null = row[f"_nn_{alias}"] or 0
            stats[name] = {
                "row_count": row_count,
                "count": row_count,
                "non_null": non_null,
                "null_count": row_count - non_null,
                "min": row[f"_min_{alias}"],
                "max": row[f"_max_{alias}"],
                "distinct_count": row[f"_dist_{alias}"] or 0,
            }
        return stats

    def sample(self, table_name: str, n: int = 10_000) -> pa.Table:
        self._assert_connected()
        rows = self._con.execute(  # type: ignore[union-attr]
            f"SELECT * FROM {_quote_identifier(table_name)} LIMIT ?",
            (n,),
        ).fetchall()
        columns = [column["name"] for column in self.list_columns(table_name)]
        return _rows_to_arrow(rows, columns)

    def execute_readonly(self, sql: str) -> pa.Table:
        self._assert_connected()
        statement = sql.strip().lower()
        if not statement.startswith(("select", "with")):
            raise ConnectorError("SQLiteConnector only allows read-only SELECT statements")
        cursor = self._con.execute(sql)  # type: ignore[union-attr]
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description or []]
        return _rows_to_arrow(rows, columns)

    def load_to_duckdb(self, con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
        self._assert_connected()
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
        loaded = []
        for table_name in self.list_tables():
            arrow_table = self.sample(table_name, n=2_147_483_647)
            temp_name = f"__headwater_sqlite_{_safe_alias(table_name)}"
            con.register(temp_name, arrow_table)
            try:
                con.execute(
                    f"CREATE OR REPLACE TABLE {_quote_identifier(schema)}."
                    f"{_quote_identifier(table_name)} AS SELECT * FROM {temp_name}"
                )
            finally:
                con.unregister(temp_name)
            loaded.append(table_name)
        return loaded

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def _assert_connected(self) -> None:
        if self._con is None:
            raise ConnectorError("Not connected -- call connect() first")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _safe_alias(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value)


def _rows_to_arrow(rows: list[sqlite3.Row], columns: list[str]) -> pa.Table:
    values = {column: [] for column in columns}
    for row in rows:
        for column in columns:
            values[column].append(row[column])
    return pa.table(values)
