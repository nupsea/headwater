"""MySQL preview connector -- bounded introspection, profiling, and sampling."""

from __future__ import annotations

import importlib
from types import ModuleType
from urllib.parse import unquote, urlparse

import pyarrow as pa

from headwater.connectors.capabilities import (
    MYSQL_PREVIEW_CAPABILITIES,
    ConnectorCapabilities,
)
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig
from headwater.core.redaction import redact_secrets

_EXCLUDED_SCHEMAS = {"information_schema", "mysql", "performance_schema", "sys"}


class MySQLConnector:
    """Connects to MySQL for safe read-only discovery operations.

    This connector is intentionally preview-only in the registry until it has
    live integration coverage. It does not bulk-copy source tables into DuckDB.
    """

    def __init__(self) -> None:
        self._conn = None
        self._database: str | None = None

    def connect(self, config: SourceConfig) -> None:
        if config.uri is None:
            raise ConnectorError("MySQLConnector requires a URI (config.uri)")
        pymysql = _require_pymysql()
        parts = _parse_mysql_uri(config.uri)
        self._database = parts["database"]
        try:
            self._conn = pymysql.connect(
                host=parts["host"],
                port=parts["port"],
                user=parts["user"],
                password=parts["password"],
                database=parts["database"],
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=15,
                read_timeout=120,
                write_timeout=120,
                autocommit=True,
            )
        except Exception as exc:
            raise ConnectorError(f"Failed to connect to MySQL: {redact_secrets(str(exc))}") from exc

    def capabilities(self) -> ConnectorCapabilities:
        return MYSQL_PREVIEW_CAPABILITIES

    def list_schemas(self) -> list[str]:
        self._assert_connected()
        with self._conn.cursor() as cursor:  # type: ignore[union-attr]
            cursor.execute("SHOW DATABASES")
            rows = cursor.fetchall()
        schemas = [row.get("Database") or next(iter(row.values())) for row in rows]
        return sorted(schema for schema in schemas if schema not in _EXCLUDED_SCHEMAS)

    def list_tables(self) -> list[str]:
        self._assert_connected()
        schema = self._database
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        """
        params: tuple[str, ...] = ()
        if schema:
            query += " AND table_schema = %s"
            params = (schema,)
        query += " ORDER BY table_schema, table_name"
        with self._conn.cursor() as cursor:  # type: ignore[union-attr]
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [
            table if schema_name == self._database else f"{schema_name}.{table}"
            for schema_name, table in (
                (row["table_schema"], row["table_name"]) for row in rows
            )
        ]

    def list_columns(self, table_name: str) -> list[dict]:
        self._assert_connected()
        schema, table = _split_table(table_name, self._database)
        query = """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        with self._conn.cursor() as cursor:  # type: ignore[union-attr]
            cursor.execute(query, (schema, table))
            rows = cursor.fetchall()
        return [
            {
                "name": row["column_name"],
                "data_type": row["data_type"],
                "is_nullable": row["is_nullable"] == "YES",
                "ordinal_position": row["ordinal_position"],
            }
            for row in rows
        ]

    def profile(self, table_name: str) -> dict:
        self._assert_connected()
        schema, table = _split_table(table_name, self._database)
        columns = self.list_columns(table_name)
        if not columns:
            return {}
        qualified = _qualified(schema, table)
        selects = ["COUNT(*) AS `_total_rows`"]
        for column in columns:
            name = column["name"]
            quoted = _quote_identifier(name)
            alias = _safe_alias(name)
            selects.extend(
                [
                    f"COUNT({quoted}) AS `_nn_{alias}`",
                    f"MIN(CAST({quoted} AS CHAR)) AS `_min_{alias}`",
                    f"MAX(CAST({quoted} AS CHAR)) AS `_max_{alias}`",
                    f"COUNT(DISTINCT {quoted}) AS `_dist_{alias}`",
                ]
            )
        with self._conn.cursor() as cursor:  # type: ignore[union-attr]
            cursor.execute(f"SELECT {', '.join(selects)} FROM {qualified}")
            row = cursor.fetchone()
        if row is None:
            return {}
        row_count = row["_total_rows"]
        stats: dict[str, dict] = {}
        for column in columns:
            name = column["name"]
            alias = _safe_alias(name)
            non_null = row.get(f"_nn_{alias}", 0) or 0
            stats[name] = {
                "row_count": row_count,
                "count": row_count,
                "non_null": non_null,
                "null_count": row_count - non_null,
                "min": row.get(f"_min_{alias}"),
                "max": row.get(f"_max_{alias}"),
                "distinct_count": row.get(f"_dist_{alias}", 0),
            }
        return stats

    def sample(self, table_name: str, n: int = 10_000) -> pa.Table:
        self._assert_connected()
        schema, table = _split_table(table_name, self._database)
        with self._conn.cursor() as cursor:  # type: ignore[union-attr]
            cursor.execute(f"SELECT * FROM {_qualified(schema, table)} LIMIT %s", (n,))
            rows = cursor.fetchall()
        return _rows_to_arrow(rows)

    def execute_readonly(self, sql: str) -> pa.Table:
        self._assert_connected()
        statement = sql.strip().lower()
        if not statement.startswith(("select", "with")):
            raise ConnectorError("MySQLConnector only allows read-only SELECT statements")
        with self._conn.cursor() as cursor:  # type: ignore[union-attr]
            cursor.execute(sql)
            rows = cursor.fetchall()
        return _rows_to_arrow(rows)

    def load_to_duckdb(self, con, schema: str) -> list[str]:  # type: ignore[no-untyped-def]
        raise NotImplementedError(
            "MySQLConnector does not support load_to_duckdb in preview. "
            "Use profile() for in-place stats and sample() for Arrow batch validation."
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _assert_connected(self) -> None:
        if self._conn is None:
            raise ConnectorError("Not connected -- call connect() first")


def _require_pymysql() -> ModuleType:
    try:
        return importlib.import_module("pymysql")
    except ImportError as exc:
        raise ConnectorError(
            "MySQL connector requires optional dependency 'pymysql'. Run: uv add pymysql"
        ) from exc


def _parse_mysql_uri(uri: str) -> dict:
    parsed = urlparse(uri)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise ConnectorError("MySQLConnector URI must start with mysql:// or mysql+pymysql://")
    database = parsed.path.lstrip("/") if parsed.path else None
    if not database:
        raise ConnectorError("MySQLConnector URI must include a database name")
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database,
    }


def _split_table(table_name: str, default_schema: str | None) -> tuple[str, str]:
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        return schema, table
    if default_schema is None:
        raise ConnectorError(
            "MySQL table name must be schema-qualified when no database is selected"
        )
    return default_schema, table_name


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _safe_alias(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value)


def _rows_to_arrow(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({})
    columns = {column: [] for column in rows[0]}
    for row in rows:
        for column in columns:
            columns[column].append(row[column])
    return pa.table(columns)
