"""Snowflake preview connector -- bounded metadata, profiling, and sampling."""

from __future__ import annotations

import importlib
from types import ModuleType
from urllib.parse import parse_qs, unquote, urlparse

import pyarrow as pa

from headwater.connectors.capabilities import (
    SNOWFLAKE_PREVIEW_CAPABILITIES,
    ConnectorCapabilities,
)
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig
from headwater.core.redaction import redact_secrets

_EXCLUDED_SCHEMAS = {"INFORMATION_SCHEMA"}


class SnowflakeConnector:
    """Connects to Snowflake for safe observe/generate-mode discovery.

    This connector never bulk-copies a warehouse. It lists metadata, runs bounded
    aggregate profiles, and fetches row-limited samples for local DuckDB validation.
    """

    def __init__(self) -> None:
        self._conn = None
        self._database: str | None = None
        self._schema: str | None = None
        self._last_query_id: str | None = None

    def connect(self, config: SourceConfig) -> None:
        if config.uri is None:
            raise ConnectorError("SnowflakeConnector requires a URI (config.uri)")
        connector_module = _require_snowflake_connector()
        parts = _parse_snowflake_uri(config.uri)
        self._database = parts.get("database")
        self._schema = parts.get("schema")
        kwargs: dict[str, object] = {
            key: value
            for key, value in parts.items()
            if key
            in {
                "account",
                "user",
                "password",
                "warehouse",
                "database",
                "schema",
                "role",
                "authenticator",
            }
            and value
        }
        kwargs.setdefault("login_timeout", 15)
        kwargs.setdefault("network_timeout", 120)
        kwargs.setdefault("client_session_keep_alive", False)
        try:
            self._conn = connector_module.connect(**kwargs)
        except Exception as exc:
            raise ConnectorError(
                f"Failed to connect to Snowflake: {redact_secrets(str(exc))}"
            ) from exc

    def capabilities(self) -> ConnectorCapabilities:
        return SNOWFLAKE_PREVIEW_CAPABILITIES

    def list_schemas(self) -> list[str]:
        self._assert_connected()
        database_filter = "WHERE catalog_name = %s" if self._database else ""
        params = (self._database,) if self._database else ()
        rows = self._fetchall(
            f"""
            SELECT catalog_name, schema_name
            FROM information_schema.schemata
            {database_filter}
            ORDER BY catalog_name, schema_name
            """,
            params,
        )
        schemas = []
        for database, schema in rows:
            if str(schema).upper() in _EXCLUDED_SCHEMAS:
                continue
            schemas.append(schema if database == self._database else f"{database}.{schema}")
        return schemas

    def list_tables(self) -> list[str]:
        self._assert_connected()
        where = ["table_type = 'BASE TABLE'"]
        params: list[str] = []
        if self._database:
            where.append("table_catalog = %s")
            params.append(self._database)
        if self._schema:
            where.append("table_schema = %s")
            params.append(self._schema)
        query = f"""
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.tables
            WHERE {' AND '.join(where)}
            ORDER BY table_catalog, table_schema, table_name
        """
        rows = self._fetchall(query, tuple(params))
        result = []
        for database, schema, table in rows:
            if str(schema).upper() in _EXCLUDED_SCHEMAS:
                continue
            result.append(_format_table(database, schema, table, self._database, self._schema))
        return result

    def list_columns(self, table_name: str) -> list[dict]:
        self._assert_connected()
        database, schema, table = _split_table(table_name, self._database, self._schema)
        rows = self._fetchall(
            """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_catalog = %s AND table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (database, schema, table),
        )
        return [
            {
                "name": name,
                "data_type": data_type,
                "is_nullable": nullable == "YES",
                "ordinal_position": ordinal,
            }
            for name, data_type, nullable, ordinal in rows
        ]

    def profile(self, table_name: str) -> dict:
        self._assert_connected()
        columns = self.list_columns(table_name)
        if not columns:
            return {}
        database, schema, table = _split_table(table_name, self._database, self._schema)
        qualified = _qualified(database, schema, table)
        selects = ["COUNT(*) AS _total_rows"]
        for column in columns:
            name = column["name"]
            quoted = _quote_identifier(name)
            alias = _safe_alias(name)
            selects.extend(
                [
                    f"COUNT({quoted}) AS _nn_{alias}",
                    f"MIN(TO_VARCHAR({quoted})) AS _min_{alias}",
                    f"MAX(TO_VARCHAR({quoted})) AS _max_{alias}",
                    f"COUNT(DISTINCT {quoted}) AS _dist_{alias}",
                ]
            )
        row = self._fetchone(f"SELECT {', '.join(selects)} FROM {qualified}", ())
        if row is None:
            return {}
        values = dict(zip([desc[0].lower() for desc in self._description()], row, strict=True))
        row_count = values["_total_rows"]
        stats: dict[str, dict] = {}
        for column in columns:
            name = column["name"]
            alias = _safe_alias(name).lower()
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
        database, schema, table = _split_table(table_name, self._database, self._schema)
        rows = self._fetchall(
            f"SELECT * FROM {_qualified(database, schema, table)} LIMIT %s",
            (int(n),),
        )
        columns = [desc[0] for desc in self._description()]
        return _rows_to_arrow(rows, columns)

    def execute_readonly(self, sql: str) -> pa.Table:
        self._assert_connected()
        statement = sql.strip().lower()
        if not statement.startswith(("select", "with")):
            raise ConnectorError("SnowflakeConnector only allows read-only SELECT statements")
        rows = self._fetchall(sql, ())
        columns = [desc[0] for desc in self._description()]
        return _rows_to_arrow(rows, columns)

    def set_query_tag(self, query_tag: str) -> None:
        """Tag subsequent Snowflake queries for cost/audit traceability."""
        self._assert_connected()
        with self._cursor() as cursor:
            cursor.execute("ALTER SESSION SET QUERY_TAG = %s", (query_tag[:256],))

    def set_statement_timeout(self, seconds: int) -> None:
        """Set the Snowflake session statement timeout for approved runs."""
        self._assert_connected()
        timeout = max(5, min(int(seconds), 3600))
        with self._cursor() as cursor:
            cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %s", (timeout,))

    def last_query_id(self) -> str | None:
        return self._last_query_id

    def estimate_row_count(self, table_name: str) -> int | None:
        self._assert_connected()
        database, schema, table = _split_table(table_name, self._database, self._schema)
        row = self._fetchone(
            """
            SELECT row_count
            FROM information_schema.tables
            WHERE table_catalog = %s AND table_schema = %s AND table_name = %s
            """,
            (database, schema, table),
        )
        return int(row[0]) if row and row[0] is not None else None

    def load_to_duckdb(self, con, schema: str) -> list[str]:  # type: ignore[no-untyped-def]
        raise NotImplementedError(
            "SnowflakeConnector does not support load_to_duckdb. "
            "Use bounded sample() or profile() instead."
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _cursor(self):
        self._assert_connected()
        return self._conn.cursor()  # type: ignore[union-attr]

    def _fetchall(self, sql: str, params: tuple) -> list[tuple]:
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            self._last_description = cursor.description
            self._last_query_id = getattr(cursor, "sfqid", None)
            return cursor.fetchall()

    def _fetchone(self, sql: str, params: tuple):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            self._last_description = cursor.description
            self._last_query_id = getattr(cursor, "sfqid", None)
            return cursor.fetchone()

    def _description(self):
        return getattr(self, "_last_description", []) or []

    def _assert_connected(self) -> None:
        if self._conn is None:
            raise ConnectorError("Not connected -- call connect() first")


def _require_snowflake_connector() -> ModuleType:
    try:
        return importlib.import_module("snowflake.connector")
    except ImportError as exc:
        raise ConnectorError(
            "Snowflake connector requires optional dependency "
            "'snowflake-connector-python'. Run: uv add snowflake-connector-python"
        ) from exc


def _parse_snowflake_uri(uri: str) -> dict[str, str | None]:
    parsed = urlparse(uri)
    if parsed.scheme != "snowflake":
        raise ConnectorError("SnowflakeConnector URI must start with snowflake://")
    account = parsed.hostname
    if not account:
        raise ConnectorError("SnowflakeConnector URI must include an account host")
    path_parts = [unquote(part) for part in parsed.path.split("/") if part]
    query = {key: values[-1] for key, values in parse_qs(parsed.query).items() if values}
    database = path_parts[0] if len(path_parts) >= 1 else query.get("database")
    schema = path_parts[1] if len(path_parts) >= 2 else query.get("schema")
    return {
        "account": account,
        "user": unquote(parsed.username or query.get("user", "")) or None,
        "password": unquote(parsed.password or query.get("password", "")) or None,
        "database": database.upper() if database else None,
        "schema": schema.upper() if schema else None,
        "warehouse": query.get("warehouse"),
        "role": query.get("role"),
        "authenticator": query.get("authenticator"),
    }


def _split_table(
    table_name: str,
    default_database: str | None,
    default_schema: str | None,
) -> tuple[str, str, str]:
    parts = table_name.split(".")
    if len(parts) == 3:
        return parts[0].upper(), parts[1].upper(), parts[2].upper()
    if len(parts) == 2 and default_database:
        return default_database.upper(), parts[0].upper(), parts[1].upper()
    if len(parts) == 1 and default_database and default_schema:
        return default_database.upper(), default_schema.upper(), parts[0].upper()
    raise ConnectorError(
        "Snowflake table names must be table, schema.table, or database.schema.table "
        "when database/schema defaults are available"
    )


def _format_table(
    database: str,
    schema: str,
    table: str,
    default_database: str | None,
    default_schema: str | None,
) -> str:
    if database == default_database and schema == default_schema:
        return table
    if database == default_database:
        return f"{schema}.{table}"
    return f"{database}.{schema}.{table}"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _qualified(database: str, schema: str, table: str) -> str:
    return ".".join(_quote_identifier(part) for part in (database, schema, table))


def _safe_alias(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value)


def _rows_to_arrow(rows: list[tuple], columns: list[str]) -> pa.Table:
    if not columns:
        return pa.table({})
    values = {column: [] for column in columns}
    for row in rows:
        for column, value in zip(columns, row, strict=False):
            values[column].append(value)
    return pa.table(values)
