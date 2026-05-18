"""Redshift preview connector -- bounded metadata, profiling, sampling, and schema filtering.

Connects to AWS Redshift for safe observe/generate-mode discovery.  This
connector never bulk-copies a warehouse.  It lists metadata, runs bounded
aggregate profiles, and fetches row-limited samples for local DuckDB
validation.

Authentication modes:
  * **User / password** — standard Redshift credentials in the URI.
  * **IAM with access keys** — set ``iam=true`` plus ``access_key_id``,
    ``secret_access_key``, ``region``, and ``db_user`` in the URI query
    string or source config.

URI format::

    redshift://user:pass@cluster-endpoint:5439/database
    redshift://user:pass@cluster-endpoint:5439/database/schema
    redshift://cluster-endpoint:5439/database?iam=true&access_key_id=AK&secret_access_key=SK&region=us-east-1&db_user=admin

Schema filtering (via URI query params or source config)::

    redshift://...?include_schemas=analytics,reporting&exclude_schemas=staging
"""

from __future__ import annotations

import importlib
import logging
from types import ModuleType
from urllib.parse import parse_qs, unquote, urlparse

import pyarrow as pa

from headwater.connectors.capabilities import (
    REDSHIFT_PREVIEW_CAPABILITIES,
    ConnectorCapabilities,
)
from headwater.connectors.schema_filter import SchemaTableFilter
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig
from headwater.core.redaction import redact_secrets

logger = logging.getLogger(__name__)

# Schemas that are always excluded from discovery.
_EXCLUDED_SCHEMAS = frozenset({
    "information_schema",
    "pg_catalog",
    "pg_toast",
    "pg_internal",
})


class RedshiftConnector:
    """Connects to AWS Redshift for safe observe/generate-mode discovery.

    This connector never bulk-copies a warehouse.  It lists metadata, runs
    bounded aggregate profiles, and fetches row-limited samples for local
    DuckDB validation.  Schema filtering is supported via
    :class:`~headwater.connectors.schema_filter.SchemaTableFilter`.
    """

    def __init__(self) -> None:
        self._conn = None
        self._database: str | None = None
        self._schema: str | None = None
        self._schema_filter = SchemaTableFilter()
        self._driver: str = "none"

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def connect(self, config: SourceConfig) -> None:
        """Open a connection to Redshift.

        The driver is resolved in order of preference:
        1. ``redshift_connector`` (official AWS driver, supports IAM).
        2. ``psycopg2`` fallback (user/password only).
        """
        if config.uri is None:
            raise ConnectorError("RedshiftConnector requires a URI (config.uri)")
        parts = _parse_redshift_uri(config.uri)
        self._database = parts.get("database")
        self._schema = parts.get("schema")

        # Build schema filter from URI query params.
        uri_filter = SchemaTableFilter.from_query_params({
            k: parts[k]
            for k in ("include_schemas", "exclude_schemas",
                      "include_tables", "exclude_tables")
            if parts.get(k)
        })
        if not uri_filter.is_empty:
            self._schema_filter = uri_filter

        iam = parts.get("iam", False)

        if iam:
            self._connect_iam(parts)
        else:
            self._connect_userpass(parts)

    def capabilities(self) -> ConnectorCapabilities:
        return REDSHIFT_PREVIEW_CAPABILITIES

    # ------------------------------------------------------------------
    # Schema filter support
    # ------------------------------------------------------------------

    def set_schema_filter(self, config: dict | None) -> None:
        """Apply a :class:`SchemaTableFilter` from a config dict.

        Called by the pipeline runner when the source ``config`` contains
        filtering keys (``include_schemas``, ``exclude_schemas``, etc.).
        """
        sf = SchemaTableFilter.from_config(config)
        if not sf.is_empty:
            self._schema_filter = sf
            logger.info(
                "Redshift schema filter applied: %s", sf.describe(),
            )

    # ------------------------------------------------------------------
    # Metadata discovery
    # ------------------------------------------------------------------

    def list_schemas(self) -> list[str]:
        """Return non-system schema names, respecting the active filter."""
        self._assert_connected()
        rows = self._fetchall(
            """
            SELECT nspname AS schema_name
            FROM pg_catalog.pg_namespace
            WHERE nspname NOT LIKE 'pg_temp_%%'
              AND nspname NOT LIKE 'pg_toast_temp_%%'
            ORDER BY schema_name
            """,
            (),
        )
        schemas = []
        for (schema_name,) in rows:
            if schema_name.lower() in _EXCLUDED_SCHEMAS:
                continue
            schemas.append(schema_name)
        return self._schema_filter.filter_schemas(schemas)

    def list_tables(self) -> list[str]:
        """Return user tables as ``schema.table``, respecting the active filter."""
        self._assert_connected()
        where = ["t.table_type = 'BASE TABLE'"]
        params: list = []

        # Redshift information_schema scopes to current database automatically.
        if self._schema:
            where.append("t.table_schema = %s")
            params.append(self._schema)

        query = f"""
            SELECT t.table_schema, t.table_name
            FROM information_schema.tables t
            WHERE {' AND '.join(where)}
            ORDER BY t.table_schema, t.table_name
        """
        rows = self._fetchall(query, tuple(params))

        visible: list[tuple[str, str]] = []
        for schema, table in rows:
            if schema.lower() in _EXCLUDED_SCHEMAS:
                continue
            if schema.startswith("pg_temp"):
                continue
            visible.append((schema, table))

        filtered = self._schema_filter.filter_tables(
            [f"{schema}.{table}" for schema, table in visible],
        )
        formatted = []
        for qualified in filtered:
            schema, table = qualified.split(".", 1)
            if schema == "public" and not self._schema:
                formatted.append(table)
            else:
                formatted.append(qualified)
        return formatted

    def list_columns(self, table_name: str) -> list[dict]:
        """Return column metadata for a single table."""
        self._assert_connected()
        schema, table = _split_table(table_name, self._schema)
        rows = self._fetchall(
            """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
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

    # ------------------------------------------------------------------
    # Profiling (aggregate push-down)
    # ------------------------------------------------------------------

    def profile(self, table_name: str) -> dict:
        """Run a single aggregate query in Redshift and return column-level stats.

        No rows are transferred — only aggregates.
        """
        self._assert_connected()
        columns = self.list_columns(table_name)
        if not columns:
            return {}

        schema, table = _split_table(table_name, self._schema)
        qualified = _qualified(schema, table)

        selects = ["COUNT(*) AS _total_rows"]
        for col in columns:
            name = col["name"]
            quoted = _quote_ident(name)
            alias = _safe_alias(name)
            selects.extend([
                f"COUNT({quoted}) AS _nn_{alias}",
                f"MIN({quoted}::varchar) AS _min_{alias}",
                f"MAX({quoted}::varchar) AS _max_{alias}",
                f"COUNT(DISTINCT {quoted}) AS _dist_{alias}",
            ])

        row = self._fetchone(
            f"SELECT {', '.join(selects)} FROM {qualified}", ()
        )
        if row is None:
            return {}

        col_names = [desc[0].lower() for desc in self._description()]
        values = dict(zip(col_names, row, strict=True))
        row_count = values["_total_rows"]

        stats: dict[str, dict] = {}
        for col in columns:
            name = col["name"]
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

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------

    def sample(self, table_name: str, n: int = 10_000) -> pa.Table:
        """Return an Arrow table of up to *n* rows from the given table."""
        self._assert_connected()
        schema, table = _split_table(table_name, self._schema)
        rows = self._fetchall(
            f"SELECT * FROM {_qualified(schema, table)} LIMIT %s",
            (int(n),),
        )
        columns = [desc[0] for desc in self._description()]
        return _rows_to_arrow(rows, columns)

    # ------------------------------------------------------------------
    # Read-only SQL execution
    # ------------------------------------------------------------------

    def execute_readonly(self, sql: str) -> pa.Table:
        """Execute a read-only SELECT statement and return results as Arrow."""
        self._assert_connected()
        statement = sql.strip().lower()
        if not statement.startswith(("select", "with")):
            raise ConnectorError(
                "RedshiftConnector only allows read-only SELECT statements"
            )
        rows = self._fetchall(sql, ())
        columns = [desc[0] for desc in self._description()]
        return _rows_to_arrow(rows, columns)

    # ------------------------------------------------------------------
    # Row estimates (Redshift-specific catalog)
    # ------------------------------------------------------------------

    def estimate_row_count(self, table_name: str) -> int | None:
        """Estimate row count from Redshift system catalog without scanning."""
        self._assert_connected()
        schema, table = _split_table(table_name, self._schema)
        row = self._fetchone(
            """
            SELECT "tbl_rows"
            FROM svv_table_info
            WHERE "schema" = %s AND "table" = %s
            """,
            (schema, table),
        )
        if row and row[0] is not None:
            return int(row[0])
        # Fallback to pg_class statistics.
        row = self._fetchone(
            """
            SELECT c.reltuples::bigint
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            """,
            (schema, table),
        )
        return int(row[0]) if row and row[0] is not None and row[0] >= 0 else None

    # ------------------------------------------------------------------
    # Session controls
    # ------------------------------------------------------------------

    def set_statement_timeout(self, seconds: int) -> None:
        """Set the Redshift session statement timeout."""
        self._assert_connected()
        timeout_ms = max(5000, min(int(seconds) * 1000, 3_600_000))
        self._execute(f"SET statement_timeout = {timeout_ms}")

    def set_query_group(self, group: str) -> None:
        """Set the Redshift query group for cost/audit traceability."""
        self._assert_connected()
        safe = group[:64].replace("'", "")
        self._execute(f"SET query_group TO '{safe}'")

    # ------------------------------------------------------------------
    # Not supported
    # ------------------------------------------------------------------

    def load_to_duckdb(self, con, schema: str) -> list[str]:
        raise NotImplementedError(
            "RedshiftConnector does not support load_to_duckdb. "
            "Use bounded sample() or profile() instead."
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    # ------------------------------------------------------------------
    # Connection helpers (private)
    # ------------------------------------------------------------------

    def _connect_iam(self, parts: dict) -> None:
        """Connect using IAM credentials (access key / secret key)."""
        mod = _require_redshift_connector()
        kwargs: dict = {
            "host": parts["host"],
            "port": int(parts.get("port") or 5439),
            "database": parts.get("database") or "dev",
            "iam": True,
        }
        for key in ("access_key_id", "secret_access_key", "session_token",
                     "region", "cluster_identifier", "db_user"):
            if parts.get(key):
                kwargs[key] = parts[key]
        kwargs.setdefault("timeout", 15)
        try:
            self._conn = mod.connect(**kwargs)
            self._driver = "redshift_connector"
        except Exception as exc:
            raise ConnectorError(
                f"Failed to connect to Redshift (IAM): {redact_secrets(str(exc))}"
            ) from exc

    def _connect_userpass(self, parts: dict) -> None:
        """Connect using user/password — tries redshift_connector then psycopg2."""
        host = parts["host"]
        port = int(parts.get("port") or 5439)
        database = parts.get("database") or "dev"
        user = parts.get("user")
        password = parts.get("password")

        # Try the official redshift_connector first.
        mod = _try_import("redshift_connector")
        if mod is not None:
            try:
                self._conn = mod.connect(
                    host=host, port=port, database=database,
                    user=user, password=password, timeout=15,
                )
                self._driver = "redshift_connector"
                return
            except Exception as exc:
                logger.debug("redshift_connector failed, trying psycopg2: %s", exc)

        # Fallback to psycopg2 (Redshift speaks PostgreSQL wire protocol).
        psycopg2 = _try_import("psycopg2")
        if psycopg2 is None:
            raise ConnectorError(
                "Redshift connector requires 'redshift-connector' or 'psycopg2-binary'. "
                "Install one: uv add redshift-connector  OR  uv add psycopg2-binary"
            )
        dsn = f"host={host} port={port} dbname={database}"
        if user:
            dsn += f" user={user}"
        if password:
            dsn += f" password={password}"
        dsn += " connect_timeout=15"
        try:
            self._conn = psycopg2.connect(dsn)
            self._driver = "psycopg2"
        except Exception as exc:
            raise ConnectorError(
                f"Failed to connect to Redshift: {redact_secrets(str(exc))}"
            ) from exc

    # ------------------------------------------------------------------
    # Query helpers (private)
    # ------------------------------------------------------------------

    def _assert_connected(self) -> None:
        if self._conn is None:
            raise ConnectorError("Not connected — call connect() first")

    def _cursor(self):
        self._assert_connected()
        return self._conn.cursor()

    def _execute(self, sql: str) -> None:
        cur = self._cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def _fetchall(self, sql: str, params: tuple) -> list[tuple]:
        cur = self._cursor()
        try:
            cur.execute(sql, params)
            self._last_description = cur.description
            return cur.fetchall()
        finally:
            cur.close()

    def _fetchone(self, sql: str, params: tuple):
        cur = self._cursor()
        try:
            cur.execute(sql, params)
            self._last_description = cur.description
            return cur.fetchone()
        finally:
            cur.close()

    def _description(self):
        return getattr(self, "_last_description", []) or []


# ---------------------------------------------------------------------------
# URI parsing
# ---------------------------------------------------------------------------


def _parse_redshift_uri(uri: str) -> dict:
    """Parse a ``redshift://`` URI into connection + filter parameters."""
    parsed = urlparse(uri)
    if parsed.scheme not in ("redshift", "redshift+iam"):
        raise ConnectorError(
            "RedshiftConnector URI must start with redshift:// or redshift+iam://"
        )
    host = parsed.hostname
    if not host:
        raise ConnectorError("RedshiftConnector URI must include a host")

    path_parts = [unquote(part) for part in parsed.path.split("/") if part]
    query = {
        key: values[-1]
        for key, values in parse_qs(parsed.query).items()
        if values
    }

    database = path_parts[0] if len(path_parts) >= 1 else query.get("database")
    schema = path_parts[1] if len(path_parts) >= 2 else query.get("schema")

    return {
        "host": host,
        "port": parsed.port or 5439,
        "user": unquote(parsed.username or "") or query.get("user") or query.get("db_user") or None,
        "password": unquote(parsed.password or "") or query.get("password") or None,
        "database": database,
        "schema": schema,
        # IAM fields
        "iam": (
            parsed.scheme == "redshift+iam"
            or query.get("iam", "").lower() in ("true", "1", "yes")
        ),
        "access_key_id": query.get("access_key_id"),
        "secret_access_key": query.get("secret_access_key"),
        "session_token": query.get("session_token"),
        "region": query.get("region"),
        "db_user": query.get("db_user") or unquote(parsed.username or "") or None,
        "cluster_identifier": query.get("cluster_identifier"),
        # Schema filter (comma-separated in query params)
        "include_schemas": query.get("include_schemas", ""),
        "exclude_schemas": query.get("exclude_schemas", ""),
        "include_tables": query.get("include_tables", ""),
        "exclude_tables": query.get("exclude_tables", ""),
    }


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def _split_table(
    table_name: str,
    default_schema: str | None,
) -> tuple[str, str]:
    """Split ``schema.table`` into ``(schema, table)``.  Default to *public*."""
    if "." in table_name:
        parts = table_name.split(".", 1)
        return parts[0], parts[1]
    return default_schema or "public", table_name


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _safe_alias(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value)


def _rows_to_arrow(rows: list[tuple], columns: list[str]) -> pa.Table:
    if not columns:
        return pa.table({})
    values: dict[str, list] = {col: [] for col in columns}
    for row in rows:
        for col, val in zip(columns, row, strict=False):
            values[col].append(val)
    return pa.table(values)


# ---------------------------------------------------------------------------
# Driver import helpers
# ---------------------------------------------------------------------------


def _require_redshift_connector() -> ModuleType:
    mod = _try_import("redshift_connector")
    if mod is None:
        raise ConnectorError(
            "IAM authentication requires the 'redshift-connector' package. "
            "Install it: uv add redshift-connector"
        )
    return mod


def _try_import(module_name: str) -> ModuleType | None:
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None
