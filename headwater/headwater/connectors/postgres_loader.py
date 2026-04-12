"""Postgres source connector -- generate mode, pushdown profiling, no bulk copy."""

from __future__ import annotations

import logging
from urllib.parse import urlparse

import pyarrow as pa

from headwater.core.exceptions import ConnectorError, HeadwaterConnectionError
from headwater.core.models import SourceConfig

logger = logging.getLogger(__name__)

# psycopg2 is an optional runtime dependency (not imported at module top level to
# allow the test suite to import this module even when psycopg2 is absent).
try:
    import psycopg2
    import psycopg2.errors
    import psycopg2.extensions

    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False

# Schemas to exclude from table listing
_EXCLUDED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}


def _require_psycopg2() -> None:
    if not _PSYCOPG2_AVAILABLE:
        raise ConnectorError(
            "psycopg2-binary is not installed. Run: uv add psycopg2-binary"
        )


def _parse_dsn_parts(dsn: str) -> dict[str, str]:
    """Extract host, port, user, dbname from a DSN string for error messages."""
    try:
        parsed = urlparse(dsn)
        return {
            "host": parsed.hostname or "localhost",
            "port": str(parsed.port or 5432),
            "user": parsed.username or "",
            "dbname": parsed.path.lstrip("/") if parsed.path else "",
        }
    except Exception:
        return {"host": "unknown", "port": "5432", "user": "unknown", "dbname": "unknown"}


def _wrap_operational_error(exc: Exception, dsn: str) -> HeadwaterConnectionError:
    """Convert a psycopg2 OperationalError into a HeadwaterConnectionError."""
    parts = _parse_dsn_parts(dsn)
    msg = str(exc).lower()

    if "could not connect" in msg or "connection refused" in msg or "could not translate" in msg:
        return HeadwaterConnectionError(
            f"Cannot reach Postgres at {parts['host']}:{parts['port']}. Check host and port."
        )
    if "password authentication failed" in msg or "authentication failed" in msg:
        return HeadwaterConnectionError(
            f"Authentication failed for user '{parts['user']}' on database '{parts['dbname']}'."
        )
    if "database" in msg and "does not exist" in msg:
        return HeadwaterConnectionError(
            f"Database '{parts['dbname']}' does not exist on {parts['host']}."
        )
    # General fallback
    return HeadwaterConnectionError(str(exc))


class PostgresConnector:
    """Postgres source connector -- generate mode.

    Profiling runs aggregate SQL directly in Postgres (no bulk copy).
    Only a small sample (default 10k rows) is fetched locally for
    validating generated SQL models.

    Implements the BaseConnector interface.
    """

    def __init__(self) -> None:
        self._dsn: str = ""
        self._conn: psycopg2.extensions.connection | None = None  # type: ignore[name-defined]

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    def connect(self, config: SourceConfig) -> None:
        """Open a psycopg2 connection; raises HeadwaterConnectionError on failure."""
        _require_psycopg2()
        if config.uri is None:
            raise ConnectorError("PostgresConnector requires a URI (config.uri)")
        self._dsn = config.uri
        try:
            self._conn = psycopg2.connect(self._dsn)
        except psycopg2.OperationalError as exc:
            raise _wrap_operational_error(exc, self._dsn) from exc

    def list_tables(self) -> list[str]:
        """Return all user tables as 'schema.table' (or just 'table' for public schema).

        Tables where the current user lacks SELECT permission are logged as
        warnings and skipped (the discovery run continues).
        """
        self._assert_connected()
        query = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN %s
            ORDER BY table_schema, table_name
        """
        with self._conn.cursor() as cur:  # type: ignore[union-attr]
            cur.execute(query, (tuple(_EXCLUDED_SCHEMAS),))
            rows = cur.fetchall()

        result = []
        for schema, table in rows:
            # Check SELECT privilege; skip tables without access
            qualified = f'"{schema}"."{table}"'
            try:
                with self._conn.cursor() as check_cur:  # type: ignore[union-attr]
                    check_cur.execute(f"SELECT 1 FROM {qualified} LIMIT 0")
            except Exception as exc:
                msg = str(exc).lower()
                if "permission denied" in msg:
                    full_name = table if schema == "public" else f"{schema}.{table}"
                    logger.warning(
                        "Permission denied on table '%s' -- skipping. "
                        "Grant SELECT to include this table.",
                        full_name,
                    )
                    self._conn.rollback()  # type: ignore[union-attr]
                    continue
                # Re-raise non-permission errors
                raise
            if schema == "public":
                result.append(table)
            else:
                result.append(f"{schema}.{table}")
        return result

    def get_column_info(self, table_name: str) -> list[dict]:
        """Return column info for a table: [{name, data_type, is_nullable, ordinal_position}]."""
        self._assert_connected()
        schema, tname = _split_table(table_name)
        query = """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        with self._conn.cursor() as cur:  # type: ignore[union-attr]
            cur.execute(query, (schema, tname))
            rows = cur.fetchall()

        return [
            {
                "name": row[0],
                "data_type": row[1],
                "is_nullable": row[2] == "YES",
                "ordinal_position": row[3],
            }
            for row in rows
        ]

    def profile(self, table_name: str) -> dict:
        """Run a single aggregate SQL query in Postgres and return column-level stats.

        No rows are transferred -- only aggregates.
        If permission is denied on the table, logs a warning and returns empty stats.

        Returns:
            dict keyed by column name. Each value has keys:
            row_count, non_null, null_count, min, max, distinct_count.
        """
        self._assert_connected()
        try:
            col_info = self.get_column_info(table_name)
        except Exception as exc:
            if "permission denied" in str(exc).lower():
                logger.warning(
                    "Permission denied profiling table '%s' -- skipping.",
                    table_name,
                )
                self._conn.rollback()  # type: ignore[union-attr]
                return {}
            raise
        if not col_info:
            return {}

        schema, tname = _split_table(table_name)
        qualified = f'"{schema}"."{tname}"'

        # Build aggregate expressions for every column in one query
        selects = ["COUNT(*) AS _total_rows"]
        for col in col_info:
            cname = col["name"]
            safe = f'"{cname}"'
            selects += [
                f"COUNT({safe}) AS _nn_{cname}",
                f"MIN({safe}::text) AS _min_{cname}",
                f"MAX({safe}::text) AS _max_{cname}",
                f"COUNT(DISTINCT {safe}) AS _dist_{cname}",
            ]

        sql = f"SELECT {', '.join(selects)} FROM {qualified}"

        with self._conn.cursor() as cur:  # type: ignore[union-attr]
            cur.execute(sql)
            row = cur.fetchone()
            col_names = [desc[0] for desc in cur.description]

        if row is None:
            return {}

        result_dict = dict(zip(col_names, row, strict=True))
        row_count = result_dict["_total_rows"]

        stats: dict[str, dict] = {}
        for col in col_info:
            cname = col["name"]
            non_null = result_dict.get(f"_nn_{cname}", 0) or 0
            stats[cname] = {
                "row_count": row_count,
                "non_null": non_null,
                "null_count": row_count - non_null,
                "min": result_dict.get(f"_min_{cname}"),
                "max": result_dict.get(f"_max_{cname}"),
                "distinct_count": result_dict.get(f"_dist_{cname}", 0),
            }
        return stats

    def sample(self, table_name: str, n: int = 10_000) -> pa.Table:
        """Fetch up to N rows from the table and return as a PyArrow table.

        Used only for local DuckDB validation of generated SQL models.
        No full table transfer -- row-limited via LIMIT clause.
        """
        self._assert_connected()
        schema, tname = _split_table(table_name)
        qualified = f'"{schema}"."{tname}"'

        with self._conn.cursor() as cur:  # type: ignore[union-attr]
            cur.execute(f"SELECT * FROM {qualified} LIMIT %s", (n,))
            rows = cur.fetchall()
            col_names = [desc[0] for desc in (cur.description or [])]

        if not col_names:
            return pa.table({})

        # Build column-oriented structure for PyArrow
        columns: dict[str, list] = {name: [] for name in col_names}
        for row in rows:
            for name, val in zip(col_names, row, strict=True):
                columns[name].append(val)

        arrays = {name: pa.array(vals) for name, vals in columns.items()}
        return pa.table(arrays)

    def load_to_duckdb(self, con, schema: str) -> list[str]:  # type: ignore[override]
        """Not used in Postgres connector (pushdown profiling only).

        Postgres connector does not bulk-copy tables into DuckDB.
        Use sample() for local Arrow-based validation instead.
        """
        raise NotImplementedError(
            "PostgresConnector does not support load_to_duckdb. "
            "Use profile() for in-place stats and sample() for Arrow batch validation."
        )

    def close(self) -> None:
        """Close the psycopg2 connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _assert_connected(self) -> None:
        if self._conn is None:
            raise ConnectorError("Not connected -- call connect() first")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _split_table(table_name: str) -> tuple[str, str]:
    """Split 'schema.table' into (schema, table). Defaults schema to 'public'."""
    if "." in table_name:
        parts = table_name.split(".", 1)
        return parts[0], parts[1]
    return "public", table_name
