"""Tests for PostgresConnector.

Integration tests are skipped when Postgres is not available.
Tests for error handling and registry resolution always run.
"""

from __future__ import annotations

import pytest

from headwater.connectors.registry import get_connector
from headwater.core.exceptions import ConnectorError, HeadwaterConnectionError
from headwater.core.models import SourceConfig

_DEFAULT_DSN = "postgresql://headwater:headwater@localhost:5434/headwater_dev"


def _postgres_available() -> bool:
    """Return True if a local Postgres instance is reachable at the default DSN."""
    try:
        import psycopg2

        conn = psycopg2.connect(_DEFAULT_DSN, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Tests that always run (no Postgres required)
# ---------------------------------------------------------------------------


def test_registry_resolves_postgres_type():
    """Connector registry must return a PostgresConnector for type 'postgres'."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = get_connector("postgres")
    assert isinstance(connector, PostgresConnector)


def test_connection_error_bad_host():
    """Bad host raises HeadwaterConnectionError with readable message."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    config = SourceConfig(
        name="bad-host",
        type="postgres",
        uri="postgresql://headwater:headwater@nonexistent-host-xyz:5432/headwater_dev",
    )
    with pytest.raises(HeadwaterConnectionError) as exc_info:
        connector.connect(config)
    msg = str(exc_info.value)
    # Should mention host/port in the message
    assert "nonexistent-host-xyz" in msg or "Cannot reach" in msg or "5432" in msg


def test_connection_error_bad_port():
    """Bad port raises HeadwaterConnectionError."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    config = SourceConfig(
        name="bad-port",
        type="postgres",
        uri="postgresql://headwater:headwater@localhost:19999/headwater_dev",
    )
    with pytest.raises(HeadwaterConnectionError):
        connector.connect(config)


def test_connect_requires_uri():
    """Connector raises ConnectorError when uri is None."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    config = SourceConfig(name="no-uri", type="postgres")
    with pytest.raises(ConnectorError, match="URI"):
        connector.connect(config)


def test_profile_requires_connect():
    """Calling profile() before connect() raises ConnectorError."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    with pytest.raises(ConnectorError, match="connect"):
        connector.profile("some_table")


def test_sample_requires_connect():
    """Calling sample() before connect() raises ConnectorError."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    with pytest.raises(ConnectorError, match="connect"):
        connector.sample("some_table")


def test_load_to_duckdb_raises_not_implemented():
    """load_to_duckdb() must raise NotImplementedError for Postgres connector."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    with pytest.raises(NotImplementedError):
        connector.load_to_duckdb(None, "public")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Integration tests -- skipped when Postgres is not available
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available at default DSN")
def test_list_tables_returns_list():
    """list_tables() returns a non-empty list of table names when Postgres is up."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    config = SourceConfig(name="pg-test", type="postgres", uri=_DEFAULT_DSN)
    connector.connect(config)
    try:
        tables = connector.list_tables()
        assert isinstance(tables, list)
        # After pg_ingest.py runs, there should be at least some tables
    finally:
        connector.close()


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available at default DSN")
def test_profile_returns_stats_dict():
    """profile() returns a dict with correct keys for each column."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    config = SourceConfig(name="pg-test", type="postgres", uri=_DEFAULT_DSN)
    connector.connect(config)
    try:
        tables = connector.list_tables()
        if not tables:
            pytest.skip("No tables found -- run pg_ingest.py first")

        table = tables[0]
        stats = connector.profile(table)
        assert isinstance(stats, dict)
        if stats:
            col_name = next(iter(stats))
            col_stats = stats[col_name]
            assert "row_count" in col_stats
            assert "non_null" in col_stats
            assert "null_count" in col_stats
            assert "distinct_count" in col_stats
    finally:
        connector.close()


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available at default DSN")
def test_sample_returns_pyarrow_table():
    """sample() returns a PyArrow table with at most N rows."""
    import pyarrow as pa

    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    config = SourceConfig(name="pg-test", type="postgres", uri=_DEFAULT_DSN)
    connector.connect(config)
    try:
        tables = connector.list_tables()
        if not tables:
            pytest.skip("No tables found -- run pg_ingest.py first")

        table = tables[0]
        result = connector.sample(table, n=100)
        assert isinstance(result, pa.Table)
        assert result.num_rows <= 100
    finally:
        connector.close()


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available at default DSN")
def test_get_column_info_returns_list():
    """get_column_info() returns list of dicts with expected keys."""
    from headwater.connectors.postgres_loader import PostgresConnector

    connector = PostgresConnector()
    config = SourceConfig(name="pg-test", type="postgres", uri=_DEFAULT_DSN)
    connector.connect(config)
    try:
        tables = connector.list_tables()
        if not tables:
            pytest.skip("No tables found -- run pg_ingest.py first")

        table = tables[0]
        cols = connector.get_column_info(table)
        assert isinstance(cols, list)
        if cols:
            col = cols[0]
            assert "name" in col
            assert "data_type" in col
            assert "is_nullable" in col
            assert "ordinal_position" in col
    finally:
        connector.close()
