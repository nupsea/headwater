"""Tests for the Redshift connector (unit-level, no live connection)."""

from __future__ import annotations

import pytest

from headwater.connectors.capabilities import REDSHIFT_PREVIEW_CAPABILITIES
from headwater.connectors.redshift_loader import (
    RedshiftConnector,
    _parse_redshift_uri,
    _split_table,
)
from headwater.connectors.registry import CONNECTOR_CATALOG, get_connector
from headwater.connectors.schema_filter import SchemaTableFilter
from headwater.core.exceptions import ConnectorError


# ---------------------------------------------------------------------------
# URI parsing
# ---------------------------------------------------------------------------


class TestParseRedshiftUri:
    def test_basic_userpass(self):
        parts = _parse_redshift_uri(
            "redshift://admin:secret@my-cluster.abc.us-east-1.redshift.amazonaws.com:5439/analytics"
        )
        assert parts["host"] == "my-cluster.abc.us-east-1.redshift.amazonaws.com"
        assert parts["port"] == 5439
        assert parts["user"] == "admin"
        assert parts["password"] == "secret"
        assert parts["database"] == "analytics"
        assert parts["iam"] is False

    def test_with_schema_path(self):
        parts = _parse_redshift_uri(
            "redshift://admin:secret@host:5439/mydb/analytics_schema"
        )
        assert parts["database"] == "mydb"
        assert parts["schema"] == "analytics_schema"

    def test_iam_via_query_param(self):
        parts = _parse_redshift_uri(
            "redshift://host:5439/mydb?iam=true&access_key_id=AKIA123&secret_access_key=SK123&region=us-east-1&db_user=admin"
        )
        assert parts["iam"] is True
        assert parts["access_key_id"] == "AKIA123"
        assert parts["secret_access_key"] == "SK123"
        assert parts["region"] == "us-east-1"
        assert parts["db_user"] == "admin"

    def test_iam_via_scheme(self):
        parts = _parse_redshift_uri(
            "redshift+iam://host:5439/mydb?access_key_id=AK&secret_access_key=SK&region=eu-west-1"
        )
        assert parts["iam"] is True
        assert parts["access_key_id"] == "AK"

    def test_schema_filter_in_query(self):
        parts = _parse_redshift_uri(
            "redshift://admin:pw@host:5439/db?include_schemas=analytics,reporting&exclude_tables=*_tmp"
        )
        assert parts["include_schemas"] == "analytics,reporting"
        assert parts["exclude_tables"] == "*_tmp"

    def test_default_port(self):
        parts = _parse_redshift_uri("redshift://admin:pw@host/mydb")
        assert parts["port"] == 5439

    def test_invalid_scheme_raises(self):
        with pytest.raises(ConnectorError, match="must start with redshift://"):
            _parse_redshift_uri("postgresql://host/db")

    def test_missing_host_raises(self):
        with pytest.raises(ConnectorError, match="must include a host"):
            _parse_redshift_uri("redshift:///db")


# ---------------------------------------------------------------------------
# Table splitting
# ---------------------------------------------------------------------------


class TestSplitTable:
    def test_schema_dot_table(self):
        assert _split_table("analytics.users", None) == ("analytics", "users")

    def test_bare_table_with_default(self):
        assert _split_table("users", "analytics") == ("analytics", "users")

    def test_bare_table_no_default(self):
        assert _split_table("users", None) == ("public", "users")


# ---------------------------------------------------------------------------
# Connector wiring
# ---------------------------------------------------------------------------


class TestRedshiftConnectorWiring:
    def test_catalog_entry_is_preview(self):
        entry = next(c for c in CONNECTOR_CATALOG if c["id"] == "redshift")
        assert entry["status"] == "preview"
        assert entry["supported"] is True
        assert entry["category"] == "Warehouse"

    def test_registry_returns_redshift_connector(self):
        connector = get_connector("redshift")
        assert isinstance(connector, RedshiftConnector)

    def test_capabilities(self):
        connector = RedshiftConnector()
        caps = connector.capabilities()
        assert caps == REDSHIFT_PREVIEW_CAPABILITIES
        assert caps.list_schemas is True
        assert caps.list_tables is True
        assert caps.profile_table is True
        assert caps.sample_arrow is True
        assert caps.execute_readonly is True
        assert caps.load_to_duckdb is False
        assert "observe" in caps.modes
        assert "generate" in caps.modes

    def test_load_to_duckdb_raises(self):
        connector = RedshiftConnector()
        with pytest.raises(NotImplementedError):
            connector.load_to_duckdb(None, "schema")

    def test_not_connected_raises(self):
        connector = RedshiftConnector()
        with pytest.raises(ConnectorError, match="Not connected"):
            connector.list_tables()


# ---------------------------------------------------------------------------
# Schema filter integration
# ---------------------------------------------------------------------------


class TestRedshiftSchemaFilter:
    def test_set_schema_filter(self):
        connector = RedshiftConnector()
        connector.set_schema_filter({
            "include_schemas": ["analytics", "reporting"],
            "exclude_tables": ["*_tmp"],
        })
        assert not connector._schema_filter.is_empty
        assert connector._schema_filter.accepts_schema("analytics")
        assert not connector._schema_filter.accepts_schema("staging")
        assert connector._schema_filter.accepts_table("dim_customers")
        assert not connector._schema_filter.accepts_table("orders_tmp")

    def test_set_schema_filter_empty(self):
        connector = RedshiftConnector()
        connector.set_schema_filter({})
        assert connector._schema_filter.is_empty

    def test_set_schema_filter_none(self):
        connector = RedshiftConnector()
        connector.set_schema_filter(None)
        assert connector._schema_filter.is_empty

    def test_uri_schema_filter_applied_during_connect(self):
        """Verify that filter params in the URI are parsed (connect will fail
        without a real server, but the filter state should be set before the
        connection attempt)."""
        connector = RedshiftConnector()
        # We can't call connect() without a real server, but we can verify
        # the URI parsing extracts filter params correctly.
        from headwater.connectors.redshift_loader import _parse_redshift_uri
        parts = _parse_redshift_uri(
            "redshift://u:p@host/db?include_schemas=a,b&exclude_schemas=c"
        )
        sf = SchemaTableFilter.from_query_params({
            k: parts[k] for k in ("include_schemas", "exclude_schemas",
                                   "include_tables", "exclude_tables")
            if parts.get(k)
        })
        assert sf.accepts_schema("a")
        assert sf.accepts_schema("b")
        assert not sf.accepts_schema("c")
        assert not sf.accepts_schema("other")

    def test_list_tables_applies_schema_filters_before_public_name_formatting(self):
        connector = RedshiftConnector()
        connector._conn = object()
        connector._schema_filter = SchemaTableFilter(include_schemas=["analytics"])
        connector._fetchall = lambda sql, params: [
            ("public", "users"),
            ("analytics", "orders"),
            ("staging", "events"),
        ]

        assert connector.list_tables() == ["analytics.orders"]
