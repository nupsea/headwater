"""Snowflake connector unit tests (no live account needed).

Verifies the pieces the H2 ingestion/query flow depends on: URI parsing
(account/database/schema/warehouse/role), table-name formatting/splitting, and
that profile SQL uses NATIVE min/max for numeric/temporal types (varchar-cast
min/max is lexicographic — "9" > "19" — the bug fixed for Redshift too).
"""

from __future__ import annotations

import pytest

from headwater.connectors.snowflake_loader import (
    SnowflakeConnector,
    _format_table,
    _orders_natively,
    _parse_snowflake_uri,
    _split_table,
)
from headwater.core.exceptions import ConnectorError


def test_parse_uri_full_form():
    parts = _parse_snowflake_uri(
        "snowflake://USER:p%40ss@myorg-acct.snowflakecomputing.com/ANALYTICS/PUBLIC"
        "?warehouse=COMPUTE_WH&role=ANALYST"
    )
    assert parts["account"] == "myorg-acct.snowflakecomputing.com"
    assert parts["user"] == "USER"
    assert parts["password"] == "p@ss"
    assert parts["database"] == "ANALYTICS"
    assert parts["schema"] == "PUBLIC"
    assert parts["warehouse"] == "COMPUTE_WH"
    assert parts["role"] == "ANALYST"


def test_parse_uri_rejects_wrong_scheme():
    with pytest.raises(ConnectorError, match="snowflake://"):
        _parse_snowflake_uri("postgres://host/db")


def test_split_and_format_table_roundtrip():
    assert _split_table("orders", "ANALYTICS", "PUBLIC") == (
        "ANALYTICS", "PUBLIC", "ORDERS",
    )
    assert _split_table("sales.orders", "ANALYTICS", None) == (
        "ANALYTICS", "SALES", "ORDERS",
    )
    assert _split_table("OTHER.SALES.ORDERS", None, None) == (
        "OTHER", "SALES", "ORDERS",
    )
    # Names are emitted relative to the connection defaults.
    assert _format_table("ANALYTICS", "PUBLIC", "ORDERS", "ANALYTICS", "PUBLIC") == "ORDERS"
    assert _format_table("ANALYTICS", "SALES", "ORDERS", "ANALYTICS", "PUBLIC") == "SALES.ORDERS"
    assert (
        _format_table("OTHER", "SALES", "ORDERS", "ANALYTICS", "PUBLIC")
        == "OTHER.SALES.ORDERS"
    )


def test_orders_natively_by_type_family():
    assert _orders_natively("NUMBER(38,2)")
    assert _orders_natively("FLOAT")
    assert _orders_natively("DATE")
    assert _orders_natively("TIMESTAMP_NTZ")
    assert not _orders_natively("VARCHAR(255)")
    assert not _orders_natively("TEXT")


class _RecordingCursor:
    """Captures executed SQL and returns a canned profile row."""

    def __init__(self, log: list[str]) -> None:
        self._log = log
        self.description = []
        self.sfqid = "q1"

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, params=()):
        self._log.append(sql)
        # _total_rows + 4 aggregates per column x 2 columns
        self.description = [
            ("_total_rows",), ("_nn_amount",), ("_min_amount",), ("_max_amount",),
            ("_dist_amount",), ("_nn_status",), ("_min_status",), ("_max_status",),
            ("_dist_status",),
        ]

    def fetchone(self):
        return (100, 100, 1.5, 99.5, 40, 100, "active", "paused", 3)

    def fetchall(self):
        return []


def test_profile_sql_uses_native_minmax_for_numbers(monkeypatch):
    con = SnowflakeConnector()
    con._conn = object()  # bypass connect
    con._database, con._schema = "ANALYTICS", "PUBLIC"
    executed: list[str] = []
    monkeypatch.setattr(con, "_cursor", lambda: _RecordingCursor(executed))
    monkeypatch.setattr(
        con,
        "list_columns",
        lambda _t: [
            {"name": "amount", "data_type": "NUMBER(10,2)", "is_nullable": True,
             "ordinal_position": 0},
            {"name": "status", "data_type": "VARCHAR(20)", "is_nullable": True,
             "ordinal_position": 1},
        ],
    )

    stats = con.profile("orders")

    sql = executed[-1]
    assert 'MIN("amount")' in sql and 'MAX("amount")' in sql  # native for NUMBER
    assert 'MIN(TO_VARCHAR("status"))' in sql  # cast only for text
    assert stats["amount"]["min"] == 1.5 and stats["amount"]["max"] == 99.5
    assert stats["amount"]["row_count"] == 100
