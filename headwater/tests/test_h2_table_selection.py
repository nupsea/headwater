"""Browse a source's table corpus and ingest only a selected subset.

Uses a local multi-table DuckDB file as a stand-in for a warehouse — the
connector interface (list_tables / list_columns) is the same, so the
browse-then-ingest-subset flow is exercised end-to-end.
"""

from __future__ import annotations

import duckdb
import pytest

from headwater.core.store import HeadwaterStore
from headwater.services.h2_source import ingest_tables, list_source_tables


@pytest.fixture()
def source(tmp_path):
    """A 3-table DuckDB source registered in a fresh H2 store."""
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE customers AS SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id, name)")
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10.0),(2,20.0)) t(id, amount)")
    con.execute("CREATE TABLE web_events AS SELECT * FROM (VALUES (1,'click')) t(id, kind)")
    con.close()

    store = HeadwaterStore(tmp_path / "h2_metadata.db")
    store.init()
    store.upsert_source("wh", "duckdb", str(db), None)
    try:
        yield store
    finally:
        store.close()


def test_browse_lists_all_tables_without_ingesting(source):
    listed = list_source_tables(source, "wh")
    names = {r["table"] for r in listed}
    assert {"customers", "orders", "web_events"} <= names
    # Nothing ingested yet — browse is metadata-only.
    assert all(r["ingested"] is False for r in listed)
    assert source.get_tables("wh") == []


def test_ingest_subset_only_pulls_selected_tables(source):
    result = ingest_tables(source, "wh", ["customers", "orders"])
    assert set(result["ingested"]) == {"customers", "orders"}
    assert result["profiled"] is True

    ingested = {t["name"] for t in source.get_tables("wh")}
    assert ingested == {"customers", "orders"}  # web_events deliberately excluded

    # Selected tables get columns + profiles; the unselected one gets nothing.
    assert source.get_columns("wh", "customers")
    assert source.get_columns("wh", "web_events") == []
    profiled_cols = {(p["table_name"], p["column_name"]) for p in source.get_profiles("wh")}
    assert ("customers", "id") in profiled_cols
    assert all(t != "web_events" for t, _ in profiled_cols)


def test_browse_flags_already_ingested_tables(source):
    ingest_tables(source, "wh", ["customers"])
    by = {r["table"]: r for r in list_source_tables(source, "wh")}
    assert by["customers"]["ingested"] is True
    assert by["orders"]["ingested"] is False
    assert by["web_events"]["ingested"] is False


def test_ingest_empty_selection_is_a_noop(source):
    result = ingest_tables(source, "wh", [])
    assert result["ingested"] == []
    assert source.get_tables("wh") == []


def test_browse_unknown_source_raises(source):
    with pytest.raises(ValueError, match="not registered"):
        list_source_tables(source, "nope")
