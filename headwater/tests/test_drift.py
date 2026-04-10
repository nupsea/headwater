"""Tests for schema drift detection (US-401, US-402)."""

from __future__ import annotations

import pytest

from headwater.core.metadata import MetadataStore
from headwater.drift.schema import compare_schemas

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def meta() -> MetadataStore:
    store = MetadataStore(":memory:")
    store.init()
    return store


def _make_snapshot(tables: dict) -> dict:
    """Build a minimal snapshot dict for testing."""
    return tables


# ---------------------------------------------------------------------------
# compare_schemas tests
# ---------------------------------------------------------------------------


def test_identical_snapshots_no_changes():
    """Identical snapshots return no_changes=True."""
    snap = {
        "orders": {
            "columns": [
                {"name": "order_id", "dtype": "int64", "nullable": False},
                {"name": "amount", "dtype": "float64", "nullable": True},
            ],
            "row_count": 100,
        }
    }
    diff = compare_schemas(snap, snap, "src", run_id_from=1, run_id_to=2)
    assert diff.no_changes is True
    assert diff.tables_added == []
    assert diff.tables_removed == []
    assert diff.tables_changed == []


def test_added_column_detected():
    """Adding a column to an existing table is detected as ColumnChange(type=added)."""
    snap_a = {
        "orders": {
            "columns": [{"name": "order_id", "dtype": "int64", "nullable": False}],
            "row_count": 100,
        }
    }
    snap_b = {
        "orders": {
            "columns": [
                {"name": "order_id", "dtype": "int64", "nullable": False},
                {"name": "created_at", "dtype": "timestamp", "nullable": True},
            ],
            "row_count": 150,
        }
    }
    diff = compare_schemas(snap_a, snap_b, "src", run_id_from=1, run_id_to=2)
    assert diff.no_changes is False
    assert len(diff.tables_changed) == 1
    table_change = diff.tables_changed[0]
    assert table_change.table_name == "orders"
    assert table_change.change_type == "columns_changed"
    added = [c for c in table_change.column_changes if c.change_type == "added"]
    assert len(added) == 1
    assert added[0].column_name == "created_at"
    assert added[0].after == "timestamp"


def test_removed_column_detected():
    """Removing a column is detected as ColumnChange(type=removed)."""
    snap_a = {
        "orders": {
            "columns": [
                {"name": "order_id", "dtype": "int64", "nullable": False},
                {"name": "legacy_field", "dtype": "text", "nullable": True},
            ],
            "row_count": 100,
        }
    }
    snap_b = {
        "orders": {
            "columns": [{"name": "order_id", "dtype": "int64", "nullable": False}],
            "row_count": 100,
        }
    }
    diff = compare_schemas(snap_a, snap_b, "src", run_id_from=1, run_id_to=2)
    assert diff.no_changes is False
    table_change = diff.tables_changed[0]
    removed = [c for c in table_change.column_changes if c.change_type == "removed"]
    assert len(removed) == 1
    assert removed[0].column_name == "legacy_field"
    assert removed[0].before == "text"
    assert removed[0].after is None


def test_type_changed_detected():
    """Changing a column dtype is detected as type_changed."""
    snap_a = {
        "products": {
            "columns": [{"name": "price", "dtype": "int64", "nullable": True}],
            "row_count": 50,
        }
    }
    snap_b = {
        "products": {
            "columns": [{"name": "price", "dtype": "float64", "nullable": True}],
            "row_count": 50,
        }
    }
    diff = compare_schemas(snap_a, snap_b, "src", run_id_from=1, run_id_to=2)
    assert diff.no_changes is False
    table_change = diff.tables_changed[0]
    type_changes = [c for c in table_change.column_changes if c.change_type == "type_changed"]
    assert len(type_changes) == 1
    assert type_changes[0].before == "int64"
    assert type_changes[0].after == "float64"


def test_nullability_changed_detected():
    """Changing nullability is detected as nullability_changed."""
    snap_a = {
        "users": {
            "columns": [{"name": "email", "dtype": "text", "nullable": True}],
            "row_count": 200,
        }
    }
    snap_b = {
        "users": {
            "columns": [{"name": "email", "dtype": "text", "nullable": False}],
            "row_count": 200,
        }
    }
    diff = compare_schemas(snap_a, snap_b, "src", run_id_from=1, run_id_to=2)
    assert diff.no_changes is False
    table_change = diff.tables_changed[0]
    null_changes = [
        c for c in table_change.column_changes if c.change_type == "nullability_changed"
    ]
    assert len(null_changes) == 1
    assert null_changes[0].before == "True"
    assert null_changes[0].after == "False"


def test_table_removed():
    """A table present in A but absent in B is listed in tables_removed."""
    _col = [{"name": "id", "dtype": "int64", "nullable": False}]
    snap_a = {
        "orders": {"columns": _col, "row_count": 10},
        "archived": {"columns": _col, "row_count": 5},
    }
    snap_b = {
        "orders": {"columns": _col, "row_count": 10},
    }
    diff = compare_schemas(snap_a, snap_b, "src", run_id_from=1, run_id_to=2)
    assert diff.no_changes is False
    assert "archived" in diff.tables_removed
    assert diff.tables_changed == []


def test_table_added():
    """A table in B but absent in A is listed in tables_added."""
    snap_a = {
        "orders": {"columns": [], "row_count": 0},
    }
    snap_b = {
        "orders": {"columns": [], "row_count": 0},
        "new_table": {
            "columns": [{"name": "id", "dtype": "int64", "nullable": False}],
            "row_count": 1,
        },
    }
    diff = compare_schemas(snap_a, snap_b, "src", run_id_from=1, run_id_to=2)
    assert diff.no_changes is False
    assert "new_table" in diff.tables_added


def test_first_run_snapshot_a_none():
    """snapshot_a=None means first run: all tables in B appear as tables_added."""
    snap_b = {
        "orders": {
            "columns": [{"name": "id", "dtype": "int64", "nullable": False}],
            "row_count": 10,
        },
        "users": {
            "columns": [{"name": "user_id", "dtype": "int64", "nullable": False}],
            "row_count": 5,
        },
    }
    diff = compare_schemas(None, snap_b, "src", run_id_from=None, run_id_to=1)
    assert diff.no_changes is False
    assert diff.run_id_from is None
    assert set(diff.tables_added) == {"orders", "users"}
    assert diff.tables_removed == []
    assert diff.tables_changed == []


def test_diff_has_correct_metadata():
    """SchemaDiff contains correct source_name, run_ids, and detected_at."""
    snap = {"t": {"columns": [], "row_count": 0}}
    diff = compare_schemas(snap, snap, "my_source", run_id_from=3, run_id_to=7)
    assert diff.source_name == "my_source"
    assert diff.run_id_from == 3
    assert diff.run_id_to == 7
    assert diff.detected_at  # non-empty ISO timestamp


# ---------------------------------------------------------------------------
# MetadataStore snapshot / drift report round-trip tests
# ---------------------------------------------------------------------------


def test_snapshot_save_and_retrieve(meta: MetadataStore):
    """Snapshot saved for a run is retrieved correctly via get_latest_snapshot."""
    meta.upsert_source("src", "json", "/data", None)
    run1 = meta.start_run("src")
    meta.finish_run(run1, table_count=2)

    snap = {
        "orders": {
            "columns": [{"name": "id", "dtype": "int64", "nullable": False}],
            "row_count": 10,
        }
    }
    meta.save_snapshot(run1, "src", snap)

    # Start a second run so we can fetch snapshot "before run 2"
    run2 = meta.start_run("src")
    meta.finish_run(run2, table_count=2)

    retrieved = meta.get_latest_snapshot("src", before_run_id=run2)
    assert retrieved is not None
    assert "orders" in retrieved
    assert retrieved["orders"]["row_count"] == 10


def test_get_latest_snapshot_none_on_first_run(meta: MetadataStore):
    """get_latest_snapshot returns None when no prior snapshot exists."""
    meta.upsert_source("src", "json", "/data", None)
    run_id = meta.start_run("src")
    retrieved = meta.get_latest_snapshot("src", before_run_id=run_id)
    assert retrieved is None


def test_get_latest_snapshot_returns_most_recent(meta: MetadataStore):
    """With multiple snapshots, get_latest_snapshot returns the most recent before run_id."""
    meta.upsert_source("src", "json", "/data", None)

    run1 = meta.start_run("src")
    meta.finish_run(run1, table_count=1)
    meta.save_snapshot(run1, "src", {"v": 1})

    run2 = meta.start_run("src")
    meta.finish_run(run2, table_count=1)
    meta.save_snapshot(run2, "src", {"v": 2})

    run3 = meta.start_run("src")
    meta.finish_run(run3, table_count=1)

    # Before run3, most recent snapshot should be from run2
    snap = meta.get_latest_snapshot("src", before_run_id=run3)
    assert snap == {"v": 2}


def test_drift_report_save_and_retrieve(meta: MetadataStore):
    """Drift report saved is retrieved by get_latest_drift_report."""
    meta.upsert_source("src", "json", "/data", None)
    run1 = meta.start_run("src")
    meta.finish_run(run1, table_count=1)
    run2 = meta.start_run("src")
    meta.finish_run(run2, table_count=1)

    diff_data = {"tables_added": ["new_table"], "no_changes": False}
    report_id = meta.save_drift_report("src", run1, run2, diff_data)
    assert report_id > 0

    report = meta.get_latest_drift_report("src")
    assert report is not None
    assert report["source_name"] == "src"
    assert report["run_id_from"] == run1
    assert report["run_id_to"] == run2
    assert report["diff"]["tables_added"] == ["new_table"]
    assert report["acknowledged"] == 0


def test_drift_report_acknowledge(meta: MetadataStore):
    """Acknowledging a drift report sets acknowledged=1."""
    meta.upsert_source("src", "json", "/data", None)
    run1 = meta.start_run("src")
    meta.finish_run(run1, table_count=1)
    run2 = meta.start_run("src")
    meta.finish_run(run2, table_count=1)

    report_id = meta.save_drift_report("src", run1, run2, {"no_changes": True})
    meta.acknowledge_drift_report(report_id)

    row = meta.con.execute(
        "SELECT acknowledged FROM drift_reports WHERE id = ?", (report_id,)
    ).fetchone()
    assert row is not None
    assert row["acknowledged"] == 1


def test_drift_report_get_latest_unfiltered(meta: MetadataStore):
    """get_latest_drift_report with no source filter returns the most recent report."""
    meta.upsert_source("src1", "json", "/data", None)
    meta.upsert_source("src2", "json", "/data2", None)
    run1 = meta.start_run("src1")
    meta.finish_run(run1, table_count=1)
    run2 = meta.start_run("src2")
    meta.finish_run(run2, table_count=1)

    meta.save_drift_report("src1", None, run1, {"no_changes": True})
    meta.save_drift_report("src2", None, run2, {"no_changes": False})

    report = meta.get_latest_drift_report()
    assert report is not None
    assert report["source_name"] == "src2"


def test_init_creates_snapshot_and_drift_tables(meta: MetadataStore):
    """schema_snapshots and drift_reports tables exist after init."""
    tables = meta.con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in tables}
    assert "schema_snapshots" in names
    assert "drift_reports" in names
