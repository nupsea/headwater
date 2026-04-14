"""Tests for re-run additive updates (US-203)."""

from __future__ import annotations

import pytest

from headwater.core.metadata import MetadataStore


@pytest.fixture()
def meta() -> MetadataStore:
    store = MetadataStore(":memory:")
    store.init()
    return store


class TestAdditiveUpdates:
    """US-203: Re-running discovery adds new tables/columns, updates changed ones."""

    def test_upsert_table_preserves_locked_description(self, meta: MetadataStore):
        """Existing locked description not reset on re-insert."""
        meta.upsert_source("src", "json", "/data", None)
        run1 = meta.start_run("src")
        meta.upsert_table("orders", "src", row_count=100, description="auto desc", run_id=run1)
        meta.finish_run(run1, table_count=1)

        # Lock the table
        meta.con.execute(
            "UPDATE tables SET locked = 1, locked_at = datetime('now'), "
            "description = 'human desc' WHERE name = 'orders' AND source_name = 'src'"
        )
        meta.con.commit()

        # Re-run: upsert with new description -- should NOT overwrite locked
        run2 = meta.start_run("src")
        meta.upsert_table("orders", "src", row_count=150, description="new auto desc", run_id=run2)
        meta.finish_run(run2, table_count=1)

        tables = meta.get_tables("src")
        orders = next(t for t in tables if t["name"] == "orders")
        assert orders["description"] == "human desc"
        assert orders["row_count"] == 150  # Non-locked field updated

    def test_upsert_column_preserves_locked_description(self, meta: MetadataStore):
        """Existing locked column description not reset on re-insert."""
        meta.upsert_source("src", "json", "/data", None)
        meta.upsert_table("orders", "src", row_count=100)
        meta.upsert_column("orders", "src", "order_id", "int64", description="auto desc")

        # Lock the column
        meta.lock_column("orders", "src", "order_id", locked=True, description="human desc")

        # Re-run: upsert with new description -- should NOT overwrite locked
        meta.upsert_column("orders", "src", "order_id", "int64", description="new auto desc")

        cols = meta.get_columns("orders", "src")
        col = next(c for c in cols if c["name"] == "order_id")
        assert col["description"] == "human desc"
        assert col["locked"] == 1

    def test_removed_tables_marked_not_deleted(self, meta: MetadataStore):
        """Removed tables are marked with removed_in_run_id, not deleted."""
        meta.upsert_source("src", "json", "/data", None)
        run1 = meta.start_run("src")
        meta.upsert_table("orders", "src", row_count=100, run_id=run1)
        meta.upsert_table("users", "src", row_count=50, run_id=run1)
        meta.finish_run(run1, table_count=2)

        # Re-run: only "orders" discovered
        run2 = meta.start_run("src")
        meta.upsert_table("orders", "src", row_count=110, run_id=run2)
        removed = meta.mark_removed_tables("src", ["orders"], run2)
        meta.finish_run(run2, table_count=1)

        assert "users" in removed
        # "users" still exists in tables
        all_tables = meta.get_tables("src")
        assert any(t["name"] == "users" for t in all_tables)
        users = next(t for t in all_tables if t["name"] == "users")
        assert users["removed_in_run_id"] == run2

        # Active tables do not include "users"
        active = meta.get_active_tables("src")
        active_names = {t["name"] for t in active}
        assert "orders" in active_names
        assert "users" not in active_names

    def test_removed_table_restored_on_reappearance(self, meta: MetadataStore):
        """A previously removed table is restored when it reappears."""
        meta.upsert_source("src", "json", "/data", None)
        run1 = meta.start_run("src")
        meta.upsert_table("orders", "src", row_count=100, run_id=run1)
        meta.upsert_table("users", "src", row_count=50, run_id=run1)
        meta.finish_run(run1, table_count=2)

        # Remove "users" in run2
        run2 = meta.start_run("src")
        meta.mark_removed_tables("src", ["orders"], run2)
        meta.finish_run(run2, table_count=1)

        # Re-run3: "users" reappears
        run3 = meta.start_run("src")
        meta.upsert_table("users", "src", row_count=60, run_id=run3)
        meta.finish_run(run3, table_count=2)

        users = next(t for t in meta.get_tables("src") if t["name"] == "users")
        assert users["removed_in_run_id"] is None
        assert users["row_count"] == 60

    def test_rerun_summary(self, meta: MetadataStore):
        """Re-run summary correctly counts unchanged, updated, added, removed."""
        meta.upsert_source("src", "json", "/data", None)
        previous_tables = ["orders", "users", "products"]
        current_tables = ["orders", "users", "inventory"]  # products removed, inventory added

        summary = meta.compute_rerun_summary("src", current_tables, previous_tables)
        assert summary["added"] == 1  # inventory
        assert summary["removed"] == 1  # products
        assert summary["updated"] == 2  # orders, users (common)

    def test_new_columns_added_on_rerun(self, meta: MetadataStore):
        """New columns in an existing table are added on re-run."""
        meta.upsert_source("src", "json", "/data", None)
        meta.upsert_table("orders", "src", row_count=100)
        meta.upsert_column("orders", "src", "order_id", "int64", ordinal=0)

        # Re-run adds a new column
        meta.upsert_column("orders", "src", "order_id", "int64", ordinal=0)
        meta.upsert_column("orders", "src", "created_at", "timestamp", ordinal=1)

        cols = meta.get_columns("orders", "src")
        col_names = {c["name"] for c in cols}
        assert "order_id" in col_names
        assert "created_at" in col_names

    def test_column_dtype_updated_on_rerun(self, meta: MetadataStore):
        """Column data type is updated on re-run."""
        meta.upsert_source("src", "json", "/data", None)
        meta.upsert_table("orders", "src", row_count=100)
        meta.upsert_column("orders", "src", "amount", "int64", ordinal=0)

        # Re-run changes dtype
        meta.upsert_column("orders", "src", "amount", "float64", ordinal=0)

        cols = meta.get_columns("orders", "src")
        col = next(c for c in cols if c["name"] == "amount")
        assert col["dtype"] == "float64"

    def test_removed_in_run_id_column_exists(self, meta: MetadataStore):
        """Verify tables table has removed_in_run_id column."""
        cols = meta.con.execute("PRAGMA table_info(tables)").fetchall()
        col_names = {c["name"] for c in cols}
        assert "removed_in_run_id" in col_names
