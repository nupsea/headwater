"""Schema drift detection -- compare two schema snapshots and produce a SchemaDiff.

Snapshot format (produced by the discovery pipeline):
    {
        "table_name": {
            "columns": [{"name": str, "dtype": str, "nullable": bool}],
            "row_count": int
        },
        ...
    }
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel


class ColumnChange(BaseModel):
    """Describes a change to a single column."""

    column_name: str
    change_type: str  # "added" | "removed" | "type_changed" | "nullability_changed"
    before: str | None = None
    after: str | None = None


class TableChange(BaseModel):
    """Describes a change to a single table (columns changed)."""

    table_name: str
    change_type: str  # "added" | "removed" | "columns_changed"
    column_changes: list[ColumnChange] = []


class SchemaDiff(BaseModel):
    """Complete diff between two schema snapshots."""

    source_name: str
    run_id_from: int | None
    run_id_to: int
    no_changes: bool
    tables_added: list[str] = []
    tables_removed: list[str] = []
    tables_changed: list[TableChange] = []
    detected_at: str


def build_snapshot_from_discovery(discovery) -> dict:
    """Build a snapshot dict from a DiscoveryResult.

    Returns:
        dict mapping table_name -> {columns: [...], row_count: int}
    """
    snapshot: dict = {}
    for table in discovery.tables:
        snapshot[table.name] = {
            "columns": [
                {
                    "name": col.name,
                    "dtype": str(col.dtype),
                    "nullable": col.nullable,
                }
                for col in table.columns
            ],
            "row_count": table.row_count,
        }
    return snapshot


def compare_schemas(
    snapshot_a: dict | None,
    snapshot_b: dict,
    source_name: str,
    run_id_from: int | None,
    run_id_to: int,
) -> SchemaDiff:
    """Compare two schema snapshots and return a SchemaDiff.

    Args:
        snapshot_a: Previous snapshot (None if this is the first run).
        snapshot_b: Current snapshot.
        source_name: Name of the data source.
        run_id_from: Run ID of the previous snapshot (None for first run).
        run_id_to: Run ID of the current snapshot.

    Returns:
        SchemaDiff with all detected changes.
    """
    detected_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    # First run -- no previous snapshot: all tables are "new"
    if snapshot_a is None:
        tables_added = sorted(snapshot_b.keys())
        return SchemaDiff(
            source_name=source_name,
            run_id_from=run_id_from,
            run_id_to=run_id_to,
            no_changes=False,
            tables_added=tables_added,
            tables_removed=[],
            tables_changed=[],
            detected_at=detected_at,
        )

    tables_a = set(snapshot_a.keys())
    tables_b = set(snapshot_b.keys())

    tables_added = sorted(tables_b - tables_a)
    tables_removed = sorted(tables_a - tables_b)

    tables_changed: list[TableChange] = []

    for table_name in sorted(tables_a & tables_b):
        col_changes = _diff_columns(
            table_name,
            snapshot_a[table_name].get("columns", []),
            snapshot_b[table_name].get("columns", []),
        )
        if col_changes:
            tables_changed.append(
                TableChange(
                    table_name=table_name,
                    change_type="columns_changed",
                    column_changes=col_changes,
                )
            )

    no_changes = not tables_added and not tables_removed and not tables_changed

    return SchemaDiff(
        source_name=source_name,
        run_id_from=run_id_from,
        run_id_to=run_id_to,
        no_changes=no_changes,
        tables_added=tables_added,
        tables_removed=tables_removed,
        tables_changed=tables_changed,
        detected_at=detected_at,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _col_index(columns: list[dict]) -> dict[str, dict]:
    """Build a name -> column_dict index."""
    return {col["name"]: col for col in columns}


def _diff_columns(
    table_name: str,
    cols_a: list[dict],
    cols_b: list[dict],
) -> list[ColumnChange]:
    """Detect added, removed, type-changed, and nullability-changed columns."""
    idx_a = _col_index(cols_a)
    idx_b = _col_index(cols_b)

    names_a = set(idx_a.keys())
    names_b = set(idx_b.keys())

    changes: list[ColumnChange] = []

    # Added columns
    for name in sorted(names_b - names_a):
        col = idx_b[name]
        changes.append(
            ColumnChange(
                column_name=name,
                change_type="added",
                before=None,
                after=col.get("dtype"),
            )
        )

    # Removed columns
    for name in sorted(names_a - names_b):
        col = idx_a[name]
        changes.append(
            ColumnChange(
                column_name=name,
                change_type="removed",
                before=col.get("dtype"),
                after=None,
            )
        )

    # Changed columns
    for name in sorted(names_a & names_b):
        a = idx_a[name]
        b = idx_b[name]

        dtype_a = a.get("dtype")
        dtype_b = b.get("dtype")
        if dtype_a != dtype_b:
            changes.append(
                ColumnChange(
                    column_name=name,
                    change_type="type_changed",
                    before=dtype_a,
                    after=dtype_b,
                )
            )

        nullable_a = a.get("nullable")
        nullable_b = b.get("nullable")
        if nullable_a != nullable_b:
            changes.append(
                ColumnChange(
                    column_name=name,
                    change_type="nullability_changed",
                    before=str(nullable_a),
                    after=str(nullable_b),
                )
            )

    return changes
