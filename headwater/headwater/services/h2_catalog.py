"""Headwater 2 S4 — Source Catalog.

Provides a structured view over the source's table/column inventory with
editable descriptions, semantic type overrides, and locks.  Locks survive
re-profiling and re-framing: the engine treats locked values as ground truth
and never overwrites them during automated inference.

All editing goes through this module; the underlying upsert_column API in
the store does the actual write.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from headwater.core.store import HeadwaterStore
from headwater.services.h2_semantics import infer_source_semantics


@dataclass(slots=True)
class CatalogColumn:
    source_name: str
    table_name: str
    column_name: str
    dtype: str
    semantic_type: str    # inferred or user-set
    description: str | None
    locked: bool
    ordinal: int
    profile_summary: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CatalogTable:
    source_name: str
    table_name: str
    row_count: int
    description: str | None
    columns: list[CatalogColumn] = field(default_factory=list)


def get_source_catalog(
    store: HeadwaterStore,
    source_name: str,
    *,
    project_id: str | None = None,
    table_name: str | None = None,
) -> list[CatalogTable]:
    """Return the full source catalog with inferred semantic types.

    Locked store values take precedence over inference.  Pass project_id to
    also apply resource-backed locks for a specific project.
    """
    source = store.get_source(source_name)
    if source is None:
        raise ValueError(f"Source '{source_name}' is not registered.")

    semantic_map = infer_source_semantics(store, source_name, project_id=project_id)
    profiles = store.get_profiles(source_name)
    profile_map: dict[str, dict[str, Any]] = {
        f"{p['table_name']}.{p['column_name']}".lower(): p["profile"]
        for p in profiles
    }

    tables = store.get_tables(source_name)
    if table_name:
        tables = [t for t in tables if t["name"] == table_name]

    result: list[CatalogTable] = []
    for table in tables:
        cols = store.get_columns(source_name, table["name"])
        catalog_cols: list[CatalogColumn] = []
        for col in cols:
            key = f"{table['name']}.{col['name']}".lower()
            inferred = semantic_map.get(key, "")
            # Stored semantic_type overrides inference only when set
            effective_type = col.get("semantic_type") or inferred
            profile = profile_map.get(key, {})
            summary = _profile_summary(profile)
            catalog_cols.append(CatalogColumn(
                source_name=source_name,
                table_name=table["name"],
                column_name=col["name"],
                dtype=col.get("dtype") or "varchar",
                semantic_type=effective_type,
                description=col.get("description"),
                locked=bool(col.get("locked")),
                ordinal=int(col.get("ordinal") or 0),
                profile_summary=summary,
            ))
        result.append(CatalogTable(
            source_name=source_name,
            table_name=table["name"],
            row_count=int(table.get("row_count") or 0),
            description=table.get("description"),
            columns=catalog_cols,
        ))
    return result


def update_column(
    store: HeadwaterStore,
    source_name: str,
    table_name: str,
    column_name: str,
    *,
    description: str | None = None,
    semantic_type: str | None = None,
    dtype: str | None = None,
    lock: bool | None = None,
) -> None:
    """Update a column's description, semantic type, dtype, and/or lock state.

    Only fields explicitly passed (not None) are updated.
    """
    cols = store.get_columns(source_name, table_name)
    col = next((c for c in cols if c["name"] == column_name), None)
    if col is None:
        raise ValueError(
            f"Column '{table_name}.{column_name}' not found in source '{source_name}'."
        )

    store.upsert_column(
        source_name,
        table_name,
        column_name,
        dtype if dtype is not None else col["dtype"],
        nullable=bool(col.get("nullable", 1)),
        is_primary_key=bool(col.get("is_primary_key", 0)),
        description=description if description is not None else col.get("description"),
        semantic_type=semantic_type if semantic_type is not None else col.get("semantic_type"),
        ordinal=int(col.get("ordinal") or 0),
        locked=lock if lock is not None else bool(col.get("locked", 0)),
    )


def lock_column(
    store: HeadwaterStore,
    source_name: str,
    table_name: str,
    column_name: str,
) -> None:
    """Lock a column so its description and semantic type survive re-profiling."""
    update_column(store, source_name, table_name, column_name, lock=True)


def unlock_column(
    store: HeadwaterStore,
    source_name: str,
    table_name: str,
    column_name: str,
) -> None:
    """Remove the lock from a column."""
    update_column(store, source_name, table_name, column_name, lock=False)


def _profile_summary(profile: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    null_rate = profile.get("null_rate")
    if null_rate is not None:
        summary["null_pct"] = int(float(null_rate) * 100)
    distinct = profile.get("distinct_count")
    if distinct is not None:
        summary["distinct"] = int(distinct)
    mean = profile.get("mean")
    if mean is not None:
        summary["mean"] = round(float(mean), 2)
    top = profile.get("top_values")
    if top:
        summary["top_values"] = [str(v[0]) for v in top[:3] if v]
    return summary
