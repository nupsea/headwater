"""Mark canonical project context items that are invalidated by re-ingestion drift."""

from __future__ import annotations

from headwater.core.models import DiscoveryResult


def reconcile_project_context_drift(
    store,
    discovery: DiscoveryResult,
    *,
    project_id: str,
    source_name: str,
    drift_report: dict | None = None,
) -> dict:
    """Move invalidated context items into review after schema changes."""
    if store is None:
        return {"items_flagged": 0, "item_ids": []}

    table_names = {table.name for table in discovery.tables}
    columns_by_table = {
        table.name: {column.name: str(column.dtype) for column in table.columns}
        for table in discovery.tables
    }
    relationship_keys = {
        (
            rel.from_table,
            rel.from_column,
            rel.to_table,
            rel.to_column,
        )
        for rel in discovery.relationships
    }

    flagged_ids: list[str] = []
    for item in store.list_project_context_items(project_id, source_name=source_name):
        if item.get("status") in {"rejected", "needs_review"}:
            continue
        reason = _invalidated_reason(item, table_names, columns_by_table, relationship_keys)
        if reason is None:
            continue
        updated = _mark_item_needs_review(
            store,
            project_id,
            item,
            reason=reason,
            drift_report=drift_report,
        )
        if updated is not None:
            flagged_ids.append(updated["id"])

    return {"items_flagged": len(flagged_ids), "item_ids": flagged_ids}


def _invalidated_reason(
    item: dict,
    table_names: set[str],
    columns_by_table: dict[str, dict[str, str]],
    relationship_keys: set[tuple[str, str, str, str]],
) -> dict | None:
    item_type = item.get("item_type")
    table_name = item.get("table_name")
    column_name = item.get("column_name")
    value = dict(item.get("value") or {})

    if item_type == "relationship":
        relation = (
            value.get("from_table"),
            value.get("from_column"),
            value.get("to_table"),
            value.get("to_column"),
        )
        if None in relation:
            return None
        if relation not in relationship_keys:
            return {
                "code": "relationship_missing",
                "summary": "Confirmed relationship no longer exists after re-ingestion.",
                "payload": {
                    "from_table": relation[0],
                    "from_column": relation[1],
                    "to_table": relation[2],
                    "to_column": relation[3],
                },
            }
        return None

    if table_name and table_name not in table_names:
        return {
            "code": "table_missing",
            "summary": f"Table '{table_name}' is no longer present after re-ingestion.",
            "payload": {"table_name": table_name},
        }

    if not table_name or not column_name:
        return None

    current_columns = columns_by_table.get(table_name) or {}
    current_dtype = current_columns.get(column_name)
    if current_dtype is None:
        return {
            "code": "column_missing",
            "summary": (
                f"Column '{table_name}.{column_name}' is no longer present after re-ingestion."
            ),
            "payload": {"table_name": table_name, "column_name": column_name},
        }

    previous_dtype = value.get("dtype")
    if previous_dtype and str(previous_dtype) != current_dtype:
        return {
            "code": "column_type_changed",
            "summary": (
                f"Column '{table_name}.{column_name}' changed type from "
                f"'{previous_dtype}' to '{current_dtype}'."
            ),
            "payload": {
                "table_name": table_name,
                "column_name": column_name,
                "before": str(previous_dtype),
                "after": current_dtype,
            },
        }

    return None


def _mark_item_needs_review(
    store,
    project_id: str,
    item: dict,
    *,
    reason: dict,
    drift_report: dict | None = None,
) -> dict | None:
    value = dict(item.get("value") or {})
    value["drift_status"] = "invalidated"
    value["drift_reason"] = reason["summary"]
    if drift_report is not None:
        value["drift_report_id"] = drift_report.get("id")

    evidence = list(item.get("evidence") or [])
    evidence.append(
        {
            "evidence_type": "schema_drift",
            "source": "context_drift",
            "summary": reason["summary"],
            "payload": {
                **(reason.get("payload") or {}),
                "drift_report_id": (drift_report or {}).get("id"),
            },
        }
    )
    return store.update_project_context_item(
        item["id"],
        project_id=project_id,
        status="needs_review",
        value=value,
        confidence=min(float(item.get("confidence") or 0.0), 0.45),
        source="context_drift",
        evidence=evidence,
    )
