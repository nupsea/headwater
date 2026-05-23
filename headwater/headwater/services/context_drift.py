"""Mark canonical project context items that are invalidated by re-ingestion drift."""

from __future__ import annotations

from headwater.core.models import DiscoveryResult

DRIFT_RULES = {
    "table_missing": {
        "drift_type": "schema",
        "severity": "critical",
        "detector": "schema.table_presence",
        "evidence_type": "schema_drift",
    },
    "column_missing": {
        "drift_type": "schema",
        "severity": "critical",
        "detector": "schema.column_presence",
        "evidence_type": "schema_drift",
    },
    "column_type_changed": {
        "drift_type": "schema",
        "severity": "high",
        "detector": "schema.column_type",
        "evidence_type": "schema_drift",
    },
    "relationship_missing": {
        "drift_type": "relationship",
        "severity": "high",
        "detector": "relationship.key_presence",
        "evidence_type": "relationship_drift",
    },
}

REVIEW_ACTION = "needs_review"


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

    flagged_items: list[dict] = []
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
            flagged_items.append(
                {
                    "id": updated["id"],
                    "drift_type": reason["drift_type"],
                    "severity": reason["severity"],
                    "code": reason["code"],
                }
            )

    return {
        "items_flagged": len(flagged_items),
        "item_ids": [item["id"] for item in flagged_items],
        "items": flagged_items,
        "drift_type_counts": _count_by(flagged_items, "drift_type"),
        "severity_counts": _count_by(flagged_items, "severity"),
    }


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
            return _drift_reason(
                "relationship_missing",
                summary="Confirmed relationship no longer exists after re-ingestion.",
                payload={
                    "from_table": relation[0],
                    "from_column": relation[1],
                    "to_table": relation[2],
                    "to_column": relation[3],
                },
            )
        return None

    if table_name and table_name not in table_names:
        return _drift_reason(
            "table_missing",
            summary=f"Table '{table_name}' is no longer present after re-ingestion.",
            payload={"table_name": table_name},
        )

    if not table_name or not column_name:
        return None

    current_columns = columns_by_table.get(table_name) or {}
    current_dtype = current_columns.get(column_name)
    if current_dtype is None:
        return _drift_reason(
            "column_missing",
            summary=f"Column '{table_name}.{column_name}' is no longer present after re-ingestion.",
            payload={"table_name": table_name, "column_name": column_name},
        )

    previous_dtype = value.get("dtype")
    if previous_dtype and str(previous_dtype) != current_dtype:
        return _drift_reason(
            "column_type_changed",
            summary=(
                f"Column '{table_name}.{column_name}' changed type from "
                f"'{previous_dtype}' to '{current_dtype}'."
            ),
            payload={
                "table_name": table_name,
                "column_name": column_name,
                "before": str(previous_dtype),
                "after": current_dtype,
            },
        )

    return None


def _drift_reason(code: str, *, summary: str, payload: dict) -> dict:
    rule = DRIFT_RULES[code]
    return {
        "code": code,
        "summary": summary,
        "payload": payload,
        "drift_type": rule["drift_type"],
        "severity": rule["severity"],
        "detector": rule["detector"],
        "evidence_type": rule["evidence_type"],
        "review_action": REVIEW_ACTION,
    }


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
    value["drift_type"] = reason["drift_type"]
    value["drift_severity"] = reason["severity"]
    value["drift_detector"] = reason["detector"]
    value["drift_review_action"] = reason["review_action"]
    if drift_report is not None:
        value["drift_report_id"] = drift_report.get("id")

    evidence = list(item.get("evidence") or [])
    evidence.append(
        {
            "evidence_type": reason["evidence_type"],
            "source": "context_drift",
            "summary": reason["summary"],
            "payload": {
                **(reason.get("payload") or {}),
                "code": reason["code"],
                "drift_type": reason["drift_type"],
                "severity": reason["severity"],
                "detector": reason["detector"],
                "review_action": reason["review_action"],
                "drift_report_id": (drift_report or {}).get("id"),
            },
        }
    )
    return store.update_project_context_item(
        item["id"],
        project_id=project_id,
        status="needs_review",
        value=value,
        confidence=min(float(item.get("confidence") or 0.0), _confidence_cap(reason["severity"])),
        source="context_drift",
        evidence=evidence,
    )


def _confidence_cap(severity: str) -> float:
    if severity == "critical":
        return 0.25
    if severity == "high":
        return 0.35
    return 0.45


def _count_by(items: list[dict], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts
