"""Compute durable model impacts from schema drift."""

from __future__ import annotations

import json
import re
from typing import Any

BREAKING_CHANGE_TYPES = {"removed", "type_changed"}


def compute_schema_drift_model_impacts(
    *,
    source_name: str,
    drift_report_id: int,
    diff: dict,
    models: list[dict],
) -> list[dict]:
    """Return model impact rows caused by a schema drift diff."""
    if diff.get("no_changes"):
        return []

    impacts: list[dict] = []
    for model in models:
        source_tables = _json_list(model.get("source_tables"))
        model_name = model["name"]
        sql_text = model.get("sql_text") or ""

        for table_name in diff.get("tables_removed") or []:
            if table_name not in source_tables:
                continue
            impacts.append(
                _impact(
                    source_name,
                    drift_report_id,
                    model_name,
                    "source_table_removed",
                    "error",
                    table_name,
                    None,
                    f"Source table '{table_name}' was removed",
                )
            )

        for table_change in diff.get("tables_changed") or []:
            table_name = table_change.get("table_name")
            if table_name not in source_tables:
                continue
            for column_change in table_change.get("column_changes") or []:
                column_name = column_change.get("column_name")
                change_type = column_change.get("change_type")
                if change_type == "added":
                    impacts.append(
                        _impact(
                            source_name,
                            drift_report_id,
                            model_name,
                            "source_column_added",
                            "info",
                            table_name,
                            column_name,
                            f"Source column '{column_name}' was added to '{table_name}'",
                            payload=column_change,
                        )
                    )
                    continue
                if not _references_column(sql_text, column_name):
                    continue
                impacts.append(
                    _impact(
                        source_name,
                        drift_report_id,
                        model_name,
                        f"source_column_{change_type}",
                        _severity_for_change(change_type),
                        table_name,
                        column_name,
                        _reason_for_column_change(table_name, column_name, change_type),
                        payload=column_change,
                    )
                )

    return impacts


def invalidated_model_names(impacts: list[dict]) -> list[str]:
    """Return models that should be invalidated by persisted impacts."""
    return sorted(
        {
            impact["model_name"]
            for impact in impacts
            if impact.get("severity") in {"error", "warning"}
        }
    )


def _impact(
    source_name: str,
    drift_report_id: int,
    model_name: str,
    impact_type: str,
    severity: str,
    source_table: str | None,
    source_column: str | None,
    reason: str,
    *,
    payload: dict | None = None,
) -> dict:
    return {
        "source_name": source_name,
        "drift_report_id": drift_report_id,
        "model_name": model_name,
        "impact_type": impact_type,
        "severity": severity,
        "source_table": source_table,
        "source_column": source_column,
        "reason": reason,
        "payload": payload,
    }


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    return []


def _references_column(sql_text: str, column_name: str | None) -> bool:
    if not column_name:
        return False
    quoted = f'"{column_name}"'
    if quoted in sql_text:
        return True
    return bool(re.search(rf"\b{re.escape(column_name)}\b", sql_text, flags=re.IGNORECASE))


def _severity_for_change(change_type: str | None) -> str:
    if change_type in BREAKING_CHANGE_TYPES:
        return "error"
    return "warning"


def _reason_for_column_change(
    table_name: str | None,
    column_name: str | None,
    change_type: str | None,
) -> str:
    label = (change_type or "changed").replace("_", " ")
    return f"Referenced source column '{column_name}' on '{table_name}' was {label}"
