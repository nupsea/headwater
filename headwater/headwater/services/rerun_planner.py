"""Plan targeted reruns after drift or review changes."""

from __future__ import annotations

from typing import Any


def build_rerun_plan(
    *,
    drift_report: dict | None,
    model_impacts: list[dict],
    latest_quality: dict | None,
    source_capabilities: dict | None = None,
) -> dict:
    """Return actionable rerun guidance from current drift and quality state."""
    source_capabilities = source_capabilities or {}
    if drift_report is None:
        return _empty_plan("No drift report is available.")

    diff = drift_report.get("diff") or {}
    source_name = drift_report.get("source_name")
    actions: list[dict] = []
    flags = {
        "regenerate_descriptions": False,
        "regenerate_models": False,
        "rerun_contracts": False,
        "human_review_required": False,
        "no_action_needed": True,
    }

    if diff.get("no_changes"):
        return {
            **flags,
            "source_name": source_name,
            "drift_report_id": drift_report.get("id"),
            "actions": [],
            "impacted_models": [],
            "impacted_contracts": [],
            "capability_notes": _capability_notes(source_capabilities),
            "summary": "No schema drift detected.",
        }

    added_tables = diff.get("tables_added") or []
    removed_tables = diff.get("tables_removed") or []
    changed_tables = diff.get("tables_changed") or []
    added_columns = _column_changes(changed_tables, "added")
    breaking_columns = _breaking_column_changes(changed_tables)
    quality_failures = int((latest_quality or {}).get("failed") or 0)
    impacted_models = sorted({impact["model_name"] for impact in model_impacts})
    impacted_contracts = _impacted_contracts(model_impacts)

    if added_tables or added_columns:
        flags["regenerate_descriptions"] = True
        flags["regenerate_models"] = True
        actions.append(
            _action(
                "regenerate_metadata",
                "recommended",
                "New tables or columns need descriptions, staging updates, and contract proposals.",
                tables=added_tables + [change["table_name"] for change in added_columns],
            )
        )

    if removed_tables or breaking_columns:
        flags["regenerate_models"] = True
        flags["rerun_contracts"] = True
        flags["human_review_required"] = True
        actions.append(
            _action(
                "regenerate_models",
                "required",
                "Removed or changed referenced source assets can break generated SQL.",
                models=impacted_models,
            )
        )

    if impacted_models:
        flags["human_review_required"] = True
        actions.append(
            _action(
                "review_impacted_models",
                "required",
                "Review impacted models before trusting downstream outputs.",
                models=impacted_models,
            )
        )

    if quality_failures or impacted_contracts or flags["regenerate_models"]:
        flags["rerun_contracts"] = True
        actions.append(
            _action(
                "rerun_contracts",
                "required" if quality_failures or impacted_contracts else "recommended",
                "Quality contracts should be checked after drift or model regeneration.",
                contracts=impacted_contracts,
            )
        )

    for note in _capability_notes(source_capabilities):
        actions.append(_action("check_capability", "info", note))

    flags["no_action_needed"] = not actions
    summary = _summary(flags, impacted_models, quality_failures)
    return {
        **flags,
        "source_name": source_name,
        "drift_report_id": drift_report.get("id"),
        "actions": actions,
        "impacted_models": impacted_models,
        "impacted_contracts": impacted_contracts,
        "capability_notes": _capability_notes(source_capabilities),
        "summary": summary,
    }


def _empty_plan(summary: str) -> dict:
    return {
        "source_name": None,
        "drift_report_id": None,
        "regenerate_descriptions": False,
        "regenerate_models": False,
        "rerun_contracts": False,
        "human_review_required": False,
        "no_action_needed": True,
        "actions": [],
        "impacted_models": [],
        "impacted_contracts": [],
        "capability_notes": [],
        "summary": summary,
    }


def _action(action: str, priority: str, reason: str, **scope: Any) -> dict:
    return {
        "action": action,
        "priority": priority,
        "reason": reason,
        "scope": {k: v for k, v in scope.items() if v},
    }


def _column_changes(changed_tables: list[dict], change_type: str) -> list[dict]:
    changes = []
    for table in changed_tables:
        for column in table.get("column_changes") or []:
            if column.get("change_type") == change_type:
                changes.append({"table_name": table.get("table_name"), **column})
    return changes


def _breaking_column_changes(changed_tables: list[dict]) -> list[dict]:
    breaking = []
    for table in changed_tables:
        for column in table.get("column_changes") or []:
            if column.get("change_type") in {"removed", "type_changed", "nullability_changed"}:
                breaking.append({"table_name": table.get("table_name"), **column})
    return breaking


def _impacted_contracts(model_impacts: list[dict]) -> list[str]:
    return sorted(
        {
            impact["contract_id"]
            for impact in model_impacts
            if impact.get("contract_id")
        }
    )


def _capability_notes(capabilities: dict) -> list[str]:
    notes = []
    if capabilities and not capabilities.get("profile_table", False):
        notes.append(
            "Source connector cannot profile in place; use bounded sampling before review."
        )
    if capabilities and not capabilities.get("execute_readonly", False):
        notes.append("Source connector cannot execute read-only validation SQL directly.")
    return notes


def _summary(flags: dict, impacted_models: list[str], quality_failures: int) -> str:
    if flags["human_review_required"]:
        return f"{len(impacted_models)} model(s) need review after drift."
    if flags["regenerate_models"]:
        return "Regenerate metadata, models, and contracts after additive schema drift."
    if quality_failures:
        return f"{quality_failures} quality check(s) need attention."
    return "No targeted rerun action is required."
