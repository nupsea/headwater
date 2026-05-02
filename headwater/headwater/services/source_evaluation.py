"""Data source evaluation for OLTP and OLAP onboarding paths."""

from __future__ import annotations

from typing import Any

from headwater.connectors.registry import (
    connector_status,
    get_connector_capabilities,
    list_connector_catalog,
)

OLTP_CONNECTORS = {"postgres", "mysql", "sqlite", "sqlserver", "oracle"}
OLAP_CONNECTORS = {
    "duckdb",
    "snowflake",
    "bigquery",
    "redshift",
    "databricks",
    "clickhouse",
    "trino",
    "athena",
}
FILE_CONNECTORS = {"json", "csv"}


def evaluate_connector_type(source_type: str, source_state: dict | None = None) -> dict:
    """Evaluate connector fit for Headwater's source evaluation workflow."""
    catalog = {item["id"]: item for item in list_connector_catalog()}
    item = catalog.get(source_type)
    capabilities = get_connector_capabilities(source_type)
    status = connector_status(source_type) or "unknown"
    workload = _workload_for(source_type, item)
    source_state = source_state or {}

    evidence = _capability_evidence(workload, capabilities.model_dump())
    evidence.extend(_source_state_evidence(workload, source_state))
    gaps = [row["detail"] for row in evidence if row["status"] == "gap"]
    warnings = [row["detail"] for row in evidence if row["status"] == "warning"]

    score = _score(status, evidence, source_state)
    readiness = _readiness(status, score, gaps, source_state)
    maturity_mode = _maturity_mode(workload, capabilities.model_dump(), source_state)

    return {
        "source_type": source_type,
        "workload": workload,
        "readiness": readiness,
        "score": score,
        "maturity_mode": maturity_mode,
        "status": status,
        "supported": status == "supported",
        "capabilities": capabilities.model_dump(),
        "evidence": evidence,
        "gaps": gaps,
        "warnings": warnings,
        "recommended_actions": _recommended_actions(workload, status, gaps, source_state),
        "profiling_policy": _profiling_policy(workload, capabilities.model_dump()),
    }


def evaluate_source(row: dict, *, schemas: int = 0, tables: int = 0, rows: int = 0) -> dict:
    """Evaluate a persisted source row with observed table/profile counts."""
    latest_run_status = row.get("latest_run_status")
    state = {
        "registered": True,
        "synced": bool(row.get("last_sync_at")) or latest_run_status == "succeeded",
        "latest_run_status": latest_run_status,
        "source_status": row.get("status"),
        "schemas": schemas,
        "tables": tables,
        "rows": rows,
        "drift_count": row.get("drift_count") or 0,
        "quality_failed": row.get("quality_failed") or 0,
        "quality_score": row.get("quality_score"),
    }
    evaluation = evaluate_connector_type(row["type"], state)
    evaluation["source_name"] = row["name"]
    return evaluation


def evaluate_all_connectors() -> list[dict]:
    """Return evaluation templates for every catalogued connector."""
    return [evaluate_connector_type(item["id"]) for item in list_connector_catalog()]


def _workload_for(source_type: str, item: dict | None) -> str:
    if source_type in FILE_CONNECTORS:
        return "files"
    if source_type in OLTP_CONNECTORS:
        return "oltp"
    if source_type in OLAP_CONNECTORS:
        return "olap"
    category = (item or {}).get("category", "").lower()
    if category == "oltp":
        return "oltp"
    if category in {"olap", "warehouse", "lakehouse", "federated"}:
        return "olap"
    return "unknown"


def _capability_evidence(workload: str, capabilities: dict[str, Any]) -> list[dict]:
    rows = [
        _evidence(
            "connectivity",
            "Connection test",
            capabilities.get("test"),
            "Connector can verify credentials before discovery.",
            "Connector cannot verify source connectivity.",
        ),
        _evidence(
            "metadata",
            "Schema metadata",
            capabilities.get("list_tables") and capabilities.get("list_columns"),
            "Connector can list tables and columns.",
            "Connector cannot list table and column metadata.",
        ),
        _evidence(
            "profiling",
            "Pushdown profiling",
            capabilities.get("profile_table"),
            "Connector can profile tables without relying only on local copies.",
            "Connector cannot profile tables through the connector contract.",
        ),
    ]

    if workload == "oltp":
        rows.append(
            _evidence(
                "constraints",
                "Declared constraints",
                capabilities.get("list_constraints"),
                "Declared PK/FK/check evidence can be imported from the source.",
                "Declared PK/FK/check import is missing; Headwater will rely on "
                "heuristics and review.",
            )
        )
        rows.append(
            _evidence(
                "readonly_sql",
                "Read-only validation",
                capabilities.get("execute_readonly"),
                "Generated SQL can be validated safely against the source.",
                "Read-only SQL validation is unavailable for this source.",
                warning=True,
            )
        )
    elif workload == "olap":
        rows.append(
            _evidence(
                "observe_mode",
                "Observe mode",
                "observe" in capabilities.get("modes", []),
                "Connector can observe warehouse metadata without requiring a full load.",
                "Observe mode is not declared for this connector.",
                warning=True,
            )
        )
        rows.append(
            _evidence(
                "row_estimates",
                "Row estimates",
                capabilities.get("estimate_row_count"),
                "Connector can estimate table sizes before profiling.",
                "Row estimates are unavailable; large-table safety depends on configured limits.",
                warning=True,
            )
        )
    else:
        rows.append(
            _evidence(
                "local_load",
                "Local analytical load",
                capabilities.get("load_to_duckdb"),
                "Source can be loaded into Headwater's analytical store.",
                "Source cannot be loaded into Headwater's analytical store.",
                warning=True,
            )
        )
    return rows


def _source_state_evidence(workload: str, state: dict[str, Any]) -> list[dict]:
    if not state:
        return []
    rows = [
        _evidence(
            "registered",
            "Registered source",
            state.get("registered"),
            "Source has been registered in Headwater.",
            "Source is not registered yet.",
        ),
        _evidence(
            "sync",
            "Discovery sync",
            state.get("synced"),
            "Source has completed at least one discovery sync.",
            "Run a sync to evaluate actual tables, profiles, and quality.",
            warning=True,
        ),
    ]
    if state.get("tables", 0) > 0:
        rows.append(
            {
                "key": "coverage",
                "label": "Observed coverage",
                "status": "ok",
                "detail": (
                    f"{state.get('tables', 0)} table(s), {state.get('schemas', 0)} schema(s), "
                    f"{state.get('rows', 0)} row(s) observed."
                ),
            }
        )
    if workload == "olap" and state.get("rows", 0) >= 1_000_000:
        rows.append(
            {
                "key": "large_table_policy",
                "label": "Large-table policy",
                "status": "warning",
                "detail": "Observed row volume is large; prefer aggregate or sample profiling.",
            }
        )
    if state.get("drift_count", 0) > 0:
        rows.append(
            {
                "key": "drift",
                "label": "Schema drift",
                "status": "warning",
                "detail": (
                    f"{state['drift_count']} unacknowledged drift report(s) "
                    "affect this source."
                ),
            }
        )
    if state.get("quality_failed", 0) > 0:
        rows.append(
            {
                "key": "quality",
                "label": "Quality checks",
                "status": "warning",
                "detail": f"{state['quality_failed']} quality issue(s) need review.",
            }
        )
    return rows


def _evidence(
    key: str,
    label: str,
    condition: Any,
    ok_detail: str,
    gap_detail: str,
    *,
    warning: bool = False,
) -> dict:
    return {
        "key": key,
        "label": label,
        "status": "ok" if condition else ("warning" if warning else "gap"),
        "detail": ok_detail if condition else gap_detail,
    }


def _score(status: str, evidence: list[dict], state: dict[str, Any]) -> int:
    if status == "planned":
        return 20
    if status == "unknown":
        return 0
    score = 55 if status == "supported" else 35
    score += sum(8 for row in evidence if row["status"] == "ok")
    score -= sum(7 for row in evidence if row["status"] == "gap")
    score -= sum(3 for row in evidence if row["status"] == "warning")
    if state.get("tables", 0) > 0:
        score += 8
    if state.get("quality_score") is not None:
        score += min(7, max(0, int(state["quality_score"]) // 15))
    return max(0, min(100, score))


def _readiness(status: str, score: int, gaps: list[str], state: dict[str, Any]) -> str:
    if status == "planned":
        return "planned"
    if status == "preview":
        return "preview"
    if state and not state.get("synced"):
        return "needs_sync"
    if score >= 85 and not gaps:
        return "ready"
    if score >= 65:
        return "needs_review"
    return "limited"


def _maturity_mode(workload: str, capabilities: dict[str, Any], state: dict[str, Any]) -> str:
    if workload == "files":
        return "raw_files"
    if workload == "oltp":
        return "oltp_with_constraints" if capabilities.get("list_constraints") else "oltp_heuristic"
    if workload == "olap":
        if state.get("tables", 0) and capabilities.get("execute_readonly"):
            return "warehouse_observe"
        return "warehouse_metadata"
    return "unknown"


def _recommended_actions(
    workload: str,
    status: str,
    gaps: list[str],
    state: dict[str, Any],
) -> list[dict]:
    actions: list[dict] = []
    if status != "supported":
        actions.append(
            {
                "priority": "blocking" if status == "planned" else "recommended",
                "title": f"Connector is {status}",
                "detail": "Use a supported connector for production evaluation.",
            }
        )
    if state and not state.get("synced"):
        actions.append(
            {
                "priority": "blocking",
                "title": "Run source sync",
                "detail": "Discovery must run before Headwater can evaluate actual coverage.",
            }
        )
    if workload == "oltp" and any("PK/FK/check" in gap for gap in gaps):
        actions.append(
            {
                "priority": "recommended",
                "title": "Review keys and relationships",
                "detail": (
                    "Declared constraints are unavailable, so confirm inferred "
                    "PK/FK evidence."
                ),
            }
        )
    if workload == "olap":
        actions.append(
            {
                "priority": "recommended",
                "title": "Use bounded aggregate profiling",
                "detail": (
                    "Keep warehouse evaluation in observe mode with row, table, "
                    "and scan limits."
                ),
            }
        )
    if not actions:
        actions.append(
            {
                "priority": "informational",
                "title": "Ready for evaluation",
                "detail": (
                    "Connector capabilities and observed source state are "
                    "sufficient for review."
                ),
            }
        )
    return actions


def _profiling_policy(workload: str, capabilities: dict[str, Any]) -> dict:
    if workload == "olap":
        return {
            "mode": "observe",
            "max_tables": 50,
            "max_columns_per_table": 200,
            "max_sample_rows": 10_000,
            "aggregate_only_above_rows": 1_000_000,
            "requires_row_estimates": bool(capabilities.get("estimate_row_count")),
        }
    if workload == "oltp":
        return {
            "mode": "metadata_first",
            "max_tables": 100,
            "max_columns_per_table": 150,
            "max_sample_rows": 5_000,
            "prefer_declared_constraints": True,
        }
    return {
        "mode": "load_or_sample",
        "max_tables": 100,
        "max_columns_per_table": 150,
        "max_sample_rows": 10_000,
    }
