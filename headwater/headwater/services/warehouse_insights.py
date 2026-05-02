"""Cost-aware warehouse insight planning and approved execution.

Execution against warehouses needs explicit budget gates because source tables
can be large and expensive.
"""

from __future__ import annotations

import time
from typing import Any

import pyarrow as pa

from headwater.connectors.registry import get_connector, get_connector_capabilities
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig

DEFAULT_BUDGET = {
    "max_queries": 20,
    "max_tables": 20,
    "max_sample_rows": 10_000,
    "max_estimated_rows": 10_000_000,
    "require_time_filter_above_rows": 1_000_000,
    "allow_full_scan": False,
}

MAX_EXECUTION_RESULT_ROWS = 100


def normalize_budget(raw: dict | None) -> dict:
    """Clamp caller-provided budget values to conservative platform bounds."""
    budget = dict(DEFAULT_BUDGET)
    if raw:
        budget.update({k: v for k, v in raw.items() if v is not None})
    budget["max_queries"] = _bounded_int(budget.get("max_queries"), 1, 50)
    budget["max_tables"] = _bounded_int(budget.get("max_tables"), 1, 100)
    budget["max_sample_rows"] = _bounded_int(budget.get("max_sample_rows"), 100, 100_000)
    budget["max_estimated_rows"] = _bounded_int(
        budget.get("max_estimated_rows"), 1_000, 500_000_000
    )
    budget["require_time_filter_above_rows"] = _bounded_int(
        budget.get("require_time_filter_above_rows"), 1_000, 500_000_000
    )
    budget["allow_full_scan"] = bool(budget.get("allow_full_scan", False))
    return budget


def build_dry_run_plan(store: Any, source_name: str, raw_budget: dict | None = None) -> dict:
    """Build and persist a dry-run warehouse insight plan for a source."""
    source = store.get_source(source_name)
    if not source:
        raise SourceNotFoundError(source_name)

    budget = normalize_budget(raw_budget)
    all_tables = store.get_active_tables(source_name)
    active_tables = sorted(all_tables, key=lambda t: int(t.get("row_count") or 0), reverse=True)
    selected_tables = active_tables[: budget["max_tables"]]
    skipped_by_limit = max(0, len(active_tables) - len(selected_tables))

    candidates: list[dict] = []
    for table in selected_tables:
        candidates.extend(_table_candidates(source, table, budget, len(candidates)))
        if _planned_query_count(candidates) >= budget["max_queries"]:
            break

    if skipped_by_limit:
        candidates.append(
            {
                "evidence_type": "table_selection",
                "artifact_type": "source",
                "artifact_id": source_name,
                "query_purpose": "table_selection_limit",
                "status": "skipped",
                "skipped_reason": f"{skipped_by_limit} tables excluded by max_tables budget",
                "confidence": 1.0,
                "cost": {"cost_tier": "none", "estimated_rows_scanned": 0},
            }
        )

    planned = [c for c in candidates if c["status"] == "planned" and c.get("sql")]
    skipped = [c for c in candidates if c["status"] == "skipped"]
    plan = {
        "source_name": source_name,
        "source_type": source.get("type"),
        "mode": "dry_run",
        "budget": budget,
        "tables_considered": len(selected_tables),
        "tables_available": len(active_tables),
        "planned_queries": len(planned),
        "skipped_queries": len(skipped),
        "policy": {
            "execute_queries": False,
            "pushdown_only": True,
            "sampling": "bounded_row_sample_only_when_needed",
            "large_table_rule": "skip_without_time_filter_or_explicit_full_scan_budget",
        },
        "candidates": candidates,
    }
    plan_id = store.insert_warehouse_insight_plan(
        source_name,
        budget=budget,
        plan=plan,
        mode="dry_run",
        status="planned",
    )
    for candidate in candidates:
        store.insert_evidence_record(
            source_name,
            candidate["evidence_type"],
            plan_id=plan_id,
            artifact_type=candidate.get("artifact_type"),
            artifact_id=candidate.get("artifact_id"),
            table_name=candidate.get("table_name"),
            query_purpose=candidate.get("query_purpose"),
            sql_text=candidate.get("sql"),
            coverage=candidate.get("coverage"),
            sample=candidate.get("sample"),
            cost=candidate.get("cost"),
            confidence=float(candidate.get("confidence") or 0.0),
            confidence_reason=candidate.get("confidence_reason"),
            status=candidate["status"],
            skipped_reason=candidate.get("skipped_reason"),
            payload={"dry_run": True, "source_type": source.get("type")},
        )
    plan["plan_id"] = plan_id
    return plan


class SourceNotFoundError(Exception):
    """Raised when the requested source does not exist."""


class PlanNotFoundError(Exception):
    """Raised when the requested insight plan does not exist."""


class PlanExecutionRejectedError(Exception):
    """Raised when an insight plan cannot be executed under the requested budget."""


def execute_approved_plan(
    store: Any,
    plan_id: int,
    *,
    approved: bool,
    max_queries: int | None = None,
    query_tag: str | None = None,
    statement_timeout_seconds: int | None = None,
) -> dict:
    """Execute planned read-only warehouse evidence queries after explicit approval."""
    if not approved:
        raise PlanExecutionRejectedError("Plan execution requires approved=true.")

    saved = store.get_warehouse_insight_plan(plan_id)
    if not saved:
        raise PlanNotFoundError(plan_id)
    source_name = saved["source_name"]
    source = store.get_source(source_name)
    if not source:
        raise SourceNotFoundError(source_name)
    capabilities = get_connector_capabilities(source["type"])
    if not capabilities.execute_readonly:
        raise PlanExecutionRejectedError(
            f"Source type '{source['type']}' does not support read-only execution."
        )

    plan = saved.get("plan") or {}
    budget = saved.get("budget") or normalize_budget(None)
    planned_candidates = [
        candidate
        for candidate in plan.get("candidates", [])
        if candidate.get("status") == "planned" and candidate.get("sql")
    ]
    execution_limit = _bounded_int(
        max_queries if max_queries is not None else budget.get("max_queries"),
        1,
        int(budget.get("max_queries") or DEFAULT_BUDGET["max_queries"]),
    )
    timeout_seconds = _bounded_int(
        statement_timeout_seconds if statement_timeout_seconds is not None else 300,
        5,
        3600,
    )
    selected = planned_candidates[:execution_limit]
    tag = query_tag or f"headwater:insight-plan:{plan_id}"

    connector = get_connector(source["type"])
    results: list[dict] = []
    status = "succeeded"
    started = time.perf_counter()
    try:
        connector.connect(
            SourceConfig(
                name=source_name,
                type=source["type"],
                path=source.get("path"),
                uri=source.get("uri"),
                mode=source.get("mode") or "observe",
            )
        )
        _set_query_tag(connector, tag)
        _set_statement_timeout(connector, timeout_seconds)
        for candidate in selected:
            result = _execute_candidate(connector, candidate)
            results.append(result)
            store.insert_evidence_record(
                source_name,
                candidate["evidence_type"],
                plan_id=plan_id,
                artifact_type=candidate.get("artifact_type"),
                artifact_id=candidate.get("artifact_id"),
                table_name=candidate.get("table_name"),
                query_purpose=candidate.get("query_purpose"),
                sql_text=candidate.get("sql"),
                coverage={
                    **(candidate.get("coverage") or {}),
                    "executed": result["status"] == "succeeded",
                    "result_rows": result.get("row_count", 0),
                },
                sample={"rows": 0, "method": "pushdown_aggregate"},
                cost={
                    **(candidate.get("cost") or {}),
                    "observed_runtime_ms": result.get("duration_ms"),
                    "query_tag": tag,
                },
                confidence=0.9 if result["status"] == "succeeded" else 0.0,
                confidence_reason=result.get("confidence_reason"),
                status=result["status"],
                skipped_reason=result.get("error"),
                query_id=result.get("query_id"),
                statement_timeout_seconds=timeout_seconds,
                payload={"dry_run": False, "rows": result.get("rows", [])},
            )
            if result["status"] != "succeeded":
                status = "failed"
                break
    finally:
        close = getattr(connector, "close", None)
        if close:
            close()

    duration_ms = int((time.perf_counter() - started) * 1000)
    execution = {
        "plan_id": plan_id,
        "source_name": source_name,
        "approved": True,
        "status": status,
        "query_tag": tag,
        "planned_queries": len(planned_candidates),
        "executed_queries": len(results),
        "duration_ms": duration_ms,
        "statement_timeout_seconds": timeout_seconds,
        "results": results,
    }
    plan["last_execution"] = execution
    store.update_warehouse_insight_plan(plan_id, status=status, plan=plan)
    return execution


def _table_candidates(source: dict, table: dict, budget: dict, candidate_count: int) -> list[dict]:
    row_count = int(table.get("row_count") or 0)
    table_ref = _table_ref(table)
    table_name = table["name"]
    source_type = source.get("type") or "unknown"
    coverage = {
        "source": "metadata_catalog",
        "row_count": row_count,
        "schema_name": table.get("schema_name"),
    }
    metadata_candidate = {
        "evidence_type": "table_profile",
        "artifact_type": "table",
        "artifact_id": f"{source['name']}.{table_name}",
        "table_name": table_name,
        "query_purpose": "catalog_row_count_and_shape",
        "sql": None,
        "status": "planned",
        "coverage": coverage,
        "sample": {"rows": 0, "method": "metadata_only"},
        "cost": {"cost_tier": "none", "estimated_rows_scanned": 0},
        "confidence": 0.75 if row_count else 0.45,
        "confidence_reason": "Uses catalog metadata; no warehouse scan required.",
    }
    if candidate_count + 1 >= budget["max_queries"]:
        return [metadata_candidate]

    aggregate_candidate = {
        "evidence_type": "warehouse_aggregate",
        "artifact_type": "table",
        "artifact_id": f"{source['name']}.{table_name}",
        "table_name": table_name,
        "query_purpose": "freshness_volume_and_null_shape",
        "sql": f"SELECT COUNT(*) AS row_count FROM {table_ref}",
        "coverage": coverage,
        "sample": {"rows": 0, "method": "pushdown_aggregate"},
        "cost": {
            "cost_tier": _cost_tier(row_count),
            "estimated_rows_scanned": row_count,
            "warehouse": source_type,
        },
        "confidence": 0.8,
        "confidence_reason": "Pushdown aggregate evidence; stronger after execution.",
    }
    if _can_plan_scan(row_count, budget):
        aggregate_candidate["status"] = "planned"
        return [metadata_candidate, aggregate_candidate]

    aggregate_candidate["status"] = "skipped"
    aggregate_candidate["skipped_reason"] = (
        "Estimated row count exceeds cost gate; require a time predicate, clustering-aware "
        "partition filter, or explicit allow_full_scan budget."
    )
    aggregate_candidate["confidence"] = 0.0
    aggregate_candidate["confidence_reason"] = (
        "Skipped before execution by warehouse budget policy."
    )
    return [metadata_candidate, aggregate_candidate]


def _planned_query_count(candidates: list[dict]) -> int:
    return sum(1 for c in candidates if c.get("status") == "planned" and c.get("sql"))


def _can_plan_scan(row_count: int, budget: dict) -> bool:
    if budget["allow_full_scan"]:
        return row_count <= budget["max_estimated_rows"]
    return row_count <= budget["require_time_filter_above_rows"]


def _cost_tier(row_count: int) -> str:
    if row_count <= 100_000:
        return "low"
    if row_count <= 1_000_000:
        return "medium"
    return "high"


def _table_ref(table: dict) -> str:
    name = _quote_identifier(table["name"])
    schema = table.get("schema_name")
    if schema:
        return f"{_quote_identifier(schema)}.{name}"
    return name


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _bounded_int(value: object, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        parsed = minimum
    return min(max(parsed, minimum), maximum)


def _execute_candidate(connector: Any, candidate: dict) -> dict:
    started = time.perf_counter()
    sql = candidate["sql"]
    try:
        table = connector.execute_readonly(sql)
        rows = _table_rows(table)
        query_id = _last_query_id(connector)
        return {
            "status": "succeeded",
            "table_name": candidate.get("table_name"),
            "query_purpose": candidate.get("query_purpose"),
            "sql": sql,
            "row_count": len(rows),
            "rows": rows[:MAX_EXECUTION_RESULT_ROWS],
            "duration_ms": int((time.perf_counter() - started) * 1000),
            "query_id": query_id,
            "confidence_reason": "Executed as an approved read-only aggregate query.",
        }
    except ConnectorError as exc:
        return _failed_execution(connector, candidate, sql, started, str(exc))
    except Exception as exc:
        return _failed_execution(connector, candidate, sql, started, f"{type(exc).__name__}: {exc}")


def _failed_execution(
    connector: Any, candidate: dict, sql: str, started: float, error: str
) -> dict:
    return {
        "status": "failed",
        "table_name": candidate.get("table_name"),
        "query_purpose": candidate.get("query_purpose"),
        "sql": sql,
        "row_count": 0,
        "rows": [],
        "duration_ms": int((time.perf_counter() - started) * 1000),
        "query_id": _last_query_id(connector),
        "error": error,
        "confidence_reason": "Execution failed; no data evidence was produced.",
    }


def _table_rows(table: Any) -> list[dict]:
    if isinstance(table, pa.Table):
        return table.to_pylist()
    to_pylist = getattr(table, "to_pylist", None)
    if to_pylist:
        return to_pylist()
    if isinstance(table, list):
        return [dict(row) if not isinstance(row, dict) else row for row in table]
    return []


def _set_query_tag(connector: Any, query_tag: str) -> None:
    setter = getattr(connector, "set_query_tag", None)
    if setter:
        setter(query_tag)


def _set_statement_timeout(connector: Any, seconds: int) -> None:
    setter = getattr(connector, "set_statement_timeout", None)
    if setter:
        setter(seconds)


def _last_query_id(connector: Any) -> str | None:
    getter = getattr(connector, "last_query_id", None)
    if getter:
        try:
            value = getter()
        except Exception:
            return None
        return str(value) if value is not None else None
    return None
