"""Quality API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from headwater.api.project_scope import scoped_pipeline
from headwater.core.events import EventType
from headwater.quality.checker import check_contracts
from headwater.quality.report import build_report
from headwater.services.contract_lifecycle import apply_contract_statuses

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/contracts")
async def list_contracts(request: Request, project_id: str | None = None):
    """List all quality contracts."""
    contracts = scoped_pipeline(request, project_id)["contracts"]
    return [
        {
            "id": c.id,
            "model_name": c.model_name,
            "column_name": c.column_name,
            "rule_type": c.rule_type,
            "severity": c.severity,
            "confidence": c.confidence,
            "status": c.status,
            "description": c.description,
        }
        for c in contracts
    ]


@router.post("/quality/check")
async def run_quality_checks(request: Request):
    """Run quality checks on all contracts (moves them to observing first)."""
    pipeline = request.app.state.pipeline
    contracts = pipeline["contracts"]
    if not contracts:
        raise HTTPException(status_code=400, detail="No contracts generated yet.")

    # Move to observing for check
    for c in contracts:
        if c.status == "proposed":
            c.status = "observing"

    con = request.app.state.duckdb_con
    results = check_contracts(con, contracts, only_active=True)
    apply_contract_statuses(contracts, results)
    report = build_report(results)
    pipeline["quality_report"] = report
    source_name = _active_source_name(pipeline)
    quality_run_id = _persist_quality_report(request, source_name, report)

    return {
        "total": report.total_contracts,
        "passed": report.passed,
        "failed": report.failed,
        "skipped": report.skipped,
        "quality_run_id": quality_run_id,
        "results": [
            {
                "rule_id": r.rule_id,
                "model_name": r.model_name,
                "passed": r.passed,
                "skipped": r.skipped,
                "message": r.message,
            }
            for r in report.results
        ],
    }


@router.get("/quality")
async def get_quality_report(request: Request, project_id: str | None = None):
    """Get the latest quality report."""
    pipeline = scoped_pipeline(request, project_id)
    report = pipeline["quality_report"]
    if not report:
        store = getattr(request.app.state, "metadata_store", None)
        source_names = pipeline.get("source_names") or []
        latest = (
            store.get_latest_quality_report(source_names[0])
            if store is not None and source_names
            else store.get_latest_quality_report() if store is not None else None
        )
        if latest:
            return {
                "total": latest["total_contracts"],
                "passed": latest["passed"],
                "failed": latest["failed"],
                "skipped": latest["skipped"],
                "score": latest["score"],
                "quality_run_id": latest["id"],
            }
        return {"total": 0, "passed": 0, "failed": 0, "results": []}
    return {
        "total": report.total_contracts,
        "passed": report.passed,
        "failed": report.failed,
        "skipped": report.skipped,
        "score": round((report.passed / report.total_contracts) * 100, 2)
        if report.total_contracts
        else 100.0,
    }


@router.post("/contracts/{rule_id}/mark-false-positive")
async def mark_false_positive(request: Request, rule_id: str):
    """Mark a quality contract alert as a false positive.

    Writes a decisions row but does not change the contract status itself.
    """
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    # Verify contract exists
    contracts = request.app.state.pipeline.get("contracts", [])
    contract = next((c for c in contracts if c.id == rule_id), None)
    if contract is None:
        raise HTTPException(status_code=404, detail=f"Contract '{rule_id}' not found.")

    store.record_decision(
        "contract",
        rule_id,
        "false_positive",
        payload={"model_name": contract.model_name, "rule_type": contract.rule_type},
    )
    return {"rule_id": rule_id, "marked": "false_positive"}


@router.get("/audit")
async def get_audit_log(request: Request, limit: int = 100):
    """Return the last N LLM audit log entries (default 100)."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        return []
    return store.get_llm_audit_log(limit=limit)


def _active_source_name(pipeline: dict) -> str:
    discovery = pipeline.get("discovery")
    source = getattr(discovery, "source", None)
    return getattr(source, "name", None) or "source"


def _persist_quality_report(request: Request, source_name: str, report):
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        return None
    run_id = store.save_quality_report(source_name, report)
    if report.failed:
        try:
            store.insert_event(
                EventType.QUALITY_CHECKS_FAILED,
                f"{report.failed} quality contract(s) failed",
                source_name=source_name,
                severity="warning",
                artifact_type="quality_run",
                artifact_id=str(run_id),
                payload={
                    "quality_run_id": run_id,
                    "total": report.total_contracts,
                    "passed": report.passed,
                    "failed": report.failed,
                },
                invalidates=["sources", "briefing", "health", "insights", "quality"],
            )
        except Exception:
            logger.exception("Failed to emit quality event for '%s'", source_name)
    elif getattr(report, "previous_failed", 0):
        try:
            store.insert_event(
                EventType.QUALITY_CHECKS_RECOVERED,
                "Quality contracts recovered",
                source_name=source_name,
                severity="info",
                artifact_type="quality_run",
                artifact_id=str(run_id),
                payload={
                    "quality_run_id": run_id,
                    "total": report.total_contracts,
                    "passed": report.passed,
                    "failed": report.failed,
                    "previous_failed": getattr(report, "previous_failed", 0),
                },
                invalidates=["sources", "briefing", "health", "insights", "quality"],
            )
        except Exception:
            logger.exception("Failed to emit quality recovery event for '%s'", source_name)
    return run_id
