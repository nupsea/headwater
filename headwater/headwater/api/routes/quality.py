"""Quality API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from headwater.quality.checker import check_contracts
from headwater.quality.report import build_report

router = APIRouter()


@router.get("/contracts")
async def list_contracts(request: Request):
    """List all quality contracts."""
    contracts = request.app.state.pipeline["contracts"]
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
    report = build_report(results)
    pipeline["quality_report"] = report

    return {
        "total": report.total_contracts,
        "passed": report.passed,
        "failed": report.failed,
        "results": [
            {
                "rule_id": r.rule_id,
                "model_name": r.model_name,
                "passed": r.passed,
                "message": r.message,
            }
            for r in report.results
        ],
    }


@router.get("/quality")
async def get_quality_report(request: Request):
    """Get the latest quality report."""
    report = request.app.state.pipeline["quality_report"]
    if not report:
        return {"total": 0, "passed": 0, "failed": 0, "results": []}
    return {
        "total": report.total_contracts,
        "passed": report.passed,
        "failed": report.failed,
    }
