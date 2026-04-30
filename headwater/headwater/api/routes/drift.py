"""Drift detection API routes -- US-402, US-403."""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request

from headwater.connectors.registry import get_connector_capabilities
from headwater.services.rerun_planner import build_rerun_plan

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/drift")
def get_drift_reports(request: Request, source: str | None = None, latest: bool = False):
    """Return drift reports, optionally filtered by source.

    Query params:
        source: optional source name filter.
        latest: if true, return only the most recent report.
    """
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    if latest:
        report = store.get_latest_drift_report(source_name=source)
        if report is None:
            return {"report": None, "message": "No drift reports found."}
        return report

    reports = store.get_drift_reports(source_name=source)
    if not reports:
        return {"reports": [], "message": "No drift reports found."}
    return {"reports": reports}


@router.patch("/drift/{report_id}/acknowledge")
def acknowledge_drift(request: Request, report_id: int):
    """Mark a drift report as acknowledged."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    # Verify report exists
    row = store.con.execute("SELECT id FROM drift_reports WHERE id = ?", (report_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Drift report {report_id} not found.")

    store.acknowledge_drift_report(report_id)
    return {"report_id": report_id, "acknowledged": True}


@router.get("/rerun-plan")
def get_rerun_plan(
    request: Request,
    source: str | None = None,
    drift_report_id: int | None = None,
):
    """Return targeted rerun guidance for the latest or selected drift report."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    report = _drift_report(store, source, drift_report_id)
    if report is None:
        return build_rerun_plan(
            drift_report=None,
            model_impacts=[],
            latest_quality=None,
            source_capabilities=None,
        )

    source_name = report.get("source_name")
    source_row = store.get_source(source_name) if source_name else None
    capabilities = (
        get_connector_capabilities(source_row["type"]).model_dump()
        if source_row is not None
        else {}
    )
    impacts = store.list_model_impacts(
        source_name=source_name,
        drift_report_id=report.get("id"),
        limit=200,
    )
    latest_quality = store.get_latest_quality_report(source_name)
    return build_rerun_plan(
        drift_report=report,
        model_impacts=impacts,
        latest_quality=latest_quality,
        source_capabilities=capabilities,
    )


def _drift_report(store, source: str | None, drift_report_id: int | None) -> dict | None:
    if drift_report_id is None:
        return store.get_latest_drift_report(source_name=source)
    row = store.con.execute(
        "SELECT * FROM drift_reports WHERE id = ?",
        (drift_report_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Drift report {drift_report_id} not found.")
    report = dict(row)
    report["diff"] = json.loads(report["diff_json"])
    return report
