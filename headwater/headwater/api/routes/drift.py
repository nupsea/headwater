"""Drift detection API routes -- US-403."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()


@router.get("/drift")
def get_drift_report(request: Request, source: str | None = None):
    """Return the latest drift report, optionally filtered by source."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    report = store.get_latest_drift_report(source_name=source)
    if report is None:
        return {"report": None, "message": "No drift reports found."}
    return report


@router.patch("/drift/{report_id}/acknowledge")
def acknowledge_drift(request: Request, report_id: int):
    """Mark a drift report as acknowledged."""
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    # Verify report exists
    # Check by querying drift_reports directly
    row = store.con.execute(
        "SELECT id FROM drift_reports WHERE id = ?", (report_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Drift report {report_id} not found.")

    store.acknowledge_drift_report(report_id)
    return {"report_id": report_id, "acknowledged": True}
