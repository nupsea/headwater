"""Confidence metrics API routes -- US-302, US-303."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

router = APIRouter()


@router.get("/confidence")
def get_confidence_metrics(request: Request, source: str | None = None):
    """Return confidence metrics for the advisory system.

    Query params:
        source: optional source name to scope metrics.
    """
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    acceptance = store.get_description_acceptance_rate(source_name=source)
    edit_distance = store.get_model_edit_distance_avg(source_name=source)
    precision = store.get_contract_precision(source_name=source)

    return {
        "description_acceptance_rate": acceptance["acceptance_rate"],
        "description_sample_size": acceptance["sample_size"],
        "description_reason": acceptance["reason"],
        "model_edit_distance_avg": edit_distance["edit_distance_avg"],
        "model_edit_distance_sample_size": edit_distance["sample_size"],
        "contract_precision": precision["precision"],
        "contract_precision_sample_size": precision["sample_size"],
    }
