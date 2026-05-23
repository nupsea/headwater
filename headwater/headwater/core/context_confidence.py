"""Calibration helpers for project-context evidence and confidence."""

from __future__ import annotations

import hashlib
import json
from typing import Any

CANONICAL_PAYLOAD_KEY = "_canonical_evidence"

_PRODUCER_ALIASES = {
    "schema": "profile",
    "table_shape": "profile",
    "row_grain": "profile",
    "table_name": "profile",
    "key_candidate": "profile",
    "temporal_column": "profile",
    "lookup_shape": "profile",
    "relationship": "profile",
    "classification": "profile",
    "profiler": "profile",
    "bootstrap": "profile",
    "schema_drift": "profile",
    "constraint": "constraint",
    "declared": "constraint",
    "declared_constraint": "constraint",
    "semantic_type": "semantic_type",
    "format": "semantic_type",
    "regex": "semantic_type",
    "resource": "resource",
    "resource_conflict": "resource",
    "resource_question": "resource",
    "llm": "llm",
    "assistant": "llm",
    "advisor_pack": "advisor_pack",
    "pack": "advisor_pack",
    "user": "user",
    "review": "user",
    "import": "import",
}


def canonical_producer(*, source: str | None = None, evidence_type: str | None = None) -> str:
    """Map legacy source/evidence labels to bounded producer categories."""
    for candidate in (source, evidence_type):
        normalized = str(candidate or "").lower()
        if normalized in _PRODUCER_ALIASES:
            return _PRODUCER_ALIASES[normalized]
    return "profile"


def calibrate_producer_confidence(
    producer: str,
    raw_confidence: float | None = None,
    *,
    method: str | None = None,
    status: str | None = None,
    evidence_count: int = 1,
    source_authority_confirmed: bool = False,
    conflict: bool = False,
) -> float:
    """Return a bounded, comparable confidence score for one producer signal."""
    producer = canonical_producer(source=producer)
    raw = _bounded(raw_confidence if raw_confidence is not None else _default_for(producer))

    if producer == "user":
        score = 1.0 if status in {"approved", "locked"} else raw
    elif producer == "constraint":
        score = min(max(raw, 0.98), 0.98)
    elif producer == "semantic_type":
        exact = method and any(token in method for token in ("regex", "format", "checksum"))
        score = min(max(raw, 0.95), 0.97) if exact else min(raw, 0.9)
    elif producer == "profile":
        score = min(raw, 0.9)
    elif producer == "resource":
        cap = 1.0 if source_authority_confirmed else 0.95
        score = min(raw, cap)
    elif producer == "advisor_pack":
        score = min(raw, 0.85)
    elif producer == "llm":
        cap = 0.85 if evidence_count >= 2 else 0.7
        score = min(raw, cap)
    elif producer == "import":
        score = 1.0 if source_authority_confirmed else min(raw, 0.95)
    else:
        score = raw

    if conflict:
        score = min(score, 0.55)
    return round(_bounded(score), 4)


def normalize_evidence_record(
    record: dict[str, Any],
    *,
    item_id: str | None = None,
    fallback_source: str = "bootstrap",
    fallback_confidence: float | None = None,
) -> dict[str, Any]:
    """Return legacy evidence plus canonical producer fields."""
    payload = dict(record.get("payload") or {})
    stored_canonical = dict(payload.pop(CANONICAL_PAYLOAD_KEY, {}) or {})
    evidence_type = (
        record.get("evidence_type")
        or stored_canonical.get("evidence_type")
        or "profile"
    )
    source = record.get("source") or stored_canonical.get("source") or fallback_source
    producer = record.get("producer") or stored_canonical.get("producer")
    producer = producer or canonical_producer(source=source, evidence_type=evidence_type)
    method = record.get("method") or stored_canonical.get("method") or evidence_type
    confidence = record.get("confidence")
    if confidence is None:
        confidence = stored_canonical.get("confidence")
    if confidence is None:
        confidence = payload.get("confidence", fallback_confidence)
    support_count = record.get("support_count", stored_canonical.get("support_count"))
    sample_size = record.get("sample_size", stored_canonical.get("sample_size"))
    observed_value = record.get("observed_value", stored_canonical.get("observed_value"))
    input_snapshot_id = record.get(
        "input_snapshot_id",
        stored_canonical.get("input_snapshot_id"),
    )
    source_ref = record.get("source_ref") or stored_canonical.get("source_ref")
    conflict = bool(record.get("conflict", stored_canonical.get("conflict", False)))
    calibrated = calibrate_producer_confidence(
        producer,
        _as_float(confidence),
        method=str(method or ""),
        evidence_count=int(record.get("evidence_count") or 1),
        conflict=conflict,
    )
    canonical = {
        "evidence_id": record.get("evidence_id")
        or stored_canonical.get("evidence_id")
        or _evidence_id(
            item_id=item_id,
            producer=producer,
            method=str(method or ""),
            source_ref=source_ref,
            payload=payload,
        ),
        "producer": producer,
        "method": method,
        "input_snapshot_id": input_snapshot_id,
        "source_ref": source_ref,
        "observed_value": observed_value,
        "support_count": support_count,
        "sample_size": sample_size,
        "confidence": calibrated,
        "conflict": conflict,
    }
    normalized_payload = dict(payload)
    normalized_payload[CANONICAL_PAYLOAD_KEY] = canonical
    return {
        "evidence_type": evidence_type,
        "source": source,
        "summary": record.get("summary") or stored_canonical.get("summary") or "",
        "payload": normalized_payload,
        **canonical,
    }


def decode_evidence_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split persisted payload into user payload and canonical fields."""
    clean_payload = dict(payload or {})
    canonical = dict(clean_payload.pop(CANONICAL_PAYLOAD_KEY, {}) or {})
    return clean_payload, canonical


def combine_evidence_confidence(
    evidence: list[dict[str, Any]],
    *,
    raw_confidence: float | None = None,
    source: str | None = None,
    status: str | None = None,
) -> float:
    """Combine producer votes deterministically without swamping strong evidence."""
    normalized = [
        normalize_evidence_record(
            record,
            fallback_source=source or "bootstrap",
            fallback_confidence=raw_confidence,
        )
        for record in evidence
    ]
    scores = [float(record.get("confidence") or 0.0) for record in normalized]
    if raw_confidence is not None:
        scores.append(
            calibrate_producer_confidence(
                source or "profile",
                raw_confidence,
                status=status,
                evidence_count=len(evidence),
            )
        )
    if not scores:
        return 0.0
    if status in {"approved", "locked"} and source == "user":
        return 1.0
    score = max(scores)
    distinct_producers = {record.get("producer") for record in normalized}
    if len(distinct_producers) > 1:
        score = min(score + 0.03 * (len(distinct_producers) - 1), 1.0)
    if any(record.get("conflict") for record in normalized):
        score = min(score, 0.55)
    return round(_bounded(score), 4)


def _default_for(producer: str) -> float:
    return {
        "constraint": 0.98,
        "semantic_type": 0.95,
        "profile": 0.55,
        "resource": 0.75,
        "advisor_pack": 0.6,
        "llm": 0.7,
        "user": 1.0,
        "import": 0.95,
    }.get(producer, 0.5)


def _bounded(value: float | int | None) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    return max(0.0, min(numeric, 1.0))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _evidence_id(
    *,
    item_id: str | None,
    producer: str,
    method: str,
    source_ref: str | None,
    payload: dict[str, Any],
) -> str:
    body = json.dumps(
        {
            "item_id": item_id,
            "producer": producer,
            "method": method,
            "source_ref": source_ref,
            "payload": payload,
        },
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha1(body.encode("utf-8")).hexdigest()[:16]
    return f"evidence:{digest}"
