"""Tests for project-context confidence calibration and evidence normalization."""

from __future__ import annotations

from headwater.core.context_confidence import (
    calibrate_producer_confidence,
    combine_evidence_confidence,
    normalize_evidence_record,
)
from headwater.core.metadata import MetadataStore


def test_calibration_ordering_is_preserved_across_producers():
    semantic = calibrate_producer_confidence(
        "semantic_type",
        0.99,
        method="regex_format",
    )
    llm = calibrate_producer_confidence("llm", 0.99, method="single_shot")
    constraint = calibrate_producer_confidence("constraint", 0.99)
    user = calibrate_producer_confidence("user", 0.8, status="approved")

    assert user > constraint > semantic > llm
    assert llm <= 0.7
    assert semantic >= 0.95


def test_confidence_combiner_is_deterministic_and_conflict_bounded():
    evidence = [
        {
            "evidence_type": "resource",
            "source": "resource",
            "summary": "Dictionary match.",
            "confidence": 0.91,
        },
        {
            "evidence_type": "classification",
            "source": "profiler",
            "summary": "Profile agrees.",
            "confidence": 0.84,
        },
    ]

    first = combine_evidence_confidence(evidence)
    second = combine_evidence_confidence(list(reversed(evidence)))
    conflicted = combine_evidence_confidence(
        [
            *evidence,
            {
                "evidence_type": "resource_conflict",
                "source": "resource",
                "summary": "Resource conflict.",
                "confidence": 0.9,
                "conflict": True,
            },
        ]
    )

    assert first == second
    assert first > 0.91
    assert conflicted <= 0.55


def test_normalized_evidence_has_stable_id_and_canonical_fields():
    record = {
        "evidence_type": "profile",
        "source": "profiler",
        "summary": "Uniqueness ratio is high.",
        "payload": {"uniqueness_ratio": 1.0},
        "support_count": 100,
        "sample_size": 100,
        "confidence": 0.88,
    }

    first = normalize_evidence_record(record, item_id="pk_candidate:orders.order_id")
    second = normalize_evidence_record(record, item_id="pk_candidate:orders.order_id")

    assert first["evidence_id"] == second["evidence_id"]
    assert first["producer"] == "profile"
    assert first["support_count"] == 100
    assert first["sample_size"] == 100
    assert first["payload"]["uniqueness_ratio"] == 1.0


def test_metadata_store_round_trips_canonical_evidence(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.replace_project_context(
        "src",
        source_name="src",
        items=[
            {
                "id": "column_semantics:orders.order_id",
                "project_id": "src",
                "source_name": "src",
                "item_type": "column_semantics",
                "scope": "column",
                "name": "order_id",
                "table_name": "orders",
                "column_name": "order_id",
                "value": {"semantic_type": "id"},
                "confidence": 0.95,
                "evidence": [
                    {
                        "evidence_type": "profile",
                        "source": "profiler",
                        "summary": "Uniqueness ratio is 1.0",
                        "payload": {"uniqueness_ratio": 1.0},
                        "support_count": 10,
                        "sample_size": 10,
                        "confidence": 0.9,
                    }
                ],
            }
        ],
    )

    item = meta.get_project_context_item(
        "column_semantics:orders.order_id",
        project_id="src",
    )

    evidence = item["evidence"][0]
    assert evidence["payload"]["uniqueness_ratio"] == 1.0
    assert evidence["producer"] == "profile"
    assert evidence["method"] == "profile"
    assert evidence["support_count"] == 10
    assert evidence["sample_size"] == 10
    assert evidence["confidence"] == 0.9
    assert evidence["evidence_id"].startswith("evidence:")
