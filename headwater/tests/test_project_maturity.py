"""Tests for project maturity scoring and blockers."""

from __future__ import annotations

from headwater.api.routes.project import _compute_maturity, _maturity_blockers


def _progress(**overrides):
    base = {
        "tables_discovered": 4,
        "tables_profiled": 4,
        "tables_reviewed": 4,
        "tables_modeled": 4,
        "tables_mart_ready": 2,
        "mart_models_review_pending": 0,
        "materialized_models": 4,
        "invalidated_models": 0,
        "impacted_models": 0,
        "columns_total": 20,
        "columns_described": 20,
        "columns_confirmed": 20,
        "relationships_detected": 3,
        "relationships_confirmed": 3,
        "metrics_defined": 5,
        "metrics_confirmed": 5,
        "dimensions_defined": 4,
        "dimensions_confirmed": 4,
        "quality_contracts": 10,
        "contracts_enforcing": 10,
        "contracts_observing": 0,
        "contracts_failing": 0,
        "contracts_recovered": 0,
        "quality_failed": 0,
        "quality_score": 100.0,
        "source_drift_count": 0,
        "catalog_coverage": 0.9,
    }
    base.update(overrides)
    return base


def test_maturity_reaches_production_when_workflow_is_clean():
    maturity, score = _compute_maturity(_progress())

    assert maturity == "production"
    assert score >= 0.8


def test_maturity_blocks_on_drift_and_failing_quality():
    progress = _progress(
        source_drift_count=1,
        invalidated_models=1,
        contracts_failing=2,
        quality_failed=2,
        quality_score=80.0,
    )

    maturity, score = _compute_maturity(progress)
    blockers = _maturity_blockers(progress)

    assert maturity != "production"
    assert score < 0.8
    assert any(blocker["title"] == "Schema drift needs review" for blocker in blockers)
    assert any(blocker["title"] == "Quality contracts failing" for blocker in blockers)
