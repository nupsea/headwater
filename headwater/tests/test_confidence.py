"""Tests for confidence metrics (US-302, US-303)."""

from __future__ import annotations

import pytest

from headwater.core.metadata import MetadataStore


@pytest.fixture()
def meta() -> MetadataStore:
    store = MetadataStore(":memory:")
    store.init()
    return store


class TestDescriptionAcceptanceRate:
    """US-302: Description acceptance rate."""

    def test_below_threshold_returns_null(self, meta: MetadataStore):
        """Returns null with reason when below minimum decisions."""
        # Only 3 decisions (below default threshold of 5)
        for i in range(3):
            meta.record_decision(
                "column", f"src.t.col{i}", "description_accepted",
            )
        result = meta.get_description_acceptance_rate()
        assert result["acceptance_rate"] is None
        assert result["sample_size"] == 3
        assert "Below minimum threshold" in result["reason"]

    def test_all_accepted(self, meta: MetadataStore):
        """100% acceptance rate when all descriptions accepted."""
        for i in range(6):
            meta.record_decision(
                "column", f"src.t.col{i}", "description_accepted",
            )
        result = meta.get_description_acceptance_rate()
        assert result["acceptance_rate"] == 1.0
        assert result["sample_size"] == 6
        assert result["reason"] is None

    def test_mixed_accepted_and_edited(self, meta: MetadataStore):
        """Correct rate with mix of accepted and edited."""
        for i in range(4):
            meta.record_decision(
                "column", f"src.t.col{i}", "description_accepted",
            )
        for i in range(6):
            meta.record_decision(
                "column", f"src.t.edit{i}", "description_edited",
            )
        result = meta.get_description_acceptance_rate()
        assert result["acceptance_rate"] == 0.4  # 4/10
        assert result["sample_size"] == 10

    def test_scoped_per_source(self, meta: MetadataStore):
        """Acceptance rate scoped to a specific source."""
        # 5 accepted for src1
        for i in range(5):
            meta.record_decision(
                "column", f"src1.t.col{i}", "description_accepted",
            )
        # 5 edited for src2
        for i in range(5):
            meta.record_decision(
                "column", f"src2.t.col{i}", "description_edited",
            )
        result_src1 = meta.get_description_acceptance_rate(source_name="src1")
        assert result_src1["acceptance_rate"] == 1.0
        assert result_src1["sample_size"] == 5

        result_src2 = meta.get_description_acceptance_rate(source_name="src2")
        assert result_src2["acceptance_rate"] == 0.0
        assert result_src2["sample_size"] == 5

    def test_custom_min_decisions(self, meta: MetadataStore):
        """Custom min_decisions threshold."""
        for i in range(3):
            meta.record_decision(
                "column", f"src.t.col{i}", "description_accepted",
            )
        result = meta.get_description_acceptance_rate(min_decisions=3)
        assert result["acceptance_rate"] == 1.0

    def test_locked_action_counts_as_edited(self, meta: MetadataStore):
        """'locked' action counts towards total but not as accepted."""
        for i in range(3):
            meta.record_decision(
                "column", f"src.t.col{i}", "description_accepted",
            )
        for i in range(3):
            meta.record_decision(
                "column", f"src.t.lock{i}", "locked",
            )
        result = meta.get_description_acceptance_rate()
        assert result["acceptance_rate"] == 0.5  # 3 accepted / 6 total
        assert result["sample_size"] == 6


class TestModelEditDistance:
    """US-303: Model edit distance."""

    def test_no_edits_returns_null(self, meta: MetadataStore):
        """Returns null when no model edits recorded."""
        result = meta.get_model_edit_distance_avg()
        assert result["edit_distance_avg"] is None
        assert result["sample_size"] == 0

    def test_single_edit(self, meta: MetadataStore):
        """Single edit returns exact distance."""
        meta.record_decision(
            "model", "mart_x", "edited",
            payload={"edit_distance": 0.25, "original_sql": "...", "new_sql": "..."},
        )
        result = meta.get_model_edit_distance_avg()
        assert result["edit_distance_avg"] == 0.25
        assert result["sample_size"] == 1

    def test_multiple_edits_averaged(self, meta: MetadataStore):
        """Multiple edits are averaged."""
        meta.record_decision(
            "model", "mart_a", "edited",
            payload={"edit_distance": 0.1},
        )
        meta.record_decision(
            "model", "mart_b", "edited",
            payload={"edit_distance": 0.3},
        )
        result = meta.get_model_edit_distance_avg()
        assert result["edit_distance_avg"] == 0.2
        assert result["sample_size"] == 2

    def test_ignores_edits_without_distance(self, meta: MetadataStore):
        """Edit decisions without edit_distance in payload are ignored."""
        meta.record_decision(
            "model", "mart_a", "edited",
            payload={"edit_distance": 0.5},
        )
        meta.record_decision(
            "model", "mart_b", "edited",
            payload={"note": "no distance"},
        )
        result = meta.get_model_edit_distance_avg()
        assert result["edit_distance_avg"] == 0.5
        assert result["sample_size"] == 1


class TestContractPrecision:
    """US-303: Contract precision."""

    def test_no_decisions_returns_null(self, meta: MetadataStore):
        """Returns null when no contract decisions."""
        result = meta.get_contract_precision()
        assert result["precision"] is None
        assert result["sample_size"] == 0

    def test_all_true_positives(self, meta: MetadataStore):
        """Precision is 1.0 when all alerts are true positives."""
        for i in range(5):
            meta.record_decision("contract", f"c{i}", "acknowledged")
        result = meta.get_contract_precision()
        assert result["precision"] == 1.0
        assert result["sample_size"] == 5

    def test_all_false_positives(self, meta: MetadataStore):
        """Precision is 0.0 when all alerts are false positives."""
        for i in range(5):
            meta.record_decision("contract", f"c{i}", "false_positive")
        result = meta.get_contract_precision()
        assert result["precision"] == 0.0
        assert result["sample_size"] == 5

    def test_mixed_precision(self, meta: MetadataStore):
        """Correct precision with mix of true and false positives."""
        meta.record_decision("contract", "c1", "acknowledged")
        meta.record_decision("contract", "c2", "acknowledged")
        meta.record_decision("contract", "c3", "false_positive")
        meta.record_decision("contract", "c4", "acknowledged")
        result = meta.get_contract_precision()
        assert result["precision"] == 0.75  # 3 true / 4 total
        assert result["sample_size"] == 4


class TestConfidenceAPI:
    """Test the GET /api/confidence endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient

        from headwater.api.app import create_app
        app = create_app(in_memory=True)
        with TestClient(app) as c:
            yield c

    def test_confidence_endpoint_returns_all_metrics(self, client):
        """Endpoint returns all three metric fields."""
        resp = client.get("/api/confidence")
        assert resp.status_code == 200
        data = resp.json()
        assert "description_acceptance_rate" in data
        assert "model_edit_distance_avg" in data
        assert "contract_precision" in data
        assert "description_sample_size" in data
        assert "model_edit_distance_sample_size" in data
        assert "contract_precision_sample_size" in data

    def test_confidence_endpoint_scoped_by_source(self, client):
        """Endpoint accepts source query param."""
        resp = client.get("/api/confidence?source=test_source")
        assert resp.status_code == 200
        data = resp.json()
        assert data["description_acceptance_rate"] is None  # No data yet

    def test_confidence_below_threshold_shows_reason(self, client):
        """Below threshold, reason field is populated."""
        resp = client.get("/api/confidence")
        assert resp.status_code == 200
        data = resp.json()
        assert data["description_acceptance_rate"] is None
        assert "Below minimum threshold" in data["description_reason"]
