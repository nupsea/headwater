"""Tests for the FastAPI layer."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from headwater.api.app import create_app

SAMPLE_DATA = str(
    Path(__file__).resolve().parent.parent.parent / "data" / "sample"
)


@pytest.fixture
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


class TestStatus:
    def test_status_before_discovery(self, client):
        resp = client.get("/api/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["discovered"] is False
        assert data["tables"] == 0

    def test_status_after_discovery(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        resp = client.get("/api/status")
        data = resp.json()
        assert data["discovered"] is True
        assert data["tables"] == 8


class TestDiscovery:
    def test_discover(self, client):
        resp = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert resp.status_code == 200
        data = resp.json()
        assert data["tables"] == 8
        assert data["profiles"] > 0
        assert data["relationships"] > 0

    def test_discover_bad_path(self, client):
        resp = client.post("/api/discover", params={"source_path": "/nonexistent"})
        assert resp.status_code == 400

    def test_list_tables(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        resp = client.get("/api/tables")
        assert resp.status_code == 200
        tables = resp.json()
        assert len(tables) == 8
        names = {t["name"] for t in tables}
        assert "zones" in names

    def test_list_tables_before_discovery(self, client):
        resp = client.get("/api/tables")
        assert resp.status_code == 400

    def test_get_table(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        resp = client.get("/api/tables/zones")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "zones"
        assert len(data["columns"]) > 0

    def test_get_table_not_found(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        resp = client.get("/api/tables/nonexistent")
        assert resp.status_code == 404

    def test_get_profile(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        resp = client.get("/api/tables/zones/profile")
        assert resp.status_code == 200
        profiles = resp.json()
        assert len(profiles) > 0

    def test_relationships(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        resp = client.get("/api/relationships")
        assert resp.status_code == 200
        rels = resp.json()
        assert len(rels) > 0


class TestModels:
    def _setup(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        return client.post("/api/generate")

    def test_generate(self, client):
        resp = self._setup(client)
        assert resp.status_code == 200
        data = resp.json()
        assert data["staging_models"] == 8
        assert data["mart_models"] >= 1  # Pattern-based: at least one mart from sample data

    def test_list_models(self, client):
        self._setup(client)
        resp = client.get("/api/models")
        assert resp.status_code == 200
        models = resp.json()
        # 8 staging + at least 1 mart (pattern-matched)
        assert len(models) >= 9

    def test_get_model(self, client):
        self._setup(client)
        resp = client.get("/api/models/stg_zones")
        assert resp.status_code == 200
        data = resp.json()
        assert "sql" in data

    def test_approve_model(self, client):
        self._setup(client)
        # Find any proposed mart to approve
        models_resp = client.get("/api/models")
        mart = next(
            m for m in models_resp.json()
            if m["model_type"] == "mart" and m["status"] == "proposed"
        )
        resp = client.post(f"/api/models/{mart['name']}/approve")
        assert resp.status_code == 200
        assert resp.json()["status"] == "approved"

    def test_reject_model(self, client):
        self._setup(client)
        models_resp = client.get("/api/models")
        mart = next(
            m for m in models_resp.json()
            if m["model_type"] == "mart" and m["status"] == "proposed"
        )
        resp = client.post(f"/api/models/{mart['name']}/reject")
        assert resp.status_code == 200
        assert resp.json()["status"] == "rejected"

    def test_approve_non_proposed(self, client):
        self._setup(client)
        # stg_zones is already approved
        resp = client.post("/api/models/stg_zones/approve")
        assert resp.status_code == 400


class TestExecution:
    def test_execute(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        client.post("/api/generate")
        resp = client.post("/api/execute")
        assert resp.status_code == 200
        results = resp.json()
        # Only staging models are approved by default
        assert len(results) == 8
        assert all(r["success"] for r in results)

    def test_execute_no_models(self, client):
        resp = client.post("/api/execute")
        assert resp.status_code == 400


class TestQuality:
    def test_quality_check(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        client.post("/api/generate")
        client.post("/api/execute")
        resp = client.post("/api/quality/check")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0
        assert data["passed"] + data["failed"] == data["total"]

    def test_quality_report(self, client):
        resp = client.get("/api/quality")
        assert resp.status_code == 200


class TestSemanticLockEndpoint:
    """US-201: PATCH /api/columns endpoint."""

    def _setup(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})

    def test_patch_column_description_locks(self, client):
        self._setup(client)
        resp = client.patch(
            "/api/columns/source/zones/zone_id",
            json={"description": "Unique zone identifier"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["locked"] is True
        assert data["description"] == "Unique zone identifier"

    def test_patch_column_lock_false(self, client):
        self._setup(client)
        # First lock it
        client.patch(
            "/api/columns/source/zones/zone_id",
            json={"description": "test desc"},
        )
        # Then unlock
        resp = client.patch(
            "/api/columns/source/zones/zone_id",
            json={"locked": False},
        )
        assert resp.status_code == 200
        assert resp.json()["locked"] is False

    def test_patch_column_records_decision(self, client):
        self._setup(client)
        client.patch(
            "/api/columns/source/zones/zone_id",
            json={"description": "Locked desc"},
        )
        store = client.app.state.metadata_store
        decisions = store.get_decisions("column", "source.zones.zone_id")
        assert len(decisions) >= 1
        assert any(d["action"] == "locked" for d in decisions)

    def test_patch_column_not_found(self, client):
        self._setup(client)
        resp = client.patch(
            "/api/columns/source/zones/nonexistent_col",
            json={"description": "test"},
        )
        assert resp.status_code == 404


class TestFalsePositive:
    """US-304: POST /api/contracts/{rule_id}/mark-false-positive."""

    def _setup(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        client.post("/api/generate")

    def test_mark_false_positive(self, client):
        self._setup(client)
        contracts = client.get("/api/contracts").json()
        assert len(contracts) > 0
        rule_id = contracts[0]["id"]
        resp = client.post(f"/api/contracts/{rule_id}/mark-false-positive")
        assert resp.status_code == 200
        data = resp.json()
        assert data["rule_id"] == rule_id
        assert data["marked"] == "false_positive"

        # Verify decisions row was written
        store = client.app.state.metadata_store
        decisions = store.get_decisions("contract", rule_id)
        assert len(decisions) >= 1
        assert any(d["action"] == "false_positive" for d in decisions)

    def test_mark_false_positive_not_found(self, client):
        self._setup(client)
        resp = client.post("/api/contracts/nonexistent/mark-false-positive")
        assert resp.status_code == 404


class TestUnlockEndpoint:
    """US-202: PATCH /api/columns/.../  with locked=false."""

    def _setup(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})

    def test_unlock_writes_decision(self, client):
        self._setup(client)
        # Lock first
        client.patch(
            "/api/columns/source/zones/zone_id",
            json={"description": "Locked desc"},
        )
        # Unlock
        resp = client.patch(
            "/api/columns/source/zones/zone_id",
            json={"locked": False},
        )
        assert resp.status_code == 200
        assert resp.json()["locked"] is False
        # Verify decisions row with action='unlocked'
        store = client.app.state.metadata_store
        decisions = store.get_decisions("column", "source.zones.zone_id")
        assert any(d["action"] == "unlocked" for d in decisions)


class TestDriftAPI:
    """US-402/403: Drift detection API endpoints."""

    def test_drift_no_reports(self, client):
        """GET /api/drift returns empty when no reports."""
        resp = client.get("/api/drift")
        assert resp.status_code == 200
        data = resp.json()
        assert data["reports"] == [] or data.get("message")

    def test_drift_latest_no_reports(self, client):
        """GET /api/drift?latest=true returns null report when none exist."""
        resp = client.get("/api/drift?latest=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["report"] is None

    def test_drift_report_creation_and_retrieval(self, client):
        """Create a drift report and retrieve it via API."""
        store = client.app.state.metadata_store
        store.upsert_source("src", "json", "/data", None)
        run1 = store.start_run("src")
        store.finish_run(run1, table_count=1)
        run2 = store.start_run("src")
        store.finish_run(run2, table_count=1)

        diff_data = {
            "source_name": "src",
            "run_id_from": run1,
            "run_id_to": run2,
            "no_changes": False,
            "tables_added": ["new_table"],
            "tables_removed": [],
            "tables_changed": [],
            "detected_at": "2026-01-01T00:00:00Z",
        }
        store.save_drift_report("src", run1, run2, diff_data)

        resp = client.get("/api/drift?latest=true&source=src")
        assert resp.status_code == 200
        data = resp.json()
        assert data["source_name"] == "src"
        assert data["run_id_to"] == run2

    def test_acknowledge_drift_report(self, client):
        """PATCH /api/drift/{id}/acknowledge marks report as acknowledged."""
        store = client.app.state.metadata_store
        store.upsert_source("src", "json", "/data", None)
        run1 = store.start_run("src")
        store.finish_run(run1, table_count=1)

        report_id = store.save_drift_report("src", None, run1, {"no_changes": True})

        resp = client.patch(f"/api/drift/{report_id}/acknowledge")
        assert resp.status_code == 200
        data = resp.json()
        assert data["acknowledged"] is True

    def test_acknowledge_nonexistent_drift_report(self, client):
        """PATCH /api/drift/999/acknowledge returns 404."""
        resp = client.patch("/api/drift/999/acknowledge")
        assert resp.status_code == 404

    def test_drift_reports_list(self, client):
        """GET /api/drift returns list of reports."""
        store = client.app.state.metadata_store
        store.upsert_source("src", "json", "/data", None)
        run1 = store.start_run("src")
        store.finish_run(run1, table_count=1)
        run2 = store.start_run("src")
        store.finish_run(run2, table_count=1)

        store.save_drift_report("src", None, run1, {"no_changes": True})
        store.save_drift_report("src", run1, run2, {"no_changes": False})

        resp = client.get("/api/drift")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["reports"]) == 2


class TestDecisionRecording:
    """US-301: verify approve/reject writes to decisions table."""

    def _setup(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        client.post("/api/generate")

    def _first_proposed_mart_name(self, client) -> str:
        models_resp = client.get("/api/models")
        mart = next(
            m for m in models_resp.json()
            if m["model_type"] == "mart" and m["status"] == "proposed"
        )
        return mart["name"]

    def test_approve_records_decision(self, client):
        self._setup(client)
        mart_name = self._first_proposed_mart_name(client)
        client.post(f"/api/models/{mart_name}/approve")
        store = client.app.state.metadata_store
        decisions = store.get_decisions("model", mart_name)
        assert len(decisions) == 1
        assert decisions[0]["action"] == "approved"

    def test_reject_records_decision(self, client):
        self._setup(client)
        mart_name = self._first_proposed_mart_name(client)
        client.post(f"/api/models/{mart_name}/reject")
        store = client.app.state.metadata_store
        decisions = store.get_decisions("model", mart_name)
        assert len(decisions) == 1
        assert decisions[0]["action"] == "rejected"

    def test_decision_payload_contains_previous_status(self, client):
        import json

        self._setup(client)
        mart_name = self._first_proposed_mart_name(client)
        client.post(f"/api/models/{mart_name}/approve")
        store = client.app.state.metadata_store
        decisions = store.get_decisions("model", mart_name)
        payload = json.loads(decisions[0]["payload_json"])
        assert payload["previous_status"] == "proposed"
