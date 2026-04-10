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
