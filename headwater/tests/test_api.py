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
        assert data["mart_models"] == 5

    def test_list_models(self, client):
        self._setup(client)
        resp = client.get("/api/models")
        assert resp.status_code == 200
        models = resp.json()
        assert len(models) == 13  # 8 staging + 5 marts

    def test_get_model(self, client):
        self._setup(client)
        resp = client.get("/api/models/stg_zones")
        assert resp.status_code == 200
        data = resp.json()
        assert "sql" in data

    def test_approve_model(self, client):
        self._setup(client)
        resp = client.post("/api/models/mart_incident_summary/approve")
        assert resp.status_code == 200
        assert resp.json()["status"] == "approved"

    def test_reject_model(self, client):
        self._setup(client)
        resp = client.post("/api/models/mart_incident_summary/reject")
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
