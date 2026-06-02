"""Tests for Headwater 2 S15 — API routes."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")
RADIOLOGY_DATA = str(Path(__file__).resolve().parents[2] / "data" / "radiology")


def _h2_client(tmp_path: Path):
    """Create a TestClient wired to the H2 store in tmp_path."""
    import os
    os.environ["HEADWATER_DATA_DIR"] = str(tmp_path)
    from headwater.core.config import get_settings
    get_settings.cache_clear()
    from headwater.api.routes.h2 import router
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI()
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                       allow_headers=["*"])
    app.include_router(router, prefix="/api")
    return TestClient(app)


class TestH2SourceRoutes:
    def test_list_sources_empty(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            r = client.get("/api/h2/sources")
            assert r.status_code == 200
            assert r.json() == []
        finally:
            get_settings.cache_clear()

    def test_discover_source(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            r = client.post("/api/h2/sources", json={
                "path": SAMPLE_DATA,
                "source_type": "json",
                "name": "sample",
            })
            assert r.status_code == 201, r.text
            data = r.json()
            assert "snapshot_id" in data
            assert data["table_count"] > 0
        finally:
            get_settings.cache_clear()

    def test_get_source_after_discover(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            client.post("/api/h2/sources", json={"path": SAMPLE_DATA, "name": "sample"})
            r = client.get("/api/h2/sources/sample")
            assert r.status_code == 200
            data = r.json()
            assert data["name"] == "sample"
            assert len(data["tables"]) > 0
        finally:
            get_settings.cache_clear()

    def test_get_unknown_source_404(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            r = client.get("/api/h2/sources/ghost")
            assert r.status_code == 404
        finally:
            get_settings.cache_clear()

    def test_catalog_returns_semantic_types(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            client.post("/api/h2/sources", json={"path": SAMPLE_DATA, "name": "sample"})
            r = client.get("/api/h2/sources/sample/catalog")
            assert r.status_code == 200
            tables = r.json()
            assert tables, "Expected at least one table"
            for tbl in tables:
                for col in tbl["columns"]:
                    assert col["semantic_type"], f"Column {col['column_name']} has no semantic type"
        finally:
            get_settings.cache_clear()

    def test_catalog_update_column(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            client.post("/api/h2/sources", json={"path": SAMPLE_DATA, "name": "sample"})
            catalog = client.get("/api/h2/sources/sample/catalog").json()
            tbl = catalog[0]
            col = tbl["columns"][0]
            r = client.patch(
                f"/api/h2/sources/sample/catalog/{tbl['table_name']}/{col['column_name']}",
                json={"description": "Test description", "locked": True},
            )
            assert r.status_code == 200
        finally:
            get_settings.cache_clear()


class TestH2ProjectRoutes:
    def _setup(self, client, source_path=SAMPLE_DATA, source_name="sample"):
        client.post("/api/h2/sources", json={"path": source_path, "name": source_name})

    def test_list_projects_empty(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            r = client.get("/api/h2/projects")
            assert r.status_code == 200
            assert r.json() == []
        finally:
            get_settings.cache_clear()

    def test_frame_project(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            self._setup(client)
            r = client.post("/api/h2/projects", json={
                "project_id": "test_proj",
                "source_name": "sample",
                "display_name": "Test Project",
                "goal": "Analyse inspection scores over time",
            })
            assert r.status_code == 201, r.text
            data = r.json()
            assert data["project_id"] == "test_proj"
            assert "proposed_questions" in data
        finally:
            get_settings.cache_clear()

    def test_get_project_404(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            r = client.get("/api/h2/projects/ghost")
            assert r.status_code == 404
        finally:
            get_settings.cache_clear()

    def test_readiness_evaluates(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            self._setup(client)
            client.post("/api/h2/projects", json={
                "project_id": "rd_test",
                "source_name": "sample",
                "display_name": "Readiness Test",
                "goal": "Analyse scores over time",
            })
            r = client.post("/api/h2/projects/rd_test/readiness")
            assert r.status_code == 200, r.text
            data = r.json()
            assert "questions" in data
            assert "certified_count" in data
        finally:
            get_settings.cache_clear()

    def test_resolve_returns_cards(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            self._setup(client)
            client.post("/api/h2/projects", json={
                "project_id": "resolve_test",
                "source_name": "sample",
                "display_name": "Resolve Test",
                "goal": "Analyse categories and score distribution",
            })
            r = client.post("/api/h2/projects/resolve_test/resolve")
            assert r.status_code == 200, r.text
            cards = r.json()
            assert isinstance(cards, list)
        finally:
            get_settings.cache_clear()

    def test_eda_runs(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            self._setup(client)
            client.post("/api/h2/projects", json={
                "project_id": "eda_test",
                "source_name": "sample",
                "display_name": "EDA Test",
                "goal": "Analyse scores over time",
            })
            r = client.post("/api/h2/projects/eda_test/eda")
            assert r.status_code == 200, r.text
            data = r.json()
            assert "findings_count" in data
            assert "insight_confidence_score" in data
        finally:
            get_settings.cache_clear()

    def test_answer_drafts_sql(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            self._setup(client)
            client.post("/api/h2/projects", json={
                "project_id": "ans_test",
                "source_name": "sample",
                "display_name": "Answer Test",
                "goal": "Analyse scores over time",
            })
            client.post("/api/h2/projects/ans_test/readiness")
            r = client.post("/api/h2/projects/ans_test/answer")
            assert r.status_code == 200, r.text
            data = r.json()
            assert "answers" in data
        finally:
            get_settings.cache_clear()

    def test_report_returns_markdown(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            self._setup(client)
            client.post("/api/h2/projects", json={
                "project_id": "rpt_test",
                "source_name": "sample",
                "display_name": "Report Test",
                "goal": "Analyse scores over time",
            })
            r = client.get("/api/h2/projects/rpt_test/report")
            assert r.status_code == 200, r.text
            assert "# Headwater Audit Report" in r.text
        finally:
            get_settings.cache_clear()

    def test_certify_returns_stable_on_no_drift(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            self._setup(client)
            client.post("/api/h2/projects", json={
                "project_id": "cert_test",
                "source_name": "sample",
                "display_name": "Certify Test",
                "goal": "Analyse scores over time",
            })
            client.post("/api/h2/projects/cert_test/readiness")
            r = client.post("/api/h2/projects/cert_test/certify")
            assert r.status_code == 200, r.text
            data = r.json()
            assert "demotions" in data
            assert "newly_certified" in data
        finally:
            get_settings.cache_clear()

    def test_ui_cannot_force_certified_state(self, monkeypatch, tmp_path):
        """The API must not expose an endpoint that directly sets state=certified."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings
        get_settings.cache_clear()
        try:
            client = _h2_client(tmp_path)
            # There is no PATCH /projects/{id}/questions/{qid}/state endpoint
            # that could let the UI bypass contract evaluation
            r = client.patch("/api/h2/projects/any/questions/any/state",
                             json={"state": "certified"})
            assert r.status_code == 405 or r.status_code == 404, (
                "No direct state-mutation endpoint should exist"
            )
        finally:
            get_settings.cache_clear()
