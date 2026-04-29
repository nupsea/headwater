"""Tests for the FastAPI layer."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from headwater.api.app import create_app

SAMPLE_DATA = str(Path(__file__).resolve().parent.parent.parent / "data" / "sample")


@pytest.fixture
def client():
    app = create_app(in_memory=True)
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


class TestSourcesCatalog:
    def test_connector_catalog_exposes_support_status(self, client):
        resp = client.get("/api/connector-catalog")
        assert resp.status_code == 200
        connectors = {c["id"]: c for c in resp.json()["connectors"]}

        assert connectors["postgres"]["status"] == "supported"
        assert connectors["json"]["status"] == "supported"
        assert connectors["csv"]["status"] == "supported"
        assert connectors["duckdb"]["status"] == "supported"
        assert connectors["mysql"]["status"] == "planned"
        assert connectors["mysql"]["supported"] is False
        assert connectors["json"]["capabilities"]["list_tables"] is True
        assert connectors["duckdb"]["capabilities"]["load_to_duckdb"] is True
        assert connectors["postgres"]["capabilities"]["execute_readonly"] is True
        assert connectors["mysql"]["capabilities"]["test"] is False

    def test_create_source_rejects_planned_connector(self, client):
        resp = client.post(
            "/api/sources",
            json={
                "name": "future_mysql",
                "type": "mysql",
                "uri": "mysql://user:pass@localhost/db",
            },
        )

        assert resp.status_code == 400
        assert "planned" in resp.json()["detail"]

    def test_source_test_endpoint_verifies_supported_source(self, client):
        create = client.post(
            "/api/sources",
            json={"name": "sample_json", "type": "json", "path": SAMPLE_DATA},
        )
        assert create.status_code == 201

        resp = client.post("/api/sources/sample_json/test")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        detail = client.get("/api/sources/sample_json").json()
        assert any(e["event_type"] == "connection_tested" for e in detail["events"])

    def test_source_sync_runs_full_pipeline_for_json_source(self, client):
        create = client.post(
            "/api/sources",
            json={"name": "sample_json", "type": "json", "path": SAMPLE_DATA},
        )
        assert create.status_code == 201

        resp = client.post("/api/sources/sample_json/sync")
        assert resp.status_code == 200
        result = resp.json()
        expected_status = "warning" if result["quality_failed"] else "healthy"
        assert result["status"] == expected_status
        assert result["tables_discovered"] == 8
        assert result["profiles"] > 0
        assert result["quality_total"] > 0

        detail = client.get("/api/sources/sample_json").json()
        assert detail["status"] == expected_status
        assert detail["tables"] == 8
        assert detail["runs"][0]["status"] == "succeeded"
        assert detail["latest_run_status"] == "succeeded"
        assert detail["latest_run_duration_ms"] is not None
        assert detail["quality_failed"] == result["quality_failed"]
        assert detail["quality_score"] == result["quality_score"]
        assert any(e["event_type"] == "sync_completed" for e in detail["events"])

        latest_quality = client.app.state.metadata_store.get_latest_quality_report("sample_json")
        assert latest_quality is not None
        assert latest_quality["total_contracts"] == result["quality_total"]
        assert latest_quality["score"] == result["quality_score"]
        assert latest_quality["sync_run_id"] == result["run_id"]

    def test_duckdb_source_can_be_registered_and_synced(self, client, tmp_path):
        import duckdb

        db_path = tmp_path / "source.duckdb"
        con = duckdb.connect(str(db_path))
        try:
            con.execute("CREATE TABLE users (user_id INTEGER, email VARCHAR)")
            con.execute("INSERT INTO users VALUES (1, 'a@example.com'), (2, 'b@example.com')")
            con.execute("CREATE TABLE orders (order_id INTEGER, user_id INTEGER, amount DOUBLE)")
            con.execute("INSERT INTO orders VALUES (10, 1, 20.5), (11, 2, 31.0)")
        finally:
            con.close()

        create = client.post(
            "/api/sources",
            json={"name": "sample_duckdb", "type": "duckdb", "path": str(db_path)},
        )
        assert create.status_code == 201

        test = client.post("/api/sources/sample_duckdb/test")
        assert test.status_code == 200
        assert test.json()["tables"] == 2

        sync = client.post("/api/sources/sample_duckdb/sync")
        assert sync.status_code == 200
        result = sync.json()
        assert result["tables_discovered"] == 2

        detail = client.get("/api/sources/sample_duckdb").json()
        assert detail["tables"] == 2
        assert detail["latest_run_status"] == "succeeded"


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
            m for m in models_resp.json() if m["model_type"] == "mart" and m["status"] == "proposed"
        )
        resp = client.post(f"/api/models/{mart['name']}/approve")
        assert resp.status_code == 200
        assert resp.json()["status"] == "approved"

    def test_reject_model(self, client):
        self._setup(client)
        models_resp = client.get("/api/models")
        mart = next(
            m for m in models_resp.json() if m["model_type"] == "mart" and m["status"] == "proposed"
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
        assert data["quality_run_id"] > 0

        latest_quality = client.app.state.metadata_store.get_latest_quality_report("source")
        assert latest_quality is not None
        assert latest_quality["total_contracts"] == data["total"]

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


class TestProjectCreation:
    """POST /api/projects -- create a new project."""

    def test_create_project_returns_201(self, client):
        resp = client.post(
            "/api/projects",
            json={
                "display_name": "My Test Project",
                "description": "A test project",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["display_name"] == "My Test Project"
        assert data["slug"] == "my-test-project"
        assert data["description"] == "A test project"
        assert "id" in data

    def test_create_project_minimal(self, client):
        resp = client.post(
            "/api/projects",
            json={"display_name": "Minimal"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["display_name"] == "Minimal"
        assert data["slug"] == "minimal"

    def test_create_project_appears_in_list(self, client):
        client.post("/api/projects", json={"display_name": "Listed Project"})
        resp = client.get("/api/projects")
        names = [p["display_name"] for p in resp.json()["projects"]]
        assert "Listed Project" in names


class TestProjectRename:
    """PATCH /api/projects/{id}/rename -- update name or description."""

    def _create(self, client, name="Original Name"):
        resp = client.post("/api/projects", json={"display_name": name})
        return resp.json()

    def test_rename_project(self, client):
        project = self._create(client)
        resp = client.patch(
            f"/api/projects/{project['id']}/rename",
            json={"display_name": "New Name"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["display_name"] == "New Name"
        assert data["slug"] == "new-name"

    def test_rename_project_not_found(self, client):
        resp = client.patch(
            "/api/projects/nonexistent-id/rename",
            json={"display_name": "Whatever"},
        )
        assert resp.status_code == 404

    def test_rename_updates_description(self, client):
        project = self._create(client)
        resp = client.patch(
            f"/api/projects/{project['id']}/rename",
            json={"description": "Updated desc"},
        )
        assert resp.status_code == 200
        assert resp.json()["description"] == "Updated desc"


class TestActivityFeed:
    """GET /api/activity -- recent activity feed."""

    def test_activity_empty_initially(self, client):
        resp = client.get("/api/activity")
        assert resp.status_code == 200
        assert resp.json()["activities"] == []

    def test_activity_after_project_creation(self, client):
        client.post("/api/projects", json={"display_name": "Activity Test"})
        resp = client.get("/api/activity")
        assert resp.status_code == 200
        activities = resp.json()["activities"]
        assert len(activities) >= 1
        assert activities[0]["action"] == "project_created"

    def test_activity_limit_parameter(self, client):
        for i in range(5):
            client.post("/api/projects", json={"display_name": f"Project {i}"})
        resp = client.get("/api/activity", params={"limit": 3})
        assert resp.status_code == 200
        assert len(resp.json()["activities"]) == 3


class TestDecisionRecording:
    """US-301: verify approve/reject writes to decisions table."""

    def _setup(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        client.post("/api/generate")

    def _first_proposed_mart_name(self, client) -> str:
        models_resp = client.get("/api/models")
        mart = next(
            m for m in models_resp.json() if m["model_type"] == "mart" and m["status"] == "proposed"
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


class TestReEnrich:
    """POST /api/pipeline/re-enrich endpoint."""

    def test_re_enrich_no_discovery(self, client):
        """Returns 400 when no discovery has been run."""
        resp = client.post("/api/pipeline/re-enrich")
        assert resp.status_code == 400

    def test_re_enrich_with_discovery(self, client):
        """Re-enriches successfully when discovery exists."""
        from unittest.mock import patch

        # Run discovery first
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})

        with patch("headwater.api.routes.pipeline.analyze") as mock_analyze:
            # Make analyze return the discovery unchanged (it mutates in place)
            mock_analyze.side_effect = lambda d, p=None, **kw: d

            resp = client.post("/api/pipeline/re-enrich")
            assert resp.status_code == 200
            data = resp.json()
            assert "columns_enriched" in data
            assert "provider" in data
            assert data["columns_enriched"] >= 0
            mock_analyze.assert_called_once()


class TestModelAnswers:
    """POST/GET /api/models/{name}/answers endpoints."""

    def _setup(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        client.post("/api/generate")

    def test_save_answers_model_not_found(self, client):
        """Returns 404 for unknown model name."""
        self._setup(client)
        resp = client.post(
            "/api/models/nonexistent_model/answers",
            json={"answers": [{"question_index": 0, "answer": "yes"}]},
        )
        assert resp.status_code == 404

    def test_save_answers_success(self, client):
        """Saves answers when model exists."""
        self._setup(client)
        resp = client.post(
            "/api/models/stg_zones/answers",
            json={
                "answers": [
                    {"question_index": 0, "answer": "Geographic zone identifier"},
                    {"question_index": 1, "answer": "Daily granularity"},
                ]
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["model_name"] == "stg_zones"
        assert data["answers_saved"] == 2

    def test_get_answers(self, client):
        """Returns previously saved answers."""
        self._setup(client)
        # Save first
        client.post(
            "/api/models/stg_zones/answers",
            json={
                "answers": [
                    {"question_index": 0, "answer": "Geographic zone identifier"},
                ]
            },
        )
        # Retrieve
        resp = client.get("/api/models/stg_zones/answers")
        assert resp.status_code == 200
        data = resp.json()
        assert data["model_name"] == "stg_zones"
        assert len(data["answers"]) == 1
        assert data["answers"][0]["answer"] == "Geographic zone identifier"


class TestDictionaryAnswersE2E:
    """End-to-end: run real pipeline, then answer dictionary questions."""

    def _discover(self, client):
        resp = client.post("/api/pipeline/run", params={"source_path": SAMPLE_DATA})
        assert resp.status_code == 200, f"Pipeline failed: {resp.text}"
        return resp.json()

    def test_dict_answers_after_discovery(self, client):
        """After real discovery, POST dict answers returns 200."""
        self._discover(client)
        # Pick a table that exists
        dict_resp = client.get("/api/dictionary")
        assert dict_resp.status_code == 200
        tables = dict_resp.json()["tables"]
        assert len(tables) > 0
        table_name = tables[0]["name"]

        # Submit answers
        resp = client.post(
            f"/api/dictionary/{table_name}/answers",
            json={"answers": [{"question_index": 0, "answer": "test answer"}]},
        )
        assert resp.status_code == 200, f"Dict answers failed: {resp.text}"
        data = resp.json()
        assert data["table_name"] == table_name
        assert data["answers_saved"] == 1

        # Retrieve
        get_resp = client.get(f"/api/dictionary/{table_name}/answers")
        assert get_resp.status_code == 200
        assert len(get_resp.json()["answers"]) == 1

    def test_dict_answers_404_unknown_table(self, client):
        """Dict answers for unknown table returns 404 (not 500)."""
        self._discover(client)
        resp = client.post(
            "/api/dictionary/nonexistent_table/answers",
            json={"answers": [{"question_index": 0, "answer": "test"}]},
        )
        assert resp.status_code == 404

    def test_model_answers_after_discovery(self, client):
        """After real discovery, POST model answers for a real model returns 200."""
        self._discover(client)
        # Get actual model names
        models_resp = client.get("/api/models")
        assert models_resp.status_code == 200
        models = models_resp.json()
        assert len(models) > 0

        # Find a model with questions
        model_with_q = next(
            (m for m in models if m.get("questions") and len(m["questions"]) > 0),
            None,
        )
        model_name = models[0]["name"] if model_with_q is None else model_with_q["name"]

        resp = client.post(
            f"/api/models/{model_name}/answers",
            json={"answers": [{"question_index": 0, "answer": "monthly"}]},
        )
        assert resp.status_code == 200, f"Model answers failed: {resp.text}"
        assert resp.json()["answers_saved"] == 1


class TestExplorerE2E:
    """End-to-end: run real pipeline, verify explorer produces meaningful suggestions."""

    def _run_pipeline(self, client):
        resp = client.post("/api/pipeline/run", params={"source_path": SAMPLE_DATA})
        assert resp.status_code == 200, f"Pipeline failed: {resp.text}"
        return resp.json()

    def test_suggestions_include_multi_table_questions(self, client):
        """Explorer must generate cross-table or relationship suggestions."""
        self._run_pipeline(client)
        resp = client.get("/api/explore/suggestions")
        assert resp.status_code == 200
        data = resp.json()
        suggestions = data["suggestions"]
        assert len(suggestions) > 0, "Explorer generated 0 suggestions"

        # Must have questions involving multiple tables
        multi_table = [
            s for s in suggestions if len(s.get("relevant_tables", [])) >= 2
        ]
        assert len(multi_table) > 0, (
            f"No multi-table suggestions generated. "
            f"Sources: {[s['source'] for s in suggestions]}"
        )

    def test_suggestions_have_sql_hints(self, client):
        """All suggestions must have executable SQL hints."""
        self._run_pipeline(client)
        resp = client.get("/api/explore/suggestions")
        suggestions = resp.json()["suggestions"]
        for s in suggestions:
            assert s.get("sql_hint"), (
                f"Suggestion missing SQL hint: {s['question']}"
            )

    def test_suggestions_cover_multiple_sources(self, client):
        """Suggestions should come from multiple sources, not just one."""
        self._run_pipeline(client)
        resp = client.get("/api/explore/suggestions")
        suggestions = resp.json()["suggestions"]
        sources = {s["source"] for s in suggestions}
        assert len(sources) >= 2, (
            f"Suggestions only from {sources}. Expected at least 2 sources."
        )

    def test_ask_with_suggested_question(self, client):
        """Clicking a suggested question should produce actual data."""
        self._run_pipeline(client)
        resp = client.get("/api/explore/suggestions")
        suggestions = resp.json()["suggestions"]
        assert len(suggestions) > 0

        # Ask the first suggested question
        question = suggestions[0]["question"]
        ask_resp = client.post(
            "/api/explore/ask", json={"question": question}
        )
        assert ask_resp.status_code == 200
        result = ask_resp.json()
        # Should either have data or a non-empty SQL
        assert result.get("data") or result.get("sql"), (
            f"Asking '{question}' produced no data and no SQL"
        )

    def test_pk_fk_suggestions_after_discovery(self, client):
        """PK/FK detection should produce suggestions for tables with _id columns."""
        self._run_pipeline(client)
        # Get tables
        tables_resp = client.get("/api/tables")
        tables = tables_resp.json()
        assert len(tables) > 0

        # Try pk-fk-suggestions for a table that likely has FK columns
        found_suggestions = False
        for table in tables[:3]:
            resp = client.get(
                f"/api/tables/{table['name']}/pk-fk-suggestions"
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("pk_candidates") or data.get("fk_candidates"):
                    found_suggestions = True
                    break

        # At least some tables in the sample data should have PK/FK suggestions
        assert found_suggestions, (
            "No PK/FK suggestions generated for any table in sample data"
        )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
class TestHealth:
    def test_health_returns_200(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("healthy", "degraded")
        assert "components" in data
        assert "version" in data
        assert "uptime_seconds" in data

    def test_health_components_present(self, client):
        resp = client.get("/api/health")
        data = resp.json()
        components = data["components"]
        assert "metadata_store" in components
        assert "analytical_engine" in components
        assert "llm_provider" in components

    def test_health_in_memory_is_healthy(self, client):
        """In-memory test mode should report healthy status."""
        resp = client.get("/api/health")
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["components"]["metadata_store"] == "ok"
        assert data["components"]["analytical_engine"] == "ok"
