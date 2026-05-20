"""Tests for the FastAPI layer."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pyarrow as pa
import pytest
import yaml
from fastapi.testclient import TestClient

from headwater.api.app import create_app
from headwater.api.routes.explore import (
    _diversify_statistical_insights,
    _rank_statistical_insights,
)
from headwater.core.models import ExplorationResult, StatisticalInsight

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


class TestProjectContext:
    def test_discovery_bootstraps_project_context(self, client):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        resp = client.get("/api/projects/source/context")
        assert resp.status_code == 200
        body = resp.json()

        assert body["project_id"] == "source"
        assert body["summary"]["item_count"] > 0
        assert body["summary"]["item_types"]["dataset_summary"] == 1
        assert "column_semantics" in body["summary"]["item_types"]
        assert "row_grain" in body["summary"]["item_types"]
        assert "row_entity" in body["summary"]["item_types"]
        assert "pk_candidate" in body["summary"]["item_types"]

    def test_discovery_snapshots_project_context(self, client):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        resp = client.get("/api/projects/source/context/snapshots")
        assert resp.status_code == 200
        snapshots = resp.json()["snapshots"]
        assert len(snapshots) == 1
        latest = snapshots[0]
        assert latest["project_id"] == "source"
        assert latest["source_name"] == "source"
        assert latest["run_id"] > 0
        assert latest["snapshot"]["summary"]["item_count"] > 0
        assert "row_grain" in latest["snapshot"]["summary"]["item_types"]

        detail = client.get(f"/api/projects/source/context/snapshots/{latest['run_id']}")
        assert detail.status_code == 200
        snapshot = detail.json()["snapshot"]
        assert snapshot["run_id"] == latest["run_id"]
        assert any(item["item_type"] == "row_grain" for item in snapshot["items"])

    def test_user_can_add_context_resource(self, client):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        create = client.post(
            "/api/projects/source/context/resources",
            json={
                "resource_type": "url",
                "title": "Business glossary",
                "location": "https://example.com/glossary",
                "metadata": {"use_for": ["glossary"]},
            },
        )
        assert create.status_code == 200
        created = create.json()
        assert created["source"] == "user"
        assert created["metadata"]["classification"] == "unknown"
        assert created["metadata"]["external_llm_allowed"] is False

        resp = client.get("/api/projects/source/context")
        resources = resp.json()["resources"]
        titles = {resource["title"] for resource in resources}
        assert "Business glossary" in titles

    def test_public_context_resource_can_be_explicitly_allowed_for_external_llm(
        self,
        client,
    ):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        create = client.post(
            "/api/projects/source/context/resources",
            json={
                "resource_type": "url",
                "title": "Public glossary",
                "location": "https://example.com/glossary",
                "metadata": {
                    "classification": "public",
                    "allow_external_llm": True,
                },
            },
        )

        assert create.status_code == 200
        created = create.json()
        assert created["metadata"]["classification"] == "public"
        assert created["metadata"]["external_llm_allowed"] is True

    def test_local_resource_addition_enriches_project_context(self, client, tmp_path):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        glossary = tmp_path / "complaints_glossary.md"
        glossary.write_text(
            "\n".join(
                [
                    "assigned_to: Staff member assigned to investigate and resolve the complaint.",
                    "status: Workflow status of the complaint case.",
                    (
                        "Confirm whether date_acknowledged should be treated "
                        "as the first service-response timestamp?"
                    ),
                ]
            ),
            encoding="utf-8",
        )

        create = client.post(
            "/api/projects/source/context/resources",
            json={
                "resource_type": "markdown",
                "title": "Complaints glossary",
                "location": str(glossary),
                "metadata": {"use_for": ["glossary", "semantic_roles"]},
            },
        )
        assert create.status_code == 200
        created = create.json()
        assert created["metadata"]["enrichment"]["items_created"] >= 2
        assert created["metadata"]["enrichment"]["evidence_id"]
        extraction_evidence = created["metadata"]["extraction_evidence"][0]
        assert extraction_evidence["resource_id"] == created["id"]
        assert extraction_evidence["created_item_ids"]
        assert extraction_evidence["classification"] == "unknown"
        assert "complaints" in created["metadata"]["matched_tables"]

        context = client.get("/api/projects/source/context").json()
        assigned_to = next(
            item
            for item in context["items"]
            if item["item_type"] == "column_semantics"
            and item["table_name"] == "complaints"
            and item["column_name"] == "assigned_to"
        )
        assert assigned_to["value"]["description"].startswith("Staff member assigned")

        extracted_question = next(
            item
            for item in context["items"]
            if item["item_type"] == "open_question"
            and "date_acknowledged" in str(item["value"].get("question") or "")
        )
        assert extracted_question["status"] == "needs_review"

    def test_inline_resource_content_enriches_project_context(self, client):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        create = client.post(
            "/api/projects/source/context/resources",
            json={
                "resource_type": "markdown",
                "title": "Inline glossary",
                "content": "\n".join(
                    [
                        "# complaints",
                        "assigned_to: Staff member assigned to investigate the complaint.",
                    ]
                ),
                "metadata": {"use_for": ["glossary"]},
            },
        )
        assert create.status_code == 200
        created = create.json()
        assert created["metadata"]["enrichment"]["items_created"] >= 1

        context = client.get("/api/projects/source/context").json()
        assigned_to = next(
            item
            for item in context["items"]
            if item["item_type"] == "column_semantics"
            and item["table_name"] == "complaints"
            and item["column_name"] == "assigned_to"
        )
        assert assigned_to["value"]["description"].startswith("Staff member assigned")

    def test_reingest_marks_missing_reviewed_context_as_needs_review(self, client, tmp_path):
        source_dir = tmp_path / "orders_source"
        source_dir.mkdir()
        orders = source_dir / "orders.json"
        orders.write_text(
            "\n".join(
                [
                    json.dumps({"order_id": 1, "status_code": "open", "amount": 12.5}),
                    json.dumps({"order_id": 2, "status_code": "closed", "amount": 7.0}),
                ]
            ),
            encoding="utf-8",
        )

        discover = client.post("/api/discover", params={"source_path": str(source_dir)})
        assert discover.status_code == 200

        context = client.get("/api/projects/source/context").json()
        target = next(
            item
            for item in context["items"]
            if item["item_type"] == "column_semantics"
            and item["table_name"] == "orders"
            and item["column_name"] == "status_code"
        )
        review = client.post(
            f"/api/projects/source/context/items/{target['id']}/approve",
            json={
                "reason": "Confirmed with source owner",
                "confidence": 0.99,
                "value": {
                    **target["value"],
                    "description": "Reviewed workflow status for the order.",
                    "role": "dimension",
                },
            },
        )
        assert review.status_code == 200

        orders.write_text(
            "\n".join(
                [
                    json.dumps({"order_id": 1, "amount": 12.5}),
                    json.dumps({"order_id": 2, "amount": 7.0}),
                ]
            ),
            encoding="utf-8",
        )

        rediscover = client.post("/api/discover", params={"source_path": str(source_dir)})
        assert rediscover.status_code == 200
        drift_summary = rediscover.json()["context_drift"]
        assert drift_summary["items_flagged"] >= 1
        assert drift_summary["drift_type_counts"]["schema"] >= 1
        assert drift_summary["severity_counts"]["critical"] >= 1

        refreshed = client.get("/api/projects/source/context").json()
        updated = next(item for item in refreshed["items"] if item["id"] == target["id"])
        assert updated["status"] == "needs_review"
        assert updated["source"] == "context_drift"
        assert updated["confidence"] == 0.25
        assert "no longer present" in updated["value"]["drift_reason"]
        assert updated["value"]["drift_type"] == "schema"
        assert updated["value"]["drift_severity"] == "critical"
        assert updated["value"]["drift_detector"] == "schema.column_presence"
        assert updated["value"]["drift_review_action"] == "needs_review"
        assert updated["evidence"][-1]["evidence_type"] == "schema_drift"
        assert updated["evidence"][-1]["payload"]["code"] == "column_missing"
        assert updated["evidence"][-1]["payload"]["severity"] == "critical"
        assert updated["evidence"][-1]["payload"]["review_action"] == "needs_review"

        history = client.get("/api/projects/source/context/history").json()
        assert history["project_id"] == "source"
        assert len(history["drift_reports"]) >= 1
        assert any(
            entry["artifact_type"] == "project_context_item"
            for entry in history["decisions"]
        )
        decision = next(
            entry
            for entry in history["decisions"]
            if entry["artifact_type"] == "project_context_item"
            and entry["artifact_id"] == target["id"]
            and entry["action"] == "approved"
        )
        payload = json.loads(decision["payload_json"])
        assert payload["item_id"] == target["id"]
        assert payload["item_type"] == "column_semantics"
        assert payload["producer"] == "user"
        assert payload["prior_status"] == target["status"]
        assert payload["new_status"] == "approved"
        assert payload["prior_confidence"] == target["confidence"]
        assert payload["new_confidence"] == 0.99
        assert payload["new_value"]["description"] == "Reviewed workflow status for the order."
        assert payload["source_snapshot"]["column_name"] == "status_code"
        assert isinstance(payload["time_to_decision_seconds"], int)

    def test_markdown_table_heading_scopes_resource_enrichment(self, client, tmp_path):
        source_dir = tmp_path / "support_source"
        source_dir.mkdir()
        (source_dir / "orders.json").write_text(
            "\n".join(
                [
                    json.dumps({"order_id": 1, "status_code": "open"}),
                    json.dumps({"order_id": 2, "status_code": "closed"}),
                ]
            ),
            encoding="utf-8",
        )
        (source_dir / "returns.json").write_text(
            "\n".join(
                [
                    json.dumps({"return_id": 1, "status_code": "requested"}),
                    json.dumps({"return_id": 2, "status_code": "approved"}),
                ]
            ),
            encoding="utf-8",
        )

        discover = client.post("/api/discover", params={"source_path": str(source_dir)})
        assert discover.status_code == 200

        glossary = tmp_path / "returns_glossary.md"
        glossary.write_text(
            "\n".join(
                [
                    "# returns",
                    "status_code: Workflow status for the return request lifecycle.",
                ]
            ),
            encoding="utf-8",
        )

        create = client.post(
            "/api/projects/source/context/resources",
            json={
                "resource_type": "markdown",
                "title": "Returns glossary",
                "location": str(glossary),
            },
        )
        assert create.status_code == 200

        context = client.get("/api/projects/source/context").json()
        returns_status = next(
            item
            for item in context["items"]
            if item["item_type"] == "column_semantics"
            and item["table_name"] == "returns"
            and item["column_name"] == "status_code"
        )
        orders_status = next(
            item
            for item in context["items"]
            if item["item_type"] == "column_semantics"
            and item["table_name"] == "orders"
            and item["column_name"] == "status_code"
        )
        assert (
            returns_status["value"]["description"]
            == "Workflow status for the return request lifecycle."
        )
        assert orders_status["value"].get("description") != returns_status["value"]["description"]

    def test_user_can_review_context_item(self, client):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        context = client.get("/api/projects/source/context").json()
        item = next(
            entry for entry in context["items"] if entry["item_type"] == "column_semantics"
        )

        review = client.patch(
            f"/api/projects/source/context/items/{item['id']}",
            json={
                "status": "approved",
                "confidence": 0.98,
                "reason": "Validated during context review",
                "value": {
                    **item["value"],
                    "description": "Reviewed business meaning.",
                    "role": "dimension",
                },
            },
        )
        assert review.status_code == 200
        reviewed = review.json()
        assert reviewed["status"] == "approved"
        assert reviewed["source"] == "user"
        assert reviewed["value"]["description"] == "Reviewed business meaning."

        refreshed = client.get("/api/projects/source/context").json()
        updated = next(entry for entry in refreshed["items"] if entry["id"] == item["id"])
        assert updated["status"] == "approved"
        assert updated["value"]["description"] == "Reviewed business meaning."

        feedback = client.get("/api/projects/source/context/feedback").json()["feedback"]
        event = next(entry for entry in feedback if entry["item_id"] == item["id"])
        assert event["action"] == "approved"
        assert event["item_type"] == item["item_type"]
        assert event["prior_confidence"] == item["confidence"]
        assert event["new_confidence"] == 0.98
        assert isinstance(event["time_to_decision_seconds"], int)
        assert event["payload"]["prior_status"] == item["status"]
        assert event["payload"]["new_status"] == "approved"

    def test_user_can_revert_context_item_decision(self, client):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        context = client.get("/api/projects/source/context").json()
        item = next(
            entry for entry in context["items"] if entry["item_type"] == "column_semantics"
        )
        approve = client.post(
            f"/api/projects/source/context/items/{item['id']}/approve",
            json={
                "reason": "Confirmed during review",
                "confidence": 0.97,
                "value": {
                    **item["value"],
                    "description": "Reviewed meaning to roll back.",
                    "role": "dimension",
                },
            },
        )
        assert approve.status_code == 200

        history = client.get("/api/projects/source/context/history").json()
        decision = next(
            entry
            for entry in history["decisions"]
            if entry["artifact_type"] == "project_context_item"
            and entry["artifact_id"] == item["id"]
            and entry["action"] == "approved"
        )

        revert = client.post(
            f"/api/projects/source/context/decisions/{decision['id']}/revert",
            json={"reason": "Undo accidental approval"},
        )

        assert revert.status_code == 200
        reverted = revert.json()
        assert reverted["reverted_decision_id"] == decision["id"]
        assert reverted["item"]["id"] == item["id"]
        assert reverted["item"]["status"] == item["status"]
        assert reverted["item"]["confidence"] == item["confidence"]
        assert reverted["item"]["source"] == item["source"]
        assert reverted["item"]["value"] == item["value"]

        refreshed = client.get("/api/projects/source/context").json()
        current = next(entry for entry in refreshed["items"] if entry["id"] == item["id"])
        assert current["value"] == item["value"]

        revert_history = client.get("/api/projects/source/context/history").json()
        revert_decision = next(
            entry
            for entry in revert_history["decisions"]
            if entry["artifact_type"] == "project_context_item"
            and entry["artifact_id"] == item["id"]
            and entry["action"] == "reverted"
        )
        payload = json.loads(revert_decision["payload_json"])
        assert payload["reverted_decision_id"] == decision["id"]
        assert payload["prior_status"] == "approved"
        assert payload["new_status"] == item["status"]
        assert payload["new_value"] == item["value"]

    def test_project_context_export_renders_yaml_and_review_markdown(self, client):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        context = client.get("/api/projects/source/context").json()
        item = next(
            entry for entry in context["items"] if entry["item_type"] == "column_semantics"
        )
        client.post(
            f"/api/projects/source/context/items/{item['id']}/lock",
            json={
                "reason": "Keep reviewed meaning stable",
                "confidence": 0.99,
                "value": {
                    **item["value"],
                    "description": "Locked business definition.",
                    "role": "identifier",
                    "semantic_type": "id",
                },
            },
        )

        exported = client.get("/api/projects/source/context/export")
        assert exported.status_code == 200
        files = exported.json()["files"]

        assert "context.yaml" in files
        assert "semantic_types.yaml" in files
        assert "semantic_schema.yaml" in files
        assert "derived_fields.yaml" in files
        assert "insight_families.yaml" in files
        assert "business_lenses.yaml" in files
        assert "presentation.yaml" in files
        assert "question_templates.yaml" in files
        assert "column_policies.yaml" in files
        assert "relationship_hints.yaml" in files
        assert "advisor_packs.yaml" in files
        assert "REVIEW.md" in files
        assert "version: 1" in files["context.yaml"]
        assert "cold_start_summary:" in files["context.yaml"]
        assert "row_grains:" in files["context.yaml"]
        assert "pk_candidates:" in files["context.yaml"]
        assert "Locked business definition." in files["semantic_types.yaml"]
        assert "role: identifier" in files["semantic_schema.yaml"]
        assert "# Project Context Review: source" in files["REVIEW.md"]
        assert "## Cold Start Summary" in files["REVIEW.md"]

    def test_project_context_import_merges_exported_files(self, client):
        discover = client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        assert discover.status_code == 200

        exported = client.get("/api/projects/source/context/export")
        assert exported.status_code == 200
        files = exported.json()["files"]
        context_yaml = yaml.safe_load(files["context.yaml"])
        context_yaml["cold_start_summary"]["fallback_questions"] = [
            "Imported cold-start question?"
        ]
        files["context.yaml"] = yaml.safe_dump(
            context_yaml,
            sort_keys=False,
            allow_unicode=False,
        )
        semantic_types = yaml.safe_load(files["semantic_types.yaml"])
        target = next(
            column
            for column in semantic_types["columns"]
            if column.get("table") and column.get("column")
        )
        target["description"] = "Imported canonical identifier"
        files["semantic_types.yaml"] = yaml.safe_dump(
            semantic_types,
            sort_keys=False,
            allow_unicode=False,
        )
        files["column_policies.yaml"] = yaml.safe_dump(
            {
                "version": 1,
                "project_id": "source",
                "column_policies": [
                    {
                        "id": "column_policy:complaints.assigned_to",
                        "name": "assigned_to_low_signal",
                        "title": "Low signal reviewer",
                        "scope": "column",
                        "table": "complaints",
                        "column": "assigned_to",
                        "status": "approved",
                        "confidence": 0.91,
                        "value": {"low_signal": True},
                    }
                ],
            },
            sort_keys=False,
            allow_unicode=False,
        )

        imported = client.post(
            "/api/projects/source/context/import",
            json={"files": files},
        )
        assert imported.status_code == 200
        body = imported.json()
        assert body["items_upserted"] > 0
        assert "semantic_types.yaml" in body["files_processed"]
        assert "column_policies.yaml" in body["files_processed"]

        context = client.get("/api/projects/source/context").json()
        updated = next(
            item
            for item in context["items"]
            if item["item_type"] == "column_semantics"
            and item["table_name"] == target["table"]
            and item["column_name"] == target["column"]
        )
        assert updated["value"].get("description") == "Imported canonical identifier"
        assert updated["status"] == target.get("status", "proposed")
        assert updated["source"] == "import"
        cold_start = next(
            item
            for item in context["items"]
            if item["item_type"] == "cold_start_summary"
        )
        assert cold_start["value"]["fallback_questions"] == [
            "Imported cold-start question?"
        ]
        policy = next(
            item
            for item in context["items"]
            if item["id"] == "column_policy:complaints.assigned_to"
        )
        assert policy["item_type"] == "column_policy"
        assert policy["value"]["low_signal"] is True


class TestInsightRanking:
    def test_semantic_insights_rank_before_generic_anomaly(self):
        anomaly = StatisticalInsight(
            metric="row_count",
            table_name="mart_events_by_period",
            insight_type="temporal_anomaly",
            description="Generic row-count anomaly",
            magnitude=5000,
            p_value=0.0,
            severity="critical",
            support_count=1000,
        )
        semantic = StatisticalInsight(
            metric="event_date",
            table_name="events",
            insight_type="coverage_period",
            description="Events cover a complete reporting window",
            magnitude=2.3,
            severity="info",
            support_count=50_000,
        )

        ranked = _rank_statistical_insights([anomaly, semantic])

        assert ranked[0] is semantic

    def test_statistical_ranking_uses_configured_type_priorities(self, monkeypatch):
        coverage = StatisticalInsight(
            metric="event_date",
            table_name="events",
            insight_type="coverage_period",
            description="Coverage window",
            magnitude=30,
            severity="info",
            support_count=1_000,
        )
        quality = StatisticalInsight(
            metric="pickup_location_id",
            table_name="events",
            insight_type="data_quality",
            description="Location IDs are missing",
            magnitude=30,
            severity="info",
            support_count=1_000,
        )
        monkeypatch.setattr(
            "headwater.api.routes.explore.insight_type_priority_weights",
            lambda: {
                "coverage_period": 10,
                "data_quality": 1,
            },
        )

        ranked = _rank_statistical_insights([quality, coverage])

        assert ranked[0] is coverage

    def test_insight_surfacing_limits_repetitive_types_and_tables(self):
        insights = [
            StatisticalInsight(
                metric=f"row_count_{idx}",
                table_name="mart_events_by_period",
                insight_type="temporal_anomaly",
                description=f"Anomaly {idx}",
                magnitude=5000 - idx,
                p_value=0.0,
                severity="critical",
                support_count=1000,
            )
            for idx in range(6)
        ]
        insights.extend(
            [
                StatisticalInsight(
                    metric="record_count",
                    table_name="events",
                    insight_type="volume_distribution",
                    description="Busiest hour",
                    magnitude=1200,
                    severity="info",
                    support_count=1200,
                ),
                StatisticalInsight(
                    metric="duration_min",
                    table_name="events",
                    insight_type="peak_period",
                    description="Weekday vs weekend",
                    magnitude=2.3,
                    severity="info",
                    support_count=50_000,
                ),
            ]
        )

        surfaced = _diversify_statistical_insights(insights, 10)

        assert sum(i.insight_type == "temporal_anomaly" for i in surfaced) <= 2
        assert len({(i.table_name, i.insight_type) for i in surfaced}) == len(surfaced)
        assert any(i.insight_type == "volume_distribution" for i in surfaced)
        assert any(i.insight_type == "peak_period" for i in surfaced)


class TestSourcesCatalog:
    def test_connector_catalog_exposes_support_status(self, client):
        resp = client.get("/api/connector-catalog")
        assert resp.status_code == 200
        connectors = {c["id"]: c for c in resp.json()["connectors"]}

        assert connectors["postgres"]["status"] == "supported"
        assert connectors["json"]["status"] == "supported"
        assert connectors["csv"]["status"] == "supported"
        assert connectors["duckdb"]["status"] == "supported"
        assert connectors["sqlite"]["status"] == "supported"
        assert connectors["mysql"]["status"] == "preview"
        assert connectors["snowflake"]["status"] == "preview"
        assert connectors["mysql"]["supported"] is False
        assert connectors["snowflake"]["supported"] is True
        assert connectors["json"]["capabilities"]["list_tables"] is True
        assert connectors["duckdb"]["capabilities"]["load_to_duckdb"] is True
        assert connectors["sqlite"]["capabilities"]["load_to_duckdb"] is True
        assert connectors["postgres"]["capabilities"]["execute_readonly"] is True
        assert connectors["mysql"]["capabilities"]["test"] is True
        assert connectors["mysql"]["capabilities"]["load_to_duckdb"] is False
        assert connectors["snowflake"]["capabilities"]["execute_readonly"] is True
        assert connectors["snowflake"]["capabilities"]["estimate_row_count"] is True

    def test_source_evaluations_expose_oltp_and_olap_templates(self, client):
        resp = client.get("/api/source-evaluations")

        assert resp.status_code == 200
        evaluations = {e["source_type"]: e for e in resp.json()["evaluations"]}
        assert evaluations["postgres"]["workload"] == "oltp"
        assert evaluations["postgres"]["maturity_mode"] == "oltp_heuristic"
        assert evaluations["duckdb"]["workload"] == "olap"
        assert evaluations["duckdb"]["profiling_policy"]["mode"] == "observe"
        assert evaluations["snowflake"]["workload"] == "olap"
        assert evaluations["snowflake"]["readiness"] == "preview"
        assert evaluations["redshift"]["readiness"] == "preview"

    def test_create_source_rejects_preview_connector(self, client):
        resp = client.post(
            "/api/sources",
            json={
                "name": "future_mysql",
                "type": "mysql",
                "uri": "mysql://user:pass@localhost/db",
            },
        )

        assert resp.status_code == 400
        assert "preview" in resp.json()["detail"]

    def test_create_source_allows_preview_supported_snowflake_connector(self, client):
        resp = client.post(
            "/api/sources",
            json={
                "name": "future_snowflake",
                "type": "snowflake",
                "uri": "snowflake://account/db/schema",
            },
        )

        assert resp.status_code == 201
        body = resp.json()
        assert body["type"] == "snowflake"
        assert body["evaluation"]["workload"] == "olap"
        assert body["evaluation"]["readiness"] == "preview"

    def test_snowflake_connection_test_reports_missing_optional_driver(self, client, monkeypatch):
        import headwater.connectors.snowflake_loader as snowflake_loader

        def missing_driver(name: str):
            if name == "snowflake.connector":
                raise ImportError("missing")
            return None

        monkeypatch.setattr(snowflake_loader.importlib, "import_module", missing_driver)

        resp = client.post(
            "/api/pipeline/test-connection",
            params={"source_path": "snowflake://account/db/schema"},
        )

        assert resp.status_code == 200
        assert resp.json()["status"] == "error"
        assert resp.json()["source_type"] == "snowflake"
        assert "snowflake-connector-python" in resp.json()["detail"]

    def test_connection_test_accepts_json_body(self, client):
        resp = client.post(
            "/api/pipeline/test-connection",
            json={"source_path": "/definitely/missing/path", "source_type": "json"},
        )

        assert resp.status_code == 200
        assert resp.json()["status"] == "error"
        assert resp.json()["source_type"] == "file"

    def test_create_project_secret_roundtrip(self, client, tmp_path, monkeypatch):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        from headwater.core.config import get_settings

        get_settings.cache_clear()
        try:
            save = client.put(
                "/api/settings/setup-drafts/create-project-secret",
                json={"password": "top-secret"},
            )
            assert save.status_code == 200
            assert save.json()["saved"] is True

            load = client.get("/api/settings/setup-drafts/create-project-secret")
            assert load.status_code == 200
            assert load.json() == {"saved": True}

            delete = client.delete("/api/settings/setup-drafts/create-project-secret")
            assert delete.status_code == 200
            assert delete.json()["deleted"] is True

            load_after = client.get("/api/settings/setup-drafts/create-project-secret")
            assert load_after.status_code == 200
            assert load_after.json() == {"saved": False}
        finally:
            get_settings.cache_clear()

    def test_source_detail_redacts_credentials(self, client):
        create = client.post(
            "/api/sources",
            json={
                "name": "snow",
                "type": "snowflake",
                "uri": "snowflake://analyst:top-secret@acme-xy123/analytics/public?warehouse=WH",
                "config": {
                    "connection": {"host": "acme-xy123", "password": "top-secret"},
                    "secret_access_key": "very-secret",
                },
            },
        )
        assert create.status_code == 201

        detail = client.get("/api/sources/snow")
        assert detail.status_code == 200
        body = detail.json()
        assert body["uri"] == "snowflake://analyst:***@acme-xy123/analytics/public?warehouse=WH"
        assert body["config"]["connection"]["password"] == "***"
        assert body["config"]["secret_access_key"] == "***"

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
        assert any("invalidates" in e for e in detail["events"])

        events = client.get("/api/events").json()["events"]
        assert any(e["event_type"] == "connection_tested" for e in events)

    def test_source_summary_includes_data_source_evaluation(self, client):
        create = client.post(
            "/api/sources",
            json={"name": "sample_json", "type": "json", "path": SAMPLE_DATA},
        )
        assert create.status_code == 201
        assert create.json()["evaluation"]["workload"] == "files"
        assert create.json()["evaluation"]["readiness"] == "needs_sync"

        evaluation = client.get("/api/sources/sample_json/evaluation")
        assert evaluation.status_code == 200
        assert evaluation.json()["source_name"] == "sample_json"
        actions = evaluation.json()["recommended_actions"]
        assert any(a["title"] == "Run source sync" for a in actions)

    def test_snowflake_insight_plan_dry_run_is_cost_gated(self, client):
        create = client.post(
            "/api/sources",
            json={
                "name": "snow",
                "type": "snowflake",
                "uri": "snowflake://account/db/schema",
            },
        )
        assert create.status_code == 201
        store = client.app.state.metadata_store
        store.upsert_table("small_orders", "snow", schema_name="PUBLIC", row_count=100_000)
        store.upsert_table("large_events", "snow", schema_name="PUBLIC", row_count=100_000_000)

        resp = client.post(
            "/api/sources/snow/insight-plan/dry-run",
            json={
                "max_queries": 5,
                "max_tables": 5,
                "require_time_filter_above_rows": 1_000_000,
                "allow_full_scan": False,
            },
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["mode"] == "dry_run"
        assert body["policy"]["execute_queries"] is False
        assert body["planned_queries"] == 1
        assert body["skipped_queries"] == 1
        assert body["plan_id"]
        planned = [c for c in body["candidates"] if c["status"] == "planned" and c.get("sql")]
        skipped = [c for c in body["candidates"] if c["status"] == "skipped"]
        assert planned[0]["table_name"] == "small_orders"
        assert 'FROM "PUBLIC"."small_orders"' in planned[0]["sql"]
        assert skipped[0]["table_name"] == "large_events"
        assert "exceeds cost gate" in skipped[0]["skipped_reason"]

        evidence = client.get("/api/evidence", params={"source": "snow"}).json()["records"]
        assert any(r["plan_id"] == body["plan_id"] for r in evidence)
        plans = client.get("/api/warehouse-insight-plans", params={"source": "snow"}).json()[
            "plans"
        ]
        assert plans[0]["id"] == body["plan_id"]

    def test_approved_insight_plan_executes_readonly_queries(self, client, monkeypatch):
        import headwater.services.warehouse_insights as warehouse_insights

        class FakeWarehouseConnector:
            def __init__(self):
                self.connected = False
                self.query_tag = None
                self.statement_timeout = None
                self.closed = False
                self.sql = []
                self._query_id = "fake-query-id-123"

            def connect(self, config):
                self.connected = True
                self.config = config

            def set_query_tag(self, query_tag):
                self.query_tag = query_tag

            def set_statement_timeout(self, seconds):
                self.statement_timeout = seconds

            def execute_readonly(self, sql):
                self.sql.append(sql)
                return pa.table({"row_count": [100_000]})

            def last_query_id(self):
                return self._query_id

            def close(self):
                self.closed = True

        connector = FakeWarehouseConnector()
        monkeypatch.setattr(warehouse_insights, "get_connector", lambda _source_type: connector)
        create = client.post(
            "/api/sources",
            json={
                "name": "snow",
                "type": "snowflake",
                "uri": "snowflake://account/db/schema",
            },
        )
        assert create.status_code == 201
        store = client.app.state.metadata_store
        store.upsert_table("small_orders", "snow", schema_name="PUBLIC", row_count=100_000)
        plan = client.post("/api/sources/snow/insight-plan/dry-run", json={"max_queries": 5})
        plan_id = plan.json()["plan_id"]

        rejected = client.post(
            f"/api/warehouse-insight-plans/{plan_id}/execute",
            json={"approved": False},
        )
        assert rejected.status_code == 400

        resp = client.post(
            f"/api/warehouse-insight-plans/{plan_id}/execute",
            json={
                "approved": True,
                "query_tag": "headwater-test",
                "statement_timeout_seconds": 42,
            },
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "succeeded"
        assert body["executed_queries"] == 1
        assert body["query_tag"] == "headwater-test"
        assert body["statement_timeout_seconds"] == 42
        assert body["results"][0]["rows"] == [{"row_count": 100_000}]
        assert connector.connected is True
        assert connector.query_tag == "headwater-test"
        assert connector.statement_timeout == 42
        assert connector.closed is True
        assert connector.sql == ['SELECT COUNT(*) AS row_count FROM "PUBLIC"."small_orders"']

        evidence = client.get(
            "/api/evidence",
            params={"source": "snow", "plan_id": plan_id},
        ).json()["records"]
        assert any(
            r["status"] == "succeeded" and r["payload"]["dry_run"] is False
            for r in evidence
        )
        assert all(
            "rows" not in r["payload"]
            for r in evidence
            if isinstance(r.get("payload"), dict)
        )
        assert any(r["query_id"] == "fake-query-id-123" for r in evidence)
        saved_plan = client.get("/api/warehouse-insight-plans", params={"source": "snow"}).json()[
            "plans"
        ][0]
        assert saved_plan["status"] == "succeeded"
        assert saved_plan["plan"]["last_execution"]["executed_queries"] == 1
        assert saved_plan["plan"]["last_execution"]["statement_timeout_seconds"] == 42

    def test_insight_plan_requires_existing_source(self, client):
        resp = client.post("/api/sources/missing/insight-plan/dry-run", json={})

        assert resp.status_code == 404

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

    def test_source_preview_returns_discovery_summary(self, client):
        create = client.post(
            "/api/sources",
            json={
                "name": "preview_json",
                "type": "json",
                "path": SAMPLE_DATA,
                "config": {"max_tables": 3, "sample_rows": 500},
            },
        )
        assert create.status_code == 201

        resp = client.post("/api/sources/preview_json/preview")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source_name"] == "preview_json"
        assert body["source_type"] == "json"
        assert body["tables_found"] == 8
        assert body["tables_considered"] == 3
        assert body["tables_skipped"] == 5
        assert body["config"]["max_tables"] == 3
        assert body["config"]["sample_rows"] == 500
        assert body["sample_rows_per_table"] == 500
        assert len(body["tables"]) == 3
        for t in body["tables"]:
            assert "name" in t
            assert "estimated_rows" in t

    def test_source_preview_not_found(self, client):
        resp = client.post("/api/sources/nonexistent/preview")
        assert resp.status_code == 404

    def test_synced_source_can_be_browsed_in_data_viewer(self, client):
        create = client.post(
            "/api/sources",
            json={"name": "sample_json", "type": "json", "path": SAMPLE_DATA},
        )
        assert create.status_code == 201
        sync = client.post("/api/sources/sample_json/sync")
        assert sync.status_code == 200

        catalog = client.get("/api/data/catalog")
        assert catalog.status_code == 200
        tables = {row["qualified_name"] for row in catalog.json()["tables"]}
        assert "src_sample_json.zones" in tables

        preview = client.get("/api/data/src_sample_json.zones/preview")
        assert preview.status_code == 200
        body = preview.json()
        assert body["row_count"] > 0
        assert "zone_id" in body["columns"]

    def test_delete_source_resets_active_source_state(self, client):
        create = client.post(
            "/api/sources",
            json={"name": "sample_json", "type": "json", "path": SAMPLE_DATA},
        )
        assert create.status_code == 201
        sync = client.post("/api/sources/sample_json/sync")
        assert sync.status_code == 200
        assert client.app.state.pipeline["discovery"] is not None

        delete = client.delete("/api/sources/sample_json")
        assert delete.status_code == 200

        assert client.app.state.pipeline["discovery"] is None
        assert client.app.state.pipeline["staging_models"] == []
        assert client.app.state.metadata_store.get_source("sample_json") is None

    def test_delete_source_backed_project_removes_underlying_source(self, client):
        create = client.post(
            "/api/sources",
            json={"name": "sample", "type": "json", "path": SAMPLE_DATA},
        )
        assert create.status_code == 201

        delete = client.delete("/api/projects/sample")

        assert delete.status_code == 200
        assert delete.json() == {"deleted": "sample"}
        assert client.app.state.metadata_store.get_source("sample") is None
        assert client.app.state.metadata_store.get_tables("sample_json") == []

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

    def test_sqlite_source_can_be_registered_and_synced(self, client, tmp_path):
        db_path = tmp_path / "source.sqlite"
        con = sqlite3.connect(db_path)
        try:
            con.execute("CREATE TABLE users (user_id INTEGER, email TEXT)")
            con.execute("INSERT INTO users VALUES (1, 'a@example.com'), (2, 'b@example.com')")
            con.execute("CREATE TABLE orders (order_id INTEGER, user_id INTEGER, amount REAL)")
            con.execute("INSERT INTO orders VALUES (10, 1, 20.5), (11, 2, 31.0)")
            con.commit()
        finally:
            con.close()

        create = client.post(
            "/api/sources",
            json={"name": "sample_sqlite", "type": "sqlite", "path": str(db_path)},
        )
        assert create.status_code == 201

        test = client.post("/api/sources/sample_sqlite/test")
        assert test.status_code == 200
        assert test.json()["tables"] == 2

        sync = client.post("/api/sources/sample_sqlite/sync")
        assert sync.status_code == 200
        result = sync.json()
        assert result["tables_discovered"] == 2

        detail = client.get("/api/sources/sample_sqlite").json()
        assert detail["tables"] == 2
        assert detail["latest_run_status"] == "succeeded"

    def test_database_pipeline_respects_large_source_sampling_policy(self, client, monkeypatch):
        import headwater.api.routes.pipeline as pipeline_route

        class FakeWarehouseConnector:
            def __init__(self):
                self.sample_limits = []

            def connect(self, _config):
                return None

            def list_tables(self):
                return ["public.users", "public.orders", "public.events"]

            def sample(self, table_name, n=10_000):
                self.sample_limits.append((table_name, n))
                return pa.table(
                    {
                        "id": [1],
                        "value": [f"{table_name}-sample"],
                    }
                )

            def close(self):
                return None

        connector = FakeWarehouseConnector()
        monkeypatch.setattr(pipeline_route, "get_connector", lambda _source_type: connector)

        resp = client.post(
            "/api/pipeline/run",
            params={
                "source_path": "postgresql://fake.local/warehouse",
                "source_type": "postgres",
                "source_name": "bounded_warehouse",
                "max_tables": 2,
                "sample_rows": 100,
            },
        )

        assert resp.status_code == 200
        result = resp.json()
        assert result["tables_loaded"] == 2
        assert result["tables_discovered"] == 2
        assert result["tables_skipped_count"] == 1
        assert result["tables_skipped"] == ["public.events"]
        assert result["profiling_policy"]["max_tables"] == 2
        assert result["profiling_policy"]["sample_rows"] == 100
        assert connector.sample_limits == [("public.users", 100), ("public.orders", 100)]


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

    def test_insights_include_ranked_statistical_cards(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        resp = client.get("/api/insights")

        assert resp.status_code == 200
        data = resp.json()
        top_insights = data["top_insights"]

        assert top_insights
        business_prefixes = (
            "temporal_peak:",
            "metric_peak:",
            "segment_concentration:",
            "metric_driver:",
            "value_distribution:",
        )
        assert any(i["id"].startswith(business_prefixes) for i in top_insights)
        assert sum(1 for i in top_insights[:5] if i["chart_type"] == "line") <= 2
        assert len({i["table"] for i in top_insights}) >= 2
        assert "semantic_highlights" in data
        assert isinstance(data["semantic_highlights"], list)
        for insight in top_insights:
            assert insight["category"] == "Did You Know"
            assert insight["title"]
            assert insight["detail"]
            assert insight["metric"]
            assert insight["chart_type"] in {"bar", "line", "pie", "histogram"}
            assert insight["chart"]
            assert {"label", "value"} <= set(insight["chart"][0])


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

    def test_generate_persists_models_for_maturity(self, client):
        self._setup(client)
        store = client.app.state.metadata_store
        models = store.get_models("source")
        assert len(models) >= 9
        assert any(m["model_type"] == "mart" for m in models)

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
        resp = client.post(
            f"/api/models/{mart['name']}/approve",
            json={
                "reviewer": "analyst@example.com",
                "reason": "Looks aligned with the source grain",
                "diff_summary": "No SQL edits",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "approved"
        persisted = {
            model["name"]: model
            for model in client.app.state.metadata_store.get_models("source")
        }
        assert persisted[mart["name"]]["status"] == "approved"
        reviews = client.get(f"/api/models/{mart['name']}/reviews").json()["reviews"]
        assert reviews[0]["decision"] == "approved"
        assert reviews[0]["reviewer"] == "analyst@example.com"
        assert reviews[0]["reason"] == "Looks aligned with the source grain"

    def test_reject_model(self, client):
        self._setup(client)
        models_resp = client.get("/api/models")
        mart = next(
            m for m in models_resp.json() if m["model_type"] == "mart" and m["status"] == "proposed"
        )
        resp = client.post(
            f"/api/models/{mart['name']}/reject",
            json={"reason": "The aggregation grain is ambiguous"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "rejected"
        store = client.app.state.metadata_store
        reviews = store.list_model_reviews(mart["name"])
        assert reviews[0]["decision"] == "rejected"
        assert reviews[0]["reason"] == "The aggregation grain is ambiguous"

    def test_model_impact_report(self, client):
        self._setup(client)
        resp = client.get("/api/models/impact")
        assert resp.status_code == 200
        data = resp.json()
        assert data["summary"]["total_models"] >= 9
        assert data["summary"]["mart_models"] >= 1
        assert data["summary"]["impacted_models"] >= 1
        assert any(
            blocker["title"] == "Needs human review"
            for blocker in data["summary"]["top_blockers"]
        )
        assert any(m["maturity_state"] == "review_pending" for m in data["models"])

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
        impact = client.get("/api/models/impact").json()
        assert impact["summary"]["materialized_models"] == 8

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

    def test_rerun_plan_for_drift_report(self, client):
        """GET /api/rerun-plan returns targeted actions from drift and impacts."""
        store = client.app.state.metadata_store
        store.upsert_source("src", "json", "/data", None)
        run1 = store.start_run("src")
        store.finish_run(run1, table_count=1)
        run2 = store.start_run("src")
        store.finish_run(run2, table_count=1)
        diff = {
            "source_name": "src",
            "run_id_from": run1,
            "run_id_to": run2,
            "no_changes": False,
            "tables_added": [],
            "tables_removed": [],
            "tables_changed": [
                {
                    "table_name": "orders",
                    "change_type": "columns_changed",
                    "column_changes": [
                        {
                            "column_name": "amount",
                            "change_type": "type_changed",
                            "before": "float64",
                            "after": "varchar",
                        }
                    ],
                }
            ],
            "detected_at": "2026-01-01T00:00:00Z",
        }
        report_id = store.save_drift_report("src", run1, run2, diff)
        store.save_model_impacts(
            [
                {
                    "source_name": "src",
                    "drift_report_id": report_id,
                    "model_name": "stg_orders",
                    "impact_type": "source_column_type_changed",
                    "severity": "error",
                    "source_table": "orders",
                    "source_column": "amount",
                    "reason": "Referenced source column changed type",
                }
            ]
        )

        resp = client.get(f"/api/rerun-plan?source=src&drift_report_id={report_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["drift_report_id"] == report_id
        assert data["regenerate_models"] is True
        assert data["rerun_contracts"] is True
        assert data["human_review_required"] is True
        assert data["impacted_models"] == ["stg_orders"]


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

    def test_source_only_entry_appears_in_project_list(self, client):
        client.app.state.metadata_store.upsert_source("sample", "json", "/data/sample", None)

        resp = client.get("/api/projects")

        assert resp.status_code == 200
        projects = {p["id"]: p for p in resp.json()["projects"]}
        assert "sample" in projects
        assert projects["sample"]["sources"] == ["sample"]


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


class TestProjectUpdate:
    def test_update_project_sources(self, client):
        client.post(
            "/api/sources",
            json={
                "name": "warehouse_a",
                "type": "json",
                "path": SAMPLE_DATA,
                "config": {"include_schemas": ["data.dim*", "prst.*"]},
            },
        )
        project = client.post("/api/projects", json={"display_name": "Retail"}).json()
        resp = client.patch(
            f"/api/projects/{project['id']}",
            json={
                "display_name": "Retail Ops",
                "description": "Updated",
                "sources": ["warehouse_a"],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["display_name"] == "Retail Ops"
        assert data["sources"] == ["warehouse_a"]


class TestSourceUpdate:
    def test_update_source_config(self, client):
        client.post(
            "/api/sources",
            json={"name": "sample_json", "type": "json", "path": SAMPLE_DATA},
        )
        resp = client.patch(
            "/api/sources/sample_json",
            json={
                "display_name": "Sample JSON",
                "config": {
                    "include_schemas": ["data.dim*", "prst.*", "view.*"],
                    "max_tables": 25,
                },
                "auto_sync": True,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["display_name"] == "Sample JSON"
        assert data["config"]["include_schemas"] == ["data.dim*", "prst.*", "view.*"]
        assert data["config"]["max_tables"] == 25
        assert data["auto_sync"] is True


class TestProjectGraph:
    """Project-scoped graph API payload shape."""

    def test_project_graph_data_includes_erd_fields(self, client):
        client.post("/api/discover", params={"source_path": SAMPLE_DATA})
        project_resp = client.post(
            "/api/projects",
            json={"display_name": "Graph Project", "sources": ["source"]},
        )
        project_id = project_resp.json()["id"]

        resp = client.get("/api/graph/data", params={"project_id": project_id})
        assert resp.status_code == 200
        data = resp.json()
        assert data["nodes"]
        for node in data["nodes"]:
            assert set(node) >= {"id", "row_count", "domain", "description"}
            assert isinstance(node["row_count"], int)
        for edge in data["edges"]:
            assert set(edge) >= {
                "source",
                "target",
                "from_column",
                "to_column",
                "rel_type",
                "confidence",
                "ref_integrity",
                "nullable",
            }


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

    def test_suggestions_include_insight_diagnostics(self, client):
        """Explore suggestions should expose family diagnostics for validation."""
        self._run_pipeline(client)
        resp = client.get("/api/explore/suggestions")
        assert resp.status_code == 200
        diagnostics = resp.json().get("diagnostics")
        assert isinstance(diagnostics, list)
        assert diagnostics, "Expected insight family diagnostics"
        assert {
            "schema_name",
            "physical_table",
            "family",
            "status",
            "required_roles",
            "found_roles",
            "generated_count",
            "reason",
        }.issubset(diagnostics[0])

    def test_suggestions_cover_multiple_sources(self, client):
        """Suggestions should come from multiple sources, not just one."""
        self._run_pipeline(client)
        resp = client.get("/api/explore/suggestions")
        suggestions = resp.json()["suggestions"]
        sources = {s["source"] for s in suggestions}
        assert len(sources) >= 2, (
            f"Suggestions only from {sources}. Expected at least 2 sources."
        )

    def test_explore_surfaces_business_insights_and_questions(self, client):
        self._run_pipeline(client)
        suggestions_resp = client.get("/api/explore/suggestions")
        assert suggestions_resp.status_code == 200
        suggestions_payload = suggestions_resp.json()
        assert suggestions_payload["business_insights"]
        assert "semantic_highlights" in suggestions_payload
        assert any(s["source"] == "business" for s in suggestions_payload["suggestions"])
        assert any(
            "changed over time" in s["question"].lower()
            for s in suggestions_payload["suggestions"]
        )

        insights_resp = client.get("/api/explore/insights")
        assert insights_resp.status_code == 200
        insights_payload = insights_resp.json()
        assert insights_payload["business_insights"]
        assert "semantic_highlights" in insights_payload

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

    def test_ask_route_runs_sync_explorer_off_event_loop(self, client, monkeypatch):
        """The async API route must not call sync ask() on the running event loop."""
        self._run_pipeline(client)

        def fake_ask(**kwargs):
            return asyncio.run(asyncio.sleep(0, result=ExplorationResult(
                question=kwargs["question"],
                sql="SELECT 1 AS ok",
                data=[{"ok": 1}],
                row_count=1,
                error=None,
            )))

        monkeypatch.setattr("headwater.api.routes.explore.ask", fake_ask)

        resp = client.post("/api/explore/ask", json={"question": "Does threading work?"})
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["error"] is None
        assert payload["data"] == [{"ok": 1}]

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
