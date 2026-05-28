"""Tests for the Headwater 2 project-centric store."""

from __future__ import annotations

from headwater.core.store import HeadwaterStore


def make_store() -> HeadwaterStore:
    store = HeadwaterStore(":memory:")
    store.init()
    return store


def test_init_creates_h2_schema() -> None:
    store = make_store()
    try:
        tables = store.con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = {row["name"] for row in tables}

        assert {
            "sources",
            "source_snapshots",
            "tables",
            "columns",
            "profiles",
            "relationships",
            "projects",
            "project_sources",
            "semantic_claims",
            "questions",
            "resolve_items",
            "readiness_contracts",
            "readiness_verdicts",
            "answer_artifacts",
            "decisions",
        }.issubset(names)
    finally:
        store.close()


def test_roundtrip_source_project_question_and_decision() -> None:
    store = make_store()
    try:
        store.upsert_source("source_a", "postgres", "/data/source_a", "postgresql://db")
        store.record_source_snapshot(
            "source_a",
            "snap_001",
            fingerprint="abc123",
            payload={"tables": ["records", "transactions"]},
        )
        store.upsert_table(
            "source_a",
            "records",
            schema_name="main",
            row_count=5000,
            description="Primary entity records",
            selected=True,
        )
        store.upsert_column(
            "source_a",
            "records",
            "record_id",
            "varchar",
            is_primary_key=True,
            locked=True,
        )
        store.upsert_profile(
            "source_a",
            "records",
            "record_id",
            "varchar",
            {"distinct_count": 5000, "null_count": 0},
            snapshot_id="snap_001",
        )
        store.insert_relationship(
            "source_a",
            "transactions",
            "record_id",
            "records",
            "record_id",
            "many_to_one",
            0.99,
            1.0,
            snapshot_id="snap_001",
        )
        store.upsert_project(
            "proj_01",
            slug="reduce-cycle-time",
            display_name="Reduce processing cycle time",
            description="Process optimisation",
            goal={
                "statement": "Reduce processing cycle time across workflow steps",
                "decision": "identify bottlenecks",
            },
        )
        store.upsert_project_source(
            "proj_01",
            "source_a",
            selected_tables=["records", "transactions"],
            scope={"time_horizon": "7d"},
        )
        store.upsert_semantic_claim(
            "claim_status_code",
            project_id="proj_01",
            source_name="source_a",
            scope_type="column",
            table_name="records",
            column_name="status_code",
            claim_type="enum_mapping",
            claim={"A": "Active", "I": "Inactive"},
            status="locked",
            confidence=0.9,
            locked=True,
        )
        store.upsert_question(
            "q1",
            project_id="proj_01",
            title="When does processing time peak across the day?",
            question={"goal": "efficiency", "metric": "cycle_time"},
            source_name="source_a",
            status="draft",
            answerability="answerable",
            confidence=0.85,
        )
        store.upsert_resolve_item(
            "r1",
            project_id="proj_01",
            question_id="q1",
            issue_kind="definition_gap",
            title="What marks the end of the processing step?",
            body="Boundary not confirmed.",
            payload={"impact": "high"},
        )
        store.upsert_readiness_contract(
            "c1",
            question_id="q1",
            contract_type="definition_consistency",
            passed=False,
            note="Boundary unresolved",
            evidence={"missing": ["step_end_marker"]},
            snapshot_id="snap_001",
        )
        store.upsert_readiness_verdict(
            "v1",
            question_id="q1",
            state="draft",
            readiness_pct=50,
            trust_bucket="forming",
            summary="Needs one definition resolved",
            source_snapshot_id="snap_001",
            freshness="2026-05-28",
        )
        store.upsert_answer_artifact(
            "a1",
            question_id="q1",
            sql_text="SELECT 1",
            chart_spec={"chart_type": "line"},
            state="draft",
            source_snapshot_id="snap_001",
        )
        store.record_decision(
            "question",
            "q1",
            "accepted",
            reason="User wants this question tracked",
            payload={"source": "manual"},
        )

        source = store.get_source("source_a")
        snapshot = store.get_latest_source_snapshot("source_a")
        table = store.get_tables("source_a")[0]
        column = store.get_columns("source_a", "records")[0]
        profile = store.get_profiles("source_a")[0]
        relationship = store.get_relationships("source_a")[0]
        project = store.get_project("proj_01")
        project_source = store.get_project_sources("proj_01")[0]
        claim = store.get_semantic_claim("claim_status_code")
        question = store.get_question("q1")
        verdict = store.get_readiness_verdict("v1")
        artifact = store.get_answer_artifact("a1")
        decision = store.list_decisions("question", "q1")[0]

        assert source is not None and source["latest_snapshot_id"] == "snap_001"
        assert snapshot is not None and snapshot["fingerprint"] == "abc123"
        assert table["selected"] == 1
        assert column["locked"] == 1
        assert profile["profile"]["distinct_count"] == 5000
        assert relationship["from_table"] == "transactions"
        assert project is not None
        assert project["goal"]["statement"] == "Reduce processing cycle time across workflow steps"
        assert project_source["selected_tables"] == ["records", "transactions"]
        assert claim is not None and claim["claim"]["A"] == "Active"
        assert question is not None and question["question"]["metric"] == "cycle_time"
        assert verdict is not None and verdict["trust_bucket"] == "forming"
        assert artifact is not None and artifact["chart_spec"]["chart_type"] == "line"
        assert decision["payload"]["source"] == "manual"
    finally:
        store.close()


def test_verdict_retains_snapshot_id_when_latest_snapshot_moves() -> None:
    store = make_store()
    try:
        store.upsert_source("source", "csv", "/data/source", None)
        store.record_source_snapshot("source", "snap_001", payload={"version": 1})
        store.upsert_project("proj", slug="proj", display_name="Project", goal={})
        store.upsert_question(
            "q1",
            project_id="proj",
            title="Question",
            question={},
            source_name="source",
        )
        store.upsert_readiness_verdict(
            "v1",
            question_id="q1",
            state="certified",
            readiness_pct=100,
            trust_bucket="certified",
            summary="Certified against snap_001",
            source_snapshot_id="snap_001",
        )

        store.record_source_snapshot("source", "snap_002", payload={"version": 2})

        source = store.get_source("source")
        verdict = store.get_readiness_verdict("v1")
        snapshot = store.get_latest_source_snapshot("source")

        assert source is not None and source["latest_snapshot_id"] == "snap_002"
        assert verdict is not None and verdict["source_snapshot_id"] == "snap_001"
        assert snapshot is not None and snapshot["id"] == "snap_002"
    finally:
        store.close()
