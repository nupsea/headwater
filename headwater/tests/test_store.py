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
        store.upsert_source("hospital", "postgres", "/data/hospital", "postgresql://db")
        store.record_source_snapshot(
            "hospital",
            "snap_001",
            fingerprint="abc123",
            payload={"tables": ["cases", "events"]},
        )
        store.upsert_table(
            "hospital",
            "cases",
            schema_name="clinical",
            row_count=3193,
            description="Patient visit header",
            selected=True,
        )
        store.upsert_column(
            "hospital",
            "cases",
            "case_id",
            "varchar",
            is_primary_key=True,
            locked=True,
        )
        store.upsert_profile(
            "hospital",
            "cases",
            "case_id",
            "varchar",
            {"distinct_count": 3193, "null_count": 0},
            snapshot_id="snap_001",
        )
        store.insert_relationship(
            "hospital",
            "events",
            "case_id",
            "cases",
            "case_id",
            "many_to_one",
            0.99,
            1.0,
            snapshot_id="snap_001",
        )
        store.upsert_project(
            "proj_wait",
            slug="reduce-wait",
            display_name="Reduce patient wait time",
            description="Registration workflow",
            goal={
                "statement": "Reduce patient wait time before imaging",
                "decision": "identify bottlenecks",
            },
        )
        store.upsert_project_source(
            "proj_wait",
            "hospital",
            selected_tables=["cases", "events"],
            scope={"time_horizon": "7d"},
        )
        store.upsert_semantic_claim(
            "claim_patient_type",
            project_id="proj_wait",
            source_name="hospital",
            scope_type="column",
            table_name="cases",
            column_name="patient_type",
            claim_type="enum_mapping",
            claim={"A": "Ambulatory", "S": "Scheduled"},
            status="locked",
            confidence=0.9,
            locked=True,
        )
        store.upsert_question(
            "q1",
            project_id="proj_wait",
            title="When is wait time worst across the day?",
            question={"goal": "wait", "metric": "total_wait_time"},
            source_name="hospital",
            status="draft",
            answerability="answerable",
            confidence=0.85,
        )
        store.upsert_resolve_item(
            "r1",
            project_id="proj_wait",
            question_id="q1",
            issue_kind="definition_gap",
            title="Where does registration end?",
            body="Boundary not confirmed.",
            payload={"impact": "high"},
        )
        store.upsert_readiness_contract(
            "c1",
            question_id="q1",
            contract_type="definition_consistency",
            passed=False,
            note="Boundary unresolved",
            evidence={"missing": ["registration_end"]},
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

        source = store.get_source("hospital")
        snapshot = store.get_latest_source_snapshot("hospital")
        table = store.get_tables("hospital")[0]
        column = store.get_columns("hospital", "cases")[0]
        profile = store.get_profiles("hospital")[0]
        relationship = store.get_relationships("hospital")[0]
        project = store.get_project("proj_wait")
        project_source = store.get_project_sources("proj_wait")[0]
        claim = store.get_semantic_claim("claim_patient_type")
        question = store.get_question("q1")
        verdict = store.get_readiness_verdict("v1")
        artifact = store.get_answer_artifact("a1")
        decision = store.list_decisions("question", "q1")[0]

        assert source is not None and source["latest_snapshot_id"] == "snap_001"
        assert snapshot is not None and snapshot["fingerprint"] == "abc123"
        assert table["selected"] == 1
        assert column["locked"] == 1
        assert profile["profile"]["distinct_count"] == 3193
        assert relationship["from_table"] == "events"
        assert project is not None
        assert project["goal"]["statement"] == "Reduce patient wait time before imaging"
        assert project_source["selected_tables"] == ["cases", "events"]
        assert claim is not None and claim["claim"]["A"] == "Ambulatory"
        assert question is not None and question["question"]["metric"] == "total_wait_time"
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
