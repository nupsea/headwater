"""Tests for SQLite metadata store and config persistence."""

from __future__ import annotations

import json

from headwater.core.metadata import MetadataStore
from headwater.core.models import ContractCheckResult, QualityReport


def test_init_creates_tables(meta: MetadataStore):
    tables = meta.con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in tables}
    assert "sources" in names
    assert "tables" in names
    assert "columns" in names
    assert "profiles" in names
    assert "relationships" in names
    assert "models" in names
    assert "contracts" in names
    assert "model_reviews" in names
    assert "model_impacts" in names
    assert "quality_runs" in names
    assert "quality_results" in names
    assert "decisions" in names
    assert "llm_audit_log" in names


def test_upsert_and_get_source(meta: MetadataStore):
    meta.upsert_source("sample", "json", "/data/sample", None)
    src = meta.get_source("sample")
    assert src is not None
    assert src["type"] == "json"
    assert src["path"] == "/data/sample"


def test_get_source_missing(meta: MetadataStore):
    assert meta.get_source("nonexistent") is None


def test_list_sources(meta: MetadataStore):
    meta.upsert_source("a", "json", "/a", None)
    meta.upsert_source("b", "csv", "/b", None)
    sources = meta.list_sources()
    assert len(sources) == 2


def test_upsert_source_idempotent(meta: MetadataStore):
    meta.upsert_source("s", "json", "/old", None)
    meta.upsert_source("s", "json", "/new", None)
    src = meta.get_source("s")
    assert src is not None
    assert src["path"] == "/new"


def test_delete_source_clears_source_scoped_state(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    run_id = meta.start_run("src")
    meta.upsert_table("readings", "src", run_id=run_id)
    meta.upsert_column("readings", "src", "reading_id", "INTEGER", is_primary_key=True)
    meta.upsert_profile("readings", "reading_id", "src", "INTEGER", {"distinct_count": 3}, run_id)
    meta.insert_relationship(
        "src",
        "readings",
        "site_id",
        "sites",
        "site_id",
        "many_to_one",
        1.0,
        1.0,
        "declared",
        run_id=run_id,
    )
    meta.save_snapshot(run_id, "src", {"tables": {}})
    meta.persist_pk_fk("readings", "src", reject_pks=["reading_id"])
    meta.upsert_model(
        "mart_readings",
        "src",
        "mart",
        "select 1",
        "Readings mart",
        source_tables=["readings"],
    )
    meta.record_model_review(
        "mart_readings",
        "approved",
        source_name="src",
        reviewer="tester",
    )
    meta.con.execute(
        "INSERT INTO contracts (id, model_name, rule_type, expression) "
        "VALUES ('rule_1', 'mart_readings', 'row_count', 'count(*) > 0')"
    )
    meta.con.commit()

    assert meta.delete_source("src") is True

    assert meta.get_source("src") is None
    assert meta.get_tables("src") == []
    assert meta.get_columns("readings", "src") == []
    assert meta.get_profiles("src") == []
    assert meta.get_relationships("src") == []
    assert meta.get_decisions("pk_candidate", "src.readings.reading_id") == []
    assert meta.get_models("src") == []
    contract = meta.con.execute(
        "SELECT * FROM contracts WHERE model_name='mart_readings'"
    ).fetchone()
    assert contract is None


def test_discovery_run_lifecycle(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    run_id = meta.start_run("src")
    assert run_id > 0
    meta.finish_run(run_id, table_count=5)
    row = meta.con.execute("SELECT * FROM discovery_runs WHERE id = ?", (run_id,)).fetchone()
    assert row["status"] == "completed"
    assert row["table_count"] == 5


def test_table_and_column_roundtrip(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.upsert_table("sites", "src", row_count=500, domain="Infrastructure")
    meta.upsert_column("sites", "src", "site_id", "varchar", is_primary_key=True, ordinal=0)
    meta.upsert_column("sites", "src", "name", "varchar", ordinal=1)

    tables = meta.get_tables("src")
    assert len(tables) == 1
    assert tables[0]["row_count"] == 500

    cols = meta.get_columns("sites", "src")
    assert len(cols) == 2
    assert cols[0]["name"] == "site_id"
    assert cols[0]["is_primary_key"] == 1


def test_profile_roundtrip(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.upsert_profile("sites", "latitude", "src", "float64", {"min": 38.0, "max": 39.5})
    rows = meta.con.execute("SELECT * FROM profiles WHERE table_name = 'sites'").fetchall()
    assert len(rows) == 1


def test_relationship_roundtrip(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.insert_relationship(
        "src",
        "sites",
        "zone_id",
        "zones",
        "zone_id",
        "many_to_one",
        0.95,
        0.98,
        "inferred_name",
    )
    rels = meta.get_relationships("src")
    assert len(rels) == 1
    assert rels[0]["from_table"] == "sites"


def test_persist_pk_fk_stores_reload_safe_relationship_values(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    result = meta.persist_pk_fk(
        "transactions",
        "src",
        confirm_fks=[
            {"from_col": "account_key", "to_table": "accounts", "to_col": "account_key"}
        ],
    )

    assert result["fks_confirmed"] == 1
    rel = meta.get_relationships("src")[0]
    assert rel["rel_type"] == "many_to_one"
    assert rel["detection_source"] == "declared"


def test_model_roundtrip(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.upsert_model(
        "stg_sites",
        "src",
        "staging",
        "SELECT * FROM sites",
        description="Staging for sites",
        status="approved",
    )
    models = meta.get_models("src")
    assert len(models) == 1
    assert models[0]["status"] == "approved"


def test_model_status_update(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.upsert_model("mart_x", "src", "mart", "SELECT 1", status="proposed")
    meta.update_model_status("mart_x", "approved")
    m = meta.get_models("src")
    assert m[0]["status"] == "approved"


def test_model_review_roundtrip(meta: MetadataStore):
    review_id = meta.record_model_review(
        "mart_x",
        "rejected",
        source_name="src",
        reviewer="analyst@example.com",
        reason="Grain needs clarification",
        diff_summary="No SQL edits",
        payload={"previous_status": "proposed"},
    )

    reviews = meta.list_model_reviews("mart_x")
    assert review_id > 0
    assert len(reviews) == 1
    assert reviews[0]["decision"] == "rejected"
    assert reviews[0]["reason"] == "Grain needs clarification"
    assert reviews[0]["payload"]["previous_status"] == "proposed"


def test_model_impact_roundtrip(meta: MetadataStore):
    ids = meta.save_model_impacts(
        [
            {
                "source_name": "src",
                "drift_report_id": 7,
                "model_name": "stg_orders",
                "impact_type": "source_column_type_changed",
                "severity": "error",
                "source_table": "orders",
                "source_column": "amount",
                "reason": "Referenced source column changed type",
                "payload": {"before": "float64", "after": "varchar"},
            }
        ]
    )

    impacts = meta.list_model_impacts(source_name="src")
    assert ids[0] > 0
    assert len(impacts) == 1
    assert impacts[0]["model_name"] == "stg_orders"
    assert impacts[0]["severity"] == "error"
    assert impacts[0]["payload"]["after"] == "varchar"


def test_contract_roundtrip(meta: MetadataStore):
    meta.upsert_contract(
        "c1",
        "stg_sites",
        "not_null",
        "site_id IS NOT NULL",
        column_name="site_id",
        severity="error",
    )
    contracts = meta.get_contracts("stg_sites")
    assert len(contracts) == 1
    assert contracts[0]["severity"] == "error"


def test_quality_report_roundtrip(meta: MetadataStore):
    report = QualityReport(
        total_contracts=2,
        passed=1,
        failed=1,
        results=[
            ContractCheckResult(
                rule_id="c1",
                model_name="stg_sites",
                passed=True,
                observed_value=0,
                message="No nulls",
            ),
            ContractCheckResult(
                rule_id="c2",
                model_name="stg_sites",
                passed=False,
                observed_value=3,
                message="3 null values found",
            ),
        ],
    )

    run_id = meta.save_quality_report("src", report)
    latest = meta.get_latest_quality_report("src")

    assert run_id > 0
    assert latest is not None
    assert latest["status"] == "failing"
    assert latest["score"] == 50.0
    assert len(latest["results"]) == 2
    assert latest["results"][1]["observed_value"] == 3


def test_quality_report_updates_contract_lifecycle(meta: MetadataStore):
    meta.upsert_contract("c1", "stg_sites", "not_null", "site_id IS NOT NULL")
    failing = QualityReport(
        total_contracts=1,
        passed=0,
        failed=1,
        results=[
            ContractCheckResult(
                rule_id="c1",
                model_name="stg_sites",
                passed=False,
                observed_value=2,
                message="2 null values found",
            )
        ],
    )
    meta.save_quality_report("src", failing)

    assert meta.get_contracts("stg_sites")[0]["status"] == "failing"
    assert failing.contract_status_transitions["failing"] == ["c1"]

    recovered = QualityReport(
        total_contracts=1,
        passed=1,
        failed=0,
        results=[
            ContractCheckResult(
                rule_id="c1",
                model_name="stg_sites",
                passed=True,
                observed_value=0,
                message="No nulls",
            )
        ],
    )
    meta.save_quality_report("src", recovered)

    assert meta.get_contracts("stg_sites")[0]["status"] == "recovered"
    assert recovered.contract_status_transitions["recovered"] == ["c1"]
    assert recovered.previous_failed == 1


# -- Decisions (US-301) ----------------------------------------------------


def test_record_decision_basic(meta: MetadataStore):
    meta.record_decision("model", "stg_zones", "approved")
    decisions = meta.get_decisions()
    assert len(decisions) == 1
    d = decisions[0]
    assert d["artifact_type"] == "model"
    assert d["artifact_id"] == "stg_zones"
    assert d["action"] == "approved"
    assert d["payload_json"] is None


def test_record_decision_with_payload(meta: MetadataStore):
    meta.record_decision(
        "model",
        "mart_x",
        "rejected",
        payload={"previous_status": "proposed", "reason": "unclear logic"},
    )
    decisions = meta.get_decisions("model", "mart_x")
    assert len(decisions) == 1
    import json

    payload = json.loads(decisions[0]["payload_json"])
    assert payload["previous_status"] == "proposed"


def test_record_multiple_decisions(meta: MetadataStore):
    meta.record_decision("model", "stg_zones", "approved")
    meta.record_decision("model", "mart_x", "rejected")
    meta.record_decision("contract", "c1", "observing")
    all_decisions = meta.get_decisions()
    assert len(all_decisions) == 3
    model_decisions = meta.get_decisions("model")
    assert len(model_decisions) == 2


def test_get_decisions_filtered_by_artifact(meta: MetadataStore):
    meta.record_decision("model", "stg_zones", "approved")
    meta.record_decision("model", "stg_zones", "rejected")
    meta.record_decision("model", "mart_x", "approved")
    decisions = meta.get_decisions("model", "stg_zones")
    assert len(decisions) == 2
    assert all(d["artifact_id"] == "stg_zones" for d in decisions)


def test_payload_json_column_exists(meta: MetadataStore):
    """Verify decisions table has payload_json column (migration)."""
    cols = meta.con.execute("PRAGMA table_info(decisions)").fetchall()
    col_names = {c["name"] for c in cols}
    assert "payload_json" in col_names


def test_sources_mode_column_exists(meta: MetadataStore):
    """Verify sources table has mode column (migration)."""
    cols = meta.con.execute("PRAGMA table_info(sources)").fetchall()
    col_names = {c["name"] for c in cols}
    assert "mode" in col_names


def test_upsert_source_with_mode(meta: MetadataStore):
    meta.upsert_source("s", "json", "/data", None, mode="observe")
    src = meta.get_source("s")
    assert src is not None
    assert src["mode"] == "observe"


def test_llm_audit_log_roundtrip(meta: MetadataStore):
    meta.insert_llm_audit(
        "anthropic",
        "claude-sonnet-4-5",
        prompt_text="analyze this table",
        response_text='{"description": "test"}',
        tokens_in=100,
        tokens_out=50,
    )
    entries = meta.get_llm_audit_log()
    assert len(entries) == 1
    e = entries[0]
    assert e["provider"] == "anthropic"
    assert e["tokens_in"] == 100
    assert e["tokens_out"] == 50


# -- v3: Activity log -------------------------------------------------------


def test_activity_log_empty(meta: MetadataStore):
    assert meta.get_activity() == []


def test_log_and_get_activity(meta: MetadataStore):
    meta.log_activity(
        "project_created", "Created project 'Test'", artifact_type="project", artifact_id="p1"
    )
    meta.log_activity(
        "table_reviewed", "Reviewed zones", artifact_type="table", artifact_id="zones"
    )
    activities = meta.get_activity()
    assert len(activities) == 2
    # Both actions present (order by id DESC since same-second timestamps)
    actions = {a["action"] for a in activities}
    assert "project_created" in actions
    assert "table_reviewed" in actions


def test_activity_log_limit(meta: MetadataStore):
    for i in range(5):
        meta.log_activity(f"action_{i}", f"Detail {i}")
    assert len(meta.get_activity(limit=3)) == 3


def test_activity_log_minimal(meta: MetadataStore):
    meta.log_activity("simple_action")
    activities = meta.get_activity()
    assert len(activities) == 1
    assert activities[0]["detail"] is None
    assert activities[0]["artifact_type"] is None


# -- v3: Model answers -------------------------------------------------------


def test_model_answers_empty(meta: MetadataStore):
    assert meta.get_model_answers("nonexistent") == []


def test_save_and_get_model_answers(meta: MetadataStore):
    answers = [
        {"question_index": 0, "answer": "monthly"},
        {"question_index": 1, "answer": "zone_id"},
    ]
    count = meta.save_model_answers("mart_compliance", answers)
    assert count == 2
    saved = meta.get_model_answers("mart_compliance")
    assert len(saved) == 2
    assert saved[0]["answer"] == "monthly"
    assert saved[1]["answer"] == "zone_id"


def test_model_answers_upsert(meta: MetadataStore):
    meta.save_model_answers("mart_x", [{"question_index": 0, "answer": "old"}])
    meta.save_model_answers("mart_x", [{"question_index": 0, "answer": "new"}])
    saved = meta.get_model_answers("mart_x")
    assert len(saved) == 1
    assert saved[0]["answer"] == "new"


def test_model_answers_isolated_by_model(meta: MetadataStore):
    meta.save_model_answers("model_a", [{"question_index": 0, "answer": "a"}])
    meta.save_model_answers("model_b", [{"question_index": 0, "answer": "b"}])
    assert len(meta.get_model_answers("model_a")) == 1
    assert meta.get_model_answers("model_a")[0]["answer"] == "a"


# -- v3: PK/FK persistence --------------------------------------------------


def test_persist_pk(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.upsert_table("zones", "src")
    meta.upsert_column("zones", "src", "zone_id", "INTEGER")
    meta.upsert_column("zones", "src", "name", "TEXT")

    result = meta.persist_pk_fk("zones", "src", confirm_pks=["zone_id"])
    assert result["pks_confirmed"] == 1

    col = meta.con.execute(
        "SELECT is_primary_key FROM columns WHERE table_name='zones' AND name='zone_id'"
    ).fetchone()
    assert col["is_primary_key"] == 1


def test_persist_reject_pk(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.upsert_table("zones", "src")
    meta.upsert_column("zones", "src", "zone_id", "INTEGER", is_primary_key=True)

    result = meta.persist_pk_fk("zones", "src", reject_pks=["zone_id"])
    assert result["pks_rejected"] == 1

    col = meta.con.execute(
        "SELECT is_primary_key FROM columns WHERE table_name='zones' AND name='zone_id'"
    ).fetchone()
    assert col["is_primary_key"] == 0
    decisions = meta.get_decisions("pk_candidate", "src.zones.zone_id")
    assert decisions[0]["action"] == "rejected"


def test_persist_confirm_fk(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    fks = [{"from_col": "zone_id", "to_table": "zones", "to_col": "zone_id"}]
    result = meta.persist_pk_fk("inspections", "src", confirm_fks=fks)
    assert result["fks_confirmed"] == 1

    rel = meta.con.execute("SELECT * FROM relationships WHERE from_table='inspections'").fetchone()
    assert rel is not None
    assert rel["to_table"] == "zones"
    assert rel["rel_type"] == "many_to_one"
    assert rel["detection_source"] == "declared"


def test_persist_reject_fk(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.con.execute(
        "INSERT INTO relationships (source_name, from_table, from_column, to_table, to_column, "
        "rel_type, confidence, ref_integrity, detection_source) "
        "VALUES ('src', 'inspections', 'zone_id', 'zones', 'zone_id', 'fk', 0.8, 0.9, 'detected')"
    )
    meta.con.commit()
    rel_id = meta.con.execute("SELECT id FROM relationships").fetchone()["id"]

    result = meta.persist_pk_fk("inspections", "src", reject_fk_ids=[rel_id])
    assert result["fks_rejected"] == 1
    assert (
        meta.con.execute("SELECT * FROM relationships WHERE id = ?", (rel_id,)).fetchone() is None
    )


def test_bulk_column_pk_review_records_replayable_decision(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.upsert_table("readings", "src")
    meta.upsert_column("readings", "src", "reading_date", "DATE", is_primary_key=True)

    meta.bulk_update_columns(
        "readings",
        "src",
        [{"name": "reading_date", "is_primary_key": False}],
        lock=True,
    )

    assert meta.get_pk_decision("src", "readings", "reading_date") == "rejected"


# -- v3: Settings file persistence -------------------------------------------


def test_save_and_load_settings(tmp_path):
    from headwater.core.config import (
        HeadwaterSettings,
        _load_settings_from_file,
        save_settings_to_file,
    )

    settings = HeadwaterSettings(data_dir=tmp_path, llm_provider="ollama", llm_model="llama3.2")
    path = save_settings_to_file(settings)

    assert path.exists()
    data = json.loads(path.read_text())
    assert data["llm_provider"] == "ollama"
    assert data["llm_model"] == "llama3.2"
    # Secrets should not be persisted
    assert "llm_api_key" not in data

    # Load back
    loaded = _load_settings_from_file(tmp_path)
    assert loaded["llm_provider"] == "ollama"
    assert loaded["llm_model"] == "llama3.2"


def test_load_settings_missing_file(tmp_path):
    from headwater.core.config import _load_settings_from_file

    assert _load_settings_from_file(tmp_path) == {}


def test_load_settings_corrupt_file(tmp_path):
    from headwater.core.config import _load_settings_from_file

    (tmp_path / "settings.json").write_text("not json{{{")
    assert _load_settings_from_file(tmp_path) == {}


def test_settings_file_filters_unknown_keys(tmp_path):
    from headwater.core.config import _load_settings_from_file

    (tmp_path / "settings.json").write_text(
        json.dumps({"llm_provider": "ollama", "unknown_key": "val"})
    )
    loaded = _load_settings_from_file(tmp_path)
    assert "unknown_key" not in loaded
    assert loaded["llm_provider"] == "ollama"


def test_new_tables_created(meta: MetadataStore):
    """Verify v3 tables exist after init."""
    tables = meta.con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in tables}
    assert "activity_log" in names
    assert "model_answers" in names
