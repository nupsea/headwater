"""Tests for SQLite metadata store."""

from __future__ import annotations

from headwater.core.metadata import MetadataStore


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
        "src", "sites", "zone_id", "zones", "zone_id",
        "many_to_one", 0.95, 0.98, "inferred_name",
    )
    rels = meta.get_relationships("src")
    assert len(rels) == 1
    assert rels[0]["from_table"] == "sites"


def test_model_roundtrip(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    meta.upsert_model(
        "stg_sites", "src", "staging", "SELECT * FROM sites",
        description="Staging for sites", status="approved",
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


def test_contract_roundtrip(meta: MetadataStore):
    meta.upsert_contract(
        "c1", "stg_sites", "not_null", "site_id IS NOT NULL",
        column_name="site_id", severity="error",
    )
    contracts = meta.get_contracts("stg_sites")
    assert len(contracts) == 1
    assert contracts[0]["severity"] == "error"


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
        "model", "mart_x", "rejected",
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
    cols = meta.con.execute(
        "PRAGMA table_info(decisions)"
    ).fetchall()
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
        "anthropic", "claude-sonnet-4-5",
        prompt_text="analyze this table",
        response_text='{"description": "test"}',
        tokens_in=100, tokens_out=50,
    )
    entries = meta.get_llm_audit_log()
    assert len(entries) == 1
    e = entries[0]
    assert e["provider"] == "anthropic"
    assert e["tokens_in"] == 100
    assert e["tokens_out"] == 50
