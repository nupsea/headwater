"""Tests for H2 enrichment (goal suggestion, descriptions) and dtype edits."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from headwater.analyzer.llm import NoLLMProvider
from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore
from headwater.services.h2_catalog import update_column
from headwater.services.h2_enrich import generate_descriptions, suggest_goal

runner = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")


class _DescProvider:
    """Stub provider returning a description map for any prompt."""

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        # Return a description for every "- name: type" line in the prompt.
        out: dict[str, str] = {}
        for line in prompt.splitlines():
            line = line.strip()
            if line.startswith("- ") and ":" in line:
                name = line[2:].split(":", 1)[0].strip()
                out[name] = f"Inferred meaning of {name}."
        return out


def _discover() -> None:
    r = runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"])
    assert r.exit_code == 0, r.output


def test_suggest_goal_falls_back_without_model(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _discover()
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            res = suggest_goal(store, "sample", provider=NoLLMProvider())
            assert res["available"] is False
            assert res["goal"]  # a generic, non-empty fallback goal
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_generate_descriptions_sets_only_empty(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _discover()
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            res = generate_descriptions(store, "sample", provider=_DescProvider())
            assert res["available"] is True
            assert res["updated"] > 0
            # A column now has an inferred description.
            tbl = store.get_tables("sample")[0]["name"]
            col = store.get_columns("sample", tbl)[0]
            assert (col.get("description") or "").startswith("Inferred meaning of")
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_generate_descriptions_unavailable_without_model(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _discover()
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            res = generate_descriptions(store, "sample", provider=NoLLMProvider())
            assert res["available"] is False
            assert res["updated"] == 0
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


class _FailingProvider:
    """Stub that mimics a reachable model whose calls silently return nothing
    (timeout / dropped connection — the Ollama provider swallows these as {})."""

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        return {}


def test_generate_descriptions_surfaces_silent_failure(monkeypatch, tmp_path):
    """A model that returns nothing must report available=False with a reason —
    never a misleading 'Generated 0 descriptions' success."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _discover()
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            res = generate_descriptions(store, "sample", provider=_FailingProvider())
            assert res["available"] is False
            assert res["updated"] == 0
            assert res["note"]  # a concrete, user-facing reason
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_check_llm_available_reports_reason_when_off(monkeypatch, tmp_path):
    from headwater.analyzer.llm import check_llm_available
    from headwater.core.config import HeadwaterSettings

    ok, msg = check_llm_available(HeadwaterSettings(llm_provider="none"))
    assert ok is False and msg

    ok, msg = check_llm_available(
        HeadwaterSettings(llm_provider="anthropic", llm_api_key=None)
    )
    assert ok is False and "key" in msg.lower()


def test_update_column_dtype_persists(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _discover()
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            tbl = store.get_tables("sample")[0]["name"]
            col = store.get_columns("sample", tbl)[0]["name"]
            update_column(store, "sample", tbl, col, dtype="varchar")
            after = next(c for c in store.get_columns("sample", tbl) if c["name"] == col)
            assert after["dtype"] == "varchar"
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


class _RecordingProvider:
    """Captures the last prompt and returns a per-code mapping when codes appear."""

    def __init__(self) -> None:
        self.last_prompt = ""

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        self.last_prompt = prompt
        return {"markdown": "| code | meaning |\n| --- | --- |\n| A | Adult |"}


def test_resolve_suggestion_feeds_enum_codes_to_model(monkeypatch, tmp_path):
    """Regression: the enum card stores codes under payload['values'].

    The suggester must read that key so the model maps the actual codes — not
    fall back to the generic '| column | meaning |' template (the bug where
    A/H/S/D were lost and the draft hallucinated).
    """
    from headwater.services.h2_enrich import suggest_resolution

    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        store.upsert_source("src", "csv")
        store.upsert_project("p", slug="p", display_name="P")
        store.upsert_resolve_item(
            "p:enum:cases.patient_type",
            project_id="p",
            issue_kind="enum_mapping_needed",
            title="Define the patient_type codes",
            body="4 codes with no defined meaning.",
            priority="high",
            status="open",
            payload={
                "table": "cases",
                "column": "patient_type",
                "values": ["A", "H", "S", "D"],
                "category": "input",
            },
        )
        prov = _RecordingProvider()
        out = suggest_resolution(store, "p", "p:enum:cases.patient_type", provider=prov)
        assert out["available"] is True
        # The enum branch ran: the prompt names the column and the actual codes.
        assert "KNOWN CODES: A, H, S, D" in prov.last_prompt
        assert "patient_type" in prov.last_prompt
        # Not the generic fallback prompt.
        assert "<column>" not in prov.last_prompt
        store.close()
    finally:
        get_settings.cache_clear()


class _MapQProvider:
    """Stub: maps a user question to the first two real columns of a table."""

    def __init__(self, measure: str, dimension: str):
        self._m = measure
        self._d = dimension

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        return {
            "title": "My custom question",
            "measure": self._m,
            "dimension": self._d,
            "intent": "segment",
        }


def test_add_custom_question_maps_and_persists(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("HEADWATER_REASONING_ENGINE", "true")
    get_settings.cache_clear()
    try:
        _discover()
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        store.upsert_project("p", slug="p", display_name="P")
        store.upsert_project_source("p", "sample", selected_tables=[])
        # pick two real columns from one table to map to
        from headwater.knowledge import make_projection
        from headwater.reasoning.nodes.llm_propose import map_user_question

        tbl = store.get_tables("sample")[0]["name"]
        cols = store.get_columns("sample", tbl)
        measure = f"{tbl}.{cols[0]['name']}"
        dimension = f"{tbl}.{cols[1]['name']}"
        proj = make_projection(get_settings(), store)
        spec = map_user_question(
            store,
            "p",
            projection=proj,
            provider=_MapQProvider(measure, dimension),
            question_text="anything",
        )
        assert spec is not None
        assert spec["needed_columns"] == [measure, dimension]
        assert spec["col_roles"][measure] == "measure"

        # a hallucinated column is rejected
        bad = map_user_question(
            store,
            "p",
            projection=proj,
            provider=_MapQProvider("nope.nada", dimension),
            question_text="anything",
        )
        assert bad is None
    finally:
        get_settings.cache_clear()
        store.close()
