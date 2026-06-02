"""LLM relationship + key inference (Move D): advisory, validated, degrades."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from headwater.analyzer.llm import NoLLMProvider
from headwater.core.store import HeadwaterStore
from headwater.services.h2_enrich import suggest_keys, suggest_relationships
from headwater.services.h2_source import discover_and_persist

SAMPLE = str(Path(__file__).resolve().parents[2] / "data" / "sample")


class _RelProvider:
    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        return {
            "relationships": [
                {
                    "from_table": "readings",
                    "from_column": "site_id",
                    "to_table": "sites",
                    "to_column": "site_id",
                    "rationale": "fk",
                    "confidence": 0.9,
                },
                # invalid column → must be filtered out
                {
                    "from_table": "readings",
                    "from_column": "ghost_col",
                    "to_table": "sites",
                    "to_column": "site_id",
                    "rationale": "x",
                    "confidence": 0.5,
                },
            ]
        }


class _KeyProvider:
    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        return {
            "keys": [
                {
                    "table": "sites",
                    "columns": ["site_id"],
                    "rationale": "unique",
                    "confidence": 0.95,
                },
                # invalid column → filtered
                {"table": "sites", "columns": ["ghost"], "rationale": "x", "confidence": 0.3},
            ]
        }


@pytest.fixture()
def store(tmp_path, monkeypatch) -> HeadwaterStore:
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    s = HeadwaterStore(tmp_path / "h2_metadata.db")
    s.init()
    discover_and_persist(SAMPLE, store=s, source_type="json", name="sample")
    try:
        yield s
    finally:
        s.close()


def test_suggest_relationships_validates_against_real_columns(store):
    result = suggest_relationships(store, "sample", provider=_RelProvider())
    assert result["available"] is True
    rels = result["relationships"]
    # The real FK is kept; the one with a nonexistent column is dropped.
    assert any(
        r["from_table"] == "readings"
        and r["from_column"] == "site_id"
        and r["to_table"] == "sites"
        for r in rels
    )
    assert all(r["from_column"] != "ghost_col" for r in rels)
    assert rels[0]["rationale"] and 0 <= rels[0]["confidence"] <= 1


def test_suggest_keys_validates_columns(store):
    result = suggest_keys(store, "sample", provider=_KeyProvider())
    assert result["available"] is True
    sites = next(k for k in result["keys"] if k["table"] == "sites")
    assert sites["columns"] == ["site_id"]  # "ghost" filtered out


def test_inference_degrades_without_a_model(store):
    assert suggest_relationships(store, "sample", provider=NoLLMProvider())["available"] is False
    assert suggest_keys(store, "sample", provider=NoLLMProvider())["available"] is False


def test_suggest_unknown_source_raises(store):
    with pytest.raises(ValueError, match="not found"):
        suggest_relationships(store, "nope", provider=_RelProvider())
