"""Tests for Headwater 2 answer execution — closing the data loop."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore
from headwater.services.h2_execute import (
    execute_project_answers,
    materialize_source,
    result_stats,
)

runner = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")


def _setup(source_path: str, source_name: str, project_id: str, goal: str) -> None:
    r = runner.invoke(app, ["discover", "--source", source_path, "--name", source_name])
    assert r.exit_code == 0, r.output
    r = runner.invoke(
        app,
        [
            "project", "frame",
            "--project-id", project_id,
            "--source", source_name,
            "--name", project_id.replace("_", " ").title(),
            "--goal", goal,
        ],
    )
    assert r.exit_code == 0, r.output
    runner.invoke(app, ["resolve", "--project-id", project_id])
    runner.invoke(app, ["readiness", "--project-id", project_id])


def test_execute_returns_real_rows(monkeypatch, tmp_path):
    """Drafted answer SQL executes against the source and returns real rows + stats."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _setup(SAMPLE_DATA, "sample", "exec_proj",
               "Analyse readings over time and by site")
        from headwater.services.h2_answer import draft_project_answers

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            drafts = draft_project_answers(store, "exec_proj")
            assert [a for a in drafts.answers if a.sql_text], \
                "expected at least one answer with SQL"

            results = execute_project_answers(store, "exec_proj")
            assert results, "expected execution results"

            ok = [r for r in results.values() if r.ok and r.sql_text]
            assert ok, "expected at least one successful execution"
            for r in ok:
                assert r.columns, "executed result should expose columns"
                assert r.stats.get("row_count") is not None
                assert "columns" in r.stats
                # Stats are aggregates only (invariant I-3).
                for col_summary in r.stats["columns"].values():
                    assert "dtype" in col_summary
                    assert "null_count" in col_summary
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_materialize_unsupported_source_raises(monkeypatch, tmp_path):
    """Warehouse sources can't be materialized locally yet — fail clearly."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            store.upsert_source("pg", "postgres", None, "postgres://x")
            with pytest.raises(ValueError, match="cannot be executed locally"):
                materialize_source(store, "pg")
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_result_stats_are_aggregates_only():
    """result_stats summarizes columns without leaking raw rows."""
    import polars as pl

    df = pl.DataFrame(
        {
            "segment": ["a", "b", "c", "a"],
            "avg_value": [1.0, 2.0, 3.0, 4.0],
        }
    )
    stats = result_stats(df)
    assert stats["row_count"] == 4
    assert stats["column_count"] == 2
    assert stats["columns"]["avg_value"]["min"] == 1.0
    assert stats["columns"]["avg_value"]["max"] == 4.0
    assert stats["columns"]["segment"]["distinct_count"] == 3
    # No raw row values present anywhere in the summary.
    assert "rows" not in stats
