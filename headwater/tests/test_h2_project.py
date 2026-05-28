"""Tests for Headwater 2 project framing and relevance."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore

runner = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")


def test_h2_project_frame_and_relevance_end_to_end(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        discover = runner.invoke(
            app,
            [
                "discover",
                "--source",
                SAMPLE_DATA,
                "--type",
                "json",
                "--name",
                "sample",
            ],
        )
        assert discover.exit_code == 0, discover.output

        frame = runner.invoke(
            app,
            [
                "project",
                "frame",
                "--project-id",
                "sample_inspection_relevance",
                "--source",
                "sample",
                "--name",
                "Inspection backlog",
                "--goal",
                "Reduce inspection backlog and identify bottlenecks over time",
                "--metric",
                "inspection score",
                "--time-horizon",
                "quarter",
            ],
        )
        assert frame.exit_code == 0, frame.output
        assert "Framed project sample_inspection_relevance" in frame.output
        assert "Relevant columns" in frame.output
        assert "Proposed questions" in frame.output

        relevance = runner.invoke(
            app,
            [
                "project",
                "relevance",
                "--project-id",
                "sample_inspection_relevance",
            ],
        )
        assert relevance.exit_code == 0, relevance.output
        assert "Source snapshot:" in relevance.output
        assert "Proposed questions" in relevance.output
        assert "How does inspection score change over time?" in relevance.output
        assert "Which inspection type has the highest" in relevance.output

        spec_path = tmp_path / "projects" / "sample_inspection_relevance.yaml"
        assert spec_path.exists()
        spec_text = spec_path.read_text(encoding="utf-8")
        assert "Inspection backlog" in spec_text

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            project = store.get_project("sample_inspection_relevance")
            assert project is not None
            assert (
                project["goal"]["statement"]
                == "Reduce inspection backlog and identify bottlenecks over time"
            )
            questions = store.list_questions("sample_inspection_relevance")
            assert len(questions) >= 3
            claims = store.list_semantic_claims("sample_inspection_relevance")
            assert claims
            assert any(claim["claim_type"] == "relevance" for claim in claims)
        finally:
            store.close()
    finally:
        get_settings.cache_clear()
