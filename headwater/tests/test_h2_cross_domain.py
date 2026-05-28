"""Tests for Headwater 2 S16 — Cross-Domain Product Validation.

Verifies the same engine produces useful metadata, relevance, questions,
resolve cards, readiness verdicts, EDA findings, and reports across:

  - Sample data: environmental-health inspection domain
  - Radiology: patient workflow / imaging domain (if available)
  - MovieLens media: movie ratings / recommendation domain (if available)

No production code changes are required to add a new fixture — only test
configuration and fixture data. Domain-specific expectations live here only,
not in the engine.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore

runner = CliRunner()

SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")
RADIOLOGY_DATA = Path(__file__).resolve().parents[2] / "data" / "radiology"
MEDIA_DATA = Path(__file__).resolve().parents[2] / "data" / "media" / "ml-latest-small"

_HAS_RADIOLOGY = (RADIOLOGY_DATA / "cases.csv").exists()
_HAS_MEDIA = (MEDIA_DATA / "ratings.csv").exists()


def _full_pipeline(tmp_path: Path, source_path: str, source_name: str,
                   project_id: str, goal: str) -> None:
    """Run the full H2 pipeline on any source without domain-specific setup."""
    r = runner.invoke(app, ["discover", "--source", source_path, "--name", source_name])
    assert r.exit_code == 0, f"discover failed: {r.output}"
    r = runner.invoke(app, [
        "project", "frame",
        "--project-id", project_id,
        "--source", source_name,
        "--name", project_id.replace("_", " ").title(),
        "--goal", goal,
    ])
    assert r.exit_code == 0, f"frame failed: {r.output}"
    runner.invoke(app, ["resolve", "--project-id", project_id])
    runner.invoke(app, ["readiness", "--project-id", project_id])
    runner.invoke(app, ["eda", "run", "--project-id", project_id])
    runner.invoke(app, ["answer", "--project-id", project_id])


class TestSampleDomainBaseline:
    """Baseline: the sample (environmental health) domain must always work."""

    def test_sample_produces_questions(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _full_pipeline(tmp_path, SAMPLE_DATA, "sample", "sample_cross",
                           "Analyse inspection score distribution over time")
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                questions = store.list_questions("sample_cross")
                assert questions, "Expected at least one question for sample domain"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_sample_produces_resolve_cards(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _full_pipeline(tmp_path, SAMPLE_DATA, "sample", "sample_resolve",
                           "Analyse inspection results by category")
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                items = store.list_resolve_items("sample_resolve")
                assert items, "Expected resolve items for sample domain"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_sample_produces_eda_findings(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _full_pipeline(tmp_path, SAMPLE_DATA, "sample", "sample_eda",
                           "Analyse inspection scores over time")
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                claims = store.list_semantic_claims("sample_eda")
                eda = [c for c in claims if c["claim_type"] == "eda_finding"]
                assert eda, "Expected EDA findings for sample domain"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_sample_report_generates(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _full_pipeline(tmp_path, SAMPLE_DATA, "sample", "sample_report",
                           "Analyse inspection results and quality patterns")
            result = runner.invoke(app, [
                "report", "--project-id", "sample_report", "--print",
            ])
            assert result.exit_code == 0, result.output
            assert "# Headwater Audit Report" in result.output
            assert "## Proposed Questions" in result.output
        finally:
            get_settings.cache_clear()

    def test_two_goals_same_source_produce_different_output(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            r = runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"])
            assert r.exit_code == 0, r.output

            r = runner.invoke(app, [
                "project", "frame",
                "--project-id", "goal_a",
                "--source", "sample",
                "--name", "Goal A",
                "--goal", "Analyse inspection score trends over time",
            ])
            assert r.exit_code == 0, r.output

            r = runner.invoke(app, [
                "project", "frame",
                "--project-id", "goal_b",
                "--source", "sample",
                "--name", "Goal B",
                "--goal", "Analyse geographic distribution of inspection results",
            ])
            assert r.exit_code == 0, r.output

            from headwater.services.h2_report import build_report

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report_a = build_report(store, "goal_a")
                report_b = build_report(store, "goal_b")
                assert "Goal A" in report_a
                assert "Goal B" in report_b
                assert report_a != report_b, (
                    "Two different goals on same source must produce different reports"
                )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestRadiologyDomain:
    @pytest.mark.skipif(not _HAS_RADIOLOGY, reason="Radiology fixture not available")
    def test_radiology_full_pipeline(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _full_pipeline(
                tmp_path, str(RADIOLOGY_DATA), "radiology", "rad_cross",
                "Analyse patient flow and wait time distribution",
            )
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                assert store.list_questions("rad_cross"), "Expected questions"
                assert store.list_resolve_items("rad_cross"), "Expected resolve items"
                eda = [c for c in store.list_semantic_claims("rad_cross")
                       if c["claim_type"] == "eda_finding"]
                assert eda, "Expected EDA findings"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(not _HAS_RADIOLOGY, reason="Radiology fixture not available")
    def test_two_radiology_projects_share_source_profile(self, monkeypatch, tmp_path):
        """Two projects on one source must reuse profiles without re-ingestion."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            r = runner.invoke(app, [
                "discover", "--source", str(RADIOLOGY_DATA), "--name", "radiology"
            ])
            assert r.exit_code == 0, r.output

            for pid, goal in [
                ("rad_p1", "Analyse patient registration bottlenecks"),
                ("rad_p2", "Analyse device utilization efficiency"),
            ]:
                r = runner.invoke(app, [
                    "project", "frame",
                    "--project-id", pid,
                    "--source", "radiology",
                    "--name", pid.replace("_", " ").title(),
                    "--goal", goal,
                ])
                assert r.exit_code == 0, r.output

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                # Both projects must have questions without a second discovery run
                for pid in ("rad_p1", "rad_p2"):
                    qs = store.list_questions(pid)
                    assert qs, f"Expected questions for {pid}"
                # Source profiles were ingested once
                snapshots = store.con.execute(
                    "SELECT COUNT(*) FROM source_snapshots WHERE source_name='radiology'"
                ).fetchone()
                assert snapshots[0] == 1, "Expected exactly one discovery run"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestMediaDomain:
    @pytest.mark.skipif(not _HAS_MEDIA, reason="MovieLens fixture not available")
    def test_media_discover_and_frame(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            r = runner.invoke(app, [
                "discover", "--source", str(MEDIA_DATA), "--type", "csv",
                "--name", "media",
            ])
            assert r.exit_code == 0, f"discover failed: {r.output}"

            r = runner.invoke(app, [
                "project", "frame",
                "--project-id", "media_engagement",
                "--source", "media",
                "--name", "Media Engagement",
                "--goal", "Understand user engagement and rating patterns over time",
            ])
            assert r.exit_code == 0, f"frame failed: {r.output}"

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                source = store.get_source("media")
                assert source is not None, "Source 'media' must be registered"
                tables = store.get_tables("media")
                assert tables, "Expected at least one table from MovieLens"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(not _HAS_MEDIA, reason="MovieLens fixture not available")
    def test_media_produces_questions(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            r = runner.invoke(app, [
                "discover", "--source", str(MEDIA_DATA), "--type", "csv",
                "--name", "media",
            ])
            assert r.exit_code == 0, r.output
            r = runner.invoke(app, [
                "project", "frame",
                "--project-id", "media_q",
                "--source", "media",
                "--name", "Media Questions",
                "--goal", "Analyse user rating distribution and movie popularity trends",
            ])
            assert r.exit_code == 0, r.output

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                questions = store.list_questions("media_q")
                assert questions, "Expected at least one question for media domain"
                answerabilities = {q["answerability"] for q in questions}
                assert "answerable" in answerabilities or "answerable_with_caveat" in answerabilities
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(not _HAS_MEDIA, reason="MovieLens fixture not available")
    def test_media_eda_and_report(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _full_pipeline(
                tmp_path, str(MEDIA_DATA), "media", "media_full",
                "Analyse rating patterns and user engagement over time",
            )
            result = runner.invoke(app, [
                "report", "--project-id", "media_full", "--print",
            ])
            assert result.exit_code == 0, result.output
            assert "# Headwater Audit Report" in result.output
            assert "## Data Overview" in result.output
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(not _HAS_MEDIA, reason="MovieLens fixture not available")
    def test_media_no_domain_hardcoding_in_engine(self, monkeypatch, tmp_path):
        """Engine must produce output via generic roles, not MovieLens-specific logic."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            r = runner.invoke(app, [
                "discover", "--source", str(MEDIA_DATA), "--type", "csv",
                "--name", "media",
            ])
            assert r.exit_code == 0, r.output
            r = runner.invoke(app, [
                "project", "frame",
                "--project-id", "media_generic",
                "--source", "media",
                "--name", "Generic Media Check",
                "--goal", "Analyse distribution patterns over time",
            ])
            assert r.exit_code == 0, r.output

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                claims = store.list_semantic_claims("media_generic")
                # No claim should contain MovieLens-specific tokens in its source field
                for claim in claims:
                    source = str(claim.get("source") or "")
                    assert "movielens" not in source.lower()
                    assert "movie" not in source.lower() or "resource:" in source.lower()
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestAddingNewFixtureDoesNotRequireCodeChanges:
    """Verifies the no-production-code-change invariant: a new fixture is
    just data + test expectations, never a production code change."""

    def test_boundary_scanner_finds_no_domain_tokens_in_h2_services(self):
        """Architecture boundary test: no domain tokens in any h2_* service."""
        from headwater.core.config import get_settings as _gs

        settings_path = Path(__file__).resolve().parents[1] / "headwater" / "services"
        forbidden = [
            "radiology", "patient", "hospital", "modality", "inspection",
            "movielens", "movie", "rating", "userId", "movieId",
            "pickup", "dropoff", "taxi", "fare",
        ]
        leaks: list[str] = []
        for path in settings_path.glob("h2_*.py"):
            text = path.read_text()
            for lineno, line in enumerate(text.splitlines(), 1):
                if line.lstrip().startswith("#"):
                    continue
                line_l = line.lower()
                for token in forbidden:
                    if token.lower() in line_l:
                        leaks.append(f"{path.name}:{lineno}:{token} — {line.strip()[:60]}")
        assert not leaks, (
            "Domain-specific tokens found in H2 service modules:\n"
            + "\n".join(leaks[:10])
        )
