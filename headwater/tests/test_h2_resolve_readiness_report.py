"""Tests for Headwater 2 S10/S11/S12: Resolve, Readiness, Report."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore

runner = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")
RADIOLOGY_DATA = str(Path(__file__).resolve().parents[2] / "data" / "radiology")


def _setup_project(tmp_path: Path, source_path: str, source_name: str, project_id: str, goal: str) -> None:
    """Helper: discover source and frame project in an isolated store."""
    result = runner.invoke(
        app,
        ["discover", "--source", source_path, "--name", source_name],
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        app,
        [
            "project",
            "frame",
            "--project-id", project_id,
            "--source", source_name,
            "--name", project_id.replace("_", " ").title(),
            "--goal", goal,
        ],
    )
    assert result.exit_code == 0, result.output


class TestResolveCardEngine:
    def test_resolve_builds_cards_from_project(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "resolve_test",
                "Analyse inspection scores and identify bottlenecks over time",
            )
            result = runner.invoke(app, ["resolve", "--project-id", "resolve_test"])
            assert result.exit_code == 0, result.output
        finally:
            get_settings.cache_clear()

    def test_resolve_persists_cards_to_store(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "resolve_persist",
                "Analyse inspection score distribution by category",
            )
            result = runner.invoke(app, ["resolve", "--project-id", "resolve_persist"])
            assert result.exit_code == 0, result.output

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                items = store.list_resolve_items("resolve_persist")
                assert len(items) > 0, "Expected at least one resolve item persisted"
                issue_kinds = {item["issue_kind"] for item in items}
                assert issue_kinds, "Expected at least one issue kind"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_resolve_raises_for_unknown_project(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            store.close()
            result = runner.invoke(app, ["resolve", "--project-id", "nonexistent_project"])
            assert result.exit_code != 0
        finally:
            get_settings.cache_clear()

    def test_resolve_cards_have_contract_impacts(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "resolve_contracts",
                "Review inspection quality by location and score trend",
            )
            from headwater.services.h2_resolve import build_resolve_cards

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                cards = build_resolve_cards(store, "resolve_contracts")
                for card in cards:
                    assert card.contract_impacts, f"Card {card.card_id} has no contract_impacts"
                    assert card.issue_kind, f"Card {card.card_id} has no issue_kind"
                    assert card.priority in ("high", "medium", "low")
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(
        not Path(__file__).resolve().parents[2].joinpath("data/radiology/cases.csv").exists(),
        reason="Radiology fixture not available",
    )
    def test_resolve_radiology_detects_code_columns(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                RADIOLOGY_DATA,
                "radiology",
                "radiology_resolve",
                "Understand patient registration workflow bottlenecks",
            )
            from headwater.services.h2_resolve import build_resolve_cards

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                cards = build_resolve_cards(store, "radiology_resolve")
                enum_cards = [c for c in cards if c.issue_kind == "enum_mapping_needed"]
                assert enum_cards, (
                    "Expected at least one enum_mapping_needed card for radiology code columns"
                )
                # Each card must declare contract_impacts and reference a column
                for card in enum_cards:
                    assert card.contract_impacts, f"Card {card.card_id} has no contract_impacts"
                    assert card.payload.get("column"), f"Card {card.card_id} has no column payload"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestReadinessContracts:
    def test_readiness_evaluates_all_questions(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "readiness_all",
                "Analyse inspection scores and identify bottlenecks over time",
            )
            result = runner.invoke(app, ["readiness", "--project-id", "readiness_all"])
            assert result.exit_code == 0, result.output
            assert "CERTIFIED" in result.output or "DRAFT" in result.output or "CANNOT ANSWER" in result.output
        finally:
            get_settings.cache_clear()

    def test_readiness_cannot_answer_never_certified(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "readiness_cannot",
                "Analyse weekly trend over 3 months for inspection scores",
            )
            from headwater.services.h2_readiness import evaluate_project_readiness

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = evaluate_project_readiness(store, "readiness_cannot")
                for q in report.questions:
                    if q.state == "cannot_answer":
                        assert q.readiness_pct == 0, (
                            "cannot_answer questions must have 0% readiness"
                        )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_readiness_certification_requires_all_contracts(self, monkeypatch, tmp_path):
        """Certification cannot be set directly — must derive from passing contracts."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "readiness_derive",
                "Analyse inspection scores over time",
            )
            from headwater.services.h2_readiness import evaluate_project_readiness

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = evaluate_project_readiness(store, "readiness_derive")
                for q in report.questions:
                    if q.state == "certified":
                        assert all(c.passed for c in q.contracts), (
                            f"Question {q.question_id} marked certified but not all contracts pass"
                        )
                    if any(not c.passed for c in q.contracts):
                        assert q.state != "certified", (
                            f"Question {q.question_id} has failing contracts but is marked certified"
                        )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_readiness_persists_verdict_and_contracts(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "readiness_persist",
                "Analyse inspection backlog and score distribution",
            )
            from headwater.services.h2_readiness import evaluate_project_readiness

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = evaluate_project_readiness(store, "readiness_persist")
                questions = store.list_questions("readiness_persist")
                for q in questions:
                    verdict_id = f"{q['id']}:verdict:latest"
                    verdict = store.get_readiness_verdict(verdict_id)
                    assert verdict is not None, (
                        f"Verdict not persisted for question {q['id']}"
                    )
                    contracts = store.list_readiness_contracts(q["id"])
                    assert len(contracts) >= 5, (
                        f"Expected >= 5 contracts for question {q['id']}, got {len(contracts)}"
                    )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestAuditReport:
    def test_report_generates_markdown(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "report_basic",
                "Analyse inspection scores and identify bottlenecks over time",
            )
            result = runner.invoke(
                app,
                ["report", "--project-id", "report_basic", "--print"],
            )
            assert result.exit_code == 0, result.output
            assert "# Headwater Audit Report" in result.output
            assert "## Project Goal" in result.output
            assert "## Proposed Questions" in result.output
            assert "## Resolve Decisions" in result.output
            assert "## Evidence Appendix" in result.output
        finally:
            get_settings.cache_clear()

    def test_report_writes_file(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "report_file",
                "Analyse inspection backlog and score distribution",
            )
            out_path = tmp_path / "audit_report.md"
            result = runner.invoke(
                app,
                ["report", "--project-id", "report_file", "--output", str(out_path)],
            )
            assert result.exit_code == 0, result.output
            assert out_path.exists(), "Expected report file to exist"
            content = out_path.read_text(encoding="utf-8")
            assert "# Headwater Audit Report" in content
            assert "## Project Goal" in content
        finally:
            get_settings.cache_clear()

    def test_report_stamps_questions_correctly(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "report_stamps",
                "Analyse inspection scores over time",
            )
            result = runner.invoke(
                app,
                ["report", "--project-id", "report_stamps", "--print"],
            )
            assert result.exit_code == 0, result.output
            output = result.output
            # At least one question should appear with a state stamp
            assert "**Certified**" in output or "*Draft*" in output or "Cannot Answer" in output
        finally:
            get_settings.cache_clear()

    def test_report_goal_appears_in_output(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "report_goal",
                "Identify inspection bottlenecks and their frequency distribution",
            )
            result = runner.invoke(
                app,
                ["report", "--project-id", "report_goal", "--print"],
            )
            assert result.exit_code == 0, result.output
            assert "bottlenecks" in result.output.lower()
        finally:
            get_settings.cache_clear()

    def test_report_source_table_in_data_overview(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                SAMPLE_DATA,
                "sample",
                "report_overview",
                "Analyse inspection scores by region and category",
            )
            result = runner.invoke(
                app,
                ["report", "--project-id", "report_overview", "--print"],
            )
            assert result.exit_code == 0, result.output
            assert "## Data Overview" in result.output
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(
        not Path(__file__).resolve().parents[2].joinpath("data/radiology/cases.csv").exists(),
        reason="Radiology fixture not available",
    )
    def test_report_radiology_registration_goal(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_project(
                tmp_path,
                RADIOLOGY_DATA,
                "radiology",
                "reg_report_01",
                "Understand patient registration workflow bottlenecks and wait time",
            )
            result = runner.invoke(
                app,
                ["report", "--project-id", "reg_report_01", "--print"],
            )
            assert result.exit_code == 0, result.output
            output = result.output
            assert "# Headwater Audit Report" in output
            assert "registration" in output.lower() or "bottleneck" in output.lower()
            # Report must never embed fixture-specific tokens as domain logic
            # (it may mention data values from profiles, but never as hard-coded logic)
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(
        not Path(__file__).resolve().parents[2].joinpath("data/radiology/cases.csv").exists(),
        reason="Radiology fixture not available",
    )
    def test_two_projects_different_reports_same_source(self, monkeypatch, tmp_path):
        """Two projects on the same source must produce different, goal-anchored reports."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            # Discover once
            result = runner.invoke(
                app,
                ["discover", "--source", RADIOLOGY_DATA, "--name", "radiology"],
            )
            assert result.exit_code == 0, result.output

            # Frame project 1: registration workflow
            result = runner.invoke(
                app,
                [
                    "project", "frame",
                    "--project-id", "rad_p1",
                    "--source", "radiology",
                    "--name", "Registration Workflow",
                    "--goal", "Understand patient registration workflow bottlenecks",
                ],
            )
            assert result.exit_code == 0, result.output

            # Frame project 2: device utilization
            result = runner.invoke(
                app,
                [
                    "project", "frame",
                    "--project-id", "rad_p2",
                    "--source", "radiology",
                    "--name", "Device Utilization",
                    "--goal", "Analyse device and modality utilization efficiency",
                ],
            )
            assert result.exit_code == 0, result.output

            from headwater.services.h2_report import build_report

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report1 = build_report(store, "rad_p1")
                report2 = build_report(store, "rad_p2")
                # Reports must differ by goal
                assert "Registration Workflow" in report1
                assert "Device Utilization" in report2
                assert report1 != report2, "Two projects on same source produced identical reports"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()
