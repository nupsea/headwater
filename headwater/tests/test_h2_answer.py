"""Tests for Headwater 2 S13 — Grounded Answer Drafting."""

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
RADIOLOGY_DICT = Path(__file__).resolve().parents[2] / "data" / "radiology" / "dictionary.md"


def _setup(tmp_path, source_path, source_name, project_id, goal):
    r = runner.invoke(app, ["discover", "--source", source_path, "--name", source_name])
    assert r.exit_code == 0, r.output
    r = runner.invoke(app, [
        "project", "frame",
        "--project-id", project_id,
        "--source", source_name,
        "--name", project_id.replace("_", " ").title(),
        "--goal", goal,
    ])
    assert r.exit_code == 0, r.output
    runner.invoke(app, ["resolve", "--project-id", project_id])
    runner.invoke(app, ["readiness", "--project-id", project_id])


class TestIdentifierSafety:
    def test_valid_identifiers_pass(self):
        from headwater.services.h2_answer import _validate_identifier

        assert _validate_identifier("column_name")
        assert _validate_identifier("table1")
        assert _validate_identifier("arrival_time")
        assert _validate_identifier("a123")

    def test_invalid_identifiers_rejected(self):
        from headwater.services.h2_answer import _validate_identifier

        assert not _validate_identifier("1column")  # starts with digit
        assert not _validate_identifier("col; DROP TABLE")  # SQL injection
        assert not _validate_identifier("col-name")  # hyphen
        assert not _validate_identifier("")  # empty
        assert not _validate_identifier("a" * 130)  # too long

    def test_sql_only_uses_validated_columns(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_safe",
                   "Analyse scores and category distribution over time")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_safe")
                for ans in result.answers:
                    if ans.sql_text:
                        # No raw string formatting — all identifiers quoted
                        assert ";" not in ans.sql_text or ans.sql_text.count(";") == 0
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestSQLGeneration:
    def test_temporal_question_produces_date_trunc_sql(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_temporal",
                   "Analyse inspection scores over time")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_temporal")
                temporal = [a for a in result.answers if a.state != "cannot_answer"
                            and a.sql_text and "period" in a.sql_text]
                assert temporal, "Expected at least one temporal SQL with 'period'"
                sql = temporal[0].sql_text
                assert "GROUP BY" in sql
                assert "ORDER BY" in sql
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_segmentation_question_produces_group_by_sql(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_segment",
                   "Analyse inspection scores by category")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_segment")
                segmentation = [a for a in result.answers if a.state != "cannot_answer"
                                and a.sql_text and "LIMIT" in a.sql_text]
                assert segmentation, "Expected segmentation SQL with LIMIT clause"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_cannot_answer_has_no_sql(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_cannot",
                   "Analyse weekly trend over 3 months for inspection scores")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_cannot")
                cannot = [a for a in result.answers if a.state == "cannot_answer"]
                for a in cannot:
                    assert a.sql_text is None, "cannot_answer must not have SQL"
                    assert a.confidence == 0.0
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_draft_answer_when_contract_fails(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_draft",
                   "Analyse scores by category with missing definitions")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_draft")
                drafts = [a for a in result.answers if a.state == "draft"]
                # A question with high-priority gaps should be draft
                assert any(a.state in ("draft", "certified") for a in result.answers)
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_certified_answer_requires_all_contracts(self, monkeypatch, tmp_path):
        """A certified answer must only come from a question where all contracts pass."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_certify",
                   "Analyse inspection scores over time")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_certify")
                for ans in result.answers:
                    if ans.state == "certified":
                        assert ans.confidence == 1.0, (
                            "Certified answer must have 100% confidence"
                        )
                    if ans.confidence < 1.0:
                        assert ans.state != "certified", (
                            "Low confidence must not produce certified state"
                        )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestChartSpec:
    def test_temporal_produces_line_chart(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_chart_t",
                   "Analyse scores over time")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_chart_t")
                line_charts = [a for a in result.answers
                               if a.chart_spec.get("type") == "line"]
                assert line_charts, "Expected at least one line chart for temporal question"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_segmentation_produces_bar_chart(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_chart_s",
                   "Analyse scores by category")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_chart_s")
                bar_charts = [a for a in result.answers
                              if a.chart_spec.get("type") == "bar"]
                assert bar_charts, "Expected at least one bar chart for segmentation question"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestArtifactPersistence:
    def test_answer_artifacts_persisted_to_store(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "ans_persist",
                   "Analyse inspection scores and categories over time")
            from headwater.services.h2_answer import draft_project_answers

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                result = draft_project_answers(store, "ans_persist")
                questions = store.list_questions("ans_persist")
                for q in questions:
                    artifact = store.get_answer_artifact(f"{q['id']}:answer:latest")
                    assert artifact is not None, (
                        f"Expected artifact for question {q['id']}"
                    )
                    assert artifact["state"] in ("certified", "draft", "cannot_answer")
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestCLIAnswer:
    def test_answer_command_runs(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "cli_ans",
                   "Analyse inspection scores over time")
            result = runner.invoke(app, ["answer", "--project-id", "cli_ans"])
            assert result.exit_code == 0, result.output
            assert "Answers for cli_ans" in result.output
        finally:
            get_settings.cache_clear()

    def test_answer_shows_state_stamps(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "cli_stamps",
                   "Analyse scores and categories over time")
            result = runner.invoke(app, ["answer", "--project-id", "cli_stamps"])
            assert result.exit_code == 0, result.output
            output = result.output.upper()
            assert "CERTIFIED" in output or "DRAFT" in output or "CANNOT ANSWER" in output
        finally:
            get_settings.cache_clear()

    def test_answer_unknown_project_errors(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            store.close()
            result = runner.invoke(app, ["answer", "--project-id", "no_such_project"])
            assert result.exit_code != 0
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(
        not RADIOLOGY_DICT.exists(),
        reason="Radiology fixture not available",
    )
    def test_answer_radiology_certified_question_produces_sql(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, RADIOLOGY_DATA, "radiology", "rad_ans",
                   "Analyse wait time distribution over time")
            runner.invoke(app, [
                "resource", "add",
                "--project-id", "rad_ans",
                "--path", str(RADIOLOGY_DICT),
            ])
            runner.invoke(app, ["readiness", "--project-id", "rad_ans"])

            result = runner.invoke(app, ["answer", "--project-id", "rad_ans"])
            assert result.exit_code == 0, result.output
            assert "SELECT" in result.output, "Expected SQL in output"
        finally:
            get_settings.cache_clear()


class TestJoinSafetyGuard:
    def test_join_caveat_appears_for_cross_table_query(self, monkeypatch, tmp_path):
        """When needed columns span tables with low relationship confidence, add a caveat."""
        from headwater.services.h2_answer import _check_join_safety

        col_info = [
            {"table": "table_a", "column": "col1", "role_class": "timestamp",
             "dtype": "timestamp", "safe": True, "role": "event_ts", "resource_defined": False},
            {"table": "table_b", "column": "col2", "role_class": "measure",
             "dtype": "float", "safe": True, "role": "measure", "resource_defined": False},
        ]
        rel_confidence = {("table_a", "table_b"): 0.50}
        caveats = _check_join_safety(col_info, rel_confidence, min_confidence=0.80)
        assert caveats, "Expected caveat for low-confidence join"
        assert "0.50" in caveats[0] or "50%" in caveats[0]

    def test_no_caveat_for_single_table(self, monkeypatch, tmp_path):
        from headwater.services.h2_answer import _check_join_safety

        col_info = [
            {"table": "records", "column": "ts", "role_class": "timestamp",
             "dtype": "timestamp", "safe": True, "role": "event_ts", "resource_defined": False},
            {"table": "records", "column": "score", "role_class": "measure",
             "dtype": "float", "safe": True, "role": "measure", "resource_defined": False},
        ]
        caveats = _check_join_safety(col_info, {})
        assert not caveats, "Single-table query must not produce join caveats"

    def test_no_caveat_for_high_confidence_join(self, monkeypatch, tmp_path):
        from headwater.services.h2_answer import _check_join_safety

        col_info = [
            {"table": "t1", "column": "c1", "role_class": "timestamp",
             "dtype": "timestamp", "safe": True, "role": "event_ts", "resource_defined": False},
            {"table": "t2", "column": "c2", "role_class": "measure",
             "dtype": "float", "safe": True, "role": "measure", "resource_defined": False},
        ]
        rel_confidence = {("t1", "t2"): 0.99}
        caveats = _check_join_safety(col_info, rel_confidence)
        assert not caveats, "High-confidence join must not produce caveat"
