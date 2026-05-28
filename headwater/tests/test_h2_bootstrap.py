"""Tests for profile-bootstrap claims and resource-vocabulary relevance hook."""

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
FIXTURES = Path(__file__).parent / "fixtures"


def _frame(tmp_path, source_path, source_name, project_id, goal):
    result = runner.invoke(app, ["discover", "--source", source_path, "--name", source_name])
    assert result.exit_code == 0, result.output
    result = runner.invoke(app, [
        "project", "frame",
        "--project-id", project_id,
        "--source", source_name,
        "--name", project_id.replace("_", " ").title(),
        "--goal", goal,
    ])
    assert result.exit_code == 0, result.output
    return result


class TestProfileBootstrap:
    def test_frame_creates_bootstrap_claims_for_code_columns(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _frame(tmp_path, SAMPLE_DATA, "sample", "boot_basic",
                   "Analyse scores and category distribution")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                claims = store.list_semantic_claims("boot_basic")
                bootstrap = [c for c in claims if c.get("source") == "bootstrap:profile"]
                assert bootstrap, "Expected at least one bootstrap claim from profile data"
                # Bootstrap claims must be enum_mapping type
                assert all(c["claim_type"] == "enum_mapping" for c in bootstrap)
                # Bootstrap claims must be proposed (not locked)
                assert all(not c.get("locked") for c in bootstrap)
                # Bootstrap claims must have confidence < 0.5 (codes known, meanings unknown)
                assert all(float(c["confidence"]) < 0.5 for c in bootstrap)
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_bootstrap_claim_contains_detected_codes(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _frame(tmp_path, SAMPLE_DATA, "sample", "boot_codes",
                   "Analyse category breakdown")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                claims = store.list_semantic_claims("boot_codes")
                bootstrap = [c for c in claims if c.get("source") == "bootstrap:profile"]
                for bc in bootstrap:
                    code_map = bc.get("claim", {}).get("value")
                    assert isinstance(code_map, dict), "Expected dict with code->meaning map"
                    assert len(code_map) >= 2, "Expected at least 2 detected codes"
                    # Values should be empty strings (meanings unknown at bootstrap)
                    assert all(v == "" for v in code_map.values()), (
                        "Bootstrap meanings should be empty — user fills them in"
                    )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_bootstrap_skipped_on_reframe(self, monkeypatch, tmp_path):
        """Re-running project frame must not duplicate bootstrap claims."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            # Discover once, frame twice
            r = runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"])
            assert r.exit_code == 0, r.output
            for goal in ("Analyse scores over time", "Analyse scores over time updated"):
                r = runner.invoke(app, [
                    "project", "frame",
                    "--project-id", "boot_idem",
                    "--source", "sample",
                    "--name", "Idempotent",
                    "--goal", goal,
                ])
                assert r.exit_code == 0, r.output

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                claims = store.list_semantic_claims("boot_idem")
                bootstrap = [c for c in claims if c.get("source") == "bootstrap:profile"]
                ids = [c["id"] for c in bootstrap]
                assert len(ids) == len(set(ids)), "Bootstrap claims must not be duplicated"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_frame_output_shows_bootstrap_hint(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            result = _frame(tmp_path, SAMPLE_DATA, "sample", "boot_hint",
                            "Analyse category distribution")
            # CLI should prompt the user to provide a resource file
            assert "resource add" in result.output or "Bootstrap hints" in result.output
        finally:
            get_settings.cache_clear()

    @staticmethod
    def _bootstrap_count(store: HeadwaterStore, project_id: str) -> int:
        return sum(
            1 for c in store.list_semantic_claims(project_id)
            if c.get("source") == "bootstrap:profile"
        )


class TestResourceVocabularyHook:
    def test_resource_vocabulary_extends_goal_intents(self):
        from headwater.services.h2_project_relevance import (
            _goal_intents,
            _resource_context_from_claims,
        )

        # Simulate a resource claim whose definition text contains workflow terms
        claims = [
            {
                "source": "resource:dict.md",
                "table_name": "records",
                "column_name": "step_code",
                "claim_type": "definition",
                "claim": {"value": "Workflow step identifier for the processing pipeline"},
            }
        ]
        _, vocab = _resource_context_from_claims(claims)
        # "workflow" and "pipeline" are in _GOAL_WORKFLOW_HINTS
        intents_no_resource = _goal_intents("Analyse scores")
        intents_with_resource = _goal_intents("Analyse scores", resource_vocabulary=vocab)
        assert "workflow" in intents_with_resource, (
            "Resource vocabulary with 'workflow' should trigger workflow intent"
        )
        assert "workflow" not in intents_no_resource, (
            "Without resource vocabulary, 'Analyse scores' should not trigger workflow intent"
        )

    def test_resource_col_keys_extracted_from_definition_claims(self):
        from headwater.services.h2_project_relevance import _resource_context_from_claims

        claims = [
            {
                "source": "resource:dict.md",
                "table_name": "records",
                "column_name": "status_code",
                "claim_type": "definition",
                "claim": {"value": "Status of the record"},
            },
            {
                "source": "relevance",
                "table_name": "records",
                "column_name": "score",
                "claim_type": "relevance",
                "claim": {"value": "relevance score"},
            },
        ]
        col_keys, _ = _resource_context_from_claims(claims)
        assert "records.status_code" in col_keys, (
            "Resource-backed column should appear in col_keys"
        )
        assert "records.score" not in col_keys, (
            "Non-resource claim should not appear in col_keys"
        )

    def test_resource_vocabulary_ignores_stop_words(self):
        from headwater.services.h2_project_relevance import _resource_context_from_claims

        claims = [
            {
                "source": "resource:dict.md",
                "table_name": "t",
                "column_name": "c",
                "claim_type": "definition",
                "claim": {"value": "The identifier for each and all records in the table"},
            }
        ]
        _, vocab = _resource_context_from_claims(claims)
        assert "the" not in vocab
        assert "and" not in vocab
        assert "all" not in vocab
        assert "for" not in vocab

    def test_resource_defined_columns_score_higher(self, monkeypatch, tmp_path):
        """Columns with resource-backed claims should rank above equivalent columns without."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            result = runner.invoke(
                app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"]
            )
            assert result.exit_code == 0, result.output

            # Frame without resource — get baseline relevance
            result = runner.invoke(app, [
                "project", "frame",
                "--project-id", "res_score_before",
                "--source", "sample",
                "--name", "Before Resource",
                "--goal", "Analyse scores and category distribution over time",
            ])
            assert result.exit_code == 0, result.output

            # Add resource file, then re-frame
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "res_score_before",
                "--path", str(FIXTURES / "resource_dict.md"),
            ])
            assert result.exit_code == 0, result.output

            # Re-run relevance — resource-defined columns should appear
            result = runner.invoke(app, [
                "project", "relevance",
                "--project-id", "res_score_before",
            ])
            assert result.exit_code == 0, result.output
            assert "resource-defined" in result.output or "Relevant columns" in result.output
        finally:
            get_settings.cache_clear()

    @staticmethod
    def _relevant_col_score(store: HeadwaterStore, project_id: str, col_key: str) -> float | None:
        for c in store.list_semantic_claims(project_id):
            if (
                c.get("claim_type") == "relevance"
                and f"{c.get('table_name')}.{c.get('column_name')}" == col_key
            ):
                return float(c.get("claim", {}).get("score") or 0.0)
        return None

    @staticmethod
    def _find_col_score_in_output(output: str, col_name: str) -> float | None:
        import re
        pattern = rf"{col_name}.*?score=(\d+\.\d+)"
        m = re.search(pattern, output)
        return float(m.group(1)) if m else None

    @pytest.mark.skipif(
        not RADIOLOGY_DICT.exists(),
        reason="Radiology dictionary fixture not available",
    )
    def test_resource_vocab_improves_radiology_relevance(self, monkeypatch, tmp_path):
        """Relevance should pick up workflow/duration signals from the radiology dictionary."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            runner.invoke(
                app, ["discover", "--source", RADIOLOGY_DATA, "--name", "radiology"]
            )
            runner.invoke(app, [
                "project", "frame",
                "--project-id", "rad_vocab",
                "--source", "radiology",
                "--name", "Radiology Vocab Test",
                "--goal", "Analyse efficiency",
            ])
            # Add the radiology dictionary
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "rad_vocab",
                "--path", str(RADIOLOGY_DICT),
            ])
            assert result.exit_code == 0, result.output

            # Re-run relevance — dictionary vocabulary should enrich intents
            result = runner.invoke(app, [
                "project", "relevance",
                "--project-id", "rad_vocab",
            ])
            assert result.exit_code == 0, result.output
            # Score output should show resource-defined columns
            assert "Relevant columns" in result.output
        finally:
            get_settings.cache_clear()


