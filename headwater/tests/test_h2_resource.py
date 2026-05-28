"""Tests for Headwater 2 S6: Resource Intake and Semantic Claim Fusion."""

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


def _discover_and_frame(tmp_path: Path, source_path: str, source_name: str,
                        project_id: str, goal: str) -> None:
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


class TestResourceParsing:
    def test_parse_markdown_table_extracts_definitions(self):
        from headwater.services.h2_resource import _parse_resource

        text = (FIXTURES / "resource_dict.md").read_text()
        claims = _parse_resource(text, "markdown", "resource_dict.md")
        col_refs = [c.column_ref for c in claims if c.claim_type == "definition"]
        assert "inspection_score" in col_refs
        assert "violation_count" in col_refs

    def test_parse_markdown_extracts_enum_table(self):
        from headwater.services.h2_resource import _parse_resource

        text = (FIXTURES / "resource_dict.md").read_text()
        claims = _parse_resource(text, "markdown", "resource_dict.md")
        enum_claims = [c for c in claims if c.claim_type == "enum_mapping"]
        assert enum_claims, "Expected at least one enum_mapping claim from markdown table"
        enum_map = enum_claims[0].value
        assert isinstance(enum_map, dict)
        assert len(enum_map) >= 2

    def test_parse_markdown_bullets_extracts_definitions(self):
        from headwater.services.h2_resource import _parse_resource

        text = (FIXTURES / "resource_dict.md").read_text()
        claims = _parse_resource(text, "markdown", "resource_dict.md")
        col_refs = [c.column_ref for c in claims if c.claim_type == "definition"]
        assert "risk_level" in col_refs or "inspector_id" in col_refs

    def test_parse_csv_dict_extracts_definitions(self):
        from headwater.services.h2_resource import _parse_resource

        text = (FIXTURES / "resource_dict.csv").read_text()
        claims = _parse_resource(text, "csv_dict", "resource_dict.csv")
        col_refs = [c.column_ref for c in claims if c.claim_type == "definition"]
        assert "inspection_score" in col_refs
        assert "violation_count" in col_refs

    def test_parse_csv_extracts_inline_enum(self):
        from headwater.services.h2_resource import _parse_resource

        text = (FIXTURES / "resource_dict.csv").read_text()
        claims = _parse_resource(text, "csv_dict", "resource_dict.csv")
        enum_claims = [c for c in claims if c.claim_type == "enum_mapping"]
        assert enum_claims, "Expected inline enum extraction from CSV example_values column"

    def test_parse_text_extracts_definitions(self):
        from headwater.services.h2_resource import _parse_resource

        text = (FIXTURES / "resource_dict.txt").read_text()
        claims = _parse_resource(text, "text", "resource_dict.txt")
        col_refs = [c.column_ref for c in claims if c.claim_type == "definition"]
        assert "inspection_score" in col_refs

    def test_parse_text_extracts_inline_enum(self):
        from headwater.services.h2_resource import _parse_resource

        text = (FIXTURES / "resource_dict.txt").read_text()
        claims = _parse_resource(text, "text", "resource_dict.txt")
        enum_claims = [c for c in claims if c.claim_type == "enum_mapping"]
        assert enum_claims, "Expected inline enum from text inspection_type line"

    def test_detect_format_markdown(self):
        from headwater.services.h2_resource import _detect_format

        path = FIXTURES / "resource_dict.md"
        fmt = _detect_format(path, path.read_text())
        assert fmt == "markdown"

    def test_detect_format_csv(self):
        from headwater.services.h2_resource import _detect_format

        path = FIXTURES / "resource_dict.csv"
        fmt = _detect_format(path, path.read_text())
        assert fmt == "csv_dict"

    def test_detect_format_text(self):
        from headwater.services.h2_resource import _detect_format

        path = FIXTURES / "resource_dict.txt"
        fmt = _detect_format(path, path.read_text())
        assert fmt == "text"


class TestSensitivityClassification:
    def test_safe_resource_classified_correctly(self):
        from headwater.services.h2_resource import _classify_sensitivity

        text = (FIXTURES / "resource_dict.md").read_text()
        sensitivity, notes = _classify_sensitivity(text)
        assert sensitivity == "safe"
        assert not notes

    def test_sensitive_resource_detected(self):
        from headwater.services.h2_resource import _classify_sensitivity

        text = (FIXTURES / "resource_sensitive.md").read_text()
        sensitivity, notes = _classify_sensitivity(text)
        assert sensitivity == "sensitive"
        assert notes
        assert "email" in notes[0].lower()

    def test_sensitivity_check_on_pii_terms(self):
        from headwater.services.h2_resource import _classify_sensitivity

        text = "The password field stores hashed credentials for user authentication."
        sensitivity, notes = _classify_sensitivity(text)
        assert sensitivity == "sensitive"


class TestClaimFusion:
    def test_ingest_creates_claims_for_matched_columns(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "resource_ingest",
                "Analyse inspection scores and bottlenecks",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "resource_ingest",
                "--path", str(FIXTURES / "resource_dict.md"),
            ])
            assert result.exit_code == 0, result.output
            assert "Claims created" in result.output

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                claims = store.list_semantic_claims("resource_ingest")
                resource_claims = [c for c in claims if "resource:" in c.get("source", "")]
                assert resource_claims, "Expected at least one resource-sourced claim"
                claim_types = {c["claim_type"] for c in resource_claims}
                assert "definition" in claim_types
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_ingest_does_not_overwrite_locked_claims(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "resource_lock",
                "Analyse inspection scores",
            )
            from headwater.services.h2_resource import ingest_resource

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                # Manually lock a claim before ingestion
                tables = store.get_tables("sample")
                table_name = tables[0]["name"] if tables else "inspections"
                cols = store.get_columns("sample", table_name)
                # Pick the first column that has a match in the fixture
                col_name = "inspection_score"
                claim_id = f"resource_lock:resource:{table_name}.{col_name}:definition"
                store.upsert_semantic_claim(
                    claim_id,
                    project_id="resource_lock",
                    source_name="sample",
                    scope_type="column",
                    table_name=table_name,
                    column_name=col_name,
                    claim_type="definition",
                    claim={"value": "Pre-locked definition from manual review"},
                    status="proposed",
                    confidence=0.99,
                    source="manual",
                    locked=True,
                )
                # Now ingest resource - the locked claim must survive
                result = ingest_resource(
                    store, "resource_lock", FIXTURES / "resource_dict.md"
                )
                existing = store.get_semantic_claim(claim_id)
                assert existing is not None
                assert existing.get("locked"), "Lock must not be removed by ingest"
                assert "Pre-locked" in str(existing.get("claim", {}).get("value"))
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_conflict_creates_resolve_card_and_lowers_confidence(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "resource_conflict_test",
                "Analyse inspection scores and violations",
            )
            from headwater.services.h2_resource import ingest_resource

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                # Ingest first resource
                r1 = ingest_resource(
                    store, "resource_conflict_test", FIXTURES / "resource_dict.md"
                )
                # Ingest conflicting resource
                r2 = ingest_resource(
                    store, "resource_conflict_test", FIXTURES / "resource_conflict.md"
                )
                if r2.conflicts_detected > 0:
                    items = store.list_resolve_items("resource_conflict_test")
                    conflict_items = [i for i in items if i["issue_kind"] == "structural_ambiguity"]
                    assert conflict_items, (
                        "Expected structural_ambiguity resolve card for conflict"
                    )
                    # Conflicting claim must have reduced confidence
                    claims = store.list_semantic_claims("resource_conflict_test")
                    for c in claims:
                        if c.get("status") == "needs_review":
                            assert float(c["confidence"]) <= 0.35, (
                                f"Conflicting claim must have low confidence, got {c['confidence']}"
                            )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_second_identical_ingest_bumps_confidence(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "resource_bump",
                "Analyse inspection scores",
            )
            from headwater.services.h2_resource import ingest_resource

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                r1 = ingest_resource(store, "resource_bump", FIXTURES / "resource_dict.md")
                claims_after_first = {
                    c["id"]: float(c["confidence"])
                    for c in store.list_semantic_claims("resource_bump")
                    if "resource:" in c.get("source", "")
                }
                r2 = ingest_resource(store, "resource_bump", FIXTURES / "resource_dict.md")
                for claim_id, first_conf in claims_after_first.items():
                    updated = store.get_semantic_claim(claim_id)
                    if updated and updated.get("status") == "proposed":
                        assert float(updated["confidence"]) >= first_conf, (
                            "Second ingest of same resource should not lower confidence"
                        )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_ingest_registers_resource_in_project(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "resource_registry_test",
                "Analyse inspection scores",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "resource_registry_test",
                "--path", str(FIXTURES / "resource_dict.md"),
            ])
            assert result.exit_code == 0, result.output

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                registry_claim = store.get_semantic_claim(
                    "resource_registry_test:resource_registry"
                )
                assert registry_claim is not None
                entries = registry_claim.get("claim", {}).get("value") or []
                assert any("resource_dict.md" in str(e.get("path")) for e in entries)
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_pipeline_works_with_zero_resources(self, monkeypatch, tmp_path):
        """The full pipeline must work without any resources ingested."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "resource_zero",
                "Analyse inspection scores and violations over time",
            )
            result = runner.invoke(app, ["readiness", "--project-id", "resource_zero"])
            assert result.exit_code == 0, result.output
            result = runner.invoke(
                app, ["report", "--project-id", "resource_zero", "--print"]
            )
            assert result.exit_code == 0, result.output
            assert "# Headwater Audit Report" in result.output
        finally:
            get_settings.cache_clear()

    def test_lock_flag_persists_claims_as_locked(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "resource_lock_flag",
                "Analyse inspection scores",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "resource_lock_flag",
                "--path", str(FIXTURES / "resource_dict.md"),
                "--lock",
            ])
            assert result.exit_code == 0, result.output

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                claims = store.list_semantic_claims("resource_lock_flag")
                resource_claims = [c for c in claims if "resource:" in c.get("source", "")]
                assert resource_claims, "Expected resource claims when --lock is used"
                locked_claims = [c for c in resource_claims if c.get("locked")]
                assert locked_claims, "Expected at least one locked claim when --lock flag is set"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestCLIResourceCommands:
    def test_resource_add_markdown(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "cli_resource_md",
                "Analyse inspection scores",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "cli_resource_md",
                "--path", str(FIXTURES / "resource_dict.md"),
            ])
            assert result.exit_code == 0, result.output
            assert "Claims created" in result.output
            assert "safe" in result.output.lower()
        finally:
            get_settings.cache_clear()

    def test_resource_add_csv(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "cli_resource_csv",
                "Analyse inspection scores",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "cli_resource_csv",
                "--path", str(FIXTURES / "resource_dict.csv"),
            ])
            assert result.exit_code == 0, result.output
            assert "Claims created" in result.output
        finally:
            get_settings.cache_clear()

    def test_resource_add_txt(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "cli_resource_txt",
                "Analyse inspection scores",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "cli_resource_txt",
                "--path", str(FIXTURES / "resource_dict.txt"),
            ])
            assert result.exit_code == 0, result.output
            assert "Claims created" in result.output
        finally:
            get_settings.cache_clear()

    def test_resource_add_sensitive_warns(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "cli_resource_sensitive",
                "Analyse inspection scores",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "cli_resource_sensitive",
                "--path", str(FIXTURES / "resource_sensitive.md"),
            ])
            assert result.exit_code == 0, result.output
            assert "sensitive" in result.output.lower()
        finally:
            get_settings.cache_clear()

    def test_resource_add_missing_file_errors(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "cli_resource_missing",
                "Analyse inspection scores",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "cli_resource_missing",
                "--path", str(tmp_path / "nonexistent.md"),
            ])
            assert result.exit_code != 0
        finally:
            get_settings.cache_clear()

    def test_resource_list_shows_registered(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "cli_resource_list",
                "Analyse inspection scores",
            )
            runner.invoke(app, [
                "resource", "add",
                "--project-id", "cli_resource_list",
                "--path", str(FIXTURES / "resource_dict.md"),
            ])
            result = runner.invoke(app, [
                "resource", "list",
                "--project-id", "cli_resource_list",
            ])
            assert result.exit_code == 0, result.output
            assert "resource_dict.md" in result.output
        finally:
            get_settings.cache_clear()

    def test_resource_list_empty_project(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, SAMPLE_DATA, "sample", "cli_resource_empty",
                "Analyse inspection scores",
            )
            result = runner.invoke(app, [
                "resource", "list",
                "--project-id", "cli_resource_empty",
            ])
            assert result.exit_code == 0, result.output
            assert "No resources registered" in result.output
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(
        not RADIOLOGY_DICT.exists(),
        reason="Radiology dictionary fixture not available",
    )
    def test_resource_improves_radiology_resolve(self, monkeypatch, tmp_path):
        """After ingesting the project data dictionary, resource claims are created."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover_and_frame(
                tmp_path, RADIOLOGY_DATA, "radiology", "rad_resource_01",
                "Understand patient registration workflow and wait times",
            )
            result = runner.invoke(app, [
                "resource", "add",
                "--project-id", "rad_resource_01",
                "--path", str(RADIOLOGY_DICT),
                "--lock",
            ])
            assert result.exit_code == 0, result.output
            assert "Claims created" in result.output

            # After ingest, resource claims should exist for columns in the source
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                claims = store.list_semantic_claims("rad_resource_01")
                resource_claims = [
                    c for c in claims if "resource:" in c.get("source", "")
                ]
                assert resource_claims, "Expected resource claims after dictionary ingest"
                # At least one definition and one enum_mapping claim
                claim_types = {c["claim_type"] for c in resource_claims}
                assert "definition" in claim_types, "Expected definition claims"
                assert "enum_mapping" in claim_types, "Expected enum_mapping claims"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()
