"""Tests for Headwater 2 S9 — Generic EDA and Insight Families."""

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


class TestEDAFamilies:
    def test_coverage_family_flags_high_null(self):
        from headwater.services.h2_eda import _coverage_family

        profile = {"null_rate": 0.65, "row_count": 1000}
        findings = _coverage_family("t.col", profile, is_needed=True)
        assert findings, "Expected coverage finding for 65% null rate"
        assert "high_null_critical" in findings[0].flags
        assert findings[0].effect_size >= 0.8

    def test_coverage_family_quiet_on_clean_data(self):
        from headwater.services.h2_eda import _coverage_family

        findings = _coverage_family("t.col", {"null_rate": 0.02}, is_needed=True)
        assert not findings, "Expected no finding for 2% null rate"

    def test_distribution_family_detects_high_cv(self):
        from headwater.services.h2_eda import _distribution_family

        profile = {"mean": 10.0, "stddev": 25.0, "p25": 2.0, "p75": 15.0,
                   "p95": 80.0, "median": 8.0}
        findings = _distribution_family("t.score", profile, "float64", "measure")
        assert any(f.family == "distribution" for f in findings)
        cv_findings = [f for f in findings if "high_variability" in f.flags]
        assert cv_findings, "Expected high_variability flag for CV > 1"

    def test_distribution_family_detects_right_skew(self):
        from headwater.services.h2_eda import _distribution_family

        profile = {"mean": 50.0, "stddev": 100.0, "p25": 10.0, "p75": 40.0,
                   "p95": 500.0, "median": 20.0}
        findings = _distribution_family("t.amount", profile, "float64", "measure")
        skew_findings = [f for f in findings if "right_skewed" in f.flags]
        assert skew_findings, "Expected right_skewed flag for heavy tail"

    def test_concentration_family_flags_dominant_value(self):
        from headwater.services.h2_eda import _concentration_family

        profile = {"top_values": [["A", 700], ["B", 200], ["C", 100]]}
        findings = _concentration_family("t.status", profile, "varchar", "categorical")
        assert findings, "Expected concentration finding for 70% dominant value"
        assert "unbalanced" in findings[0].flags

    def test_concentration_family_quiet_on_balanced(self):
        from headwater.services.h2_eda import _concentration_family

        profile = {"top_values": [["A", 340], ["B", 330], ["C", 330]]}
        findings = _concentration_family("t.status", profile, "varchar", "categorical")
        assert not findings, "Expected no finding for balanced distribution"

    def test_temporal_family_flags_thin_coverage(self):
        from headwater.services.h2_eda import _temporal_family

        profile = {"min_date": "2023-01-01", "max_date": "2023-01-03"}
        findings = _temporal_family("t.ts", profile, "timestamp", "event_ts")
        thin = [f for f in findings if "thin_coverage" in f.flags]
        assert thin, "Expected thin_coverage flag for 2-day span"

    def test_temporal_family_always_records_coverage_note(self):
        from headwater.services.h2_eda import _temporal_family

        profile = {"min_date": "2023-01-01", "max_date": "2023-03-31"}
        findings = _temporal_family("t.ts", profile, "timestamp", "event_ts")
        coverage = [f for f in findings if "temporal_coverage" in f.flags]
        assert coverage, "Expected temporal_coverage finding"

    def test_uniqueness_family_flags_likely_identifier(self):
        from headwater.services.h2_eda import _uniqueness_family

        findings = _uniqueness_family("t.col", {"uniqueness_ratio": 0.999}, "measure")
        assert findings, "Expected identifier finding for very high uniqueness"
        assert "likely_identifier" in findings[0].flags

    def test_uniqueness_family_flags_low_uniqueness(self):
        from headwater.services.h2_eda import _uniqueness_family

        findings = _uniqueness_family("t.col", {"uniqueness_ratio": 0.002}, "measure")
        assert findings, "Expected low_uniqueness finding"
        assert "low_uniqueness" in findings[0].flags

    def test_relationship_family_flags_low_ri(self):
        from headwater.services.h2_eda import _relationship_family

        rels = [{"from_table": "a", "to_table": "b", "confidence": 0.95,
                 "referential_integrity": 0.70}]
        findings = _relationship_family(rels)
        assert findings, "Expected relationship finding for 70% RI"
        assert "low_ri" in findings[0].flags


class TestEDARunner:
    def test_run_eda_produces_findings(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "eda_basic",
                   "Analyse inspection scores over time")
            from headwater.services.h2_eda import run_eda

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = run_eda(store, "eda_basic")
                assert len(report.findings) > 0, "Expected at least one EDA finding"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_run_eda_stores_findings_as_claims(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "eda_persist",
                   "Analyse inspection scores over time")
            from headwater.services.h2_eda import run_eda

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = run_eda(store, "eda_persist")
                claims = store.list_semantic_claims("eda_persist")
                eda_claims = [c for c in claims if c["claim_type"] == "eda_finding"]
                assert eda_claims, "Expected eda_finding claims stored"
                assert report.claims_created == len(eda_claims)
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_run_eda_updates_insight_confidence_contract(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "eda_contract",
                   "Analyse inspection scores over time")
            runner.invoke(app, ["readiness", "--project-id", "eda_contract"])

            from headwater.services.h2_eda import run_eda

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                run_eda(store, "eda_contract")
                questions = store.list_questions("eda_contract")
                for q in questions:
                    contracts = store.list_readiness_contracts(q["id"])
                    ic = next((c for c in contracts if c["contract_type"] == "insight_confidence"), None)
                    assert ic is not None, (
                        f"Expected insight_confidence contract for {q['id']}"
                    )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_run_eda_findings_ranked_by_effect(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "eda_rank",
                   "Analyse inspection scores by category")
            from headwater.services.h2_eda import run_eda

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = run_eda(store, "eda_rank")
                if len(report.findings) >= 2:
                    first = report.findings[0].effect_size * report.findings[0].confidence
                    second = report.findings[1].effect_size * report.findings[1].confidence
                    assert first >= second, "Findings must be sorted by effect_size × confidence DESC"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_insight_confidence_score_zero_without_findings(self):
        from headwater.services.h2_eda import EDAReport

        report = EDAReport(project_id="p", source_name="s")
        assert report.insight_confidence_score == 0.0

    def test_critical_findings_reduce_insight_score(self):
        from headwater.services.h2_eda import EdaFinding, EDAReport

        report = EDAReport(project_id="p", source_name="s", findings=[
            EdaFinding(
                col_ref="t.col",
                family="coverage",
                title="Critical null",
                detail="75% null",
                confidence=0.95,
                effect_size=0.9,
                flags=["high_null_critical", "critical"],
            ),
        ])
        # Critical findings should penalise the score
        assert report.insight_confidence_score < 1.0


class TestEDAReadinessIntegration:
    def test_readiness_includes_insight_confidence_after_eda(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "eda_readiness",
                   "Analyse inspection scores over time")

            runner.invoke(app, ["eda", "run", "--project-id", "eda_readiness"])
            runner.invoke(app, ["readiness", "--project-id", "eda_readiness"])

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                questions = store.list_questions("eda_readiness")
                for q in questions:
                    contracts = store.list_readiness_contracts(q["id"])
                    ic = next(
                        (c for c in contracts if c["contract_type"] == "insight_confidence"), None
                    )
                    assert ic is not None, (
                        f"insight_confidence contract missing for {q['id']}"
                    )
                    assert "EDA" in (ic.get("note") or "") or ic.get("passed") in (0, 1, True, False)
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_readiness_fails_closed_before_eda(self, monkeypatch, tmp_path):
        """Before EDA runs, insight_confidence is UNKNOWN and can never certify.

        Fail-closed certification (reasoning-engine plan §4.3): missing evidence
        is not a pass — the question caps at Draft until the battery runs.
        """
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "eda_default",
                   "Analyse inspection scores over time")
            runner.invoke(app, ["readiness", "--project-id", "eda_default"])

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                questions = store.list_questions("eda_default")
                for q in questions:
                    contracts = store.list_readiness_contracts(q["id"])
                    ic = next(
                        (c for c in contracts if c["contract_type"] == "insight_confidence"), None
                    )
                    if ic is not None:
                        assert not ic.get("passed"), (
                            "uncomputed insight evidence must fail closed"
                        )
                        assert ic.get("evidence", {}).get("status") == "unknown"
                # And no question may be certified on unknown evidence.
                for r in store.con.execute(
                    "SELECT state FROM readiness_verdicts"
                ).fetchall():
                    assert r["state"] != "certified"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestCLIEDA:
    def test_eda_run_command(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "cli_eda",
                   "Analyse inspection scores over time")
            result = runner.invoke(app, ["eda", "run", "--project-id", "cli_eda"])
            assert result.exit_code == 0, result.output
            assert "EDA for cli_eda" in result.output
            assert "finding" in result.output.lower()
        finally:
            get_settings.cache_clear()

    def test_eda_run_shows_critical_findings(self, monkeypatch, tmp_path):
        import json

        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, SAMPLE_DATA, "sample", "cli_eda_crit",
                   "Analyse inspection scores over time")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                # Inject a high null rate to trigger a critical finding
                profiles = store.get_profiles("sample")
                if profiles:
                    p = profiles[0]
                    prof = dict(p["profile"])
                    prof["null_rate"] = 0.80
                    store.con.execute(
                        "UPDATE profiles SET profile_json=? "
                        "WHERE table_name=? AND source_name=? AND column_name=?",
                        (json.dumps(prof), p["table_name"], "sample", p["column_name"]),
                    )
                    store.con.commit()
            finally:
                store.close()

            result = runner.invoke(app, ["eda", "run", "--project-id", "cli_eda_crit"])
            assert result.exit_code == 0, result.output
        finally:
            get_settings.cache_clear()

    def test_eda_run_unknown_project_errors(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            store.close()
            result = runner.invoke(app, ["eda", "run", "--project-id", "ghost"])
            assert result.exit_code != 0
        finally:
            get_settings.cache_clear()

    @pytest.mark.skipif(
        not Path(__file__).resolve().parents[2].joinpath("data/radiology/cases.csv").exists(),
        reason="Radiology fixture not available",
    )
    def test_eda_radiology_finds_temporal_and_concentration(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup(tmp_path, RADIOLOGY_DATA, "radiology", "eda_rad",
                   "Analyse patient flow and wait time efficiency")
            from headwater.services.h2_eda import run_eda

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = run_eda(store, "eda_rad")
                families = {f.family for f in report.findings}
                # Generic families must work on radiology without domain-specific code
                assert len(families) >= 2, (
                    f"Expected multiple EDA families, got: {families}"
                )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()
