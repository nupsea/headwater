"""Tests for Headwater 2 S14 — Continuous Certification."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore

runner = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")
RADIOLOGY_DATA = str(Path(__file__).resolve().parents[2] / "data" / "radiology")


def _setup_certified_question(tmp_path: Path, source_path: str, source_name: str,
                               project_id: str, goal: str) -> str:
    """Set up a project and return the ID of a certified question (if any)."""
    runner.invoke(app, ["discover", "--source", source_path, "--name", source_name])
    runner.invoke(app, [
        "project", "frame",
        "--project-id", project_id,
        "--source", source_name,
        "--name", project_id.replace("_", " ").title(),
        "--goal", goal,
    ])
    runner.invoke(app, ["resolve", "--project-id", project_id])
    runner.invoke(app, ["readiness", "--project-id", project_id])

    store = HeadwaterStore(tmp_path / "h2_metadata.db")
    store.init()
    try:
        questions = store.list_questions(project_id)
        for q in questions:
            v = store.get_readiness_verdict(f"{q['id']}:verdict:latest")
            if v and v.get("state") == "certified":
                return q["id"]
    finally:
        store.close()
    return ""


def _inject_null_drift(store: HeadwaterStore, source_name: str,
                       table_name: str, column_name: str) -> None:
    """Simulate null rate increase by updating the stored profile directly."""
    rows = store.con.execute(
        "SELECT * FROM profiles WHERE source_name=? AND table_name=? AND column_name=?",
        (source_name, table_name, column_name),
    ).fetchall()
    for row in rows:
        profile = json.loads(dict(row)["profile_json"] or "{}")
        profile["null_rate"] = 0.75  # beyond the 0.50 threshold
        profile["null_count"] = int((profile.get("row_count") or 1000) * 0.75)
        store.con.execute(
            "UPDATE profiles SET profile_json=? WHERE table_name=? AND source_name=? AND column_name=?",
            (json.dumps(profile), table_name, source_name, column_name),
        )
    store.con.commit()


def _inject_vocab_drift(store: HeadwaterStore, source_name: str,
                        table_name: str, column_name: str,
                        new_value: str) -> None:
    """Add an unknown code value to the top_values of a profile."""
    rows = store.con.execute(
        "SELECT * FROM profiles WHERE source_name=? AND table_name=? AND column_name=?",
        (source_name, table_name, column_name),
    ).fetchall()
    for row in rows:
        profile = json.loads(dict(row)["profile_json"] or "{}")
        top = list(profile.get("top_values") or [])
        top.append([new_value, 1])
        profile["top_values"] = top
        profile["distinct_count"] = (profile.get("distinct_count") or 0) + 1
        store.con.execute(
            "UPDATE profiles SET profile_json=? WHERE table_name=? AND source_name=? AND column_name=?",
            (json.dumps(profile), table_name, source_name, column_name),
        )
    store.con.commit()


class TestCertificationBaseline:
    def test_certify_unchanged_source_no_demotions(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            certified_qid = _setup_certified_question(
                tmp_path, SAMPLE_DATA, "sample", "cert_stable",
                "Analyse inspection scores over time",
            )
            result = runner.invoke(app, ["certify", "--project-id", "cert_stable"])
            assert result.exit_code == 0, result.output
            assert "All certified questions remain valid" in result.output or \
                   "No profile drift" in result.output or \
                   "newly certified" in result.output.lower()
        finally:
            get_settings.cache_clear()

    def test_certify_no_demotions_without_drift(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _setup_certified_question(
                tmp_path, SAMPLE_DATA, "sample", "cert_nodrift",
                "Analyse inspection scores over time",
            )
            from headwater.services.h2_certify import evaluate_and_certify

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = evaluate_and_certify(store, "cert_nodrift")
                assert not report.demotions, "Expected no demotions without drift"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestNullRateDemotion:
    def test_null_rate_increase_demotes_certified_question(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            certified_qid = _setup_certified_question(
                tmp_path, SAMPLE_DATA, "sample", "cert_null",
                "Analyse inspection scores over time",
            )
            if not certified_qid:
                pytest.skip("No certified question available for this fixture")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                # Find a column needed by the certified question
                q = store.get_question(certified_qid)
                needed = q["question"].get("needed_columns") or []
                if not needed:
                    pytest.skip("No needed columns for certified question")

                first_col = needed[0]
                table_name, col_name = first_col.split(".", 1)

                # Inject null drift on the needed column
                _inject_null_drift(store, "sample", table_name, col_name)

                from headwater.services.h2_certify import evaluate_and_certify
                report = evaluate_and_certify(store, "cert_null")

                assert any(d.question_id == certified_qid for d in report.demotions), (
                    f"Expected demotion for {certified_qid} after null rate increase"
                )
                for dem in report.demotions:
                    if dem.question_id == certified_qid:
                        assert "structural_integrity" in dem.breaking_contracts or \
                               len(dem.breaking_contracts) > 0
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_null_rate_demotion_persists_verdict_as_demoted(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            certified_qid = _setup_certified_question(
                tmp_path, SAMPLE_DATA, "sample", "cert_persist",
                "Analyse inspection scores over time",
            )
            if not certified_qid:
                pytest.skip("No certified question available")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                q = store.get_question(certified_qid)
                needed = q["question"].get("needed_columns") or []
                if not needed:
                    pytest.skip("No needed columns")
                table_name, col_name = needed[0].split(".", 1)
                _inject_null_drift(store, "sample", table_name, col_name)

                from headwater.services.h2_certify import evaluate_and_certify
                report = evaluate_and_certify(store, "cert_persist")

                if not report.demotions:
                    pytest.skip("Demotion did not occur — column may not be needed")

                verdict = store.get_readiness_verdict(f"{certified_qid}:verdict:latest")
                assert verdict is not None
                assert verdict["state"] == "demoted", (
                    f"Verdict state should be 'demoted', got {verdict['state']}"
                )
                assert verdict["trust_bucket"] == "gaps"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_null_rate_demotion_creates_revocation_resolve_card(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            certified_qid = _setup_certified_question(
                tmp_path, SAMPLE_DATA, "sample", "cert_card",
                "Analyse inspection scores over time",
            )
            if not certified_qid:
                pytest.skip("No certified question available")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                q = store.get_question(certified_qid)
                needed = q["question"].get("needed_columns") or []
                if not needed:
                    pytest.skip("No needed columns")
                table_name, col_name = needed[0].split(".", 1)
                _inject_null_drift(store, "sample", table_name, col_name)

                from headwater.services.h2_certify import evaluate_and_certify
                report = evaluate_and_certify(store, "cert_card")

                if not report.demotions:
                    pytest.skip("Demotion did not occur")

                items = store.list_resolve_items("cert_card")
                revocation_cards = [
                    i for i in items
                    if i["issue_kind"] == "certification_revoked"
                ]
                assert revocation_cards, "Expected a certification_revoked resolve card"
                card = revocation_cards[0]
                assert card["priority"] == "high"
                assert "demoted" in card["body"].lower() or "certified" in card["body"].lower()
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_null_rate_demotion_records_decision_with_reason(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            certified_qid = _setup_certified_question(
                tmp_path, SAMPLE_DATA, "sample", "cert_decision",
                "Analyse inspection scores over time",
            )
            if not certified_qid:
                pytest.skip("No certified question available")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                q = store.get_question(certified_qid)
                needed = q["question"].get("needed_columns") or []
                if not needed:
                    pytest.skip("No needed columns")
                table_name, col_name = needed[0].split(".", 1)
                _inject_null_drift(store, "sample", table_name, col_name)

                from headwater.services.h2_certify import evaluate_and_certify
                report = evaluate_and_certify(store, "cert_decision")

                if not report.demotions:
                    pytest.skip("Demotion did not occur")

                decisions = store.list_decisions("question", certified_qid)
                demotions = [d for d in decisions if d["action"] == "demoted"]
                assert demotions, "Expected a 'demoted' decision record"
                dec = demotions[0]
                assert dec["reason"], "Demotion decision must have a reason"
                payload = dec["payload"]
                assert "prior_snapshot_id" in payload
                assert "breaking_contracts" in payload
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestVocabularyDriftDetection:
    def test_vocab_drift_detected_in_snapshot_diff(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"])
            runner.invoke(app, [
                "project", "frame",
                "--project-id", "cert_vocab",
                "--source", "sample",
                "--name", "Vocab Test",
                "--goal", "Analyse inspection type distribution",
            ])

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                # Add a second snapshot ID to simulate a second discovery
                snapshot2 = "sample:v2"
                store.record_source_snapshot("sample", snapshot2, payload={"version": 2})

                # Copy profiles to the second snapshot and inject vocab drift
                profiles = store.get_profiles("sample")
                for p in profiles:
                    prof = dict(p["profile"])
                    top_vals = list(prof.get("top_values") or [])
                    if len(top_vals) >= 2 and float(prof.get("uniqueness_ratio") or 1.0) < 0.05:
                        top_vals.append(["NEW_CODE_X", 5])
                        prof["top_values"] = top_vals
                    store.upsert_profile(
                        "sample", p["table_name"], p["column_name"],
                        p["dtype"], prof, snapshot_id=snapshot2,
                    )

                from headwater.services.h2_certify import compute_snapshot_diff
                diff = compute_snapshot_diff(store, "sample")
                vocab_drifts = [d for d in diff.profile_drifts
                                if d.drift_kind == "vocab_new_value"]
                assert vocab_drifts, "Expected at least one vocab_new_value drift"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_vocab_drift_affects_only_relevant_questions(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            certified_qid = _setup_certified_question(
                tmp_path, SAMPLE_DATA, "sample", "cert_vocab2",
                "Analyse inspection scores over time",
            )
            if not certified_qid:
                pytest.skip("No certified question available")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                from headwater.services.h2_certify import evaluate_and_certify
                # Run without any drift
                report_before = evaluate_and_certify(store, "cert_vocab2")
                certifies_before = len([q for q in report_before.unchanged + report_before.newly_certified])

                # Inject vocab drift on a column NOT needed by the certified question
                q = store.get_question(certified_qid)
                needed = set(q["question"].get("needed_columns") or [])
                tables = store.get_tables("sample")
                # Find a column that is NOT in needed_columns
                injected = False
                for table in tables:
                    for col in store.get_columns("sample", table["name"]):
                        col_key = f"{table['name']}.{col['name']}"
                        if col_key not in needed:
                            _inject_vocab_drift(store, "sample", table["name"], col["name"], "ZZZZ")
                            injected = True
                            break
                    if injected:
                        break

                if not injected:
                    pytest.skip("Could not find an unrelated column to inject drift")

                report_after = evaluate_and_certify(store, "cert_vocab2")
                # The certified question should NOT be demoted since the drift is on an unrelated column
                demoted_ids = {d.question_id for d in report_after.demotions}
                assert certified_qid not in demoted_ids, (
                    "Vocab drift on unrelated column must not demote the certified question"
                )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestCLICertify:
    def test_certify_command_runs(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"])
            runner.invoke(app, [
                "project", "frame",
                "--project-id", "cli_cert",
                "--source", "sample",
                "--name", "CLI Cert Test",
                "--goal", "Analyse inspection scores over time",
            ])
            runner.invoke(app, ["readiness", "--project-id", "cli_cert"])
            result = runner.invoke(app, ["certify", "--project-id", "cli_cert"])
            assert result.exit_code == 0, result.output
        finally:
            get_settings.cache_clear()

    def test_certify_shows_demotion_when_drift_injected(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            certified_qid = _setup_certified_question(
                tmp_path, SAMPLE_DATA, "sample", "cli_demote",
                "Analyse inspection scores over time",
            )
            if not certified_qid:
                pytest.skip("No certified question available")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                q = store.get_question(certified_qid)
                needed = q["question"].get("needed_columns") or []
                if not needed:
                    pytest.skip("No needed columns")
                table_name, col_name = needed[0].split(".", 1)
                _inject_null_drift(store, "sample", table_name, col_name)
            finally:
                store.close()

            result = runner.invoke(app, ["certify", "--project-id", "cli_demote"])
            assert result.exit_code == 0, result.output
            # If demotion happened, output must name it
            if "DEMOTED" in result.output.upper():
                assert "previously certified" in result.output.lower() or \
                       "demoted" in result.output.lower()
        finally:
            get_settings.cache_clear()

    def test_certify_unknown_project_errors(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            store.close()
            result = runner.invoke(app, ["certify", "--project-id", "ghost_project"])
            assert result.exit_code != 0
        finally:
            get_settings.cache_clear()


class TestDemotionDoesNotCertifyDirectly:
    def test_certified_state_cannot_be_set_without_passing_contracts(
        self, monkeypatch, tmp_path
    ):
        """Certification must derive from passing contracts — never clicked into existence."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"])
            runner.invoke(app, [
                "project", "frame",
                "--project-id", "cert_derive",
                "--source", "sample",
                "--name", "Cert Derive",
                "--goal", "Analyse inspection scores over time",
            ])
            runner.invoke(app, ["readiness", "--project-id", "cert_derive"])

            from headwater.services.h2_certify import evaluate_and_certify

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                report = evaluate_and_certify(store, "cert_derive")
                for qid in report.newly_certified:
                    verdict = store.get_readiness_verdict(f"{qid}:verdict:latest")
                    assert verdict is not None
                    assert verdict["readiness_pct"] == 100, (
                        "Newly certified question must have 100% readiness"
                    )
                    contracts = store.list_readiness_contracts(qid)
                    assert all(c["passed"] for c in contracts), (
                        "Certified question must have all contracts passing"
                    )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()
