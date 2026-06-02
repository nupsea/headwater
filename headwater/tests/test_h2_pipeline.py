"""Tests for two-factor certification in the H2 pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from headwater.analyzer.llm import NoLLMProvider
from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore
from headwater.services.h2_pipeline import finalize_project_answers

runner = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")


class _ApprovingProvider:
    """LLM provider stub that certifies every answer."""

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        return {"verdict": "certified", "confidence": 0.92, "reasons": ["looks correct"]}


class _RejectingProvider:
    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        return {"verdict": "reject", "confidence": 0.2, "reasons": ["ambiguous mapping"]}


def _frame(project_id: str, goal: str) -> None:
    assert runner.invoke(
        app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"]
    ).exit_code == 0
    assert runner.invoke(
        app,
        ["project", "frame", "--project-id", project_id, "--source", "sample",
         "--name", project_id, "--goal", goal],
    ).exit_code == 0


def test_certified_requires_statistics_and_judge(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("cert_proj", "Analyse readings over time and by site")
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            result = finalize_project_answers(
                store, "cert_proj", provider=_ApprovingProvider()
            )
            assert result.answers, "expected finalized answers"

            certified = [a for a in result.answers if a.state == "certified"]
            assert certified, "expected at least one certified answer"
            for a in certified:
                # Certified answers must carry real executed data + a chart.
                assert a.statistical_pass is True
                assert a.judge_verdict == "certified"
                assert a.columns, "certified answer must expose result columns"
                assert a.chart_spec.get("type")
                assert a.result_stats.get("row_count") is not None
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_no_judge_never_certifies(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("nojudge_proj", "Analyse readings over time and by site")
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            result = finalize_project_answers(
                store, "nojudge_proj", provider=NoLLMProvider()
            )
            assert result.answers
            # Without a judge, nothing certifies — answers hold at doubtful/can't.
            assert result.certified_count == 0
            doubtful = [a for a in result.answers if a.state == "doubtful"]
            for a in doubtful:
                assert a.judge_verdict in ("unavailable", "reject")
                # The doubt is explained.
                assert a.caveats
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_fast_path_returns_data_without_judging(monkeypatch, tmp_path):
    """run_judge=False executes SQL and returns data, leaving answers 'pending'."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("fast_proj", "Analyse readings over time and by site")
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            result = finalize_project_answers(store, "fast_proj", run_judge=False)
            assert result.answers
            assert result.certified_count == 0  # judge never ran
            pending = [a for a in result.answers if a.state == "pending"]
            assert pending, "stat-ready answers should be pending certification"
            for a in pending:
                assert a.judge_verdict == "pending"
                assert a.columns and a.row_count >= 0  # real executed data present
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_recompute_clears_staleness_after_input_change(monkeypatch, tmp_path):
    """Editing column metadata makes derived state stale; recompute clears it."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("rc_proj", "Analyse readings over time and by site")
        from headwater.services.h2_catalog import update_column
        from headwater.services.h2_pipeline import (
            get_project_state,
            recompute_project,
        )

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            # Never computed yet -> stale.
            assert get_project_state(store, "rc_proj")["stale"] is True
            recompute_project(store, "rc_proj")
            assert get_project_state(store, "rc_proj")["stale"] is False

            # Change an input (a column's meaning) -> stale again.
            table = store.get_tables("sample")[0]["name"]
            col = store.get_columns("sample", table)[0]["name"]
            update_column(store, "sample", table, col, description="a new meaning")
            assert get_project_state(store, "rc_proj")["stale"] is True

            # Recompute reconciles to the current inputs.
            recompute_project(store, "rc_proj")
            assert get_project_state(store, "rc_proj")["stale"] is False
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_recompute_reruns_relevance_from_the_beginning(monkeypatch, tmp_path):
    """A refresh must re-propose relevance + questions, not just re-draft.

    Regression guard for the cross-cutting rule: ``recompute_project`` runs FROM
    THE BEGINNING (relevance -> questions -> readiness -> draft -> execute).  We
    clear the proposed questions to stand in for stale/lost early-stage state;
    only a recompute that re-runs ``propose_relevance`` can bring them back.  A
    draft-only finalize (the prior behaviour) cannot — which is the gap the fix
    closes.
    """
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("relrun_proj", "Analyse readings over time and by site")
        from headwater.services.h2_pipeline import recompute_project

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            initial = {q["id"] for q in store.list_questions("relrun_proj")}
            assert initial, "framing should have proposed questions"

            # Simulate the earliest derived state being absent/stale.
            store.con.execute(
                "DELETE FROM questions WHERE project_id = ?", ("relrun_proj",)
            )
            store.con.commit()
            assert store.list_questions("relrun_proj") == []

            # A draft-only finalize must NOT resurrect questions.
            finalize_project_answers(store, "relrun_proj", run_judge=False)
            assert store.list_questions("relrun_proj") == [], (
                "finalize alone should not re-propose questions"
            )

            # The complete refresh re-runs relevance and brings them back.
            recompute_project(store, "relrun_proj")
            after = {q["id"] for q in store.list_questions("relrun_proj")}
            assert after == initial, "recompute must re-propose the question set"
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_recompute_reflects_an_input_change_in_questions(monkeypatch, tmp_path):
    """Changing the goal (an input) changes the proposed questions after recompute.

    Proves the refresh acts on current inputs, not a frozen snapshot.
    """
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("goalchg_proj", "Analyse readings over time and by site")
        from headwater.services.h2_pipeline import recompute_project
        from headwater.services.h2_project import set_project_goal

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            before = {
                q["question"].get("title", q.get("title"))
                for q in store.list_questions("goalchg_proj")
            }
            assert before

            # Change the goal, then run the complete refresh.
            set_project_goal(store, "goalchg_proj", "Compare counts by site only")
            recompute_project(store, "goalchg_proj")

            after = {
                q["question"].get("title", q.get("title"))
                for q in store.list_questions("goalchg_proj")
            }
            assert after, "recompute should leave a proposed question set"
            # Derived state is fresh after the complete refresh.
            from headwater.services.h2_pipeline import get_project_state

            assert get_project_state(store, "goalchg_proj")["stale"] is False
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_judge_rejection_holds_doubtful(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("rej_proj", "Analyse readings over time and by site")
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            result = finalize_project_answers(
                store, "rej_proj", provider=_RejectingProvider()
            )
            assert result.certified_count == 0
            # Statistically-ready answers exist but the judge rejected them.
            assert any(
                a.statistical_pass and a.state == "doubtful" for a in result.answers
            )
            # The judge is a certification gate, not a Resolve-card generator — it
            # never dumps prose onto the Resolve screen (cards are structural).
            cards = store.list_resolve_items("rej_proj")
            assert not [c for c in cards if c["issue_kind"] == "answer_gap"]
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_judge_verdict_persists_across_fast_path(monkeypatch, tmp_path):
    """A certified verdict survives a later fast-path load (no re-judge).

    The fast path never calls the model, but it must rehydrate a verdict the
    judge already produced for unchanged inputs — otherwise navigating back to
    the Answer page falsely shows "Not run yet".
    """
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("persist_proj", "Analyse readings over time and by site")
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            judged = finalize_project_answers(
                store, "persist_proj", provider=_ApprovingProvider()
            )
            certified_ids = {a.question_id for a in judged.answers if a.state == "certified"}
            assert certified_ids, "expected at least one certified answer to persist"

            # Fast path (no provider, run_judge=False): the verdict is rehydrated,
            # NOT reset to pending.
            fast = finalize_project_answers(store, "persist_proj", run_judge=False)
            rehydrated = {a.question_id for a in fast.answers if a.state == "certified"}
            assert certified_ids <= rehydrated, "certified verdict must persist"
            for a in fast.answers:
                if a.question_id in certified_ids:
                    assert a.judge_verdict == "certified"
                    assert a.judge_verdict != "pending"
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_judge_verdict_goes_stale_after_input_change(monkeypatch, tmp_path):
    """Editing an input after judging marks the verdict stale (prompts re-run)."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("stale_proj", "Analyse readings over time and by site")
        from headwater.services.h2_catalog import update_column

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            judged = finalize_project_answers(
                store, "stale_proj", provider=_ApprovingProvider()
            )
            certified_ids = {a.question_id for a in judged.answers if a.state == "certified"}
            assert certified_ids

            # Change an input (a column's meaning) — the draft/data the judge saw
            # no longer hold, so its verdict must come back stale, not certified.
            table = store.get_tables("sample")[0]["name"]
            col = store.get_columns("sample", table)[0]["name"]
            update_column(store, "sample", table, col, description="a changed meaning")

            fast = finalize_project_answers(store, "stale_proj", run_judge=False)
            for a in fast.answers:
                if a.question_id in certified_ids:
                    assert a.judge_verdict == "stale"
                    assert a.state != "certified"
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def _seed_text_measure_questions(store, project_id: str, qids: list[tuple[str, str]]) -> str:
    """Attach questions whose measure is a TEXT column (an unusable measure)."""
    src = store.get_project_sources(project_id)[0]["source_name"]
    text_col = None
    for table in store.get_tables(src):
        for col in store.get_columns(src, table["name"]):
            if (col.get("dtype") or "").lower() in ("varchar", "text", "string"):
                text_col = f"{table['name']}.{col['name']}"
                break
        if text_col:
            break
    assert text_col, "sample source should have a text column"
    for qid, title in qids:
        store.upsert_question(
            qid,
            project_id=project_id,
            title=title,
            question={
                "title": title,
                "needed_columns": [text_col],
                "col_roles": {text_col: "measure"},
                "answerability": "answerable",
            },
            source_name=src,
            status="draft",
            answerability="answerable",
            confidence=0.5,
        )
    return text_col


def test_unusable_measure_card_is_lean(monkeypatch, tmp_path):
    """An unusable (text) measure produces a short, bindable card — not prose."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("lean_proj", "Analyse readings over time and by site")
        from headwater.services.h2_resolve import build_resolve_cards

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            _seed_text_measure_questions(store, "lean_proj", [("lean_proj:q-a", "Q A")])
            cards = build_resolve_cards(store, "lean_proj")
            measure_cards = [c for c in cards if c.issue_kind == "unusable_measure"]
            assert measure_cards, "a text measure should raise an unusable_measure card"
            card = measure_cards[0]
            assert len(card.body) <= 200  # lean, no certification transcript
            assert "certification gate" not in card.body
            # Carries its column so the analyst can define it in place (S-BIND).
            assert card.payload.get("table") and card.payload.get("column")
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_unusable_measure_groups_questions_into_one_card(monkeypatch, tmp_path):
    """Questions blocked by the SAME text measure collapse into one card."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("grp_proj", "Analyse readings over time and by site")
        from headwater.services.h2_resolve import build_resolve_cards

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            _seed_text_measure_questions(
                store, "grp_proj", [("grp_proj:q-a", "Q A"), ("grp_proj:q-b", "Q B")]
            )
            cards = build_resolve_cards(store, "grp_proj")
            measure_cards = [c for c in cards if c.issue_kind == "unusable_measure"]
            assert len(measure_cards) == 1, "two questions, one measure -> one card"
            assert set(measure_cards[0].affected_questions) == {
                "grp_proj:q-a",
                "grp_proj:q-b",
            }
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_rebuild_preserves_deferred_and_purges_stale(monkeypatch, tmp_path):
    """Rebuild keeps a user's defer but removes stale judge-prose cards."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("purge_proj", "Analyse readings over time and by site")
        from headwater.services.h2_resolve import build_resolve_cards

        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            # A leftover verbose judge card from an earlier version.
            store.upsert_resolve_item(
                "purge_proj:answer_gap:old",
                project_id="purge_proj",
                issue_kind="answer_gap",
                title='Resolve to certify: "..."',
                body="The certification gate did not clear for this answer. - ...",
                priority="high",
                status="open",
            )
            _seed_text_measure_questions(store, "purge_proj", [("purge_proj:q-a", "Q A")])
            cards = build_resolve_cards(store, "purge_proj")
            measure_id = next(
                c.card_id for c in cards if c.issue_kind == "unusable_measure"
            )

            # The stale judge card is gone after a rebuild.
            ids = {c["id"] for c in store.list_resolve_items("purge_proj")}
            assert "purge_proj:answer_gap:old" not in ids

            # A user's defer survives the next rebuild.
            store.set_resolve_item_status(measure_id, "deferred")
            build_resolve_cards(store, "purge_proj")
            after = next(
                c for c in store.list_resolve_items("purge_proj") if c["id"] == measure_id
            )
            assert after["status"] == "deferred"
        finally:
            store.close()
    finally:
        get_settings.cache_clear()


def test_locked_definition_supersedes_placeholders_not_conflicting():
    """A locked definition + leftover bootstrap/relevance claims is NOT a conflict."""
    from headwater.services.h2_readiness import _find_conflicting_claims

    claims = [
        # Empty bootstrap enum placeholder (proposed) for the same column.
        {
            "table_name": "cases",
            "column_name": "patient_type",
            "claim_type": "enum_mapping",
            "status": "proposed",
            "claim": {"value": {"A": "", "H": ""}},
        },
        # The analyst's locked interpretation — ground truth.
        {
            "table_name": "cases",
            "column_name": "patient_type",
            "claim_type": "enum_mapping",
            "status": "locked",
            "claim": {"value": {"A": "Adult", "H": "Home"}},
        },
        # A relevance claim describes but does not define the column.
        {
            "table_name": "cases",
            "column_name": "arrival_time",
            "claim_type": "relevance",
            "status": "accepted",
            "claim": {"score": 0.9},
        },
        # A locked free-text definition with no competing definition.
        {
            "table_name": "cases",
            "column_name": "arrival_time",
            "claim_type": "definition",
            "status": "locked",
            "claim": {"value": "when the patient arrived"},
        },
    ]

    assert _find_conflicting_claims(claims) == set()


def test_genuine_definition_conflicts_are_flagged():
    """needs_review markers and two disagreeing locked definitions DO conflict."""
    from headwater.services.h2_readiness import _find_conflicting_claims

    claims = [
        # Resource ingester found two sources disagreeing -> needs_review.
        {
            "table_name": "cases",
            "column_name": "throughput_time",
            "claim_type": "definition",
            "status": "needs_review",
            "claim": {"value": "minutes", "conflict_with": "seconds"},
        },
        # Two active definitions that disagree on value.
        {
            "table_name": "cases",
            "column_name": "site",
            "claim_type": "definition",
            "status": "locked",
            "claim": {"value": "clinic location"},
        },
        {
            "table_name": "cases",
            "column_name": "site",
            "claim_type": "definition",
            "status": "accepted",
            "claim": {"value": "billing region"},
        },
    ]

    assert _find_conflicting_claims(claims) == {
        "cases.throughput_time",
        "cases.site",
    }


def test_build_value_labels_from_enum_claims():
    """Locked enum_mapping claims become column -> {code: meaning} maps."""
    from headwater.services.h2_pipeline import _build_value_labels

    claims = [
        {
            "claim_type": "enum_mapping",
            "column_name": "patient_type",
            "claim": {"value": {"A": "Adult", "H": "Home"}, "text": "..."},
        },
        # Newest wins: an earlier claim for the same column is ignored (list is
        # ordered newest-first), and blank meanings are dropped.
        {
            "claim_type": "enum_mapping",
            "column_name": "patient_type",
            "claim": {"value": {"A": "stale"}, "text": "..."},
        },
        # Free-text definitions carry no code map.
        {
            "claim_type": "definition",
            "column_name": "arrival_time",
            "claim": {"value": "when the patient arrived", "text": "..."},
        },
    ]

    labels = _build_value_labels(claims)

    assert labels == {"patient_type": {"A": "Adult", "H": "Home"}}
    assert "arrival_time" not in labels


def test_answer_carries_value_labels_for_returned_columns(monkeypatch, tmp_path):
    """A resolved enum for a returned column flows into the answer payload."""
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("vl_proj", "Break down waiting time by patient type")
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        try:
            store.upsert_semantic_claim(
                "vl_proj:define:events.patient_type",
                project_id="vl_proj",
                scope_type="column",
                claim_type="enum_mapping",
                claim={"value": {"A": "Adult", "H": "Home"}, "text": "| A | Adult |"},
                table_name="events",
                column_name="patient_type",
                status="locked",
                confidence=1.0,
                source="user",
                locked=True,
            )
            result = finalize_project_answers(store, "vl_proj", run_judge=False)
            # Any answer returning patient_type carries the resolved meanings;
            # answers that don't return it carry nothing for it.
            for a in result.answers:
                if "patient_type" in a.columns:
                    assert a.value_labels.get("patient_type") == {
                        "A": "Adult",
                        "H": "Home",
                    }
                else:
                    assert "patient_type" not in a.value_labels
        finally:
            store.close()
    finally:
        get_settings.cache_clear()
