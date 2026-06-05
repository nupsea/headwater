"""PR2 — recompute routed through the reasoning graph.

Proves the engine path is byte-parity with the legacy linear refresh, and that it
makes recompute incremental (a run with no input change skips every stage, which
the legacy path could never do). Uses the real CLI framing + sample data; no LLM
on this path (run_judge=False is fully deterministic/heuristic).
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore
from headwater.knowledge import make_projection
from headwater.reasoning import NodeCache, NodeCtx, NodeRunner, ProjectState
from headwater.reasoning.ledger import ProvenanceLedger
from headwater.reasoning.nodes import build_recompute_graph
from headwater.services.h2_pipeline import recompute_project

cli = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")


def _frame(project_id: str, goal: str) -> None:
    assert cli.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"]).exit_code == 0
    assert (
        cli.invoke(
            app,
            [
                "project",
                "frame",
                "--project-id",
                project_id,
                "--source",
                "sample",
                "--name",
                project_id,
                "--goal",
                goal,
            ],
        ).exit_code
        == 0
    )


def _open_store(tmp_path) -> HeadwaterStore:
    # The H2 CLI persists to ``h2_metadata.db`` under the data dir.
    store = HeadwaterStore(tmp_path / "h2_metadata.db")
    store.init()
    return store


def _derived_snapshot(store: HeadwaterStore, pid: str) -> dict[str, object]:
    """Observable derived state: question answerability + verdict/answer states."""
    qs = {q["id"]: q.get("answerability") for q in store.list_questions(pid)}
    verdicts = {
        r["question_id"]: (r["state"], r["readiness_pct"])
        for r in store.con.execute(
            "SELECT question_id, state, readiness_pct FROM readiness_verdicts"
        ).fetchall()
    }
    answers = {
        r["question_id"]: r["state"]
        for r in store.con.execute("SELECT question_id, state FROM answer_artifacts").fetchall()
    }
    return {"questions": qs, "verdicts": verdicts, "answers": answers}


def _counts(result: dict) -> dict:
    return {k: v for k, v in result.items() if k.endswith("_count")}


def _engine_settings():
    s = get_settings()
    s.reasoning_engine = True
    return s


def test_engine_recompute_parity_with_legacy(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("parity", "reduce patient wait time at registration")
        store = _open_store(tmp_path)

        legacy = recompute_project(store, "parity", run_judge=False)  # flag off
        legacy_state = _derived_snapshot(store, "parity")

        engine = recompute_project(store, "parity", settings=_engine_settings(), run_judge=False)
        engine_state = _derived_snapshot(store, "parity")

        assert _counts(engine) == _counts(legacy)
        assert engine_state == legacy_state
        assert legacy_state["questions"]  # the project actually produced questions
        store.close()
    finally:
        get_settings.cache_clear()


def test_engine_second_run_skips_all_stages(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("incr", "improve throughput")
        store = _open_store(tmp_path)
        settings = _engine_settings()

        # First engine run populates the node cache.
        recompute_project(store, "incr", settings=settings, run_judge=False)

        # Re-run the graph directly to inspect what executed: nothing changed, so
        # both stages must be cache hits.
        projection = make_projection(settings)
        state = ProjectState("incr", store, projection)
        ctx = NodeCtx(settings=settings, llm=None, run_slow=False)
        runner = NodeRunner(NodeCache(store), projection, ProvenanceLedger(store))
        report = runner.run(build_recompute_graph(run_judge=False), state, ctx)

        assert report.ran == []
        assert set(report.skipped) == {"relevance", "answers"}
        store.close()
    finally:
        get_settings.cache_clear()


def test_goal_edit_reexecutes_relevance(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        _frame("goaledit", "reduce wait time")
        store = _open_store(tmp_path)
        settings = _engine_settings()
        recompute_project(store, "goaledit", settings=settings, run_judge=False)

        # Edit the goal: relevance's input hash changes -> it must re-run.
        store.upsert_project(
            "goaledit",
            slug="goaledit",
            display_name="goaledit",
            goal={"statement": "grow throughput per shift"},
        )
        projection = make_projection(settings)
        state = ProjectState("goaledit", store, projection)
        ctx = NodeCtx(settings=settings, llm=None, run_slow=False)
        runner = NodeRunner(NodeCache(store), projection, ProvenanceLedger(store))
        report = runner.run(build_recompute_graph(run_judge=False), state, ctx)

        assert "relevance" in report.ran
        store.close()
    finally:
        get_settings.cache_clear()


def test_judge_run_has_distinct_answers_identity(monkeypatch, tmp_path):
    """A judge run must not be served from the fast-path cache.

    Proven by input-hash identity (no live LLM): the answers node encodes
    run_judge in its inputs, so fast and judge runs hash differently; relevance is
    judge-independent and hashes the same.
    """
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        from headwater.reasoning.nodes.recompute import AnswersNode, RelevanceNode

        _frame("judgeid", "reduce wait time")
        store = _open_store(tmp_path)
        state = ProjectState("judgeid", store, make_projection(_engine_settings()))

        assert AnswersNode(run_judge=False).input_hash(state) != AnswersNode(
            run_judge=True
        ).input_hash(state)
        assert RelevanceNode().input_hash(state) == RelevanceNode().input_hash(state)
        store.close()
    finally:
        get_settings.cache_clear()
