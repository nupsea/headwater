"""Deleting a project removes its derived state but never the shared source."""

from __future__ import annotations

from headwater.core.store import HeadwaterStore


def _store(tmp_path) -> HeadwaterStore:
    s = HeadwaterStore(tmp_path / "h2_metadata.db")
    s.init()
    return s


def test_delete_project_cascades_but_spares_source_and_siblings(tmp_path):
    s = _store(tmp_path)
    s.upsert_source("shared", "duckdb", "/x", None)
    s.upsert_table("shared", "t1", schema_name=None, row_count=1)
    s.upsert_column("shared", "t1", "c1", "int")
    for pid in ("p_drop", "p_keep"):
        s.upsert_project(pid, slug=pid, display_name=pid)
        s.upsert_project_source(pid, "shared", selected_tables=["t1"])
    s.upsert_question(
        "p_drop:q1",
        project_id="p_drop",
        title="q",
        question={"title": "q"},
        source_name="shared",
        status="draft",
        answerability="answerable",
        confidence=0.5,
    )
    s.upsert_readiness_verdict(
        "p_drop:q1:verdict:latest",
        question_id="p_drop:q1",
        state="draft",
        readiness_pct=40,
        trust_bucket="in_progress",
        summary="x",
        source_snapshot_id=None,
    )

    s.delete_project("p_drop")

    # The project and its derived rows are gone.
    assert s.get_project("p_drop") is None
    assert s.list_questions("p_drop") == []
    assert (
        s.con.execute(
            "SELECT COUNT(*) FROM readiness_verdicts WHERE question_id = 'p_drop:q1'"
        ).fetchone()[0]
        == 0
    )
    # The shared source and the sibling project are untouched.
    assert s.get_source("shared") is not None
    assert s.get_tables("shared")
    assert s.get_project("p_keep") is not None
