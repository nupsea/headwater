"""Deleting a source cascades to its catalog and any orphaned project."""

from __future__ import annotations

from headwater.core.store import HeadwaterStore


def _store(tmp_path) -> HeadwaterStore:
    s = HeadwaterStore(tmp_path / "h2_metadata.db")
    s.init()
    return s


def test_delete_source_removes_catalog_and_orphaned_project(tmp_path):
    s = _store(tmp_path)
    s.upsert_source("keep", "duckdb", "/x", None)
    s.upsert_source("drop", "duckdb", "/y", None)
    s.upsert_table("drop", "t1", schema_name=None, row_count=1)
    s.upsert_column("drop", "t1", "c1", "int")
    s.upsert_project("p_keep", slug="p_keep", display_name="Keep")
    s.upsert_project("p_drop", slug="p_drop", display_name="Drop")
    s.upsert_project_source("p_keep", "keep", selected_tables=[])
    s.upsert_project_source("p_drop", "drop", selected_tables=["t1"])
    s.upsert_question(
        "p_drop:q1",
        project_id="p_drop",
        title="q",
        question={"title": "q"},
        source_name="drop",
        status="draft",
        answerability="answerable",
        confidence=0.5,
    )

    result = s.delete_source("drop")

    assert result == {"source": "drop", "deleted_projects": ["p_drop"]}
    # Source + its catalog gone.
    assert s.get_source("drop") is None
    assert s.get_tables("drop") == []
    assert s.get_columns("drop", "t1") == []
    # The project that used only "drop" is gone, with its questions.
    assert s.get_project("p_drop") is None
    assert s.list_questions("p_drop") == []
    # The unrelated source + project are untouched.
    assert s.get_source("keep") is not None
    assert s.get_project("p_keep") is not None
    s.close()


def test_delete_source_keeps_projects_with_another_source(tmp_path):
    s = _store(tmp_path)
    s.upsert_source("a", "duckdb", "/a", None)
    s.upsert_source("b", "duckdb", "/b", None)
    s.upsert_project("multi", slug="multi", display_name="Multi")
    s.upsert_project_source("multi", "a", selected_tables=[])
    s.upsert_project_source("multi", "b", selected_tables=[])

    result = s.delete_source("a")

    assert result["deleted_projects"] == []  # still has source "b"
    assert s.get_project("multi") is not None
    s.close()
