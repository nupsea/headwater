"""Warehouse (pushdown) paths through ingest, query, and the reasoning engine.

A stub connector stands in for Redshift/Postgres: list/describe/profile from
metadata, execute read-only SQL over an in-memory DuckDB. Verifies that
- ingest registers schema AND pushdown statistics (profiles),
- ad-hoc query / batch answer execution / text sampling push down,
- the reasoning-engine recompute runs end-to-end on a warehouse source.
"""

from __future__ import annotations

import duckdb
import pytest

from headwater.core.config import HeadwaterSettings
from headwater.core.store import HeadwaterStore
from headwater.services.h2_execute import (
    execute_answers,
    query_source,
    sample_text_columns,
)
from headwater.services.h2_source import ingest_tables


class StubWarehouseConnector:
    """Minimal warehouse connector: metadata + bounded pushdown over DuckDB."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._con: duckdb.DuckDBPyConnection | None = None

    def connect(self, config) -> None:
        self._con = duckdb.connect(self._db_path, read_only=True)

    def list_tables(self) -> list[str]:
        assert self._con is not None
        rows = self._con.execute("SHOW TABLES").fetchall()
        return [f"data.{r[0]}" for r in rows]

    def list_columns(self, table_name: str) -> list[dict]:
        assert self._con is not None
        bare = table_name.split(".")[-1]
        rows = self._con.execute(f'DESCRIBE "{bare}"').fetchall()
        return [
            {"name": r[0], "data_type": r[1], "is_nullable": True, "ordinal_position": i}
            for i, r in enumerate(rows)
        ]

    def profile(self, table_name: str) -> dict:
        assert self._con is not None
        bare = table_name.split(".")[-1]
        stats: dict[str, dict] = {}
        total = self._con.execute(f'SELECT COUNT(*) FROM "{bare}"').fetchone()[0]
        for col in self.list_columns(table_name):
            name = col["name"]
            # Mirror the real connector: numeric/temporal min-max on the native
            # type, varchar cast only for text-ish columns.
            numeric = any(
                k in str(col["data_type"]).lower()
                for k in ("int", "decimal", "numeric", "double", "float", "date", "time")
            )
            mm = f'MIN("{name}"), MAX("{name}")' if numeric else (
                f'MIN(CAST("{name}" AS VARCHAR)), MAX(CAST("{name}" AS VARCHAR))'
            )
            non_null, dist, lo, hi = self._con.execute(
                f'SELECT COUNT("{name}"), COUNT(DISTINCT "{name}"), {mm} FROM "{bare}"'
            ).fetchone()
            stats[name] = {
                "row_count": total,
                "count": total,
                "non_null": non_null,
                "null_count": total - non_null,
                "min": lo,
                "max": hi,
                "distinct_count": dist,
            }
        return stats

    def execute_readonly(self, sql: str):
        assert self._con is not None
        # The warehouse sees schema-qualified names ("data.t" or '"data"."t"');
        # the stub stores them bare.
        rewritten = sql.replace('"data".', "").replace("data.", "")
        return self._con.execute(rewritten).to_arrow_table()

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None


@pytest.fixture()
def warehouse(tmp_path, monkeypatch):
    """A stub warehouse source registered in a fresh store, connector patched in."""
    db = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        "CREATE TABLE subscriptions AS SELECT * FROM (VALUES "
        "(1, 'active', 9.99), (2, 'cancelled', 19.99), (3, 'active', 9.99)"
        ") t(sub_id, status, monthly_price)"
    )
    con.execute(
        "CREATE TABLE events AS SELECT * FROM (VALUES "
        "(1, 'signup'), (2, 'signup'), (2, 'cancel')"
        ") t(sub_id, event_type)"
    )
    con.close()

    connector = StubWarehouseConnector(str(db))
    import headwater.services.h2_execute as h2_execute_mod
    import headwater.services.h2_source as h2_source_mod

    monkeypatch.setattr(h2_source_mod, "get_connector", lambda _t: connector)
    monkeypatch.setattr(h2_execute_mod, "get_connector", lambda _t: connector)

    store = HeadwaterStore(tmp_path / "h2_metadata.db")
    store.init()
    store.upsert_source("wh", "redshift", None, "redshift://stub:5439/dev")
    try:
        yield store
    finally:
        store.close()


def test_warehouse_ingest_stores_pushdown_profiles(warehouse):
    result = ingest_tables(warehouse, "wh", ["data.subscriptions", "data.events"])
    assert result["failed"] == []
    assert set(result["ingested"]) == {"data.subscriptions", "data.events"}

    tables = {t["name"]: t for t in warehouse.get_tables("wh")}
    assert tables["data.subscriptions"]["row_count"] == 3

    profiles = {
        (p["table_name"], p["column_name"]): p["profile"]
        for p in warehouse.get_profiles("wh")
    }
    status = profiles[("data.subscriptions", "status")]
    assert status["distinct_count"] == 2
    assert status["null_rate"] == 0.0
    price = profiles[("data.subscriptions", "monthly_price")]
    assert price["min_value"] == 9.99 and price["max_value"] == 19.99


def test_warehouse_query_pushdown(warehouse):
    ingest_tables(warehouse, "wh", ["data.subscriptions"])
    r = query_source(warehouse, "wh", "SELECT * FROM data.subscriptions LIMIT 100")
    assert r.error is None
    assert r.row_count == 3 and "status" in r.columns


def test_warehouse_execute_answers_isolates_failures(warehouse):
    ingest_tables(warehouse, "wh", ["data.subscriptions"])
    results = execute_answers(
        warehouse,
        "wh",
        [
            ("q1", "SELECT status, COUNT(*) AS n FROM data.subscriptions GROUP BY status"),
            ("q2", "SELECT broken FROM data.nope"),
        ],
    )
    assert results["q1"].error is None and results["q1"].row_count == 2
    assert results["q2"].error  # bad SQL fails alone, not the batch


def test_warehouse_sample_text_columns_pushdown(warehouse):
    ingest_tables(warehouse, "wh", ["data.subscriptions"])
    out = sample_text_columns(
        warehouse, "wh", [("data.subscriptions", "status")], persist=False
    )
    assert set(out["data.subscriptions.status"]) == {"active", "cancelled"}


def test_verified_empty_table_fails_no_misleading():
    """A question needing a PROFILED 0-row table cannot read as answerable."""
    from headwater.services.h2_readiness import evaluate_question

    q = {
        "id": "q1",
        "title": "t",
        "answerability": "answerable",
        "question": {"needed_columns": ["data.t.a", "data.t.b"]},
    }
    r = evaluate_question(
        question=q,
        profile_map={"data.t.a": {}, "data.t.b": {}},
        high_priority_open=set(),
        conflicting_cols=set(),
        snapshot_id=None,
        eda_contract={"passed": True, "note": "ok", "evidence": {}},
        empty_tables={"data.t"},
    )
    nm = next(c for c in r.contracts if c.contract_type == "no_misleading")
    assert not nm.passed
    assert "EMPTY" in nm.note
    assert r.state == "draft"  # never certifiable on an empty table


def test_stats_refresh_flips_staleness_fingerprint(warehouse, tmp_path):
    """A new source snapshot (Refresh stats / re-ingest) must mark derived
    state stale so the recompute banner offers re-verification."""
    from headwater.services.h2_pipeline import project_input_fingerprint
    from headwater.services.h2_project import frame_project

    ingest_tables(warehouse, "wh", ["data.subscriptions"])
    settings = HeadwaterSettings(data_dir=tmp_path, llm_provider="none")
    frame_project(
        store=warehouse,
        project_id="p_fp",
        source_name="wh",
        display_name="P",
        goal_statement="Understand subscription activity over time.",
        selected_tables=["data.subscriptions"],
        settings=settings,
    )
    fp_before = project_input_fingerprint(warehouse, "p_fp")
    # Refresh stats records a new snapshot for the source.
    warehouse.record_source_snapshot("wh", "wh:refresh1", fingerprint="r1", payload={})
    fp_after = project_input_fingerprint(warehouse, "p_fp")
    assert fp_before != fp_after


def test_reasoning_engine_recompute_on_warehouse(warehouse, tmp_path):
    """Engine recompute must complete on a schema+stats (no local data) source."""
    from headwater.services.h2_pipeline import recompute_project
    from headwater.services.h2_project import frame_project, propose_relevance

    ingest_tables(warehouse, "wh", ["data.subscriptions", "data.events"])
    settings = HeadwaterSettings(
        data_dir=tmp_path,
        reasoning_engine=True,
        llm_provider="none",  # deterministic vertical; no live model in tests
    )
    frame_project(
        store=warehouse,
        project_id="p_wh",
        source_name="wh",
        display_name="Warehouse project",
        goal_statement="Understand subscription cancellations and revenue.",
        selected_tables=["data.subscriptions", "data.events"],
        settings=settings,
    )
    propose_relevance(store=warehouse, project_id="p_wh")

    result = recompute_project(warehouse, "p_wh", settings=settings)

    assert result["project_id"] == "p_wh"
    assert {
        "certified_count",
        "doubtful_count",
        "pending_count",
        "cannot_answer_count",
    } <= set(result)
    # The engine must have produced questions and verdicts (fail-closed is fine;
    # silent emptiness is not).
    assert warehouse.list_questions("p_wh")
