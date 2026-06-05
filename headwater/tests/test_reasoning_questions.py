"""PR4 — goal-aware question generation (ontology.map -> goal.parse -> question.gen).

Hermetic traversal proofs use a synthetic ontology graph; the goal parser and the
end-to-end vertical use real CLI-framed sample data. No LLM (deterministic path).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore
from headwater.knowledge import GraphEdge, GraphNode, SQLiteGraphBackend
from headwater.reasoning import NodeCtx, ProjectState
from headwater.reasoning.nodes import parse_goal
from headwater.reasoning.nodes.question_gen import QuestionGenNode
from headwater.reasoning.nodes.vertical import run_question_vertical

cli = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")


# ── goal parser (the keyword verifier) ────────────────────────────────────────


def test_goal_parser_maps_delays_in_hours():
    intent = parse_goal("where do delays occur in hours for different visits")
    assert "duration" in intent.target_measure_kinds
    assert "location" in intent.by_dimension_kinds
    assert intent.unit == "hours"
    assert intent.comparison in ("segment", "rank")


def test_goal_parser_maps_volume_over_time_to_trend():
    intent = parse_goal("how does the number of cases change over time")
    assert "count" in intent.target_measure_kinds
    assert intent.comparison == "trend"


def test_two_goals_yield_different_intents():
    a = parse_goal("where do delays occur in hours")
    b = parse_goal("how does volume grow over time")
    assert a.to_dict() != b.to_dict()


# ── question.gen traversal over a synthetic ontology graph ─────────────────────


@pytest.fixture()
def graph_state():
    store = HeadwaterStore(":memory:")
    store.init()
    proj = SQLiteGraphBackend(store)
    # events.total_duration (Measure, duration) joined to sites.zone (Location)
    proj.apply(
        [
            GraphNode(
                "col:events.total_duration",
                "Measure",
                {"ref": "events.total_duration", "table": "events", "unit": "duration"},
            ),
            GraphNode(
                "col:events.activity",
                "Dimension",
                {"ref": "events.activity", "table": "events", "kind": "step"},
            ),
            GraphNode("col:events.day", "TimeAnchor", {"ref": "events.day", "table": "events"}),
            GraphNode("col:events.site_id", "Identifier", {"table": "events"}),
            GraphNode(
                "col:sites.zone",
                "Location",
                {"ref": "sites.zone", "table": "sites", "kind": "location"},
            ),
            GraphNode("col:sites.id", "Identifier", {"table": "sites"}),
            GraphEdge("col:events.site_id", "REFERENCES", "col:sites.id"),
        ]
    )
    state = ProjectState("p", store, proj)
    yield state
    store.close()


def _gen(state, intent: dict):
    state.adopt("goal.parse", intent)
    state.adopt("ontology.map", {"concepts": {}})
    return QuestionGenNode().compute(state, NodeCtx(settings=None))


def test_delays_in_hours_maps_to_duration_by_step_with_unit(graph_state):
    intent = parse_goal("where do delays occur in hours").to_dict()
    result = _gen(graph_state, intent)
    titles = [q["title"] for q in result.output]
    # The duration measure is ranked by a step/location dimension, carrying the unit.
    assert any("duration" in t and "hours" in t for t in titles)
    target = next(q for q in result.output if "duration" in q["title"])
    assert "events.total_duration" in target["needed_columns"]
    assert target["col_roles"]["events.total_duration"] == "measure"
    # provenance: a Question node with DERIVED_FROM edges was emitted.
    qnodes = [f for f in result.facts if getattr(f, "type", None) == "Question"]
    assert qnodes and result.provenance is not None


def test_cross_table_question_uses_join(graph_state):
    intent = parse_goal("where do delays occur by location").to_dict()
    result = _gen(graph_state, intent)
    # duration (events) x zone (sites) requires the events.site_id->sites.id join.
    target = next((q for q in result.output if "zone" in q["title"]), None)
    assert target is not None
    assert "sites.zone" in target["needed_columns"]


def test_trend_goal_produces_temporal_question(graph_state):
    intent = parse_goal("how does duration change over time").to_dict()
    result = _gen(graph_state, intent)
    assert any(q["intent"] == "trend" for q in result.output)
    trend = next(q for q in result.output if q["intent"] == "trend")
    assert trend["col_roles"].get("events.day") == "event_ts"


def test_two_goals_diverge_on_same_graph(graph_state):
    where = _gen(graph_state, parse_goal("where do delays occur in hours").to_dict())
    trend = _gen(graph_state, parse_goal("how does duration change over time").to_dict())
    assert [q["title"] for q in where.output] != [q["title"] for q in trend.output]


# ── end-to-end vertical over real sample data ─────────────────────────────────


def test_vertical_runs_on_sample_data(monkeypatch, tmp_path):
    monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        assert (
            cli.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "sample"]).exit_code
            == 0
        )
        assert (
            cli.invoke(
                app,
                [
                    "project",
                    "frame",
                    "--project-id",
                    "v",
                    "--source",
                    "sample",
                    "--name",
                    "v",
                    "--goal",
                    "which site has the most incidents",
                ],
            ).exit_code
            == 0
        )
        store = HeadwaterStore(tmp_path / "h2_metadata.db")
        store.init()
        specs = run_question_vertical(store, "v", settings=get_settings())
        # The ontology graph was populated and the goal parsed without error;
        # specs is a list (possibly empty if the schema has no matching pattern).
        assert isinstance(specs, list)
        graph = SQLiteGraphBackend(store)
        assert graph.nodes_of_type("Measure", "Dimension", "Location", "Identifier")
        store.close()
    finally:
        get_settings.cache_clear()
