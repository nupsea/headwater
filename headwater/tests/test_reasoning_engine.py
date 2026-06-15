"""PR1 — reasoning engine core (dark). Runner orchestration, surgical recompute,
cache memoization, provenance ledger, two-speed gating, model-optional L nodes.

Real behavior, in-memory SQLite — no mocks (python-patterns).
"""

from __future__ import annotations

import pytest

from headwater.core.store import HeadwaterStore
from headwater.knowledge.projection import GraphNode, NullProjection
from headwater.reasoning import (
    BaseNode,
    Graph,
    GraphCycleError,
    LLMNode,
    NodeCache,
    NodeCtx,
    NodeResult,
    NodeRunner,
    ProjectState,
    ProvenanceLedger,
    ProvenanceRef,
)
from headwater.reasoning.node import register_resolver

# A resolver-backed input family the tests can mutate to simulate input changes.
VARS: dict[str, str] = {}
register_resolver("var", lambda state, ref: VARS.get(ref.split(":", 1)[1]))


def _state(store: HeadwaterStore) -> ProjectState:
    return ProjectState("p1", store, NullProjection())


def _runner(store: HeadwaterStore) -> NodeRunner:
    return NodeRunner(NodeCache(store), NullProjection(), ProvenanceLedger(store))


class VarNode(BaseNode):
    """Output derives from a mutable 'var:' input; records run count."""

    def __init__(self, node_id: str, var_key: str, runs: dict[str, int]) -> None:
        self.id = node_id
        self._var_key = var_key
        self._runs = runs

    def inputs(self, state):
        return [f"var:{self._var_key}"]

    def compute(self, state, ctx):
        self._runs[self.id] = self._runs.get(self.id, 0) + 1
        return NodeResult(output=f"{VARS.get(self._var_key)}:{self.id}")


class DerivedNode(BaseNode):
    """Depends on an upstream node's output (node:<id>)."""

    def __init__(self, node_id: str, upstream: str, runs: dict[str, int]) -> None:
        self.id = node_id
        self._upstream = upstream
        self._runs = runs

    def inputs(self, state):
        return [f"node:{self._upstream}"]

    def compute(self, state, ctx):
        self._runs[self.id] = self._runs.get(self.id, 0) + 1
        return NodeResult(output=f"{state.output_of(self._upstream)}->{self.id}")


@pytest.fixture()
def store():
    s = HeadwaterStore(":memory:")
    s.init()
    yield s
    s.close()


@pytest.fixture(autouse=True)
def _reset_vars():
    VARS.clear()
    VARS.update({"goal": "reduce-wait", "other": "static"})
    yield


def test_graph_topo_sort_orders_dependencies_first(store):
    runs: dict[str, int] = {}
    g = Graph()
    g.add(DerivedNode("B", "A", runs)).add(VarNode("A", "goal", runs))
    order = [n.id for n in g.topo_sort(_state(store))]
    assert order.index("A") < order.index("B")


def test_graph_cycle_raises(store):
    class Loop(BaseNode):
        def __init__(self, i, dep):
            self.id = i
            self._dep = dep

        def inputs(self, state):
            return [f"node:{self._dep}"]

        def compute(self, state, ctx):
            return NodeResult(output=None)

    g = Graph()
    g.add(Loop("X", "Y")).add(Loop("Y", "X"))
    with pytest.raises(GraphCycleError):
        g.topo_sort(_state(store))


def test_runner_runs_all_nodes_first_time(store):
    runs: dict[str, int] = {}
    g = Graph().add(VarNode("A", "goal", runs)).add(DerivedNode("B", "A", runs))
    report = _runner(store).run(g, _state(store), NodeCtx(settings=None))
    assert set(report.ran) == {"A", "B"}
    assert report.skipped == []
    assert runs == {"A": 1, "B": 1}


def test_unchanged_inputs_hit_cache_and_skip(store):
    runs: dict[str, int] = {}
    g = Graph().add(VarNode("A", "goal", runs)).add(DerivedNode("B", "A", runs))
    ctx = NodeCtx(settings=None)
    _runner(store).run(g, _state(store), ctx)
    report = _runner(store).run(g, _state(store), ctx)  # nothing changed
    assert set(report.skipped) == {"A", "B"}
    assert report.ran == []
    assert runs == {"A": 1, "B": 1}  # never re-executed


def test_surgical_recompute_only_changed_subgraph(store):
    """Editing one input re-runs that node + its descendants; siblings stay cached.

    This is the P1 keystone proof: a goal edit must NOT re-run untouched branches.
    """
    runs: dict[str, int] = {}
    g = (
        Graph()
        .add(VarNode("goal.parse", "goal", runs))
        .add(DerivedNode("question.gen", "goal.parse", runs))
        .add(VarNode("profile.stats", "other", runs))
    )
    ctx = NodeCtx(settings=None)
    _runner(store).run(g, _state(store), ctx)
    assert runs == {"goal.parse": 1, "question.gen": 1, "profile.stats": 1}

    VARS["goal"] = "grow-revenue"  # the user edits the goal
    report = _runner(store).run(g, _state(store), ctx)

    assert set(report.ran) == {"goal.parse", "question.gen"}
    assert report.skipped == ["profile.stats"]
    assert runs == {"goal.parse": 2, "question.gen": 2, "profile.stats": 1}


def test_facts_and_provenance_persisted(store):
    class FactNode(BaseNode):
        id = "ontology.map"

        def inputs(self, state):
            return ["var:goal"]

        def compute(self, state, ctx):
            fact = GraphNode(id="col:events.duration", type="Measure")
            return NodeResult(
                output={"mapped": 1},
                facts=[fact],
                provenance=ProvenanceRef(
                    produced_by=self.id,
                    input_hash="h",
                    lane="L",
                    fact_ids=(fact.id,),
                ),
            )

    g = Graph().add(FactNode())
    report = _runner(store).run(g, _state(store), NodeCtx(settings=None))
    assert report.facts_written == 1
    prov = ProvenanceLedger(store).for_fact("col:events.duration")
    assert len(prov) == 1 and prov[0]["produced_by"] == "ontology.map"


def test_slow_lane_node_skipped_unless_run_slow(store):
    runs: dict[str, int] = {}

    class JudgeNode(VarNode):
        lane = "L"

    g = Graph().add(JudgeNode("judge", "goal", runs))
    # Fast lane: the L node is gated out.
    report = _runner(store).run(g, _state(store), NodeCtx(settings=None, run_slow=False))
    assert report.skipped == ["judge"] and "judge" not in report.ran
    assert runs == {}
    # Slow lane: it runs.
    report = _runner(store).run(g, _state(store), NodeCtx(settings=None, run_slow=True))
    assert report.ran == ["judge"]


def test_forced_change_reexecutes_even_on_cache_hit(store):
    runs: dict[str, int] = {}
    g = Graph().add(VarNode("A", "goal", runs))
    ctx = NodeCtx(settings=None)
    _runner(store).run(g, _state(store), ctx)
    report = _runner(store).run(g, _state(store), ctx, changed={"var:goal"})
    assert report.ran == ["A"]
    assert runs == {"A": 2}


def test_llm_node_falls_back_without_model():
    class MapNode(LLMNode):
        id = "goal.parse"

        def inputs(self, state):
            return ["var:goal"]

        def propose(self, state, ctx):
            raise AssertionError("propose must not be called without a model")

        def verify(self, proposal, state, ctx):
            return NodeResult(output={"intent": "heuristic", "proposal": proposal})

    node = MapNode()
    out = node.compute(state=None, ctx=NodeCtx(settings=None, llm=None))
    assert out.output == {"intent": "heuristic", "proposal": {}}
