"""NodeRunner — the incremental executor.

Surgical recompute falls out of one rule: a node runs iff there is no cached
output for its *current* input hash. Because a node's input hash includes its
upstream nodes' outputs (resolved from state), an upstream change propagates
downstream automatically, while untouched branches hit the cache and are skipped.

Two-speed: M/L (slow) nodes only execute when ``ctx.run_slow`` is set or the node
is explicitly allowed in the fast lane; otherwise their last cached output stands.
"""

from __future__ import annotations

import time

from headwater.knowledge.projection import KnowledgeProjection
from headwater.reasoning.cache import NodeCache
from headwater.reasoning.graph import Graph
from headwater.reasoning.ledger import ProvenanceLedger
from headwater.reasoning.node import Node
from headwater.reasoning.types import (
    InputRef,
    NodeCost,
    NodeCtx,
    NodeResult,
    ProjectState,
    RunReport,
)


class NodeRunner:
    def __init__(
        self,
        cache: NodeCache,
        projection: KnowledgeProjection,
        ledger: ProvenanceLedger,
    ) -> None:
        self._cache = cache
        self._projection = projection
        self._ledger = ledger

    def run(
        self,
        graph: Graph,
        state: ProjectState,
        ctx: NodeCtx,
        *,
        changed: set[InputRef] | None = None,
        slow_allow: set[str] | None = None,
    ) -> RunReport:
        changed = changed or set()
        slow_allow = slow_allow or set()
        report = RunReport()

        for node in graph.topo_sort(state):
            ihash = node.input_hash(state)
            forced = bool(set(node.inputs(state)) & changed)
            cached = None if forced else self._cache.get(node.id, ihash)

            if cached is not None:
                state.adopt(node.id, cached)
                report.skipped.append(node.id)
                continue

            if self._slow_gated(node, ctx, slow_allow):
                # Cannot run a slow node now; keep whatever last stood (none here).
                report.skipped.append(node.id)
                continue

            result = self._execute(node, state, ctx)
            self._cache.put(node.id, ihash, result.output)
            if result.facts:
                self._projection.apply(result.facts)
                report.facts_written += len(result.facts)
            if result.provenance is not None:
                self._ledger.record(result.provenance)
            state.adopt(node.id, result.output)
            report.ran.append(node.id)

        return report

    @staticmethod
    def _slow_gated(node: Node, ctx: NodeCtx, slow_allow: set[str]) -> bool:
        return node.lane in ("M", "L") and not ctx.run_slow and node.id not in slow_allow

    @staticmethod
    def _execute(node: Node, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        start = time.perf_counter()
        result = node.compute(state, ctx)
        if result.cost is None:
            wall_ms = int((time.perf_counter() - start) * 1000)
            result.cost = NodeCost(lane=node.lane, wall_ms=wall_ms)
        return result
