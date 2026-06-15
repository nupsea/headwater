"""The reasoning graph: a DAG of typed nodes.

Edges are implicit: node B depends on node A when B lists ``node:A`` among its
inputs. The graph topo-sorts on demand and can compute a node's descendants for
surgical re-execution.
"""

from __future__ import annotations

from headwater.reasoning.node import Node
from headwater.reasoning.types import ProjectState


class GraphCycleError(ValueError):
    """Raised when the declared inputs form a cycle (the graph must be a DAG)."""


class Graph:
    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}

    def add(self, node: Node) -> Graph:
        if not node.id:
            raise ValueError("node.id must be a non-empty string")
        if node.id in self._nodes:
            raise ValueError(f"duplicate node id: {node.id!r}")
        self._nodes[node.id] = node
        return self

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def __len__(self) -> int:
        return len(self._nodes)

    def _deps(self, node: Node, state: ProjectState) -> set[str]:
        """Upstream node ids this node depends on (present in the graph)."""
        return {
            ref[len("node:") :]
            for ref in node.inputs(state)
            if ref.startswith("node:") and ref[len("node:") :] in self._nodes
        }

    def topo_sort(self, state: ProjectState) -> list[Node]:
        """Kahn's algorithm; raises GraphCycleError on a cycle."""
        deps = {nid: self._deps(node, state) for nid, node in self._nodes.items()}
        dependents: dict[str, set[str]] = {nid: set() for nid in self._nodes}
        for nid, ds in deps.items():
            for d in ds:
                dependents[d].add(nid)
        ready = sorted(nid for nid, ds in deps.items() if not ds)
        order: list[str] = []
        remaining = {nid: set(ds) for nid, ds in deps.items()}
        while ready:
            nid = ready.pop(0)
            order.append(nid)
            for dep in sorted(dependents[nid]):
                remaining[dep].discard(nid)
                if not remaining[dep]:
                    ready.append(dep)
                    ready.sort()
        if len(order) != len(self._nodes):
            stuck = [nid for nid in self._nodes if nid not in order]
            raise GraphCycleError(f"cycle among nodes: {sorted(stuck)}")
        return [self._nodes[nid] for nid in order]

    def descendants(self, node_id: str, state: ProjectState) -> set[str]:
        """Transitive set of nodes that (in)directly depend on ``node_id``."""
        deps = {nid: self._deps(node, state) for nid, node in self._nodes.items()}
        out: set[str] = set()
        frontier = {node_id}
        while frontier:
            nxt: set[str] = set()
            for nid, ds in deps.items():
                if nid not in out and frontier & ds:
                    out.add(nid)
                    nxt.add(nid)
            frontier = nxt
        return out
