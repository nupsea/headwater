"""KnowledgeProjection interface + graph data types.

The single seam between the engine and any graph store. Nothing in the engine
talks to a backend directly — it talks to this Protocol. The default backend
(``SQLiteGraphBackend``, added in P2) is plain nodes/edges tables inside the
existing SQLite metadata DB; DuckPGQ/Kuzu remain optional behind this interface.

This module deliberately has NO dependency on ``reasoning`` so the import edge is
one-directional (reasoning -> knowledge).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from headwater.core.config import HeadwaterSettings
    from headwater.core.store import HeadwaterStore


@dataclass(frozen=True, slots=True)
class GraphNode:
    """A vertex: a column, a concept assignment, a question, an insight."""

    id: str
    type: str
    props: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GraphEdge:
    """A typed relation between two nodes, e.g. MEASURES / SEGMENTS / DERIVED_FROM."""

    src: str
    rel: str
    dst: str
    props: dict[str, Any] = field(default_factory=dict)


GraphFact = GraphNode | GraphEdge


@dataclass(frozen=True, slots=True)
class Path:
    """A traversal result: an ordered chain of nodes connected by edges."""

    nodes: tuple[str, ...]
    edges: tuple[GraphEdge, ...]

    @property
    def hops(self) -> int:
        return len(self.edges)


@dataclass(frozen=True, slots=True)
class Match:
    """A (measure x dimension) pairing with the join path that connects them."""

    measure: str
    dimension: str
    join_path: Path | None
    score: float


@runtime_checkable
class KnowledgeProjection(Protocol):
    """Derived, droppable semantic projection. SQLite stays the system of record."""

    def apply(self, facts: list[GraphFact]) -> None: ...

    def upsert_node(self, n: GraphNode) -> None: ...

    def upsert_edge(self, e: GraphEdge) -> None: ...

    def neighbors(self, node_id: str, rel: str | None = None) -> list[GraphNode]: ...

    def nodes_of_type(self, *types: str) -> list[GraphNode]: ...

    def paths(self, src: str, dst: str, *, max_hops: int = 3) -> list[Path]: ...

    def match_measure_dimension(
        self, *, measure_kinds: set[str], dim_kinds: set[str], max_hops: int = 2
    ) -> list[Match]: ...

    def drop_and_rebuild(self) -> None: ...


class NullProjection:
    """No-op projection used until a real backend is wired (P2).

    Lets the reasoning runner execute end to end (facts are counted by the runner
    itself) without yet persisting a graph. Replaced by ``SQLiteGraphBackend``.
    """

    def apply(self, facts: list[GraphFact]) -> None:
        return None

    def upsert_node(self, n: GraphNode) -> None:
        return None

    def upsert_edge(self, e: GraphEdge) -> None:
        return None

    def neighbors(self, node_id: str, rel: str | None = None) -> list[GraphNode]:
        return []

    def nodes_of_type(self, *types: str) -> list[GraphNode]:
        return []

    def paths(self, src: str, dst: str, *, max_hops: int = 3) -> list[Path]:
        return []

    def match_measure_dimension(
        self, *, measure_kinds: set[str], dim_kinds: set[str], max_hops: int = 2
    ) -> list[Match]:
        return []

    def drop_and_rebuild(self) -> None:
        return None


def make_projection(
    settings: HeadwaterSettings, store: HeadwaterStore | None = None
) -> KnowledgeProjection:
    """Return the configured backend.

    ``sqlite`` (default) returns the persistent ``SQLiteGraphBackend`` when a store
    is given, else a ``NullProjection`` (e.g. unit contexts with no graph). duckpgq
    and kuzu stay ``NotImplementedError`` until benchmarked into existence.
    """
    backend = getattr(settings, "knowledge_backend", "sqlite")
    if backend in ("duckpgq", "kuzu"):
        raise NotImplementedError(
            f"knowledge_backend={backend!r} is not implemented yet; use 'sqlite'."
        )
    if store is None:
        return NullProjection()
    from headwater.knowledge.sqlite_backend import SQLiteGraphBackend

    return SQLiteGraphBackend(store)
