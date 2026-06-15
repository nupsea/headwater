"""SQLite-adjacency knowledge backend (the default projection).

Plain ``graph_node`` / ``graph_edge`` tables inside the existing metadata DB, with
Python traversal. At Headwater's scale (tens of tables, hundreds of columns,
<=3-hop join paths) this is microseconds and needs zero new dependencies — the
review's "smallest durable projection." DuckPGQ/Kuzu remain optional behind the
same ``KnowledgeProjection`` interface.

The projection is derived and droppable: ``drop_and_rebuild`` clears it; the
reasoning nodes repopulate it. SQLite stays the system of record (I-1).
"""

from __future__ import annotations

import json
from collections import deque
from typing import TYPE_CHECKING

from headwater.knowledge.projection import GraphEdge, GraphFact, GraphNode, Match, Path

if TYPE_CHECKING:
    from headwater.core.store import HeadwaterStore

# Edge relations that represent a join (a traversable hop between tables).
_JOIN_RELS = {"REFERENCES"}


class SQLiteGraphBackend:
    def __init__(self, store: HeadwaterStore) -> None:
        self._store = store

    @property
    def _con(self):
        return self._store.con

    # ── writes ────────────────────────────────────────────────────────────────
    def apply(self, facts: list[GraphFact]) -> None:
        for f in facts:
            if isinstance(f, GraphNode):
                self.upsert_node(f)
            else:
                self.upsert_edge(f)

    def upsert_node(self, n: GraphNode) -> None:
        self._con.execute(
            """
            INSERT INTO graph_node (id, type, props_json, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                type = excluded.type,
                props_json = excluded.props_json,
                updated_at = datetime('now')
            """,
            (n.id, n.type, json.dumps(n.props, sort_keys=True)),
        )
        self._con.commit()

    def upsert_edge(self, e: GraphEdge) -> None:
        self._con.execute(
            """
            INSERT INTO graph_edge (src, rel, dst, props_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(src, rel, dst) DO UPDATE SET props_json = excluded.props_json
            """,
            (e.src, e.rel, e.dst, json.dumps(e.props, sort_keys=True)),
        )
        self._con.commit()

    # ── reads ─────────────────────────────────────────────────────────────────
    def _node(self, node_id: str) -> GraphNode | None:
        row = self._con.execute(
            "SELECT id, type, props_json FROM graph_node WHERE id = ?", (node_id,)
        ).fetchone()
        if row is None:
            return None
        return GraphNode(row["id"], row["type"], json.loads(row["props_json"] or "{}"))

    def nodes_of_type(self, *types: str) -> list[GraphNode]:
        if not types:
            return []
        marks = ",".join("?" * len(types))
        rows = self._con.execute(
            f"SELECT id, type, props_json FROM graph_node WHERE type IN ({marks})",
            types,
        ).fetchall()
        return [GraphNode(r["id"], r["type"], json.loads(r["props_json"] or "{}")) for r in rows]

    def neighbors(self, node_id: str, rel: str | None = None) -> list[GraphNode]:
        if rel is None:
            rows = self._con.execute(
                "SELECT dst FROM graph_edge WHERE src = ?", (node_id,)
            ).fetchall()
        else:
            rows = self._con.execute(
                "SELECT dst FROM graph_edge WHERE src = ? AND rel = ?", (node_id, rel)
            ).fetchall()
        out = [self._node(r["dst"]) for r in rows]
        return [n for n in out if n is not None]

    def _adjacency(self, rels: set[str]) -> dict[str, list[GraphEdge]]:
        """Undirected adjacency over the given relations (joins go both ways)."""
        marks = ",".join("?" * len(rels))
        rows = self._con.execute(
            f"SELECT src, rel, dst, props_json FROM graph_edge WHERE rel IN ({marks})",
            tuple(rels),
        ).fetchall()
        adj: dict[str, list[GraphEdge]] = {}
        for r in rows:
            e = GraphEdge(r["src"], r["rel"], r["dst"], json.loads(r["props_json"] or "{}"))
            adj.setdefault(e.src, []).append(e)
            adj.setdefault(e.dst, []).append(GraphEdge(e.dst, e.rel, e.src, e.props))
        return adj

    def paths(self, src: str, dst: str, *, max_hops: int = 3) -> list[Path]:
        """All simple undirected paths from src to dst within max_hops (BFS)."""
        if src == dst:
            return [Path((src,), ())]
        adj = self._adjacency(_JOIN_RELS | {"BELONGS_TO"})
        results: list[Path] = []
        queue: deque[tuple[str, tuple[str, ...], tuple[GraphEdge, ...]]] = deque(
            [(src, (src,), ())]
        )
        while queue:
            node, nodes, edges = queue.popleft()
            if len(edges) >= max_hops:
                continue
            for e in adj.get(node, ()):
                if e.dst in nodes:
                    continue  # simple path: no repeated node
                new_nodes = (*nodes, e.dst)
                new_edges = (*edges, e)
                if e.dst == dst:
                    results.append(Path(new_nodes, new_edges))
                else:
                    queue.append((e.dst, new_nodes, new_edges))
        return results

    def match_measure_dimension(
        self, *, measure_kinds: set[str], dim_kinds: set[str], max_hops: int = 2
    ) -> list[Match]:
        """Find (Measure x Dimension) pairs joined within max_hops, ranked.

        A measure matches when its ``unit`` is in ``measure_kinds``; a dimension
        matches when its ``kind`` (or its type, e.g. Location) is in ``dim_kinds``.
        Same-table pairs are hop-0; cross-table pairs need a join path between
        their tables. Score prefers fewer hops, then lower-cardinality dimensions.
        """
        measures = [
            m for m in self.nodes_of_type("Measure") if m.props.get("unit") in measure_kinds
        ]
        dims = [
            d
            for d in self.nodes_of_type("Dimension", "Location")
            if (d.props.get("kind") in dim_kinds) or (d.type.lower() in dim_kinds)
        ]
        if not measures or not dims:
            return []

        out: list[Match] = []
        for m in measures:
            m_table = m.props.get("table", "")
            for d in dims:
                d_table = d.props.get("table", "")
                if m_table and m_table == d_table:
                    out.append(Match(m.id, d.id, None, score=1.0))
                    continue
                join = self._table_join_path(m_table, d_table, max_hops)
                if join is not None:
                    score = round(1.0 / (1 + join.hops), 4)
                    out.append(Match(m.id, d.id, join, score=score))
        out.sort(key=lambda mt: (-mt.score, mt.measure, mt.dimension))
        return out

    def _table_join_path(self, a: str, b: str, max_hops: int) -> Path | None:
        """Shortest join path between two tables via REFERENCES edges, or None."""
        if not a or not b or a == b:
            return None
        # Column-level paths whose endpoints live in tables a and b.
        for src in self._cols_in_table(a):
            for dst in self._cols_in_table(b):
                found = self.paths(src, dst, max_hops=max_hops + 1)
                if found:
                    return min(found, key=lambda p: p.hops)
        return None

    def _cols_in_table(self, table: str) -> list[str]:
        rows = self._con.execute("SELECT id, props_json FROM graph_node").fetchall()
        return [r["id"] for r in rows if json.loads(r["props_json"] or "{}").get("table") == table]

    def drop_and_rebuild(self) -> None:
        self._con.execute("DELETE FROM graph_edge")
        self._con.execute("DELETE FROM graph_node")
        self._con.commit()
