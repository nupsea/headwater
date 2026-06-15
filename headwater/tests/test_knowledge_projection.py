"""PR3 — knowledge projection (SQLiteGraphBackend) + ontology classifier.

Real in-memory SQLite; exercises upsert/neighbors, bounded simple paths, and
measure x dimension matching (same-table, cross-table via a join, no-path), plus
the domain-agnostic ontology predicates.
"""

from __future__ import annotations

import pytest

from headwater.core.config import HeadwaterSettings
from headwater.core.store import HeadwaterStore
from headwater.knowledge import (
    ColumnStats,
    GraphEdge,
    GraphNode,
    SQLiteGraphBackend,
    classify_column,
    make_projection,
)


@pytest.fixture()
def backend():
    store = HeadwaterStore(":memory:")
    store.init()
    yield SQLiteGraphBackend(store)
    store.close()


def _measure(ref: str, unit: str) -> GraphNode:
    table = ref.rsplit(".", 1)[0]
    return GraphNode(f"col:{ref}", "Measure", {"ref": ref, "table": table, "unit": unit})


def _dimension(ref: str, kind: str, type_: str = "Dimension") -> GraphNode:
    table = ref.rsplit(".", 1)[0]
    return GraphNode(f"col:{ref}", type_, {"ref": ref, "table": table, "kind": kind})


def test_upsert_and_neighbors_roundtrip(backend):
    backend.apply(
        [
            _measure("events.total_duration", "duration"),
            _dimension("events.activity", "step"),
            GraphEdge("col:events.total_duration", "SEGMENTS", "col:events.activity"),
        ]
    )
    nbrs = backend.neighbors("col:events.total_duration", "SEGMENTS")
    assert [n.id for n in nbrs] == ["col:events.activity"]
    # upsert is idempotent: same id updates, never duplicates.
    backend.upsert_node(_measure("events.total_duration", "duration"))
    assert len(backend.nodes_of_type("Measure")) == 1


def test_paths_bounded_and_simple(backend):
    # A chain a-b-c-d via REFERENCES; max_hops caps reachability.
    for x, y in [("a", "b"), ("b", "c"), ("c", "d")]:
        backend.apply([GraphNode(x, "Identifier"), GraphNode(y, "Identifier")])
        backend.upsert_edge(GraphEdge(x, "REFERENCES", y))
    assert backend.paths("a", "d", max_hops=2) == []  # too far
    found = backend.paths("a", "d", max_hops=3)
    assert len(found) == 1 and found[0].hops == 3
    assert found[0].nodes == ("a", "b", "c", "d")
    # same node is a trivial zero-hop path.
    assert backend.paths("a", "a")[0].hops == 0


def test_match_same_table_is_hop_zero(backend):
    backend.apply(
        [
            _measure("events.total_duration", "duration"),
            _dimension("events.activity", "step"),
        ]
    )
    matches = backend.match_measure_dimension(measure_kinds={"duration"}, dim_kinds={"step"})
    assert len(matches) == 1
    assert matches[0].join_path is None and matches[0].score == 1.0


def test_match_cross_table_via_join(backend):
    # duration lives in events; location lives in sites; joined events.site_id->sites.id
    backend.apply(
        [
            _measure("events.total_duration", "duration"),
            GraphNode("col:events.site_id", "Identifier", {"table": "events"}),
            _dimension("sites.zone", "location", type_="Location"),
            GraphNode("col:sites.id", "Identifier", {"table": "sites"}),
            GraphEdge("col:events.site_id", "REFERENCES", "col:sites.id"),
        ]
    )
    matches = backend.match_measure_dimension(
        measure_kinds={"duration"}, dim_kinds={"location"}, max_hops=2
    )
    assert len(matches) == 1
    m = matches[0]
    assert m.measure == "col:events.total_duration"
    assert m.dimension == "col:sites.zone"
    assert m.join_path is not None and m.join_path.hops >= 1
    assert 0.0 < m.score < 1.0


def test_match_returns_empty_when_no_join(backend):
    backend.apply(
        [
            _measure("events.total_duration", "duration"),
            _dimension("sites.zone", "location", type_="Location"),
        ]
    )  # no REFERENCES edge between the tables
    assert (
        backend.match_measure_dimension(
            measure_kinds={"duration"}, dim_kinds={"location"}, max_hops=2
        )
        == []
    )


def test_drop_and_rebuild_clears(backend):
    backend.apply([_measure("events.x", "duration")])
    backend.drop_and_rebuild()
    assert backend.nodes_of_type("Measure") == []


def test_make_projection_sqlite_with_store():
    store = HeadwaterStore(":memory:")
    store.init()
    proj = make_projection(HeadwaterSettings(knowledge_backend="sqlite"), store)
    assert isinstance(proj, SQLiteGraphBackend)
    store.close()


def test_make_projection_unimplemented_backend_raises():
    with pytest.raises(NotImplementedError):
        make_projection(HeadwaterSettings(knowledge_backend="kuzu"))


# ── ontology classifier (domain-agnostic) ─────────────────────────────────────


def _stats(ref, dtype, distinct, total=1000, **kw):
    return ColumnStats(ref=ref, dtype=dtype, distinct=distinct, total=total, **kw)


def test_classifier_recognises_concepts():
    assert classify_column(_stats("t.created_at", "datetime", 900)).concept == "TimeAnchor"
    assert classify_column(_stats("t.visit_id", "int", 1000, is_key=True)).concept == "Identifier"
    m = classify_column(_stats("t.total_duration", "float", 800))
    assert m.concept == "Measure" and m.props["unit"] == "duration"
    assert classify_column(_stats("t.site_zone", "string", 12)).concept == "Location"
    d = classify_column(_stats("t.activity", "string", 8))
    assert d.concept == "Dimension" and d.props["kind"] == "step"


def test_classifier_id_not_mistaken_for_measure():
    # A numeric near-unique key is an Identifier, not a Measure.
    a = classify_column(_stats("t.order_id", "int", 1000, total=1000))
    assert a.concept == "Identifier"


def test_assignment_to_node_carries_table_and_meta():
    node = classify_column(_stats("events.total_duration", "float", 800)).to_node()
    assert node.id == "col:events.total_duration"
    assert node.type == "Measure"
    assert node.props["table"] == "events" and node.props["unit"] == "duration"


def test_classifier_handles_engine_dtype_families():
    # Real engines emit int64/float64/timestamp_ns/... — match by family, not exact.
    assert classify_column(_stats("t.value", "float64", 900)).concept == "Measure"
    assert classify_column(_stats("t.qty", "int64", 500)).concept == "Measure"
    assert classify_column(_stats("t.ts", "timestamp_ns", 900)).concept == "TimeAnchor"
    # bool is not a measure.
    assert classify_column(_stats("t.is_active", "bool", 2)).concept != "Measure"
