"""Headwater knowledge projection (data plane).

Holds the ontology, columns, relationships, claims, questions, insights, and the
provenance edges between them, behind a ``KnowledgeProjection`` interface so the
backend (SQLite-adjacency default, DuckPGQ/Kuzu optional) is swappable.
"""

from headwater.knowledge.projection import (
    GraphEdge,
    GraphFact,
    GraphNode,
    KnowledgeProjection,
    Match,
    NullProjection,
    Path,
    make_projection,
)

__all__ = [
    "GraphEdge",
    "GraphFact",
    "GraphNode",
    "KnowledgeProjection",
    "Match",
    "NullProjection",
    "Path",
    "make_projection",
]
