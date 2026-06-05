"""Headwater knowledge projection (data plane).

Holds the ontology, columns, relationships, claims, questions, insights, and the
provenance edges between them, behind a ``KnowledgeProjection`` interface so the
backend (SQLite-adjacency default, DuckPGQ/Kuzu optional) is swappable.
"""

from headwater.knowledge.ontology import (
    ColumnStats,
    Concept,
    ConceptAssignment,
    Relation,
    classify_column,
)
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
from headwater.knowledge.sqlite_backend import SQLiteGraphBackend

__all__ = [
    "ColumnStats",
    "Concept",
    "ConceptAssignment",
    "GraphEdge",
    "GraphFact",
    "GraphNode",
    "KnowledgeProjection",
    "Match",
    "NullProjection",
    "Path",
    "Relation",
    "SQLiteGraphBackend",
    "classify_column",
    "make_projection",
]
