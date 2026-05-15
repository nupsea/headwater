"""Build derived runtime assets for discovery and exploration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from headwater.analyzer.catalog import index_catalog
from headwater.core.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class PipelineAssets:
    """Runtime assets derived from discovery and catalog state."""

    summary: dict[str, Any]
    graph_store: Any | None = None
    vector_store: Any | None = None


def build_graph_and_index(discovery, catalog, source_name: str) -> PipelineAssets:
    """Build Kuzu graph store and LanceDB index for the current source."""
    result: dict[str, Any] = {"graph": {}, "vector_index": 0}
    graph_store = None
    vector_store = None

    try:
        from headwater.core.graph_store import GraphStore

        settings = get_settings()
        graph_store = GraphStore(settings.graph_store_path)
        graph_store.clear()

        table_dicts = [
            {
                "name": table.name,
                "row_count": table.row_count,
                "domain": table.domain or "",
                "description": table.description or "",
            }
            for table in discovery.tables
        ]
        node_count = graph_store.load_tables(table_dicts)

        rel_dicts = [
            {
                "from_table": rel.from_table,
                "from_column": rel.from_column,
                "to_table": rel.to_table,
                "to_column": rel.to_column,
                "rel_type": rel.type,
                "confidence": rel.confidence,
                "ref_integrity": rel.referential_integrity,
            }
            for rel in discovery.relationships
        ]
        edge_count = graph_store.load_relationships(rel_dicts)

        conformed = graph_store.find_conformed_dimensions()
        stars = graph_store.find_star_schemas()
        chains = graph_store.find_chains()
        nullable_warnings = graph_store.find_nullable_fk_warnings()

        result["graph"] = {
            "nodes": node_count,
            "edges": edge_count,
            "conformed_dimensions": len(conformed),
            "star_schemas": len(stars),
            "chains": len(chains),
            "nullable_fk_warnings": len(nullable_warnings),
        }
        logger.info(
            "Graph built: %d nodes, %d edges, %d conformed dims, %d stars",
            node_count,
            edge_count,
            len(conformed),
            len(stars),
        )
    except Exception:
        logger.exception("Failed to build graph store")
        graph_store = None

    try:
        from headwater.core.vector_store import VectorStore

        settings = get_settings()
        vector_store = VectorStore(settings.vector_store_path)
        indexed = index_catalog(catalog, source_name, vector_store)
        result["vector_index"] = indexed
        logger.info("Indexed %d catalog entries in LanceDB", indexed)
    except Exception:
        logger.exception("Failed to build vector index")
        vector_store = None

    return PipelineAssets(summary=result, graph_store=graph_store, vector_store=vector_store)
