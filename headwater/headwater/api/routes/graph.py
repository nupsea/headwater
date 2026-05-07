"""Graph API -- relationship graph data and pattern queries for visualization."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from headwater.api.project_scope import scoped_pipeline
from headwater.core.config import get_settings
from headwater.core.graph_store import GraphStore

router = APIRouter()
logger = logging.getLogger(__name__)


def _get_graph_store(request: Request) -> GraphStore:
    """Get or create a GraphStore for the current session."""
    # Check if graph store is already on app state
    graph = getattr(request.app.state, "graph_store", None)
    if graph is not None:
        return graph

    settings = get_settings()
    graph_path = settings.graph_store_path
    if not graph_path.exists():
        raise HTTPException(
            status_code=400,
            detail="Graph store not initialized. Run discovery first.",
        )
    return GraphStore(graph_path)


@router.get("/graph/data")
async def get_graph_data(request: Request, project_id: str | None = None):
    """Return full graph data (nodes + edges) for D3 visualization."""
    if project_id:
        discovery = scoped_pipeline(request, project_id).get("discovery")
        if not discovery:
            return {"nodes": [], "edges": []}
        nodes = [
            {
                "id": table.name,
                "label": table.name,
                "type": "table",
                "domain": table.domain,
            }
            for table in discovery.tables
        ]
        edges = [
            {
                "source": rel.from_table,
                "target": rel.to_table,
                "label": f"{rel.from_column} -> {rel.to_column}",
                "type": rel.type,
                "confidence": rel.confidence,
            }
            for rel in discovery.relationships
        ]
        return {"nodes": nodes, "edges": edges}
    graph = _get_graph_store(request)
    try:
        data = graph.get_graph_data()
    except Exception as e:
        logger.warning("Failed to read graph data: %s", e)
        return {"nodes": [], "edges": []}
    return data


@router.get("/graph/patterns")
async def get_graph_patterns(request: Request, project_id: str | None = None):
    """Return discovered graph patterns: conformed dims, star schemas, chains, warnings."""
    if project_id:
        return {
            "conformed_dimensions": [],
            "star_schemas": [],
            "chains": [],
            "nullable_warnings": [],
        }
    graph = _get_graph_store(request)
    try:
        conformed = graph.find_conformed_dimensions()
        stars = graph.find_star_schemas()
        chains = graph.find_chains()
        nullable_warnings = graph.find_nullable_fk_warnings()
    except Exception as e:
        logger.warning("Failed to compute graph patterns: %s", e)
        return {
            "conformed_dimensions": [],
            "star_schemas": [],
            "chains": [],
            "nullable_warnings": [],
        }

    return {
        "conformed_dimensions": conformed,
        "star_schemas": stars,
        "chains": chains,
        "nullable_warnings": nullable_warnings,
    }


@router.get("/graph/join-path")
async def get_join_path(
    from_table: str,
    to_table: str,
    request: Request,
):
    """Find shortest FK path between two tables."""
    graph = _get_graph_store(request)
    path = graph.get_join_path(from_table, to_table)
    if path is None:
        return {
            "from_table": from_table,
            "to_table": to_table,
            "path": None,
            "message": f"No FK path found between {from_table} and {to_table}.",
        }
    return {
        "from_table": from_table,
        "to_table": to_table,
        "path": path,
        "hop_count": len(path),
    }
