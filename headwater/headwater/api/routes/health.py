"""Health check endpoint -- component-level status for monitoring."""

from __future__ import annotations

import time

from fastapi import APIRouter, Request

router = APIRouter()

_STARTUP_TIME: float = time.monotonic()

VERSION = "0.3.0"


@router.get("/health")
async def health_check(request: Request):
    """Return component-level health status."""
    components: dict[str, str] = {}

    # Metadata store (SQLite)
    try:
        store = request.app.state.metadata_store
        store.list_sources()
        components["metadata_store"] = "ok"
    except Exception as e:
        components["metadata_store"] = f"error: {e}"

    # Analytical engine (DuckDB)
    try:
        con = request.app.state.duckdb_con
        con.execute("SELECT 1").fetchone()
        components["analytical_engine"] = "ok"
    except Exception as e:
        components["analytical_engine"] = f"error: {e}"

    # LLM provider
    try:
        from headwater.analyzer.llm import get_provider

        provider = get_provider()
        components["llm_provider"] = "configured" if provider else "not_configured"
    except Exception:
        components["llm_provider"] = "not_configured"

    # Graph store (Kuzu) -- optional
    try:
        graph_store = request.app.state.pipeline.get("graph_store")
        components["graph_store"] = "ok" if graph_store else "not_initialized"
    except Exception:
        components["graph_store"] = "not_initialized"

    all_ok = all(
        v in ("ok", "configured", "not_configured", "not_initialized")
        for v in components.values()
    )

    return {
        "status": "healthy" if all_ok else "degraded",
        "components": components,
        "version": VERSION,
        "uptime_seconds": round(time.monotonic() - _STARTUP_TIME, 1),
    }
