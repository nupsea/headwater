"""FastAPI application -- Headwater API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from headwater.api.routes import (
    confidence,
    data,
    dictionary,
    discovery,
    drift,
    execute,
    explore,
    insights,
    models,
    pipeline,
    quality,
)
from headwater.core.metadata import MetadataStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application state: DuckDB connection, metadata store, and pipeline state."""
    app.state.duckdb_con = duckdb.connect(":memory:")
    app.state.metadata_store = MetadataStore()
    app.state.metadata_store.init()
    app.state.pipeline: dict[str, Any] = {
        "discovery": None,
        "staging_models": [],
        "mart_models": [],
        "contracts": [],
        "execution_results": [],
        "quality_report": None,
    }
    yield
    app.state.duckdb_con.close()
    app.state.metadata_store.close()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Headwater",
        description="Advisory data platform for data professionals.",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(data.router, prefix="/api", tags=["data"])
    app.include_router(dictionary.router, prefix="/api", tags=["dictionary"])
    app.include_router(discovery.router, prefix="/api", tags=["discovery"])
    app.include_router(models.router, prefix="/api", tags=["models"])
    app.include_router(quality.router, prefix="/api", tags=["quality"])
    app.include_router(execute.router, prefix="/api", tags=["execute"])
    app.include_router(insights.router, prefix="/api", tags=["insights"])
    app.include_router(explore.router, prefix="/api", tags=["explore"])
    app.include_router(pipeline.router, prefix="/api", tags=["pipeline"])
    app.include_router(drift.router, prefix="/api", tags=["drift"])
    app.include_router(confidence.router, prefix="/api", tags=["confidence"])

    @app.get("/api/status")
    async def api_status():
        pipeline = app.state.pipeline
        has_discovery = pipeline["discovery"] is not None
        tables = pipeline["discovery"].tables if has_discovery else []
        reviewed = sum(1 for t in tables if t.review_status == "reviewed")
        return {
            "status": "ok",
            "discovered": has_discovery,
            "tables": len(tables),
            "staging_models": len(pipeline["staging_models"]),
            "mart_models": len(pipeline["mart_models"]),
            "contracts": len(pipeline["contracts"]),
            "executed": len(pipeline["execution_results"]),
            "dictionary_reviewed": reviewed,
            "dictionary_complete": reviewed == len(tables) and len(tables) > 0,
        }

    return app


app = create_app()
