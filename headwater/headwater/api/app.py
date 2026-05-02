"""FastAPI application -- Headwater API."""

from __future__ import annotations

import logging
import traceback
from contextlib import asynccontextmanager
from typing import Any

import duckdb
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from headwater.api.routes import (
    briefing,
    confidence,
    data,
    dictionary,
    discovery,
    drift,
    execute,
    explore,
    graph,
    health,
    insights,
    models,
    pipeline,
    project,
    quality,
    settings,
    sources,
    warehouse,
)
from headwater.core.config import get_settings
from headwater.core.metadata import MetadataStore

# Ensure headwater loggers are visible at INFO level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logging.getLogger("headwater").setLevel(logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application state: DuckDB connection, metadata store, and pipeline state."""
    in_memory = getattr(app.state, "_in_memory", False)
    logger.info("Headwater startup: in_memory=%s", in_memory)
    if in_memory:
        app.state.duckdb_con = duckdb.connect(":memory:")
        app.state.metadata_store = MetadataStore()
        logger.info("Using in-memory stores (test mode)")
    else:
        settings = get_settings()
        settings.ensure_dirs()
        logger.info(
            "Using file-backed stores: metadata=%s, analytical=%s",
            settings.metadata_db_path,
            settings.analytical_db_path,
        )
        app.state.duckdb_con = duckdb.connect(str(settings.analytical_db_path))
        app.state.metadata_store = MetadataStore(settings.metadata_db_path)
    store = app.state.metadata_store
    store.init()
    logger.info("Metadata store initialized")

    # Try to restore previous discovery from persisted metadata
    restored_discovery = None
    if not in_memory:
        sources = store.list_sources()
        logger.info("Persisted sources found: %s", [s["name"] for s in sources])
        if sources:
            # Pick the most recent source that has actual tables
            source_name = None
            for s in reversed(sources):
                test_name = s["name"]
                try:
                    candidate = store.rebuild_discovery(test_name)
                    if candidate and candidate.tables:
                        source_name = test_name
                        restored_discovery = candidate
                        break
                except Exception:
                    continue
            if not source_name:
                source_name = sources[-1]["name"]
            logger.info("Attempting to restore discovery for source '%s'", source_name)
            try:
                if not restored_discovery:
                    restored_discovery = store.rebuild_discovery(source_name)
                if restored_discovery:
                    logger.info(
                        "Restored discovery: %d tables, %d profiles, %d relationships",
                        len(restored_discovery.tables),
                        len(restored_discovery.profiles),
                        len(restored_discovery.relationships),
                    )
                    reviewed = sum(
                        1 for t in restored_discovery.tables if t.review_status == "reviewed"
                    )
                    logger.info(
                        "Review status: %d/%d tables reviewed",
                        reviewed,
                        len(restored_discovery.tables),
                    )
                else:
                    logger.warning("rebuild_discovery returned None for source '%s'", source_name)
            except Exception:
                logger.exception("Failed to restore discovery from metadata")

    # Try to restore catalog from metadata
    restored_catalog = None
    if restored_discovery and not in_memory:
        try:
            source_name = sources[0]["name"]
            metrics_raw = store.get_catalog_metrics(source_name)
            dims_raw = store.get_catalog_dimensions(source_name)
            ents_raw = store.get_catalog_entities(source_name)
            if metrics_raw or dims_raw or ents_raw:
                from headwater.core.models import (
                    DimensionDefinition,
                    EntityDefinition,
                    MetricDefinition,
                    SemanticCatalog,
                )

                def _remap(d: dict) -> dict:
                    """Remap DB column names to Pydantic field names."""
                    out = dict(d)
                    if "column_name" in out:
                        out["column"] = out.pop("column_name")
                    if "table_name" in out:
                        out["table"] = out.pop("table_name")
                    out.pop("project_id", None)
                    return out

                restored_catalog = SemanticCatalog(
                    metrics=[MetricDefinition(**_remap(m)) for m in metrics_raw],
                    dimensions=[DimensionDefinition(**_remap(d)) for d in dims_raw],
                    entities=[EntityDefinition(**_remap(e)) for e in ents_raw],
                )
                logger.info(
                    "Restored catalog: %d metrics, %d dimensions, %d entities",
                    len(restored_catalog.metrics),
                    len(restored_catalog.dimensions),
                    len(restored_catalog.entities),
                )
        except Exception:
            logger.exception("Failed to restore catalog from metadata")

    # Try to restore models, contracts, and execution results from metadata
    restored_staging: list = []
    restored_marts: list = []
    restored_contracts: list = []
    restored_exec_results: list = []
    if restored_discovery and not in_memory:
        try:
            restored_staging, restored_marts = _restore_models(store)
            restored_contracts = _restore_contracts(store)
            restored_exec_results = _restore_execution_results(store)
        except Exception:
            logger.exception("Failed to restore pipeline state from metadata")

    app.state.pipeline: dict[str, Any] = {
        "discovery": restored_discovery,
        "catalog": restored_catalog,
        "staging_models": restored_staging,
        "mart_models": restored_marts,
        "contracts": restored_contracts,
        "execution_results": restored_exec_results,
        "quality_report": None,
    }
    yield
    app.state.duckdb_con.close()
    app.state.metadata_store.close()


def _restore_models(store: MetadataStore) -> tuple[list, list]:
    """Restore generated models from SQLite, split into staging and mart lists."""
    from headwater.core.models import GeneratedModel

    rows = store.get_models()
    staging, marts = [], []
    for r in rows:
        try:
            m = GeneratedModel(
                name=r["name"],
                model_type=r["model_type"],
                sql=r["sql_text"],
                description=r.get("description", ""),
                source_tables=_parse_json_list(r.get("source_tables")),
                depends_on=_parse_json_list(r.get("depends_on")),
                status=r.get("status", "proposed"),
                assumptions=_parse_json_list(r.get("assumptions")),
                questions=_parse_json_list(r.get("questions")),
            )
            if m.model_type == "staging":
                staging.append(m)
            else:
                marts.append(m)
        except Exception:
            logger.warning("Skipping malformed model row: %s", r.get("name"))
    if staging or marts:
        logger.info(
            "Restored %d staging + %d mart models from metadata",
            len(staging),
            len(marts),
        )
    return staging, marts


def _restore_contracts(store: MetadataStore) -> list:
    """Restore data contracts from SQLite."""
    from headwater.core.models import ContractRule

    rows = store.get_contracts()
    contracts = []
    for r in rows:
        try:
            contracts.append(
                ContractRule(
                    id=r["id"],
                    model_name=r["model_name"],
                    column_name=r.get("column_name"),
                    rule_type=r["rule_type"],
                    expression=r["expression"],
                    severity=r.get("severity", "warning"),
                    description=r.get("description", ""),
                    confidence=r.get("confidence", 0.8),
                    status=r.get("status", "proposed"),
                )
            )
        except Exception:
            logger.warning("Skipping malformed contract row: %s", r.get("id"))
    if contracts:
        logger.info("Restored %d contracts from metadata", len(contracts))
    return contracts


def _restore_execution_results(store: MetadataStore) -> list:
    """Restore most recent execution results from SQLite."""
    from headwater.core.models import ExecutionResult

    rows = store.get_execution_results()
    results = []
    for r in rows:
        try:
            results.append(
                ExecutionResult(
                    model_name=r["model_name"],
                    success=bool(r["success"]),
                    row_count=r.get("row_count"),
                    execution_time_ms=r.get("execution_time_ms", 0.0),
                    error=r.get("error"),
                )
            )
        except Exception:
            logger.warning("Skipping malformed exec result: %s", r.get("model_name"))
    if results:
        logger.info("Restored %d execution results from metadata", len(results))
    return results


def _parse_json_list(val: str | list | None) -> list:
    """Parse a JSON string or passthrough a list."""
    if val is None:
        return []
    if isinstance(val, list):
        return val
    try:
        import json

        return json.loads(val)
    except (ValueError, TypeError):
        return []


def create_app(*, in_memory: bool = False) -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Headwater",
        description="Advisory data platform for data professionals.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state._in_memory = in_memory

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # --- Global exception handler: log full tracebacks for 500s ---
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.error(
            "Unhandled exception on %s %s:\n%s",
            request.method,
            request.url.path,
            traceback.format_exc(),
        )
        return JSONResponse(
            status_code=500,
            content={"detail": f"{type(exc).__name__}: {exc}"},
        )

    # --- Request logging middleware ---
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        logger.info("%s %s", request.method, request.url.path)
        response = await call_next(request)
        if response.status_code >= 400:
            logger.warning(
                "%s %s -> %d",
                request.method,
                request.url.path,
                response.status_code,
            )
        return response

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
    app.include_router(project.router, prefix="/api", tags=["project"])
    app.include_router(graph.router, prefix="/api", tags=["graph"])
    app.include_router(settings.router, prefix="/api", tags=["settings"])
    app.include_router(health.router, prefix="/api", tags=["health"])
    app.include_router(sources.router, prefix="/api", tags=["sources"])
    app.include_router(briefing.router, prefix="/api", tags=["briefing"])
    app.include_router(warehouse.router, prefix="/api", tags=["warehouse"])

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
