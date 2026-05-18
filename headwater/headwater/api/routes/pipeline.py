"""Pipeline API -- one-click full pipeline execution for demos and real sources."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from headwater.analyzer.catalog import build_catalog
from headwater.analyzer.eval import evaluate_catalog
from headwater.analyzer.llm import get_provider
from headwater.analyzer.semantic import analyze
from headwater.connectors.registry import get_connector
from headwater.core.config import get_settings
from headwater.core.models import SourceConfig
from headwater.core.runtime_state import get_runtime_state
from headwater.services.context_bootstrap import bootstrap_project_context
from headwater.services.discovery_persistence import (
    persist_catalog_data,
    persist_discovery_data,
    persist_semantic_data,
)
from headwater.services.pipeline_assets import build_graph_and_index
from headwater.services.pipeline_runner import (
    DEFAULT_MAX_TABLES as _DEFAULT_MAX_TABLES,
)
from headwater.services.pipeline_runner import (
    DEFAULT_SAMPLE_ROWS as _DEFAULT_SAMPLE_ROWS,
)
from headwater.services.pipeline_runner import (
    connector_type_from_uri as _connector_type_from_uri,
)
from headwater.services.pipeline_runner import (
    is_db_uri as _is_db_uri,
)
from headwater.services.pipeline_runner import (
    run_pipeline,
)
from headwater.services.project_context import load_retrieved_metadata

logger = logging.getLogger(__name__)

router = APIRouter()
class ConnectionTestRequest(BaseModel):
    source_path: str
    source_type: str = "auto"
    include_schemas: list[str] | None = None
    exclude_schemas: list[str] | None = None
    include_tables: list[str] | None = None
    exclude_tables: list[str] | None = None

@router.post("/pipeline/test-connection")
async def test_connection(
    request: Request,
    source_path: str | None = None,
    source_type: str = "auto",
    include_schemas: list[str] | None = None,
    exclude_schemas: list[str] | None = None,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
):
    """Test connectivity to a data source without running the full pipeline.

    Returns connection status, table count, and any error details.
    """
    payload = None
    try:
        payload = await request.json()
    except Exception:
        payload = None

    if isinstance(payload, dict) and payload:
        request_body = ConnectionTestRequest.model_validate(payload)
    else:
        request_body = ConnectionTestRequest(
            source_path=source_path or "",
            source_type=source_type,
            include_schemas=include_schemas,
            exclude_schemas=exclude_schemas,
            include_tables=include_tables,
            exclude_tables=exclude_tables,
        )
    source_path = request_body.source_path
    source_type = request_body.source_type
    if not source_path:
        raise HTTPException(status_code=422, detail="source_path is required")
    if _is_db_uri(source_path):
        resolved_type = (
            source_type if source_type != "auto" else _connector_type_from_uri(source_path)
        )
        source = SourceConfig(name="__test__", type=resolved_type, uri=source_path)
        try:
            connector = get_connector(resolved_type)
            connector.connect(source)
            # Apply filters if provided
            if hasattr(connector, "set_schema_filter"):
                filter_config = {
                    "include_schemas": request_body.include_schemas,
                    "exclude_schemas": request_body.exclude_schemas,
                    "include_tables": request_body.include_tables,
                    "exclude_tables": request_body.exclude_tables,
                }
                connector.set_schema_filter(filter_config)
            tables = connector.list_tables()
            connector.close()
            return {
                "status": "ok",
                "source_type": resolved_type,
                "tables": len(tables),
                "table_names": tables,
                "detail": f"Connected. Found {len(tables)} table(s).",
            }
        except Exception as exc:
            return {
                "status": "error",
                "source_type": resolved_type,
                "tables": 0,
                "table_names": [],
                "detail": str(exc),
            }
    else:
        data_path = Path(source_path).resolve()
        if not data_path.exists():
            return {
                "status": "error",
                "source_type": "file",
                "tables": 0,
                "table_names": [],
                "detail": f"Path not found: {data_path}",
            }
        return {
            "status": "ok",
            "source_type": "file",
            "tables": 0,
            "table_names": [],
            "detail": f"Path exists: {data_path}",
        }


@router.post("/pipeline/run")
def run_full_pipeline(
    request: Request,
    source_path: str = "postgresql://headwater:headwater@localhost:5434/headwater_dev",
    source_type: str = "auto",
    source_name: str = "source",
    source_schema: str = "public",
    target_schema: str = "staging",
    max_tables: int = _DEFAULT_MAX_TABLES,
    sample_rows: int = _DEFAULT_SAMPLE_ROWS,
):
    """Run the entire pipeline: discover -> generate -> execute -> quality check.

    source_path accepts either:
    - A filesystem path to JSON/CSV data  (e.g. /data/sample)
    - A database DSN                      (e.g. postgresql://user:pass@host:5434/db)

    source_type defaults to 'auto' which infers the connector from the source value.
    """
    pipeline = get_runtime_state(request)
    in_memory = getattr(request.app.state, "_in_memory", False)

    if in_memory:
        # Test mode: single in-memory DuckDB, must reuse the shared connection
        con = request.app.state.duckdb_con
        return _run_pipeline_inner(
            con, request, pipeline, source_path, source_type,
            source_name, source_schema, target_schema, max_tables, sample_rows,
        )

    # Production: use a DEDICATED DuckDB connection for the pipeline run.
    # The shared app.state.duckdb_con is used by all API requests;
    # DuckDB is not thread-safe, so a long-running sync pipeline on
    # the threadpool would block every other request.
    settings = get_settings()
    con = duckdb.connect(str(settings.analytical_db_path))
    try:
        return _run_pipeline_inner(
            con, request, pipeline, source_path, source_type,
            source_name, source_schema, target_schema, max_tables, sample_rows,
        )
    finally:
        con.close()


def _run_pipeline_inner(
    con,
    request: Request,
    pipeline: dict,
    source_path: str,
    source_type: str,
    source_name: str,
    source_schema: str,
    target_schema: str,
    max_tables: int = _DEFAULT_MAX_TABLES,
    sample_rows: int = _DEFAULT_SAMPLE_ROWS,
):
    """Inner pipeline logic using a dedicated DuckDB connection."""
    try:
        return run_pipeline(
            con,
            pipeline=pipeline,
            metadata_store=getattr(request.app.state, "metadata_store", None),
            source_path=source_path,
            source_type=source_type,
            source_name=source_name,
            source_schema=source_schema,
            target_schema=target_schema,
            max_tables=max_tables,
            sample_rows=sample_rows,
            auto_confirm=_auto_confirm,
            connector_factory=get_connector,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/pipeline/re-enrich")
def re_enrich(request: Request, force: bool = False):
    """Re-run semantic analysis on the existing discovery without re-ingesting data.

    Uses the current LLM settings to enrich column descriptions and semantic types.

    When ``force=False`` (default), only tables that have not yet been LLM-enriched
    (i.e. no semantic_detail or inference_confidence == 0) are processed.  This lets
    re-runs build on partial work instead of starting from zero.

    When ``force=True``, all unlocked tables are re-enriched.
    """
    pipeline = get_runtime_state(request)
    discovery = pipeline.get("discovery")
    if not discovery:
        raise HTTPException(status_code=400, detail="No discovery run yet. Run the pipeline first.")

    # Clear cached settings so we pick up any changes from the Settings page
    get_settings.cache_clear()
    settings = get_settings()
    logger.info(
        "Re-enrich: provider=%s, model=%s, force=%s",
        settings.llm_provider,
        settings.llm_model,
        force,
    )
    store = getattr(request.app.state, "metadata_store", None)
    provider = get_provider(settings, store=store)
    logger.info("Re-enrich: provider class=%s", type(provider).__name__)

    # Count already LLM-enriched vs remaining tables.
    # Heuristic-only enrichment sets inference_confidence <= 0.5;
    # LLM enrichment sets it to 0.6 (compact/Ollama) or 0.8 (full).
    llm_threshold = 0.55
    already_enriched = [
        t.name
        for t in discovery.tables
        if not t.locked
        and t.semantic_detail is not None
        and getattr(t.semantic_detail, "inference_confidence", 0)
        >= llm_threshold
    ]
    total_unlocked = sum(1 for t in discovery.tables if not t.locked)
    remaining = total_unlocked - len(already_enriched)

    if not force and remaining == 0:
        return {
            "columns_enriched": 0,
            "provider": settings.llm_provider,
            "skipped": len(already_enriched),
            "message": "All tables already enriched. Use force=true to re-enrich.",
        }

    # If not forcing, temporarily mark already-enriched tables as locked
    # so the analyzer skips them, then restore after.
    tables_temporarily_locked: list[str] = []
    if not force:
        for t in discovery.tables:
            if t.name in already_enriched and not t.locked:
                t.locked = True
                tables_temporarily_locked.append(t.name)
        logger.info(
            "Skipping %d already-enriched tables: %s",
            len(tables_temporarily_locked),
            ", ".join(tables_temporarily_locked),
        )

    # Re-run semantic analysis with per-table checkpointing
    source_name = getattr(getattr(discovery, "source", None), "name", "source")
    try:
        analyze(discovery, provider, store=store, source_name=source_name)
    except Exception as exc:
        logger.exception("Re-enrich analysis failed")
        raise HTTPException(
            status_code=500,
            detail=f"Semantic analysis failed: {exc}",
        ) from exc
    finally:
        # Restore temporarily locked tables
        for t in discovery.tables:
            if t.name in tables_temporarily_locked:
                t.locked = False

    # Count enriched columns (across ALL tables, not just newly enriched)
    columns_enriched = sum(
        1
        for table in discovery.tables
        for col in table.columns
        if col.description or col.semantic_type
    )
    project_context = bootstrap_project_context(discovery, project_id=source_name)
    pipeline["project_context"] = project_context
    metadata = None
    if store is not None:
        store.upsert_source(
            source_name,
            discovery.source.type,
            discovery.source.path,
            discovery.source.uri,
            mode=discovery.source.mode,
        )
        store.replace_project_context(
            source_name,
            source_name=source_name,
            items=[item.model_dump(mode="json") for item in project_context.items],
            resources=[
                resource.model_dump(mode="json")
                for resource in project_context.resources
            ],
        )

    # --- Propagate enrichment downstream ---
    # Rebuild catalog from enriched discovery (new descriptions improve
    # metric/dimension extraction and entity narratives)
    if store is not None:
        metadata = load_retrieved_metadata(store, discovery, project_id=source_name)
    catalog = build_catalog(
        discovery,
        project_id=source_name,
        metadata=metadata,
    )
    pipeline["catalog"] = catalog
    logger.info(
        "Re-enrich: rebuilt catalog: %d metrics, %d dims, %d entities",
        len(catalog.metrics),
        len(catalog.dimensions),
        len(catalog.entities),
    )

    # Auto-confirm high-confidence items (no DuckDB con for re-enrich;
    # composite PK detection already ran during initial pipeline)
    _auto_confirm(discovery, catalog, con=None, schema_name=None)

    # Evaluate catalog quality
    evaluation = evaluate_catalog(
        catalog, discovery.tables, discovery.profiles
    )

    try:
        assets = build_graph_and_index(discovery, catalog, source_name)
        pipeline["graph_store"] = assets.graph_store
        pipeline["vector_store"] = assets.vector_store
        logger.info("Re-enrich: rebuilt graph and vector index")
    except Exception:
        logger.exception("Re-enrich: graph/index rebuild failed (non-fatal)")

    # Persist ALL updated data to SQLite
    if store is not None:
        try:
            persist_discovery_data(store, discovery, source_name)
            persist_semantic_data(store, discovery, source_name)
            persist_catalog_data(store, catalog, evaluation, source_name)
            logger.info("Re-enrich: persisted discovery + catalog to SQLite")
        except Exception:
            logger.exception("Re-enrich: persistence failed (non-fatal)")

        store.log_activity(
            "re_enrichment",
            f"Re-enriched {columns_enriched} columns, "
            f"rebuilt catalog ({len(catalog.metrics)} metrics, "
            f"{len(catalog.dimensions)} dims)",
            artifact_type="pipeline",
            artifact_id="re_enrich",
        )

    return {
        "columns_enriched": columns_enriched,
        "provider": settings.llm_provider,
        "skipped": len(tables_temporarily_locked),
        "tables_processed": remaining if not force else total_unlocked,
        "catalog_metrics": len(catalog.metrics),
        "catalog_dimensions": len(catalog.dimensions),
        "catalog_entities": len(catalog.entities),
        "catalog_confidence": catalog.confidence,
        "relationships": len(discovery.relationships),
    }


# ---------------------------------------------------------------------------
# Auto-confirmation of high-confidence items
# ---------------------------------------------------------------------------

# Thresholds for auto-confirmation (items above these are auto-approved)
_AUTO_CONFIRM_PK_THRESHOLD = 0.85
_AUTO_CONFIRM_COLUMN_THRESHOLD = 0.8
_AUTO_CONFIRM_CATALOG_THRESHOLD = 0.8


def _auto_confirm(discovery_result, catalog, con=None, schema_name=None) -> dict[str, int]:
    """Auto-confirm high-confidence items to minimize manual review.

    1. Runs single-column PK detection from profile stats.
    2. If no single-column PK found AND DuckDB connection available,
       runs composite PK detection via SQL verification.
    3. Auto-locks columns with confidence >= threshold.
    4. Auto-confirms tables where all columns are locked.
    5. Auto-confirms catalog metrics/dimensions above threshold.

    Returns counts of auto-confirmed items by category.
    """
    from datetime import datetime

    from headwater.profiler.key_detection import (
        detect_composite_keys,
        suggest_primary_keys,
    )

    stats = {
        "columns": 0, "tables": 0, "metrics": 0,
        "dimensions": 0, "pks": 0, "composite_pks": 0,
    }

    # Build profile index for PK detection
    table_row_counts = {t.name: t.row_count for t in discovery_result.tables}
    profile_by_table: dict[str, list[dict]] = {}
    for p in discovery_result.profiles:
        tbl = p.table_name
        if tbl not in profile_by_table:
            profile_by_table[tbl] = []
        profile_by_table[tbl].append({
            "column_name": p.column_name,
            "dtype": p.dtype,
            "stats": {
                "row_count": table_row_counts.get(tbl, 0),
                "distinct_count": p.distinct_count,
                "null_count": p.null_count,
                "min": p.min_value,
                "max": p.max_value,
            },
        })

    # --- PK detection for every table ---
    for table in discovery_result.tables:
        if table.locked:
            continue
        if any(c.is_primary_key for c in table.columns):
            continue
        profiles = profile_by_table.get(table.name, [])
        if not profiles:
            continue

        # Try single-column PK first
        pk_candidates = suggest_primary_keys(table.name, profiles)
        if pk_candidates and pk_candidates[0].confidence >= _AUTO_CONFIRM_PK_THRESHOLD:
            best = pk_candidates[0]
            col = next((c for c in table.columns if c.name == best.column), None)
            if col:
                col.is_primary_key = True
                col.semantic_type = "primary_key"
                col.role = "identifier"
                col.confidence = max(col.confidence, best.confidence)
                stats["pks"] += 1
                logger.info(
                    "Auto-detected PK: %s.%s (confidence=%.2f, reasons=%s)",
                    table.name, best.column, best.confidence, best.reasons,
                )
            continue

        # No single-column PK found -- try composite keys (requires DuckDB)
        if con is None or schema_name is None:
            continue
        try:
            composite = detect_composite_keys(
                con, table.name, schema_name, profiles,
                columns=table.columns,
            )
        except Exception:
            logger.exception("Composite PK detection failed for %s", table.name)
            continue

        if composite and composite[0].confidence >= 0.7:
            best_comp = composite[0]
            for col_name in best_comp.columns:
                col = next((c for c in table.columns if c.name == col_name), None)
                if col:
                    col.is_primary_key = True
                    col.confidence = max(col.confidence, best_comp.confidence)
            stats["composite_pks"] += 1
            logger.info(
                "Auto-detected composite PK: %s.(%s) (confidence=%.2f, reasons=%s)",
                table.name,
                ", ".join(best_comp.columns),
                best_comp.confidence,
                best_comp.reasons,
            )

    # --- Auto-lock high-confidence columns ---
    for table in discovery_result.tables:
        if table.locked:
            continue
        for col in table.columns:
            if col.locked:
                continue
            if col.confidence >= _AUTO_CONFIRM_COLUMN_THRESHOLD:
                col.locked = True
                stats["columns"] += 1

    # --- Auto-confirm tables where ALL columns are locked ---
    for table in discovery_result.tables:
        if table.review_status in ("reviewed", "skipped"):
            continue
        if all(c.locked for c in table.columns):
            table.review_status = "reviewed"
            table.reviewed_at = datetime.now()
            table.locked = True
            stats["tables"] += 1

    # --- Auto-confirm high-confidence catalog metrics ---
    for metric in catalog.metrics:
        if metric.status == "proposed" and metric.confidence >= _AUTO_CONFIRM_CATALOG_THRESHOLD:
            metric.status = "confirmed"
            stats["metrics"] += 1

    # --- Auto-confirm high-confidence catalog dimensions ---
    for dim in catalog.dimensions:
        if dim.status == "proposed" and dim.confidence >= _AUTO_CONFIRM_CATALOG_THRESHOLD:
            dim.status = "confirmed"
            stats["dimensions"] += 1

    return stats
