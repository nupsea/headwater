"""Pipeline API -- one-click full pipeline execution for demos and real sources."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
from fastapi import APIRouter, HTTPException, Request

from headwater.analyzer.catalog import build_catalog
from headwater.analyzer.companion import discover_companion_docs, match_docs_to_tables
from headwater.analyzer.eval import evaluate_catalog
from headwater.analyzer.llm import get_provider
from headwater.analyzer.semantic import analyze
from headwater.connectors.registry import get_connector
from headwater.core.config import get_settings
from headwater.core.models import SourceConfig
from headwater.executor.duckdb_backend import DuckDBBackend
from headwater.executor.runner import run_models
from headwater.generator.contracts import generate_contracts
from headwater.generator.marts import generate_mart_models
from headwater.generator.staging import generate_staging_models
from headwater.profiler.engine import discover
from headwater.quality.checker import check_contracts
from headwater.quality.report import build_report

logger = logging.getLogger(__name__)

router = APIRouter()

_DB_SCHEMES = {"postgresql", "postgres", "mysql", "sqlite"}


def _is_db_uri(source: str) -> bool:
    """Return True if source looks like a database URI rather than a file path."""
    return any(source.startswith(f"{scheme}://") for scheme in _DB_SCHEMES)


def _connector_type_from_uri(uri: str) -> str:
    """Infer connector type from URI scheme."""
    if uri.startswith("postgresql://") or uri.startswith("postgres://"):
        return "postgres"
    return "json"


@router.post("/pipeline/test-connection")
def test_connection(source_path: str, source_type: str = "auto"):
    """Test connectivity to a data source without running the full pipeline.

    Returns connection status, table count, and any error details.
    """
    if _is_db_uri(source_path):
        resolved_type = (
            source_type if source_type != "auto" else _connector_type_from_uri(source_path)
        )
        source = SourceConfig(name="__test__", type=resolved_type, uri=source_path)
        connector = get_connector(resolved_type)
        try:
            connector.connect(source)
            tables = connector.list_tables()
            connector.close()
            return {
                "status": "ok",
                "source_type": resolved_type,
                "tables": len(tables),
                "table_names": tables[:20],
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
):
    """Run the entire pipeline: discover -> generate -> execute -> quality check.

    source_path accepts either:
    - A filesystem path to JSON/CSV data  (e.g. /data/sample)
    - A database DSN                      (e.g. postgresql://user:pass@host:5434/db)

    source_type defaults to 'auto' which infers the connector from the source value.
    """
    pipeline = request.app.state.pipeline
    in_memory = getattr(request.app.state, "_in_memory", False)

    if in_memory:
        # Test mode: single in-memory DuckDB, must reuse the shared connection
        con = request.app.state.duckdb_con
        return _run_pipeline_inner(
            con, request, pipeline, source_path, source_type,
            source_name, source_schema, target_schema,
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
            source_name, source_schema, target_schema,
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
):
    """Inner pipeline logic using a dedicated DuckDB connection."""
    # --- Resolve source type and build SourceConfig ---
    if _is_db_uri(source_path):
        resolved_type = (
            source_type if source_type != "auto" else _connector_type_from_uri(source_path)
        )
        source = SourceConfig(name=source_name, type=resolved_type, uri=source_path)
        connector = get_connector(resolved_type)
        connector.connect(source)

        # Sample each table into DuckDB for profiling (no bulk copy — Arrow batches only)
        import polars as _pl

        try:
            table_names = connector.list_tables()
            if not table_names:
                raise HTTPException(status_code=400, detail="No tables found in the database.")

            _duckdb_schema = source_schema.replace(".", "_")
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{_duckdb_schema}"')

            for tname in table_names:
                logger.info("Sampling table '%s' from Postgres...", tname)
                arrow_batch = connector.sample(tname, n=10_000)
                df = _pl.from_arrow(arrow_batch)
                safe_name = tname.replace(".", "_")
                con.register(f"_arrow_{safe_name}", df)
                con.execute(
                    f'CREATE OR REPLACE TABLE "{_duckdb_schema}"."{safe_name}" AS '
                    f'SELECT * FROM "_arrow_{safe_name}"'
                )
                con.unregister(f"_arrow_{safe_name}")
                logger.info("Loaded '%s' into DuckDB", safe_name)
        finally:
            connector.close()

        tables_loaded = [t.replace(".", "_") for t in table_names]

    else:
        # File-based source (JSON / CSV)
        resolved_type = source_type if source_type != "auto" else "json"
        data_path = Path(source_path).resolve()
        if not data_path.exists():
            raise HTTPException(status_code=400, detail=f"Path not found: {data_path}")
        source = SourceConfig(name=source_name, type=resolved_type, path=str(data_path))
        connector = get_connector(resolved_type)
        connector.connect(source)
        tables_loaded = connector.load_to_duckdb(con, source_schema)
        _duckdb_schema = source_schema

    discovery_result = discover(con, _duckdb_schema, source)

    # Companion doc discovery (file-based sources only)
    companion_docs = discover_companion_docs(source)
    if companion_docs:
        table_names = [t.name for t in discovery_result.tables]
        match_docs_to_tables(companion_docs, table_names)
        discovery_result.companion_docs = companion_docs

    # Semantic analysis (heuristic enrichment + deep descriptions)
    store = getattr(request.app.state, "metadata_store", None)
    analyze(discovery_result, store=store, source_name=source_name)
    pipeline["discovery"] = discovery_result

    # Step 1b: Build semantic catalog (v2)
    catalog = build_catalog(discovery_result)
    pipeline["catalog"] = catalog
    logger.info(
        "Catalog built: %d metrics, %d dimensions, %d entities (confidence=%.2f)",
        len(catalog.metrics),
        len(catalog.dimensions),
        len(catalog.entities),
        catalog.confidence,
    )

    # Step 1c: Auto-confirm high-confidence items to reduce manual review burden
    auto_stats = _auto_confirm(discovery_result, catalog, con, _duckdb_schema)
    logger.info(
        "Auto-confirmed: %d columns, %d tables, %d metrics, %d dimensions, %d PKs (%d composite)",
        auto_stats["columns"],
        auto_stats["tables"],
        auto_stats["metrics"],
        auto_stats["dimensions"],
        auto_stats["pks"],
        auto_stats["composite_pks"],
    )

    # Evaluate catalog quality
    evaluation = evaluate_catalog(catalog, discovery_result.tables, discovery_result.profiles)
    logger.info("Catalog evaluation: overall=%.2f", evaluation.confidence)

    # Build graph store (Kuzu) + vector index (LanceDB)
    from headwater.api.routes.discovery import (
        _build_graph_and_index,
        _persist_catalog_data,
        _persist_discovery_data,
        _persist_semantic_data,
    )

    _build_graph_and_index(request, discovery_result, catalog, source_name, evaluation)

    # Persist all data to metadata store
    _persist_discovery_data(request, discovery_result, source_name)
    _persist_semantic_data(request, discovery_result, source_name)
    _persist_catalog_data(request, catalog, evaluation, source_name)
    logger.info("Metadata persistence complete")

    # Step 2: Generate
    staging = generate_staging_models(
        discovery_result.tables, source_schema=_duckdb_schema, target_schema=target_schema
    )
    marts = generate_mart_models(discovery_result, target_schema="marts")
    contracts = generate_contracts(discovery_result.profiles, target_schema=target_schema)
    pipeline["staging_models"] = staging
    pipeline["mart_models"] = marts
    pipeline["contracts"] = contracts

    # Persist generated models and contracts to SQLite
    if store is not None:
        _persist_models(store, staging + marts, source_name)
        _persist_contracts(store, contracts)

    # Step 3: Execute models
    # Staging models are auto-approved. In the demo pipeline, approve marts
    # too so the full analytical layer is available for exploration.
    backend = DuckDBBackend(con)
    backend.ensure_schema(target_schema)
    backend.ensure_schema("marts")
    for m in marts:
        if m.status == "proposed":
            m.status = "approved"
    exec_results = run_models(backend, staging + marts, only_approved=True)
    pipeline["execution_results"] = exec_results

    # Persist execution results to SQLite
    if store is not None:
        _persist_execution_results(store, exec_results)

    # Step 4: Quality checks
    for c in contracts:
        if c.status == "proposed":
            c.status = "observing"
    check_results = check_contracts(con, contracts, only_active=True)
    report = build_report(check_results)
    pipeline["quality_report"] = report
    quality_run_id = None
    if store is not None:
        quality_run_id = _persist_quality_report(store, source_name, report)

    return {
        "tables_loaded": len(tables_loaded),
        "tables_discovered": len(discovery_result.tables),
        "profiles": len(discovery_result.profiles),
        "relationships": len(discovery_result.relationships),
        "staging_models": len(staging),
        "mart_models": len(marts),
        "contracts": len(contracts),
        "models_executed": len(exec_results),
        "models_succeeded": sum(1 for r in exec_results if r.success),
        "quality_total": report.total_contracts,
        "quality_passed": report.passed,
        "quality_failed": report.failed,
        "quality_score": round((report.passed / report.total_contracts) * 100, 2)
        if report.total_contracts
        else 100.0,
        "quality_run_id": quality_run_id,
        "catalog_metrics": len(catalog.metrics),
        "catalog_dimensions": len(catalog.dimensions),
        "catalog_entities": len(catalog.entities),
        "catalog_confidence": catalog.confidence,
        "auto_confirmed": auto_stats,
    }


@router.post("/pipeline/re-enrich")
def re_enrich(request: Request, force: bool = False):
    """Re-run semantic analysis on the existing discovery without re-ingesting data.

    Uses the current LLM settings to enrich column descriptions and semantic types.

    When ``force=False`` (default), only tables that have not yet been LLM-enriched
    (i.e. no semantic_detail or inference_confidence == 0) are processed.  This lets
    re-runs build on partial work instead of starting from zero.

    When ``force=True``, all unlocked tables are re-enriched.
    """
    pipeline = request.app.state.pipeline
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
    source_name = "source"
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

    # --- Propagate enrichment downstream ---
    # Rebuild catalog from enriched discovery (new descriptions improve
    # metric/dimension extraction and entity narratives)
    catalog = build_catalog(discovery)
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

    # Rebuild graph (Kuzu) + vector index (LanceDB)
    from headwater.api.routes.discovery import (
        _build_graph_and_index,
        _persist_catalog_data,
        _persist_discovery_data,
        _persist_semantic_data,
    )

    try:
        _build_graph_and_index(
            request, discovery, catalog, source_name, evaluation
        )
        logger.info("Re-enrich: rebuilt graph and vector index")
    except Exception:
        logger.exception("Re-enrich: graph/index rebuild failed (non-fatal)")

    # Persist ALL updated data to SQLite
    if store is not None:
        try:
            _persist_discovery_data(request, discovery, source_name)
            _persist_semantic_data(request, discovery, source_name)
            _persist_catalog_data(
                request, catalog, evaluation, source_name
            )
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


# ---------------------------------------------------------------------------
# Pipeline state persistence helpers
# ---------------------------------------------------------------------------


def _persist_models(store: object, models: list, source_name: str) -> None:
    """Persist generated models to SQLite."""
    for m in models:
        try:
            store.upsert_model(  # type: ignore[union-attr]
                name=m.name,
                source_name=source_name,
                model_type=m.model_type,
                sql_text=m.sql,
                description=m.description,
                source_tables=m.source_tables,
                depends_on=m.depends_on,
                status=m.status,
                assumptions=getattr(m, "assumptions", []),
                questions=getattr(m, "questions", []),
            )
        except Exception:
            logger.exception("Failed to persist model %s", m.name)
    logger.info("Persisted %d models to metadata store", len(models))


def _persist_contracts(store: object, contracts: list) -> None:
    """Persist generated contracts to SQLite."""
    for c in contracts:
        try:
            store.upsert_contract(  # type: ignore[union-attr]
                id_=c.id or f"{c.model_name}_{c.rule_type}_{c.column_name}",
                model_name=c.model_name,
                rule_type=c.rule_type,
                expression=c.expression,
                column_name=c.column_name,
                severity=c.severity,
                description=c.description,
                confidence=c.confidence,
                status=c.status,
            )
        except Exception:
            logger.exception("Failed to persist contract %s", c.id)
    logger.info("Persisted %d contracts to metadata store", len(contracts))


def _persist_execution_results(store: object, results: list) -> None:
    """Persist execution results to SQLite."""
    for r in results:
        try:
            store.save_execution_result(  # type: ignore[union-attr]
                model_name=r.model_name,
                success=r.success,
                row_count=r.row_count,
                execution_time_ms=r.execution_time_ms,
                error=r.error,
            )
        except Exception:
            logger.exception("Failed to persist exec result for %s", r.model_name)
    logger.info("Persisted %d execution results", len(results))


def _persist_quality_report(
    store: object,
    source_name: str,
    report,
    sync_run_id: int | None = None,
):
    """Persist quality report and emit an event when contracts fail."""
    run_id = store.save_quality_report(  # type: ignore[union-attr]
        source_name,
        report,
        sync_run_id=sync_run_id,
    )
    if report.failed:
        try:
            store.insert_event(  # type: ignore[union-attr]
                "quality_checks_failed",
                f"{report.failed} quality contract(s) failed",
                source_name=source_name,
                severity="warning",
                artifact_type="quality_run",
                artifact_id=str(run_id),
                payload={
                    "quality_run_id": run_id,
                    "total": report.total_contracts,
                    "passed": report.passed,
                    "failed": report.failed,
                },
                invalidates=["sources", "briefing", "health", "insights", "quality"],
            )
        except Exception:
            logger.exception("Failed to emit quality event for '%s'", source_name)
    logger.info("Persisted quality run %s for source '%s'", run_id, source_name)
    return run_id
