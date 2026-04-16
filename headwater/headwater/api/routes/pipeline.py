"""Pipeline API -- one-click full pipeline execution for demos and real sources."""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from headwater.analyzer.catalog import build_catalog
from headwater.analyzer.companion import discover_companion_docs, match_docs_to_tables
from headwater.analyzer.eval import evaluate_catalog
from headwater.analyzer.semantic import analyze
from headwater.connectors.registry import get_connector
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


@router.post("/pipeline/run")
async def run_full_pipeline(
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
    con = request.app.state.duckdb_con
    pipeline = request.app.state.pipeline

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

        table_names = connector.list_tables()
        if not table_names:
            raise HTTPException(status_code=400, detail="No tables found in the database.")

        _duckdb_schema = source_schema.replace(".", "_")
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{_duckdb_schema}"')

        for tname in table_names:
            arrow_batch = connector.sample(tname, n=10_000)
            df = _pl.from_arrow(arrow_batch)
            safe_name = tname.replace(".", "_")
            con.register(f"_arrow_{safe_name}", df)
            con.execute(
                f'CREATE OR REPLACE TABLE "{_duckdb_schema}"."{safe_name}" AS '
                f'SELECT * FROM "_arrow_{safe_name}"'
            )
            con.unregister(f"_arrow_{safe_name}")

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
    analyze(discovery_result)
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

    # Step 4: Quality checks
    for c in contracts:
        if c.status == "proposed":
            c.status = "observing"
    check_results = check_contracts(con, contracts, only_active=True)
    report = build_report(check_results)
    pipeline["quality_report"] = report

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
        "catalog_metrics": len(catalog.metrics),
        "catalog_dimensions": len(catalog.dimensions),
        "catalog_entities": len(catalog.entities),
        "catalog_confidence": catalog.confidence,
    }
