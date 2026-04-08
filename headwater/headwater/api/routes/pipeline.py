"""Pipeline API -- one-click full pipeline execution for demos."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from headwater.analyzer.heuristics import build_domain_map, enrich_tables
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

router = APIRouter()


@router.post("/pipeline/run")
async def run_full_pipeline(
    request: Request,
    source_path: str,
    source_type: str = "json",
    source_name: str = "source",
    source_schema: str = "env_health",
    target_schema: str = "staging",
):
    """Run the entire pipeline: discover -> generate -> execute -> quality check."""
    data_path = Path(source_path).resolve()
    if not data_path.exists():
        raise HTTPException(status_code=400, detail=f"Path not found: {data_path}")

    con = request.app.state.duckdb_con
    pipeline = request.app.state.pipeline

    # Step 1: Load + Discover
    source = SourceConfig(name=source_name, type=source_type, path=str(data_path))
    connector = get_connector(source.type)
    connector.connect(source)
    tables_loaded = connector.load_to_duckdb(con, source_schema)

    discovery_result = discover(con, source_schema, source)
    enrich_tables(
        discovery_result.tables, discovery_result.profiles, discovery_result.relationships
    )
    discovery_result.domains = build_domain_map(discovery_result.tables)
    pipeline["discovery"] = discovery_result

    # Step 2: Generate
    staging = generate_staging_models(
        discovery_result.tables, source_schema=source_schema, target_schema=target_schema
    )
    marts = generate_mart_models(discovery_result, target_schema=target_schema)
    contracts = generate_contracts(discovery_result.profiles, target_schema=target_schema)
    pipeline["staging_models"] = staging
    pipeline["mart_models"] = marts
    pipeline["contracts"] = contracts

    # Step 3: Execute approved models
    backend = DuckDBBackend(con)
    backend.ensure_schema(target_schema)
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
    }
