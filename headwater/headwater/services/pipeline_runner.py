"""Application service for orchestrating the full pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

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
from headwater.services.context_bootstrap import bootstrap_project_context
from headwater.services.context_drift import reconcile_project_context_drift
from headwater.services.contract_lifecycle import apply_contract_statuses
from headwater.services.discovery_persistence import (
    persist_catalog_data,
    persist_discovery_data,
    persist_semantic_data,
)
from headwater.services.pipeline_assets import build_graph_and_index
from headwater.services.pipeline_state import (
    persist_contracts,
    persist_execution_results,
    persist_models,
    persist_quality_report,
)
from headwater.services.project_context import load_retrieved_metadata

logger = logging.getLogger(__name__)

DB_SCHEMES = {
    "postgresql",
    "postgres",
    "mysql",
    "mysql+pymysql",
    "sqlite",
    "snowflake",
    "redshift",
    "redshift+iam",
}
DEFAULT_MAX_TABLES = 50
DEFAULT_SAMPLE_ROWS = 10_000
MAX_TABLES_CAP = 500
SAMPLE_ROWS_CAP = 50_000


def is_db_uri(source: str) -> bool:
    return any(source.startswith(f"{scheme}://") for scheme in DB_SCHEMES)


def connector_type_from_uri(uri: str) -> str:
    if uri.startswith("postgresql://") or uri.startswith("postgres://"):
        return "postgres"
    if uri.startswith("mysql://") or uri.startswith("mysql+pymysql://"):
        return "mysql"
    if uri.startswith("sqlite://"):
        return "sqlite"
    if uri.startswith("snowflake://"):
        return "snowflake"
    if uri.startswith(("redshift://", "redshift+iam://")):
        return "redshift"
    return "json"


def bounded_int(value: int | str | None, default: int, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def run_pipeline(
    con,
    *,
    pipeline: dict,
    metadata_store,
    source_path: str,
    source_type: str,
    source_name: str,
    source_schema: str,
    target_schema: str,
    max_tables: int = DEFAULT_MAX_TABLES,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    schema_filter_config: dict | None = None,
    auto_confirm,
    connector_factory=get_connector,
) -> dict:
    """Run the end-to-end pipeline against a source."""
    max_tables = bounded_int(max_tables, DEFAULT_MAX_TABLES, minimum=1, maximum=MAX_TABLES_CAP)
    sample_rows = bounded_int(
        sample_rows,
        DEFAULT_SAMPLE_ROWS,
        minimum=100,
        maximum=SAMPLE_ROWS_CAP,
    )
    profiling_policy = {
        "max_tables": max_tables,
        "sample_rows": sample_rows,
        "mode": "bounded_sample" if is_db_uri(source_path) else "load_to_duckdb",
    }
    skipped_tables: list[str] = []

    if is_db_uri(source_path):
        resolved_type = (
            source_type if source_type != "auto" else connector_type_from_uri(source_path)
        )
        source = SourceConfig(name=source_name, type=resolved_type, uri=source_path)
        connector = connector_factory(resolved_type)
        connector.connect(source)
        # Apply schema/table filter if the connector supports it.
        if schema_filter_config and hasattr(connector, "set_schema_filter"):
            connector.set_schema_filter(schema_filter_config)
        try:
            all_table_names = connector.list_tables()
            if not all_table_names:
                raise ValueError("No tables found in the database.")
            table_names = all_table_names[:max_tables]
            skipped_tables = all_table_names[max_tables:]

            duckdb_schema = source_schema.replace(".", "_")
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{duckdb_schema}"')

            for table_name in table_names:
                logger.info(
                    "Sampling table '%s' from %s with row limit %d...",
                    table_name,
                    resolved_type,
                    sample_rows,
                )
                arrow_batch = connector.sample(table_name, n=sample_rows)
                dataframe = pl.from_arrow(arrow_batch)
                safe_name = table_name.replace(".", "_")
                con.register(f"_arrow_{safe_name}", dataframe)
                con.execute(
                    f'CREATE OR REPLACE TABLE "{duckdb_schema}"."{safe_name}" AS '
                    f'SELECT * FROM "_arrow_{safe_name}"'
                )
                con.unregister(f"_arrow_{safe_name}")
                logger.info("Loaded '%s' into DuckDB", safe_name)
        finally:
            connector.close()

        tables_loaded = [table.replace(".", "_") for table in table_names]
    else:
        resolved_type = source_type if source_type != "auto" else "json"
        data_path = Path(source_path).resolve()
        if not data_path.exists():
            raise ValueError(f"Path not found: {data_path}")
        source = SourceConfig(name=source_name, type=resolved_type, path=str(data_path))
        connector = connector_factory(resolved_type)
        connector.connect(source)
        tables_loaded = connector.load_to_duckdb(con, source_schema)
        duckdb_schema = source_schema

    discovery_result = discover(con, duckdb_schema, source)

    companion_docs = discover_companion_docs(source)
    if companion_docs:
        table_names = [table.name for table in discovery_result.tables]
        match_docs_to_tables(companion_docs, table_names)
        discovery_result.companion_docs = companion_docs

    if metadata_store is not None:
        metadata_store.upsert_source(
            source_name,
            discovery_result.source.type,
            discovery_result.source.path,
            discovery_result.source.uri,
            mode=discovery_result.source.mode,
        )
        existing_metadata = load_retrieved_metadata(
            metadata_store,
            discovery_result,
            project_id=source_name,
        )
    else:
        existing_metadata = None
    analyze(
        discovery_result,
        store=metadata_store,
        source_name=source_name,
        project_id=source_name,
        metadata=existing_metadata,
    )
    if metadata_store is not None:
        metadata_store.apply_key_decisions_to_discovery(discovery_result)
    pipeline["discovery"] = discovery_result

    project_context = bootstrap_project_context(discovery_result, project_id=source_name)
    pipeline["project_context"] = project_context
    metadata = None
    context_drift = {"items_flagged": 0, "item_ids": []}
    if metadata_store is not None:
        metadata_store.replace_project_context(
            source_name,
            source_name=source_name,
            items=[item.model_dump(mode="json") for item in project_context.items],
            resources=[resource.model_dump(mode="json") for resource in project_context.resources],
        )
        run_id = persist_discovery_data(metadata_store, discovery_result, source_name)
        persist_semantic_data(metadata_store, discovery_result, source_name)
        context_drift = reconcile_project_context_drift(
            metadata_store,
            discovery_result,
            project_id=source_name,
            source_name=source_name,
            drift_report=metadata_store.get_latest_drift_report(source_name),
        )
        if run_id is not None:
            metadata_store.save_project_context_snapshot(
                run_id,
                project_id=source_name,
                source_name=source_name,
            )
        metadata = load_retrieved_metadata(
            metadata_store,
            discovery_result,
            project_id=source_name,
        )

    catalog = build_catalog(
        discovery_result,
        project_id=source_name,
        metadata=metadata,
    )
    pipeline["catalog"] = catalog
    logger.info(
        "Catalog built: %d metrics, %d dimensions, %d entities (confidence=%.2f)",
        len(catalog.metrics),
        len(catalog.dimensions),
        len(catalog.entities),
        catalog.confidence,
    )

    auto_stats = auto_confirm(discovery_result, catalog, con, duckdb_schema)
    logger.info(
        "Auto-confirmed: %d columns, %d tables, %d metrics, %d dimensions, %d PKs (%d composite)",
        auto_stats["columns"],
        auto_stats["tables"],
        auto_stats["metrics"],
        auto_stats["dimensions"],
        auto_stats["pks"],
        auto_stats["composite_pks"],
    )

    evaluation = evaluate_catalog(catalog, discovery_result.tables, discovery_result.profiles)
    logger.info("Catalog evaluation: overall=%.2f", evaluation.confidence)

    assets = build_graph_and_index(discovery_result, catalog, source_name)
    pipeline["graph_store"] = assets.graph_store
    pipeline["vector_store"] = assets.vector_store

    persist_catalog_data(metadata_store, catalog, evaluation, source_name)
    logger.info("Metadata persistence complete")

    staging = generate_staging_models(
        discovery_result.tables,
        source_schema=duckdb_schema,
        target_schema=target_schema,
    )
    marts = generate_mart_models(discovery_result, target_schema="marts")
    contracts = generate_contracts(discovery_result.profiles, target_schema=target_schema)
    pipeline["staging_models"] = staging
    pipeline["mart_models"] = marts
    pipeline["contracts"] = contracts

    if metadata_store is not None:
        persist_models(metadata_store, staging + marts, source_name)
        persist_contracts(metadata_store, contracts)

    backend = DuckDBBackend(con)
    backend.ensure_schema(target_schema)
    backend.ensure_schema("marts")
    for model in marts:
        if model.status == "proposed":
            model.status = "approved"
    exec_results = run_models(backend, staging + marts, only_approved=True)
    pipeline["execution_results"] = exec_results

    if metadata_store is not None:
        persist_execution_results(metadata_store, exec_results)

    for contract in contracts:
        if contract.status == "proposed":
            contract.status = "observing"
    check_results = check_contracts(con, contracts, only_active=True)
    apply_contract_statuses(contracts, check_results)
    report = build_report(check_results)
    pipeline["quality_report"] = report
    quality_run_id = None
    if metadata_store is not None:
        quality_run_id = persist_quality_report(metadata_store, source_name, report)

    return {
        "tables_loaded": len(tables_loaded),
        "tables_discovered": len(discovery_result.tables),
        "profiles": len(discovery_result.profiles),
        "relationships": len(discovery_result.relationships),
        "staging_models": len(staging),
        "mart_models": len(marts),
        "contracts": len(contracts),
        "models_executed": len(exec_results),
        "models_succeeded": sum(1 for result in exec_results if result.success),
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
        "context_drift": context_drift,
        "profiling_policy": profiling_policy,
        "tables_skipped": skipped_tables,
        "tables_skipped_count": len(skipped_tables),
        **assets.summary,
    }
