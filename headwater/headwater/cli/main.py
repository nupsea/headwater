"""Headwater CLI -- entry point."""

from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(
    name="headwater",
    help="Headwater -- Advisory data platform for data professionals.",
    no_args_is_help=True,
)


@app.command()
def version() -> None:
    """Print the Headwater version."""
    from headwater import __version__

    typer.echo(f"headwater {__version__}")


@app.command()
def demo(
    dataset: str = typer.Option(
        "sample",
        help="Dataset to use: 'sample' or path to a data directory.",
    ),
    llm: bool = typer.Option(False, help="Enable LLM enrichment (requires API key)."),
) -> None:
    """Run the full Headwater demo: discover, generate, execute, and report."""
    import duckdb

    from headwater.analyzer.heuristics import build_domain_map, enrich_tables
    from headwater.cli.display import (
        console,
        show_contracts,
        show_discovery_summary,
        show_execution_results,
        show_models,
        show_quality_report,
    )
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

    # Resolve dataset path
    data_path = _resolve_data_path(dataset)
    if not data_path.exists():
        typer.echo(f"Error: data path not found: {data_path}", err=True)
        raise typer.Exit(1)

    console.print(f"\n[bold]Headwater Demo[/bold] -- dataset: {data_path}\n")

    # Step 1: Load data
    console.print("[bold]1/5[/bold] Loading data...")
    source = SourceConfig(name=dataset, type="json", path=str(data_path))
    con = duckdb.connect(":memory:")
    connector = get_connector(source.type)
    connector.connect(source)
    tables_loaded = connector.load_to_duckdb(con, "env_health")
    console.print(f"  Loaded {len(tables_loaded)} tables: {', '.join(tables_loaded)}")

    # Step 2: Discover
    console.print("[bold]2/5[/bold] Profiling and discovering relationships...")
    discovery = discover(con, "env_health", source)

    # Enrich with heuristics
    enrich_tables(discovery.tables, discovery.profiles, discovery.relationships)
    discovery.domains = build_domain_map(discovery.tables)

    show_discovery_summary(discovery)

    # Step 3: Generate models + contracts
    console.print("\n[bold]3/5[/bold] Generating models and contracts...")
    staging_models = generate_staging_models(
        discovery.tables, source_schema="env_health"
    )
    mart_models = generate_mart_models(discovery, target_schema="staging")
    contracts = generate_contracts(discovery.profiles)

    all_models = staging_models + mart_models
    show_models(all_models)
    show_contracts(contracts)

    # Step 4: Execute staging models (auto-approved)
    console.print("\n[bold]4/5[/bold] Executing approved models...")
    backend = DuckDBBackend(con)
    backend.ensure_schema("staging")
    exec_results = run_models(backend, all_models, only_approved=True)
    show_execution_results(exec_results)

    # Step 5: Quality checks (move contracts to observing for demo)
    console.print("\n[bold]5/5[/bold] Running quality checks...")
    for c in contracts:
        c.status = "observing"
    check_results = check_contracts(con, contracts, only_active=True)
    report = build_report(check_results)
    show_quality_report(report)

    console.print("\n[bold green]Demo complete.[/bold green]")
    console.print(
        f"  {len(staging_models)} staging models executed, "
        f"{len(mart_models)} mart models proposed for review."
    )


@app.command()
def discover_cmd(
    source_path: str = typer.Argument(..., help="Path to the data directory."),
    source_type: str = typer.Option("json", help="Source type: json, csv."),
    name: str = typer.Option("source", help="Name for this source."),
) -> None:
    """Discover tables, profiles, and relationships from a data source."""
    import duckdb

    from headwater.analyzer.heuristics import build_domain_map, enrich_tables
    from headwater.cli.display import console, show_discovery_summary
    from headwater.connectors.registry import get_connector
    from headwater.core.config import get_settings
    from headwater.core.metadata import MetadataStore
    from headwater.core.models import SourceConfig
    from headwater.drift.schema import compare_schemas
    from headwater.profiler.engine import discover

    data_path = Path(source_path).resolve()
    if not data_path.exists():
        typer.echo(f"Error: path not found: {data_path}", err=True)
        raise typer.Exit(1)

    source = SourceConfig(name=name, type=source_type, path=str(data_path))
    con = duckdb.connect(":memory:")
    connector = get_connector(source.type)
    connector.connect(source)
    connector.load_to_duckdb(con, "env_health")

    console.print("Profiling...")
    discovery = discover(con, "env_health", source)
    enrich_tables(discovery.tables, discovery.profiles, discovery.relationships)
    discovery.domains = build_domain_map(discovery.tables)

    show_discovery_summary(discovery)

    # -- Schema drift detection (US-401, US-402) ---------------------------
    # Non-fatal: drift tracking is advisory. Errors here do not abort the run.
    settings = get_settings()
    try:
        settings.ensure_dirs()
        store = MetadataStore(settings.metadata_db_path)
        try:
            store.init()
            store.upsert_source(name, source_type, str(data_path), None)
            run_id = store.start_run(name)

            # Build current snapshot
            snapshot: dict = {}
            for table in discovery.tables:
                snapshot[table.name] = {
                    "columns": [
                        {"name": col.name, "dtype": str(col.dtype), "nullable": col.nullable}
                        for col in table.columns
                    ],
                    "row_count": table.row_count,
                }

            # Get previous snapshot (before this run)
            prev_snapshot = store.get_latest_snapshot(name, before_run_id=run_id)

            # Save current snapshot
            store.save_snapshot(run_id, name, snapshot)
            store.finish_run(run_id, table_count=len(discovery.tables))

            # Compare and report drift
            run_id_from = None if prev_snapshot is None else run_id - 1
            diff = compare_schemas(prev_snapshot, snapshot, name, run_id_from, run_id)

            if diff.no_changes:
                console.print("[dim]No schema changes since last run.[/dim]")
            else:
                n_added = len(diff.tables_added)
                n_removed = len(diff.tables_removed)
                n_changed = len(diff.tables_changed)
                n_col_added = sum(
                    sum(1 for c in t.column_changes if c.change_type == "added")
                    for t in diff.tables_changed
                )
                n_col_removed = sum(
                    sum(1 for c in t.column_changes if c.change_type == "removed")
                    for t in diff.tables_changed
                )
                console.print(
                    f"[yellow]Schema drift detected:[/yellow] "
                    f"{n_changed} table(s) changed, "
                    f"{n_added} added, "
                    f"{n_removed} removed, "
                    f"{n_col_added} column(s) added, "
                    f"{n_col_removed} column(s) removed."
                )
                store.save_drift_report(
                    name,
                    diff.run_id_from,
                    run_id,
                    diff.model_dump(),
                )
        finally:
            store.close()
    except Exception as exc:
        console.print(f"[dim]Warning: drift tracking unavailable: {exc}[/dim]")


@app.command()
def generate(
    source_path: str = typer.Argument(..., help="Path to the data directory."),
    source_type: str = typer.Option("json", help="Source type: json, csv."),
    source_schema: str = typer.Option("env_health", help="Schema name for source data."),
    target_schema: str = typer.Option("staging", help="Target schema for models."),
) -> None:
    """Generate staging models, mart models, and quality contracts."""
    import duckdb

    from headwater.cli.display import console, show_contracts, show_models
    from headwater.connectors.registry import get_connector
    from headwater.core.models import SourceConfig
    from headwater.generator.contracts import generate_contracts
    from headwater.generator.marts import generate_mart_models
    from headwater.generator.staging import generate_staging_models
    from headwater.profiler.engine import discover

    data_path = Path(source_path).resolve()
    if not data_path.exists():
        typer.echo(f"Error: path not found: {data_path}", err=True)
        raise typer.Exit(1)

    source = SourceConfig(name="source", type=source_type, path=str(data_path))
    con = duckdb.connect(":memory:")
    connector = get_connector(source.type)
    connector.connect(source)
    connector.load_to_duckdb(con, source_schema)

    console.print("Discovering...")
    discovery = discover(con, source_schema, source)

    staging_models = generate_staging_models(
        discovery.tables, source_schema=source_schema, target_schema=target_schema
    )
    mart_models = generate_mart_models(discovery, target_schema=target_schema)
    contracts = generate_contracts(discovery.profiles)

    show_models(staging_models + mart_models)
    show_contracts(contracts)

    console.print(
        f"\n{len(staging_models)} staging (auto-approved), "
        f"{len(mart_models)} mart (proposed), "
        f"{len(contracts)} contracts generated."
    )


@app.command()
def status() -> None:
    """Show current Headwater status and configuration."""
    from headwater import __version__
    from headwater.cli.display import console
    from headwater.core.config import get_settings

    settings = get_settings()
    console.print(f"\n[bold]Headwater[/bold] v{__version__}")
    console.print(f"  Data dir:     {settings.data_dir}")
    console.print(f"  LLM provider: {settings.llm_provider}")
    console.print(f"  LLM model:    {settings.llm_model}")
    console.print(f"  Sample size:  {settings.sample_size:,}")
    console.print(f"  Metadata DB:  {settings.metadata_db_path}")
    console.print(f"  Analytical DB: {settings.analytical_db_path}")


def _resolve_data_path(dataset: str) -> Path:
    """Resolve a dataset name or path to an absolute path."""
    if dataset == "sample":
        # Look relative to the project root (../../data/sample from this file)
        candidates = [
            Path(__file__).resolve().parent.parent.parent.parent / "data" / "sample",
            Path.cwd() / "data" / "sample",
            Path.cwd().parent / "data" / "sample",
        ]
        for c in candidates:
            if c.exists():
                return c
        return candidates[0]  # Return first candidate for error message
    return Path(dataset).resolve()
