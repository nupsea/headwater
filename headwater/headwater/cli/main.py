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

    from headwater.analyzer.companion import discover_companion_docs, match_docs_to_tables
    from headwater.analyzer.semantic import analyze
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

    from rich.panel import Panel

    console.print(
        Panel(
            f"[bold]Headwater Demo[/bold]\n\nDataset: {data_path}\n\n"
            "This demo runs the full discovery-to-quality pipeline:\n"
            "  1. Load data into an in-memory analytical engine\n"
            "  2. Profile columns, detect types, and discover relationships\n"
            "  3. Generate staging + mart SQL models and quality contracts\n"
            "  4. Execute staging models (auto-approved, no business logic)\n"
            "  5. Run quality contract checks in observation mode",
            title="Headwater",
            border_style="blue",
        )
    )

    # Step 1: Load data
    console.print(
        Panel(
            "[bold]Step 1: Load Data[/bold]\n\n"
            "Reading source files and registering them as tables in DuckDB.\n"
            "No data leaves your machine -- everything runs locally.",
            border_style="dim",
        )
    )
    source = SourceConfig(name=dataset, type="json", path=str(data_path))
    con = duckdb.connect(":memory:")
    connector = get_connector(source.type)
    connector.connect(source)
    tables_loaded = connector.load_to_duckdb(con, "raw")
    console.print(f"  Loaded {len(tables_loaded)} tables: {', '.join(tables_loaded)}")

    # Step 2: Discover
    console.print(
        Panel(
            "[bold]Step 2: Profile & Discover[/bold]\n\n"
            "Computing column-level statistics (nulls, uniqueness, ranges),\n"
            "detecting primary/foreign keys, and mapping relationships\n"
            "across tables -- all using generic heuristics.",
            border_style="dim",
        )
    )
    discovery = discover(con, "raw", source)

    # Companion doc discovery + semantic analysis
    companion_docs = discover_companion_docs(source)
    if companion_docs:
        table_names = [t.name for t in discovery.tables]
        match_docs_to_tables(companion_docs, table_names)
        discovery.companion_docs = companion_docs
    analyze(discovery)

    show_discovery_summary(discovery)

    # Step 3: Generate models + contracts
    console.print(
        Panel(
            "[bold]Step 3: Generate Models & Contracts[/bold]\n\n"
            "Creating staging models (mechanical transforms, auto-approved)\n"
            "and mart models (analytical, require human review).\n"
            "Quality contracts capture expectations from the profiling data.",
            border_style="dim",
        )
    )
    staging_models = generate_staging_models(discovery.tables, source_schema="raw")
    mart_models = generate_mart_models(discovery, target_schema="staging")
    contracts = generate_contracts(discovery.profiles)

    all_models = staging_models + mart_models
    show_models(all_models)
    show_contracts(contracts)

    # Step 4: Execute staging models (auto-approved)
    console.print(
        Panel(
            "[bold]Step 4: Execute Approved Models[/bold]\n\n"
            "Materializing staging models in DuckDB. Staging models are\n"
            "auto-approved because they contain no business logic (only\n"
            "rename, cast, deduplicate). Mart models stay proposed.",
            border_style="dim",
        )
    )
    backend = DuckDBBackend(con)
    backend.ensure_schema("staging")
    exec_results = run_models(backend, all_models, only_approved=True)
    show_execution_results(exec_results)

    # Step 5: Quality checks (move contracts to observing for demo)
    console.print(
        Panel(
            "[bold]Step 5: Quality Checks[/bold]\n\n"
            "Running quality contracts in observation mode. No contract\n"
            "skips the observation phase -- violations are tracked silently\n"
            "before enforcement.",
            border_style="dim",
        )
    )
    for c in contracts:
        c.status = "observing"
    check_results = check_contracts(con, contracts, only_active=True)
    report = build_report(check_results)
    show_quality_report(report)

    # Summary
    total_cols = sum(len(t.columns) for t in discovery.tables)
    total_rels = len(discovery.relationships)
    total_domains = len(discovery.domains)

    console.print(
        Panel(
            f"[bold green]Demo Complete[/bold green]\n\n"
            f"[bold]What happened:[/bold]\n"
            f"  Tables discovered:   {len(discovery.tables)}\n"
            f"  Columns profiled:    {total_cols}\n"
            f"  Relationships found: {total_rels}\n"
            f"  Domains detected:    {total_domains}\n"
            f"  Staging models:      {len(staging_models)} (executed)\n"
            f"  Mart models:         {len(mart_models)} (proposed, awaiting review)\n"
            f"  Quality contracts:   {len(contracts)} "
            f"({report.passed} passed, {report.failed} failed)\n\n"
            f"[bold]Next steps:[/bold]\n"
            f"  1. Start the API server:   uv run uvicorn headwater.api.app:app\n"
            f"  2. Open the UI:            cd ui && npm run dev\n"
            f"  3. Review mart models:     Each encodes business logic assumptions\n"
            f"  4. Discover your own data: headwater discover --source /path/to/data\n"
            f"  5. Connect a Postgres DB:  headwater discover --source postgres://...",
            title="Summary",
            border_style="green",
        )
    )


@app.command()
def discover(
    source: str = typer.Option(
        ...,
        "--source",
        help="Data source: path to directory or DSN (e.g. postgres://...).",
    ),
    source_type: str | None = typer.Option(
        None,
        "--type",
        help="Source type: json, csv, postgres. Auto-detected if omitted.",
    ),
    name: str | None = typer.Option(
        None,
        "--name",
        help="Name for this source (hostname for DB, dir name for files).",
    ),
    mode: str = typer.Option(
        "generate",
        "--mode",
        help="Mode: generate (default) or observe.",
    ),
) -> None:
    """Discover tables, profiles, and relationships from a data source."""
    import duckdb

    from headwater.analyzer.companion import discover_companion_docs, match_docs_to_tables
    from headwater.analyzer.semantic import analyze
    from headwater.cli.display import console, show_discovery_summary
    from headwater.connectors.registry import get_connector
    from headwater.core.config import get_settings
    from headwater.core.metadata import MetadataStore
    from headwater.core.models import SourceConfig
    from headwater.drift.schema import compare_schemas
    from headwater.profiler.engine import discover as run_discover

    # Validate mode
    if mode == "observe":
        console.print(
            "[red]Error:[/red] Observe mode is not yet implemented. "
            "Observe-mode connectors (Snowflake, BigQuery, Redshift) are planned for Phase 2."
        )
        raise typer.Exit(1)
    if mode not in ("generate", "observe"):
        console.print(f"[red]Error:[/red] Invalid mode '{mode}'. Use 'generate' or 'observe'.")
        raise typer.Exit(1)

    # Auto-detect source type from URI scheme
    detected_type = source_type
    source_uri = None
    source_path = None
    if detected_type is None:
        if source.startswith("postgres://") or source.startswith("postgresql://"):
            detected_type = "postgres"
        elif source.startswith("mysql://"):
            detected_type = "mysql"
        else:
            # Assume it's a file path; detect from contents
            data_path = Path(source).resolve()
            if data_path.is_dir():
                # Look for json or csv files
                json_files = list(data_path.glob("*.json"))
                csv_files = list(data_path.glob("*.csv"))
                if json_files:
                    detected_type = "json"
                elif csv_files:
                    detected_type = "csv"
                else:
                    detected_type = "json"  # Default
            else:
                detected_type = "csv" if source.endswith(".csv") else "json"

    # Determine source name
    if name is None:
        if detected_type == "postgres":
            from urllib.parse import urlparse

            parsed = urlparse(source)
            name = parsed.hostname or "postgres"
        else:
            name = Path(source).resolve().name

    # Build SourceConfig
    if detected_type == "postgres":
        source_uri = source
    else:
        source_path = str(Path(source).resolve())
        if not Path(source_path).exists():
            typer.echo(f"Error: path not found: {source_path}", err=True)
            raise typer.Exit(1)

    config = SourceConfig(
        name=name,
        type=detected_type,
        path=source_path,
        uri=source_uri,
        mode=mode,
    )

    con = duckdb.connect(":memory:")
    connector = get_connector(config.type)
    connector.connect(config)

    # For file-based connectors, load to DuckDB first
    schema_name = name.replace("-", "_").replace(".", "_")
    if config.type in ("json", "csv"):
        connector.load_to_duckdb(con, schema_name)

    console.print("Profiling...")
    discovery = run_discover(con, schema_name, config)

    # Companion doc discovery
    companion_docs = discover_companion_docs(config)
    if companion_docs:
        table_names = [t.name for t in discovery.tables]
        match_docs_to_tables(companion_docs, table_names)
        discovery.companion_docs = companion_docs
        console.print(f"  Found {len(companion_docs)} companion doc(s)")

    # Semantic analysis (heuristic enrichment + deep descriptions)
    analyze(discovery)

    show_discovery_summary(discovery)

    # -- Schema drift detection (US-401, US-402) ---------------------------
    # Non-fatal: drift tracking is advisory. Errors here do not abort the run.
    settings = get_settings()
    try:
        settings.ensure_dirs()
        store = MetadataStore(settings.metadata_db_path)
        try:
            store.init()
            store.upsert_source(name, detected_type, source_path, source_uri, mode=mode)
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

            # Persist semantic details and companion docs
            for table in discovery.tables:
                if table.semantic_detail:
                    store.upsert_semantic_detail(
                        table.name,
                        name,
                        table.semantic_detail.model_dump(),
                        run_id=run_id,
                    )
            for doc in discovery.companion_docs:
                store.upsert_companion_doc(
                    source_name=name,
                    filename=doc.filename,
                    content=doc.content,
                    doc_type=doc.doc_type,
                    matched_tables=doc.matched_tables,
                    confidence=doc.confidence,
                    run_id=run_id,
                )

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
def unlock(
    source_name: str = typer.Option(..., "--source", help="Source name."),
    table: str = typer.Option(..., "--table", help="Table name."),
    column: str = typer.Option(
        None,
        "--column",
        help="Column name. Omit or use --all to unlock all columns in the table.",
    ),
    all_columns: bool = typer.Option(False, "--all", help="Unlock all columns in the table."),
) -> None:
    """Unlock a column (or all columns in a table) to allow re-enrichment on the next run."""
    from headwater.cli.display import console
    from headwater.core.config import get_settings
    from headwater.core.metadata import MetadataStore

    settings = get_settings()
    settings.ensure_dirs()
    store = MetadataStore(settings.metadata_db_path)
    try:
        store.init()
        if all_columns or column is None:
            # Unlock all columns in the table
            cols = store.get_columns(table, source_name)
            if not cols:
                console.print(f"[red]No columns found for {source_name}.{table}[/red]")
                raise typer.Exit(1)
            count = 0
            for col in cols:
                if col["locked"]:
                    store.lock_column(table, source_name, col["name"], locked=False)
                    store.record_decision(
                        "column",
                        f"{source_name}.{table}.{col['name']}",
                        "unlocked",
                    )
                    count += 1
            console.print(f"Unlocked {count} column(s) in {source_name}.{table}")
        else:
            store.lock_column(table, source_name, column, locked=False)
            store.record_decision(
                "column",
                f"{source_name}.{table}.{column}",
                "unlocked",
            )
            console.print(f"Unlocked {source_name}.{table}.{column}")
    finally:
        store.close()


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
    from headwater.core.metadata import MetadataStore

    settings = get_settings()
    console.print(f"\n[bold]Headwater[/bold] v{__version__}")
    console.print(f"  Data dir:     {settings.data_dir}")
    console.print(f"  LLM provider: {settings.llm_provider}")
    console.print(f"  LLM model:    {settings.llm_model}")
    console.print(f"  Sample size:  {settings.sample_size:,}")
    console.print(f"  Metadata DB:  {settings.metadata_db_path}")
    console.print(f"  Analytical DB: {settings.analytical_db_path}")

    # Show connected sources from metadata store
    try:
        if settings.metadata_db_path.exists():
            store = MetadataStore(settings.metadata_db_path)
            try:
                store.init()
                sources = store.list_sources()
                if sources:
                    console.print("\n[bold]Connected Sources:[/bold]")
                    for src in sources:
                        src_type = src.get("type", "unknown")
                        src_mode = src.get("mode", "generate")
                        src_path = src.get("uri") or src.get("path") or ""
                        console.print(f"  {src['name']}: {src_type} ({src_mode}) -- {src_path}")
                else:
                    console.print("\n  No sources configured yet.")
            finally:
                store.close()
    except Exception:
        pass  # Status command should never fail due to metadata issues


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
