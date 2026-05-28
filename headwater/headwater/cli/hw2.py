"""Headwater 2 CLI entry point."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from headwater import __version__

app = typer.Typer(
    name="hw2",
    help="Headwater 2 -- project-centric readiness workflow.",
    no_args_is_help=True,
)
project_app = typer.Typer(
    name="project",
    help="Project framing and relevance.",
    no_args_is_help=True,
)
app.add_typer(project_app, name="project")

console = Console()


@app.command()
def version() -> None:
    """Print the Headwater 2 version."""
    typer.echo(f"hw2 {__version__}")


@app.command()
def status() -> None:
    """Show the current Headwater 2 scaffold status."""
    from headwater.core.config import get_settings

    settings = get_settings()
    store = _open_h2_store()
    try:
        projects = store.list_projects()
    except Exception:
        projects = []
    finally:
        store.close()
    console.print("[bold]Headwater 2[/bold] scaffold is active.")
    console.print("  Scope: source -> project -> relevance -> readiness -> answer")
    console.print("  Entry points: metadata store, project spec, and dataset-agnostic guards")
    console.print(f"  Store: {settings.data_dir / 'h2_metadata.db'}")
    console.print(f"  Projects: {len(projects)}")


@app.command()
def discover(
    source: str = typer.Option(..., "--source", help="Data source path or DSN."),
    source_type: str | None = typer.Option(
        None,
        "--type",
        help="Source type: json, csv, duckdb, sqlite, postgres.",
    ),
    name: str | None = typer.Option(
        None,
        "--name",
        help="Optional source name. Defaults to the path or host-derived name.",
    ),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """Discover a source and persist H2 source-owned metadata."""
    from headwater.services.h2_source import discover_and_persist

    store = _open_h2_store(store_path)
    try:
        outcome = discover_and_persist(
            source,
            store=store,
            source_type=source_type,
            name=name,
        )
    finally:
        store.close()

    discovery = outcome.discovery
    console.print(
        f"Persisted {len(discovery.tables)} tables, "
        f"{len(discovery.profiles)} profiles, "
        f"{len(discovery.relationships)} relationships "
        f"to {store_path or _default_store_path()}"
    )
    console.print(f"Snapshot: {outcome.snapshot_id}")


@project_app.command("frame")
def project_frame(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    source_name: str = typer.Option(..., "--source", help="Registered H2 source name."),
    display_name: str = typer.Option(..., "--name", help="Display name for the project."),
    goal: str = typer.Option(..., "--goal", help="Plain-language goal statement."),
    decision: str | None = typer.Option(None, "--decision", help="Decision this project informs."),
    metric: str | None = typer.Option(None, "--metric", help="Target metric or outcome."),
    time_horizon: str | None = typer.Option(
        None,
        "--time-horizon",
        help="Optional time window or coverage expectation.",
    ),
    entity: list[str] = typer.Option([], "--entity", help="Entity or subject of analysis."),
    table: list[str] = typer.Option([], "--table", help="Preselected source table."),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """Persist a project spec and compute its initial relevance slice."""
    from headwater.core.config import get_settings
    from headwater.services.h2_project import frame_project, propose_relevance

    settings = get_settings()
    store = _open_h2_store(store_path)
    try:
        spec = frame_project(
            store=store,
            project_id=project_id,
            source_name=source_name,
            display_name=display_name,
            goal_statement=goal,
            selected_tables=table,
            decision=decision,
            target_metric=metric,
            entities=entity,
            time_horizon=time_horizon,
            settings=settings,
        )
        relevance = propose_relevance(store=store, project_id=project_id)
    finally:
        store.close()

    console.print(f"Framed project {spec.project_id} on source {spec.source_name}")
    console.print(f"Spec: {settings.data_dir / 'projects' / f'{project_id}.yaml'}")
    console.print(f"Selected tables: {', '.join(spec.selected_tables) or 'none yet'}")
    _print_relevance_result(relevance)


@project_app.command("relevance")
def project_relevance(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """Recompute project relevance and question proposals."""
    from headwater.services.h2_project import propose_relevance

    store = _open_h2_store(store_path)
    try:
        relevance = propose_relevance(store=store, project_id=project_id)
    finally:
        store.close()

    _print_relevance_result(relevance)


def _default_store_path() -> Path:
    from headwater.core.config import get_settings

    settings = get_settings()
    settings.ensure_dirs()
    return settings.data_dir / "h2_metadata.db"


def _open_h2_store(store_path: Path | None = None):
    from headwater.core.store import HeadwaterStore

    path = store_path or _default_store_path()
    store = HeadwaterStore(path)
    store.init()
    return store


def _print_relevance_result(relevance) -> None:
    console.print(f"Source snapshot: {relevance.source_snapshot_id or 'unknown'}")
    if relevance.selected_tables:
        console.print(f"Selected tables: {', '.join(relevance.selected_tables)}")
    if relevance.relevant_columns:
        console.print("\n[bold]Relevant columns[/bold]")
        for column in relevance.relevant_columns[:10]:
            selected = " [selected]" if column.selected else ""
            console.print(
                f"  {column.table_name}.{column.column_name} "
                f"({column.semantic_role or 'unknown'}) "
                f"score={column.score:.2f}{selected} -- {column.reason}"
            )
    if relevance.proposed_questions:
        console.print("\n[bold]Proposed questions[/bold]")
        for question in relevance.proposed_questions:
            console.print(
                f"  {question.answerability}: {question.title} "
                f"[{', '.join(question.needed_columns) or 'no columns'}]"
            )
            console.print(f"    {question.reason}")
    if relevance.notes:
        console.print("\n[bold]Notes[/bold]")
        for note in relevance.notes:
            console.print(f"  - {note}")
