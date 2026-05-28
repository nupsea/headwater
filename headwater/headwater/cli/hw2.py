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
resource_app = typer.Typer(
    name="resource",
    help="Resource intake and semantic claim fusion.",
    no_args_is_help=True,
)
app.add_typer(project_app, name="project")
app.add_typer(resource_app, name="resource")

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
    entity: list[str] = typer.Option([], "--entity", help="Entity or subject of analysis."),  # noqa: B008
    table: list[str] = typer.Option([], "--table", help="Preselected source table."),  # noqa: B008
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
    _print_bootstrap_hints(store_path, project_id)


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


@app.command()
def resolve(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """Build and display resolve cards for a project."""
    from headwater.services.h2_resolve import build_resolve_cards

    store = _open_h2_store(store_path)
    try:
        cards = build_resolve_cards(store, project_id)
    finally:
        store.close()

    if not cards:
        console.print("No resolve items — nothing outstanding.")
        return

    console.print(f"\n[bold]Resolve items for {project_id}[/bold] ({len(cards)} total)\n")
    for card in cards:
        color = "red" if card.priority == "high" else "yellow" if card.priority == "medium" else "dim"  # noqa: E501
        console.print(f"[{color}][{card.priority.upper()}][/{color}] {card.title}")
        console.print(f"  {card.body[:120]}...")
        if card.affected_questions:
            console.print(f"  Affects: {len(card.affected_questions)} question(s)")
        console.print()


@app.command()
def readiness(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """Evaluate and display per-question readiness for a project."""
    from headwater.services.h2_readiness import evaluate_project_readiness

    store = _open_h2_store(store_path)
    try:
        report = evaluate_project_readiness(store, project_id)
    finally:
        store.close()

    console.print(f"\n[bold]Readiness for {project_id}[/bold]\n")
    console.print(
        f"  Certified: {report.certified_count}  |  "
        f"Draft: {report.draft_count}  |  "
        f"Cannot answer: {report.cannot_answer_count}"
    )
    console.print()
    for q in report.questions:
        state_color = (
            "green" if q.state == "certified"
            else "red" if q.state == "cannot_answer"
            else "yellow"
        )
        console.print(
            f"[{state_color}]{q.state.upper().replace('_', ' ')}[/{state_color}] "
            f"({q.readiness_pct}%)  {q.question_id}"
        )
        if q.summary:
            console.print(f"  {q.summary[:120]}")
        console.print()


@app.command()
def report(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            help="Output file path. Defaults to <data_dir>/reports/<project_id>.md.",
        ),
    ] = None,
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
    print_report: bool = typer.Option(False, "--print", help="Print the report to stdout."),  # noqa: B008
) -> None:
    """Generate the Markdown audit report for a project."""
    from headwater.core.config import get_settings
    from headwater.services.h2_report import build_report, write_report

    settings = get_settings()
    store = _open_h2_store(store_path)
    try:
        if print_report:
            text = build_report(store, project_id)
            console.print(text)
        else:
            out_path = output or (
                settings.data_dir / "reports" / f"{project_id}.md"
            )
            written = write_report(store, project_id, out_path)
            console.print(f"Report written to: {written}")
    finally:
        store.close()


@app.command()
def answer(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """Draft SQL answers and chart specs for all project questions."""
    from headwater.services.h2_answer import draft_project_answers

    store = _open_h2_store(store_path)
    try:
        result = draft_project_answers(store, project_id)
    finally:
        store.close()

    console.print(
        f"\n[bold]Answers for {project_id}[/bold]  "
        f"Certified: {result.certified_count}  "
        f"Draft: {result.draft_count}  "
        f"Cannot answer: {result.cannot_answer_count}\n"
    )
    for ans in result.answers:
        state_color = (
            "green" if ans.state == "certified"
            else "red" if ans.state == "cannot_answer"
            else "yellow"
        )
        stamp = f"[{state_color}]{ans.state.upper().replace('_', ' ')}[/{state_color}]"
        console.print(f"{stamp} ({int(ans.confidence * 100)}%)  {ans.question_title}")
        if ans.caveats:
            for caveat in ans.caveats:
                console.print(f"  [yellow]Caveat:[/yellow] {caveat}")
        if ans.sql_text:
            console.print(f"  Chart: {ans.chart_spec.get('type', '?')}")
            console.print()
            for line in ans.sql_text.splitlines():
                console.print(f"    {line}")
        console.print()


@app.command()
def certify(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """Re-evaluate readiness and auto-demote certified questions whose contracts now fail."""
    from headwater.services.h2_certify import evaluate_and_certify

    store = _open_h2_store(store_path)
    try:
        report = evaluate_and_certify(store, project_id)
    finally:
        store.close()

    if report.snapshot_diff and report.snapshot_diff.has_changes:
        console.print(
            f"\n[yellow]Drift detected[/yellow] — "
            f"{len(report.snapshot_diff.profile_drifts)} profile change(s):"
        )
        for d in report.snapshot_diff.profile_drifts[:5]:
            console.print(f"  {d.description}")
    else:
        console.print("\nNo profile drift detected since the last snapshot.")

    if report.demotions:
        console.print(
            f"\n[red]Demoted {len(report.demotions)} question(s)[/red] "
            f"(previously certified):\n"
        )
        for rec in report.demotions:
            console.print(f"  [red]DEMOTED[/red]  {rec.question_title}")
            console.print(f"    Was certified under: {rec.prior_snapshot_id}")
            console.print(f"    Reason: {rec.drift_summary[:120]}")
            console.print()
    elif report.newly_certified:
        console.print(
            f"\n[green]{len(report.newly_certified)} question(s) newly certified.[/green]"
        )
    else:
        console.print(
            f"\n[green]All certified questions remain valid.[/green] "
            f"({len(report.unchanged)} unchanged)"
        )

    if report.newly_certified:
        for qid in report.newly_certified:
            console.print(f"  [green]CERTIFIED[/green]  {qid}")


@resource_app.command("add")
def resource_add(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    path: Path = typer.Option(..., "--path", help="Path to the resource file."),  # noqa: B008
    lock: bool = typer.Option(  # noqa: B008
        False,
        "--lock",
        help="Lock extracted definitions so they survive re-runs.",
    ),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """Ingest a resource file (Markdown, text, CSV) into the project's semantic layer."""
    from headwater.services.h2_resource import ingest_resource

    store = _open_h2_store(store_path)
    try:
        result = ingest_resource(store, project_id, path, lock_on_ingest=lock)
    finally:
        store.close()

    sens_label = (
        "[red]sensitive[/red]" if result.sensitivity == "sensitive" else "[green]safe[/green]"
    )
    console.print(f"Resource: {result.resource_path} ({result.resource_format}, {sens_label})")
    if result.sensitivity_notes:
        for note in result.sensitivity_notes:
            console.print(f"  [yellow]Warning:[/yellow] {note}")
    console.print(
        f"  Claims created: {result.claims_created}  "
        f"Updated: {result.claims_updated}  "
        f"Locked (skipped): {result.claims_skipped_locked}  "
        f"Conflicts: {result.conflicts_detected}"
    )
    if result.notes:
        for note in result.notes:
            console.print(f"  Note: {note}")


@resource_app.command("list")
def resource_list(
    project_id: str = typer.Option(..., "--project-id", help="Project identifier."),
    store_path: Annotated[
        Path | None,
        typer.Option(
            "--store",
            help="Optional path to the H2 SQLite store. Defaults to ~/.headwater/h2_metadata.db.",
        ),
    ] = None,
) -> None:
    """List resources registered for a project."""
    store = _open_h2_store(store_path)
    try:
        claim = store.get_semantic_claim(f"{project_id}:resource_registry")
    finally:
        store.close()

    if claim is None:
        console.print("No resources registered for this project.")
        return
    registry = claim.get("claim", {}).get("value") or []
    if not registry:
        console.print("No resources registered for this project.")
        return
    console.print(f"\n[bold]Resources for {project_id}[/bold] ({len(registry)} total)\n")
    for entry in registry:
        console.print(f"  {entry['format']:10} {entry['ingested_at']}  {entry['path']}")


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


def _print_bootstrap_hints(store_path: Path | None, project_id: str) -> None:
    """Print guidance when bootstrap claims were created from profile data."""
    store = _open_h2_store(store_path)
    try:
        claims = store.list_semantic_claims(project_id)
    finally:
        store.close()

    bootstrap_cols = [
        f"{c['table_name']}.{c['column_name']}"
        for c in claims
        if c.get("source") == "bootstrap:profile"
        and c.get("claim_type") == "enum_mapping"
    ]
    if not bootstrap_cols:
        return
    console.print(
        f"\n[bold]Bootstrap hints[/bold]: {len(bootstrap_cols)} code-like column(s) "
        "detected from profile data."
    )
    for col in bootstrap_cols[:5]:
        console.print(f"  {col}")
    if len(bootstrap_cols) > 5:
        console.print(f"  ... and {len(bootstrap_cols) - 5} more")
    console.print(
        "\nProvide definitions and code meanings with:\n"
        f"  hw2 resource add --project-id {project_id} --path <your-dictionary.md>"
    )


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
