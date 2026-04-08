"""Rich console output helpers for the CLI."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from headwater.core.models import (
    ContractRule,
    DiscoveryResult,
    ExecutionResult,
    GeneratedModel,
    QualityReport,
)

console = Console()


def show_discovery_summary(discovery: DiscoveryResult) -> None:
    """Print a summary of the discovery result."""
    console.print(f"\n[bold]Source:[/bold] {discovery.source.name} ({discovery.source.type})")

    table = Table(title="Discovered Tables")
    table.add_column("Table", style="cyan")
    table.add_column("Rows", justify="right")
    table.add_column("Columns", justify="right")
    table.add_column("Domain")
    table.add_column("Description", max_width=50)

    for t in discovery.tables:
        table.add_row(
            t.name,
            f"{t.row_count:,}",
            str(len(t.columns)),
            t.domain or "-",
            (t.description or "-")[:50],
        )
    console.print(table)

    if discovery.relationships:
        rel_table = Table(title="Detected Relationships")
        rel_table.add_column("From")
        rel_table.add_column("To")
        rel_table.add_column("Type")
        rel_table.add_column("Confidence", justify="right")
        for r in discovery.relationships:
            rel_table.add_row(
                f"{r.from_table}.{r.from_column}",
                f"{r.to_table}.{r.to_column}",
                r.type,
                f"{r.confidence:.0%}",
            )
        console.print(rel_table)

    if discovery.domains:
        console.print("\n[bold]Domains:[/bold]")
        for domain, tables in discovery.domains.items():
            console.print(f"  {domain}: {', '.join(tables)}")

    profile_count = len(discovery.profiles)
    console.print(f"\n[bold]{profile_count}[/bold] column profiles computed.")


def show_models(models: list[GeneratedModel]) -> None:
    """Print a summary of generated models."""
    table = Table(title="Generated Models")
    table.add_column("Model", style="cyan")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Sources")
    table.add_column("Questions", justify="right")

    for m in models:
        status_style = {
            "approved": "green",
            "proposed": "yellow",
            "rejected": "red",
            "executed": "blue",
        }.get(m.status, "")
        table.add_row(
            m.name,
            m.model_type,
            f"[{status_style}]{m.status}[/{status_style}]",
            ", ".join(m.source_tables[:3]),
            str(len(m.questions)) if m.questions else "-",
        )
    console.print(table)


def show_contracts(rules: list[ContractRule]) -> None:
    """Print a summary of quality contracts."""
    table = Table(title="Quality Contracts")
    table.add_column("Model", style="cyan")
    table.add_column("Column")
    table.add_column("Rule")
    table.add_column("Severity")
    table.add_column("Confidence", justify="right")
    table.add_column("Status")

    for r in rules:
        table.add_row(
            r.model_name,
            r.column_name or "(table)",
            r.rule_type,
            r.severity,
            f"{r.confidence:.0%}",
            r.status,
        )
    console.print(table)
    console.print(f"\n[bold]{len(rules)}[/bold] contracts generated.")


def show_execution_results(results: list[ExecutionResult]) -> None:
    """Print execution results."""
    table = Table(title="Execution Results")
    table.add_column("Model", style="cyan")
    table.add_column("Status")
    table.add_column("Rows", justify="right")
    table.add_column("Time (ms)", justify="right")
    table.add_column("Error", max_width=40)

    for r in results:
        status = "[green]OK[/green]" if r.success else "[red]FAIL[/red]"
        table.add_row(
            r.model_name,
            status,
            f"{r.row_count:,}" if r.row_count is not None else "-",
            f"{r.execution_time_ms:.0f}",
            r.error or "",
        )
    console.print(table)


def show_quality_report(report: QualityReport) -> None:
    """Print the quality report."""
    console.print("\n[bold]Quality Report[/bold]")
    console.print(
        f"  Total: {report.total_contracts}  "
        f"[green]Passed: {report.passed}[/green]  "
        f"[red]Failed: {report.failed}[/red]  "
        f"Skipped: {report.skipped}"
    )
    if report.failed > 0:
        console.print("\n[bold red]Failed contracts:[/bold red]")
        for r in report.results:
            if not r.passed:
                console.print(f"  [red]FAIL[/red] {r.model_name}: {r.message}")
