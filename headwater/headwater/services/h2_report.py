"""Headwater 2 Markdown audit report generator.

Generates a goal-anchored, consultant-facing audit report that explains:
  - what the data can credibly answer
  - what it cannot answer and why
  - what decisions are needed from domain experts
  - per-question certification state with provenance

The report is the first Headwater 2 deliverable. It must be useful without the UI
and deterministic enough for golden tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from headwater.core.store import HeadwaterStore
from headwater.services.h2_readiness import ProjectReadinessReport, QuestionReadiness
from headwater.services.h2_resolve import ResolveCard

_STATE_STAMP: dict[str, str] = {
    "certified": "**Certified**",
    "draft": "*Draft*",
    "cannot_answer": "Cannot Answer",
    "demoted": "*Demoted*",
}

_PRIORITY_LABEL: dict[str, str] = {
    "high": "High",
    "medium": "Medium",
    "low": "Low",
}


@dataclass(slots=True)
class AuditReportInput:
    project_id: str
    project: dict[str, Any]
    source_name: str
    source_snapshot_id: str | None
    tables: list[dict[str, Any]]
    questions: list[dict[str, Any]]
    readiness: ProjectReadinessReport
    resolve_cards: list[ResolveCard]
    profiles_summary: dict[str, Any]
    generated_at: str


def build_report(
    store: HeadwaterStore,
    project_id: str,
    *,
    readiness: ProjectReadinessReport | None = None,
    resolve_cards: list[ResolveCard] | None = None,
) -> str:
    """Build and return the Markdown audit report."""
    from headwater.services.h2_readiness import evaluate_project_readiness
    from headwater.services.h2_resolve import build_resolve_cards

    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")

    source_name = project_sources[0]["source_name"]
    snapshot = store.get_latest_source_snapshot(source_name)
    snapshot_id = snapshot["id"] if snapshot else None
    tables = store.get_tables(source_name)
    questions = store.list_questions(project_id)
    profiles = store.get_profiles(source_name)

    if resolve_cards is None:
        resolve_cards = build_resolve_cards(store, project_id)
    if readiness is None:
        readiness = evaluate_project_readiness(store, project_id)

    profiles_summary = _summarize_profiles(tables, profiles)

    inputs = AuditReportInput(
        project_id=project_id,
        project=project,
        source_name=source_name,
        source_snapshot_id=snapshot_id,
        tables=tables,
        questions=questions,
        readiness=readiness,
        resolve_cards=resolve_cards,
        profiles_summary=profiles_summary,
        generated_at=datetime.now().isoformat(timespec="seconds"),
    )
    return _render(inputs)


def write_report(
    store: HeadwaterStore,
    project_id: str,
    output_path: Path | str,
    *,
    readiness: ProjectReadinessReport | None = None,
    resolve_cards: list[ResolveCard] | None = None,
) -> Path:
    """Write the audit report to a file and return its path."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    report_text = build_report(
        store,
        project_id,
        readiness=readiness,
        resolve_cards=resolve_cards,
    )
    path.write_text(report_text, encoding="utf-8")
    return path


def _render(inp: AuditReportInput) -> str:
    goal = inp.project.get("goal") or {}
    lines: list[str] = []

    lines += _section_header(inp, goal)
    lines += _section_data_overview(inp)
    lines += _section_questions(inp)
    lines += _section_data_trustworthiness(inp)
    lines += _section_resolve(inp)
    lines += _section_appendix(inp)

    return "\n".join(lines) + "\n"


def _section_header(inp: AuditReportInput, goal: dict[str, Any]) -> list[str]:
    display_name = inp.project.get("display_name") or inp.project_id
    lines = [
        f"# Headwater Audit Report: {display_name}",
        "",
        "## Project Goal",
        "",
        f"**Goal:** {goal.get('statement') or 'Not specified'}",
    ]
    if goal.get("decision"):
        lines.append(f"**Decision this informs:** {goal['decision']}")
    if goal.get("target_metric"):
        lines.append(f"**Target metric:** {goal['target_metric']}")
    if goal.get("time_horizon"):
        lines.append(f"**Time horizon:** {goal['time_horizon']}")
    if goal.get("entities"):
        entities = ", ".join(str(e) for e in goal["entities"])
        lines.append(f"**Entities:** {entities}")
    lines += [
        "",
        f"*Generated: {inp.generated_at}*  ",
        f"*Source snapshot: `{inp.source_snapshot_id or 'unknown'}`*",
        "",
    ]
    return lines


def _section_data_overview(inp: AuditReportInput) -> list[str]:
    ps = inp.profiles_summary
    total_rows = ps.get("total_rows", 0)
    lines = [
        "## Data Overview",
        "",
        f"**Source:** `{inp.source_name}`  ",
        f"**Tables:** {len(inp.tables)}  ",
        f"**Total rows (approx):** {total_rows:,}",
        "",
        "| Table | Rows | Columns | Key Columns |",
        "|---|---|---|---|",
    ]
    for table in inp.tables:
        key_cols = ", ".join(
            c for c in (ps.get("key_columns", {}).get(table["name"]) or [])
        )
        lines.append(
            f"| `{table['name']}` | {table.get('row_count', 0):,} "
            f"| {ps.get('column_counts', {}).get(table['name'], '?')} "
            f"| {key_cols or '—'} |"
        )
    lines.append("")
    return lines


def _section_questions(inp: AuditReportInput) -> list[str]:
    lines = ["## Proposed Questions", ""]

    readiness_by_id = {r.question_id: r for r in inp.readiness.questions}
    questions_by_answerability: dict[str, list[dict[str, Any]]] = {
        "answerable": [],
        "answerable_with_caveat": [],
        "cannot_answer": [],
    }
    for q in inp.questions:
        bucket = q.get("answerability", "answerable")
        if bucket not in questions_by_answerability:
            bucket = "answerable"
        questions_by_answerability[bucket].append(q)

    if questions_by_answerability["answerable"]:
        lines.append("### Can Answer")
        lines.append("")
        for q in questions_by_answerability["answerable"]:
            r = readiness_by_id.get(q["id"])
            stamp = _STATE_STAMP.get(r.state if r else "draft", "*Draft*")
            lines.append(f"**{q['title']}** — {stamp}")
            reason = q["question"].get("reason") or ""
            if reason:
                lines.append(f"> {reason}")
            if r:
                lines += _question_contract_summary(r)
            lines.append("")

    if questions_by_answerability["answerable_with_caveat"]:
        lines.append("### Can Answer (with Caveats)")
        lines.append("")
        for q in questions_by_answerability["answerable_with_caveat"]:
            r = readiness_by_id.get(q["id"])
            stamp = _STATE_STAMP.get(r.state if r else "draft", "*Draft*")
            lines.append(f"**{q['title']}** — {stamp}")
            reason = q["question"].get("reason") or ""
            if reason:
                lines.append(f"> {reason}")
            if r:
                lines += _question_contract_summary(r)
            lines.append("")

    if questions_by_answerability["cannot_answer"]:
        lines.append("### Cannot Answer")
        lines.append("")
        for q in questions_by_answerability["cannot_answer"]:
            lines.append(f"**{q['title']}** — Cannot Answer")
            reason = q["question"].get("reason") or ""
            if reason:
                lines.append(f"> {reason}")
            lines.append("")

    return lines


def _question_contract_summary(r: QuestionReadiness) -> list[str]:
    lines = []
    if r.state == "certified":
        lines.append(f"  - Readiness: {r.readiness_pct}% — all contracts pass")
    else:
        failed = [c for c in r.contracts if not c.passed]
        if failed:
            reasons = "; ".join(c.note for c in failed[:2])
            lines.append(f"  - Readiness: {r.readiness_pct}% — {reasons}")
        else:
            lines.append(f"  - Readiness: {r.readiness_pct}%")
    return lines


def _section_data_trustworthiness(inp: AuditReportInput) -> list[str]:
    ps = inp.profiles_summary
    lines = [
        "## What the Data Supports",
        "",
    ]

    # What it has
    have = ps.get("have") or []
    if have:
        lines.append("**Available evidence:**")
        for item in have:
            lines.append(f"- {item}")
        lines.append("")

    # Risky
    risky = ps.get("risky") or []
    if risky:
        lines.append("**Risky / needs review:**")
        for item in risky:
            lines.append(f"- {item}")
        lines.append("")

    # Missing
    missing_evidence = ps.get("missing") or []
    if missing_evidence:
        lines.append("**Missing:**")
        for item in missing_evidence:
            lines.append(f"- {item}")
        lines.append("")

    if not have and not risky and not missing_evidence:
        lines.append("*No specific evidence observations generated from profiles.*")
        lines.append("")

    return lines


def _section_resolve(inp: AuditReportInput) -> list[str]:
    if not inp.resolve_cards:
        return ["## Resolve Decisions", "", "*No outstanding decisions needed.*", ""]

    lines = ["## Resolve Decisions", ""]

    by_priority: dict[str, list[ResolveCard]] = {"high": [], "medium": [], "low": []}
    for card in inp.resolve_cards:
        by_priority.setdefault(card.priority, []).append(card)

    for priority in ("high", "medium", "low"):
        cards = by_priority.get(priority) or []
        if not cards:
            continue
        lines.append(f"### {_PRIORITY_LABEL[priority]} Priority")
        lines.append("")
        for card in cards:
            lines.append(f"**{card.title}**")
            lines.append(f"> {card.body}")
            if card.affected_questions:
                lines.append(
                    f"*Affects {len(card.affected_questions)} question(s). "
                    f"Clears: {', '.join(card.contract_impacts)}.*"
                )
            lines.append("")

    return lines


def _section_appendix(inp: AuditReportInput) -> list[str]:
    lines = [
        "## Evidence Appendix",
        "",
        "### Source Snapshot",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| Source | `{inp.source_name}` |",
        f"| Snapshot ID | `{inp.source_snapshot_id or 'unknown'}` |",
        f"| Tables | {len(inp.tables)} |",
        f"| Profiles | {inp.profiles_summary.get('profile_count', '?')} |",
        "",
        "### Question Readiness Detail",
        "",
    ]

    readiness_by_id = {r.question_id: r for r in inp.readiness.questions}
    for q in inp.questions:
        r = readiness_by_id.get(q["id"])
        if r is None:
            continue
        lines.append(f"**{q['title']}**")
        lines.append(f"- State: {r.state.replace('_', ' ').title()}")
        lines.append(f"- Readiness: {r.readiness_pct}%")
        for contract in r.contracts:
            mark = "PASS" if contract.passed else "FAIL"
            lines.append(f"  - [{mark}] {contract.contract_type}: {contract.note}")
        lines.append("")

    lines += [
        "### Contract Legend",
        "",
        "| Contract | Weight | Meaning |",
        "|---|---|---|",
        "| columns_profiled | 25% | All needed columns have a profile entry |",
        "| no_blocking_gaps | 25% | No high-priority open resolve items for needed columns |",
        "| structural_integrity | 20% | Needed columns have acceptable null rates |",
        "| no_misleading | 15% | No misleading quality patterns detected |",
        "| definition_consistent | 15% | No conflicting semantic definitions |",
        "",
        "---",
        f"*Report generated by Headwater 2 on {inp.generated_at}.*",
    ]
    return lines


def _summarize_profiles(
    tables: list[dict[str, Any]],
    profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    total_rows = sum(t.get("row_count") or 0 for t in tables)
    column_counts: dict[str, int] = {}
    key_columns: dict[str, list[str]] = {}

    profile_by_table: dict[str, list[dict[str, Any]]] = {}
    for p in profiles:
        profile_by_table.setdefault(p["table_name"], []).append(p)

    for table in tables:
        tname = table["name"]
        cols = profile_by_table.get(tname) or []
        column_counts[tname] = len(cols)
        key_cols = [
            p["column_name"]
            for p in cols
            if _is_key_candidate(p["column_name"], p.get("profile") or {})
        ]
        key_columns[tname] = key_cols[:3]

    have: list[str] = []
    risky: list[str] = []
    missing: list[str] = []

    temporal_cols = [
        f"{p['table_name']}.{p['column_name']}"
        for p in profiles
        if p.get("dtype", "").lower() in ("timestamp", "date", "datetime")
        or "time" in p["column_name"].lower()
        or "date" in p["column_name"].lower()
    ]
    if temporal_cols:
        have.append(f"Temporal coverage: {', '.join(temporal_cols[:3])}")

    high_null = [
        (
            f"{p['table_name']}.{p['column_name']} "
            f"({int(float(p['profile'].get('null_rate', 0) * 100))}% null)"
        )
        for p in profiles
        if float(p.get("profile", {}).get("null_rate") or 0.0) >= 0.20
    ]
    if high_null:
        risky.extend(high_null[:3])

    code_cols = [
        (
            f"{p['table_name']}.{p['column_name']} (codes: "
            f"{', '.join(str(v[0]) for v in (p['profile'].get('top_values') or [])[:4])})"
        )
        for p in profiles
        if _is_code_like_profile(p.get("profile") or {})
    ]
    if code_cols:
        risky.extend(code_cols[:3])

    return {
        "total_rows": total_rows,
        "column_counts": column_counts,
        "key_columns": key_columns,
        "profile_count": len(profiles),
        "have": have,
        "risky": risky,
        "missing": missing,
    }


def _is_key_candidate(col_name: str, profile: dict[str, Any]) -> bool:
    import re
    return bool(re.search(r"(^id$|_id$|^key$|_key$)", col_name.lower()))


def _is_code_like_profile(profile: dict[str, Any]) -> bool:
    dtype = (profile.get("dtype") or "").lower()
    if dtype not in ("varchar", "text", "string", "category", "object"):
        return False
    distinct = profile.get("distinct_count") or 0
    avg_len = profile.get("avg_length") or 0.0
    uniqueness = profile.get("uniqueness_ratio") or 0.0
    return (
        int(distinct) >= 2
        and int(distinct) <= 30
        and float(avg_len) <= 4.0
        and float(uniqueness) <= 0.05
    )
