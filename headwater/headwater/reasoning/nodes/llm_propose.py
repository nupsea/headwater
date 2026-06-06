"""LLM question proposal, grounded and verified (the real comprehension step).

This is the "L proposes, D verifies" rule made concrete: the model reads the goal
and an I-3-safe schema brief (column names + inferred concept roles + relationships
— never raw rows) and proposes goal-grounded questions, mapping the user's business
vocabulary ("modality", "department") to the closest ACTUAL columns. Every proposal
is then verified deterministically: columns must exist (drop hallucinations) and a
cross-table pair must have a real relationship (no fabricated joins).

Falls back to nothing (caller uses the deterministic traversal) when the model is
absent, errors, times out, or returns no verifiable question.
"""

from __future__ import annotations

import asyncio
from typing import Any

from headwater.core.store import HeadwaterStore
from headwater.knowledge.projection import KnowledgeProjection

_SYSTEM = (
    "You are a senior data analyst. Given a business GOAL and a DATA SCHEMA (columns "
    "with inferred roles and the relationships between tables), propose 3 to 5 precise "
    "analytical questions that DIRECTLY serve the goal. Map the user's business terms "
    "to the CLOSEST ACTUAL columns in the schema (e.g. a goal about 'modality' may map "
    "to a column like 'exam_type' or 'patient_type'). Each question must name one "
    "measure column and one grouping-or-time column, using ONLY columns listed in the "
    "schema. NEVER invent columns. Respond with STRICT JSON, no prose:\n"
    '{"questions": [{"title": "...", "intent": "ranking|trend|segment", '
    '"measure": "table.column", "dimension": "table.column", "reason": "..."}]}'
)

_INTENTS = {"ranking", "trend", "segment"}


def _invoke(provider: Any, prompt: str, system: str) -> dict[str, Any]:
    """Call the async provider from sync code, tolerating a running loop."""
    try:
        return asyncio.run(provider.analyze(prompt, system))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(provider.analyze(prompt, system))
        finally:
            loop.close()


def _concept_map(projection: KnowledgeProjection) -> dict[str, tuple[str, str]]:
    out: dict[str, tuple[str, str]] = {}
    for n in projection.nodes_of_type(
        "Measure", "Dimension", "Location", "TimeAnchor", "Identifier", "Code"
    ):
        ref = str(n.props.get("ref") or n.id.removeprefix("col:"))
        hint = str(n.props.get("unit") or n.props.get("kind") or "")
        out[ref] = (n.type, hint)
    return out


def _selected_tables(store: HeadwaterStore, project_id: str) -> tuple[str, list[str]]:
    sources = store.get_project_sources(project_id)
    source = sources[0]["source_name"]
    tables = [t["name"] for t in store.get_tables(source)]
    selected = sources[0].get("selected_tables") or tables
    return source, list(selected)


def build_schema_brief(
    store: HeadwaterStore, project_id: str, projection: KnowledgeProjection
) -> str:
    """Compact, I-3-safe schema: columns + inferred role + table relationships."""
    source, selected = _selected_tables(store, project_id)
    cmap = _concept_map(projection)
    lines: list[str] = []
    for t in selected:
        lines.append(f"TABLE {t}:")
        for c in store.get_columns(source, t):
            ref = f"{t}.{c['name']}"
            concept, hint = cmap.get(ref, ("", ""))
            tag = f" [{concept}{(' ' + hint) if hint else ''}]" if concept else ""
            lines.append(f"  {ref}{tag}")
    rels = store.get_relationships(source)
    if rels:
        lines.append("RELATIONSHIPS:")
        for r in rels:
            conf = float(r.get("confidence") or 0)
            lines.append(
                f"  {r['from_table']}.{r['from_column']} -> "
                f"{r['to_table']}.{r['to_column']} (confidence {conf:.2f})"
            )
    return "\n".join(lines)


def propose_and_verify(
    store: HeadwaterStore,
    project_id: str,
    *,
    projection: KnowledgeProjection,
    provider: Any,
    goal_text: str,
    brief: str | None = None,
) -> list[dict[str, Any]]:
    """Ask the model for goal-grounded questions; return only verified ones."""
    if brief is None:
        brief = build_schema_brief(store, project_id, projection)
    prompt = f"GOAL:\n{goal_text.strip()}\n\nDATA SCHEMA:\n{brief}\n\nReturn JSON only."
    try:
        result = _invoke(provider, prompt, _SYSTEM)
    except Exception:
        return []
    raw = result.get("questions") if isinstance(result, dict) else None
    if not isinstance(raw, list):
        return []

    source, selected = _selected_tables(store, project_id)
    real: set[str] = set()
    for t in selected:
        for c in store.get_columns(source, t):
            real.add(f"{t}.{c['name']}")
    rel_pairs = {
        frozenset((r["from_table"], r["to_table"])) for r in store.get_relationships(source)
    }
    cmap = _concept_map(projection)  # ground the role in the verified concept

    specs: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for q in raw[:6]:
        if not isinstance(q, dict):
            continue
        measure = str(q.get("measure") or "").strip()
        dimension = str(q.get("dimension") or "").strip()
        title = str(q.get("title") or "").strip()
        # Verify: real columns only (kills hallucinations), distinct measure/dim.
        if not title or measure not in real or dimension not in real:
            continue
        if measure == dimension:
            continue
        mt, dt = measure.split(".")[0], dimension.split(".")[0]
        # No fabricated joins: a cross-table pair needs a real relationship.
        if mt != dt and frozenset((mt, dt)) not in rel_pairs:
            continue
        # Ground intent/role in the dimension's verified concept, not the model's
        # label: a non-time dimension can never be a temporal "trend".
        dim_is_time = cmap.get(dimension, ("", ""))[0] == "TimeAnchor"
        if dim_is_time:
            intent, role = "trend", "event_ts"
        else:
            claimed = q.get("intent")
            intent = claimed if claimed in {"ranking", "segment"} else "segment"
            role = "categorical"
        key = (intent, measure, dimension)
        if key in seen:
            continue
        seen.add(key)
        specs.append(
            {
                "title": title,
                "intent": intent,
                "needed_columns": [measure, dimension],
                "col_roles": {measure: "measure", dimension: role},
                "reason": str(q.get("reason") or "Proposed from your goal and the schema."),
                "unit": None,
                "score": 0.95,
            }
        )
    return specs
