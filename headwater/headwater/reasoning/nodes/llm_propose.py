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
import logging
import time
from typing import Any

from headwater.core.store import HeadwaterStore
from headwater.knowledge.projection import KnowledgeProjection

logger = logging.getLogger(__name__)

_SYSTEM = (
    "You are a senior data analyst. Given a business GOAL and a DATA SCHEMA (tables, "
    "columns with inferred roles and descriptions, and the relationships between "
    "tables), propose 6 to 10 precise analytical questions that DIRECTLY serve the "
    "goal. Cover DIFFERENT facets of the goal — rankings, trends over time, and "
    "segment comparisons — not variations of one idea. Map the user's business terms "
    "to the CLOSEST ACTUAL columns in the schema (e.g. a goal about 'modality' may map "
    "to a column like 'exam_type'). When two tables are RELATED, prefer questions that "
    "JOIN them — a measure from one table grouped by a dimension from a related table "
    "is usually more insightful than a single-table count. The measure's table and the "
    "dimension's table must be the SAME table, or a pair listed under RELATIONSHIPS — "
    "never combine two tables that are not directly related. Each question must name "
    "one measure column and one grouping-or-time column, using ONLY columns listed in "
    'the schema. For how-many / most-popular questions set "measure": "count" (row '
    "count per group). NEVER invent columns. Respond with STRICT JSON, no prose:\n"
    '{"questions": [{"title": "...", "intent": "ranking|trend|segment", '
    '"measure": "table.column" | "count", "dimension": "table.column", "reason": "..."}]}'
)

_INTENTS = {"ranking", "trend", "segment"}

# The COLUMN is the last dot-component; the table itself may be schema-qualified
# ("data.dim_subscription.cancel_date" -> table "data.dim_subscription").
def _table_of(col_ref: str) -> str:
    return col_ref.rsplit(".", 1)[0] if "." in col_ref else ""


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


# Concepts worth offering the model as measures/dimensions/axes. Identifiers and
# unclassified columns are omitted to keep the prompt small and focused (faster,
# more reliable local-model inference).
_USEFUL_CONCEPTS = {"Measure", "Dimension", "Location", "TimeAnchor", "Code"}

_MAX_DESC_CHARS = 90


def build_schema_brief(
    store: HeadwaterStore, project_id: str, projection: KnowledgeProjection
) -> str:
    """Compact, I-3-safe schema: usable columns + role + description + relationships.

    Only concept-bearing columns (measures, dimensions, locations, time anchors,
    codes) are listed — that is everything a question can be built from.  Column
    DESCRIPTIONS (human- or AI-written data-dictionary entries) ride along: they
    are the analyst's curated meaning, and the single highest-value comprehension
    signal the model can get.
    """
    source, selected = _selected_tables(store, project_id)
    table_rows = {t["name"]: t.get("row_count") or 0 for t in store.get_tables(source)}
    # Tables PROVEN empty (profiled, zero rows) cannot evidence any answer —
    # exclude them so the model never builds questions on them. An unprofiled
    # row_count of 0 means "unknown", not empty, and stays in.
    profiled_tables = {p["table_name"] for p in store.get_profiles(source)}
    cmap = _concept_map(projection)
    lines: list[str] = []
    for t in selected:
        if table_rows.get(t) == 0 and t in profiled_tables:
            logger.info(
                "schema.brief: excluding %s — profiled as EMPTY (0 rows)", t
            )
            continue
        rows = []
        for c in store.get_columns(source, t):
            ref = f"{t}.{c['name']}"
            concept, hint = cmap.get(ref, ("", ""))
            if concept not in _USEFUL_CONCEPTS:
                continue
            desc = str(c.get("description") or "").strip()
            if len(desc) > _MAX_DESC_CHARS:
                desc = desc[: _MAX_DESC_CHARS - 1] + "…"
            tail = f" — {desc}" if desc else ""
            rows.append(f"  {ref} [{concept}{(' ' + hint) if hint else ''}]{tail}")
        if rows:
            count = table_rows.get(t)
            suffix = f" ({count:,} rows)" if count else ""
            lines.append(f"TABLE {t}{suffix}:")
            lines.extend(rows)
    rels = store.get_relationships(source)
    if rels:
        lines.append("RELATIONSHIPS (tables you may JOIN):")
        for r in rels:
            conf = float(r.get("confidence") or 0)
            lines.append(
                f"  {r['from_table']}.{r['from_column']} -> "
                f"{r['to_table']}.{r['to_column']} (confidence {conf:.2f})"
            )
    return "\n".join(lines)


_USER_Q_SYSTEM = (
    "You map ONE analytical question to the columns needed to answer it. Given a "
    "QUESTION and a DATA SCHEMA, pick exactly one measure column and one "
    "grouping-or-time column, using ONLY columns listed in the schema. Map the "
    "user's words to the closest real columns. Respond with STRICT JSON:\n"
    '{"title": "a clear question title", "measure": "table.column", '
    '"dimension": "table.column", "intent": "ranking|trend|segment"}\n'
    "If the question cannot be answered from these columns, return {}."
)


def map_user_question(
    store: HeadwaterStore,
    project_id: str,
    *,
    projection: KnowledgeProjection,
    provider: Any,
    question_text: str,
    brief: str | None = None,
) -> dict[str, Any] | None:
    """Map one user-typed question to verified columns, or None if it can't be."""
    if brief is None:
        brief = build_schema_brief(store, project_id, projection)
    prompt = f"QUESTION:\n{question_text.strip()}\n\nDATA SCHEMA:\n{brief}\n\nReturn JSON only."
    try:
        result = _invoke(provider, prompt, _USER_Q_SYSTEM)
    except Exception:
        return None
    if not isinstance(result, dict):
        return None

    measure = str(result.get("measure") or "").strip()
    dimension = str(result.get("dimension") or "").strip()
    title = str(result.get("title") or question_text).strip()

    source, selected = _selected_tables(store, project_id)
    real = {
        f"{t}.{c['name']}" for t in selected for c in store.get_columns(source, t)
    }
    if measure not in real or dimension not in real or measure == dimension:
        return None
    mt, dt = _table_of(measure), _table_of(dimension)
    if mt != dt:
        rel_pairs = {
            frozenset((r["from_table"], r["to_table"]))
            for r in store.get_relationships(source)
        }
        if frozenset((mt, dt)) not in rel_pairs:
            return None

    dim_is_time = _concept_map(projection).get(dimension, ("", ""))[0] == "TimeAnchor"
    if dim_is_time:
        intent, role = "trend", "event_ts"
    else:
        claimed = result.get("intent")
        intent = claimed if claimed in {"ranking", "segment"} else "segment"
        role = "categorical"
    return {
        "title": title,
        "intent": intent,
        "needed_columns": [measure, dimension],
        "col_roles": {measure: "measure", dimension: role},
        "reason": "Added by you, mapped to your columns.",
        "score": 0.9,
    }


def propose_and_verify(
    store: HeadwaterStore,
    project_id: str,
    *,
    projection: KnowledgeProjection,
    provider: Any,
    goal_text: str,
    brief: str | None = None,
    avoid_titles: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Ask the model for goal-grounded questions; return only verified ones.

    ``avoid_titles`` (e.g. the user's own kept questions, or a prior set being
    regenerated away from) is shown to the model so it proposes NEW angles
    instead of restating what already exists.
    """
    if brief is None:
        brief = build_schema_brief(store, project_id, projection)
    avoid = ""
    if avoid_titles:
        listed = "\n".join(f"- {t}" for t in avoid_titles[:12])
        avoid = f"\n\nALREADY ASKED (propose DIFFERENT questions):\n{listed}"
    prompt = f"GOAL:\n{goal_text.strip()}\n\nDATA SCHEMA:\n{brief}{avoid}\n\nReturn JSON only."
    logger.info(
        "question.propose: asking model — goal=%r, brief=%d chars (%d tables), "
        "avoiding %d existing title(s)",
        goal_text.strip()[:80],
        len(brief),
        brief.count("TABLE "),
        len(avoid_titles or []),
    )
    started = time.perf_counter()
    try:
        result = _invoke(provider, prompt, _SYSTEM)
    except Exception as exc:
        logger.warning(
            "question.propose: model call FAILED after %.1fs (%s) — falling back "
            "to deterministic traversal",
            time.perf_counter() - started,
            exc.__class__.__name__,
        )
        return []
    raw = result.get("questions") if isinstance(result, dict) else None
    if not isinstance(raw, list):
        logger.warning(
            "question.propose: model returned no 'questions' list after %.1fs "
            "(keys: %s) — falling back to deterministic traversal",
            time.perf_counter() - started,
            sorted(result.keys()) if isinstance(result, dict) else type(result).__name__,
        )
        return []
    logger.info(
        "question.propose: model returned %d candidate(s) in %.1fs — verifying",
        len(raw),
        time.perf_counter() - started,
    )

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
    avoided = {t.strip().lower() for t in (avoid_titles or [])}
    for q in raw[:12]:
        if not isinstance(q, dict):
            continue
        measure = str(q.get("measure") or "").strip()
        dimension = str(q.get("dimension") or "").strip()
        title = str(q.get("title") or "").strip()
        # Verify: real columns only (kills hallucinations), distinct measure/dim.
        # Every drop is logged with its reason — this is the comprehension trail.
        if not title:
            logger.info("question.verify: DROP (no title) measure=%s dim=%s", measure, dimension)
            continue
        if title.lower() in avoided:
            logger.info("question.verify: DROP %r — restates an existing question", title)
            continue
        # A row-count measure ("most popular X") groups by the dimension and
        # counts — no measure column needed or named.
        is_count = measure.lower().replace("(*)", "").strip() in {"count", "rows", "*"}
        if is_count:
            if dimension not in real:
                logger.info(
                    "question.verify: DROP %r — hallucinated column: %s",
                    title,
                    dimension,
                )
                continue
            d_concept = cmap.get(dimension, ("", ""))[0]
            if d_concept not in {"Dimension", "Code", "Location", "TimeAnchor"}:
                logger.info(
                    "question.verify: DROP %r — dimension %s is %s, not a "
                    "grouping axis",
                    title,
                    dimension,
                    d_concept or "unclassified",
                )
                continue
            dim_is_time = d_concept == "TimeAnchor"
            key = ("count", "count", dimension)
            if key in seen:
                logger.info("question.verify: DROP %r — duplicate count question", title)
                continue
            seen.add(key)
            logger.info(
                "question.verify: KEEP %r — row count by %s", title, dimension
            )
            specs.append(
                {
                    "title": title,
                    "intent": "trend" if dim_is_time else "ranking",
                    "needed_columns": [dimension],
                    "col_roles": {
                        dimension: "event_ts" if dim_is_time else "categorical"
                    },
                    "reason": str(
                        q.get("reason") or "Proposed from your goal and the schema."
                    ),
                    "unit": None,
                    "score": 0.9,
                }
            )
            continue
        if measure not in real or dimension not in real:
            missing = [c for c in (measure, dimension) if c not in real]
            logger.info(
                "question.verify: DROP %r — hallucinated column(s): %s",
                title,
                ", ".join(missing),
            )
            continue
        if measure == dimension:
            logger.info("question.verify: DROP %r — measure equals dimension", title)
            continue
        mt, dt = _table_of(measure), _table_of(dimension)
        # No fabricated joins: a cross-table pair needs a real relationship.
        if mt != dt and frozenset((mt, dt)) not in rel_pairs:
            logger.info(
                "question.verify: DROP %r — no relationship between %s and %s "
                "(only directly-related tables can be joined)",
                title,
                mt,
                dt,
            )
            continue
        # Ground the ROLES in the verified ontology, not the model's say-so:
        # the measure must be an aggregatable Measure (a flag, id, or timestamp
        # can never rank), and the dimension must be a real grouping axis.
        m_concept = cmap.get(measure, ("", ""))[0]
        d_concept = cmap.get(dimension, ("", ""))[0]
        if m_concept != "Measure":
            logger.info(
                "question.verify: DROP %r — measure %s is %s, not an "
                "aggregatable Measure",
                title,
                measure,
                m_concept or "unclassified",
            )
            continue
        if d_concept not in {"Dimension", "Code", "Location", "TimeAnchor"}:
            logger.info(
                "question.verify: DROP %r — dimension %s is %s, not a "
                "grouping axis",
                title,
                dimension,
                d_concept or "unclassified",
            )
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
            logger.info(
                "question.verify: DROP %r — duplicate of an accepted (%s, %s, %s)",
                title,
                intent,
                measure,
                dimension,
            )
            continue
        seen.add(key)
        logger.info(
            "question.verify: KEEP %r — %s of %s by %s%s",
            title,
            intent,
            measure,
            dimension,
            " [cross-table join]" if mt != dt else "",
        )
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
    cross = sum(
        1 for s in specs if len({_table_of(c) for c in s["needed_columns"]}) > 1
    )
    logger.info(
        "question.propose: %d of %d verified (%d cross-table, %d dropped)",
        len(specs),
        len(raw),
        cross,
        len(raw) - len(specs),
    )
    return specs
