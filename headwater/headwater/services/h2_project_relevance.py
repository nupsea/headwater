"""H2 project relevance engine."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any

from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    Relationship,
    SourceConfig,
    TableInfo,
)
from headwater.core.store import HeadwaterStore
from headwater.services.h2_column_kinds import MEASURE_ROLES as _MEASURE_ROLES
from headwater.services.h2_project_types import (
    _CATEGORY_NAME_HINTS,
    _CATEGORY_ROLES,
    _GEOGRAPHIC_HINTS,
    _GOAL_QUALITY_HINTS,
    _GOAL_SEGMENT_HINTS,
    _GOAL_TIME_HINTS,
    _GOAL_WORKFLOW_HINTS,
    _RESOURCE_HINTS,
    _TEMPORAL_ROLE_PREFIXES,
    _TIME_NAME_HINTS,
    _WORKFLOW_HINTS,
    _WORKFLOW_NAME_HINTS,
    H2QuestionProposal,
    H2RelevanceResult,
    H2RelevantColumn,
)
from headwater.services.h2_project_types import (
    candidate_parts as _candidate_parts,
)
from headwater.services.h2_project_types import (
    friendly_name as _friendly_name,
)
from headwater.services.h2_project_types import (
    looks_code_like as _looks_code_like,
)
from headwater.services.h2_semantics import infer_source_semantics


def propose_relevance(
    *,
    store: HeadwaterStore,
    project_id: str,
) -> H2RelevanceResult:
    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered in the H2 store.")
    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' is not linked to any source.")

    source_name = project_sources[0]["source_name"]
    source_view = build_discovery_from_store(store, source_name)
    source_snapshot_id = (store.get_latest_source_snapshot(source_name) or {}).get("id")
    project_goal = _goal_from_project(project)

    # H2 semantic typing — replaces the H1 analyzer / context-suite dependency
    semantic_map = infer_source_semantics(store, source_name, project_id=project_id)

    selected_table_names = project_sources[0]["selected_tables"] or []
    goal_text = _goal_text(project_goal)

    # Load any resource-backed semantic claims and derive context from them.
    existing_claims = store.list_semantic_claims(project_id)
    resource_col_keys, resource_vocabulary = _resource_context_from_claims(existing_claims)

    goal_intents = _goal_intents(goal_text, resource_vocabulary=resource_vocabulary)
    relevance_scores = _score_relevance(
        source_view,
        semantic_map,
        goal_intents,
        selected_table_names,
        resource_col_keys=resource_col_keys,
    )
    relevant_columns = sorted(
        relevance_scores,
        key=lambda item: (-item.score, item.table_name, item.column_name),
    )
    for column in relevant_columns[:10]:
        store.upsert_semantic_claim(
            f"{project_id}:relevance:{column.table_name}.{column.column_name}",
            project_id=project_id,
            source_name=source_name,
            scope_type="column",
            table_name=column.table_name,
            column_name=column.column_name,
            claim_type="relevance",
            claim={
                "score": column.score,
                "reason": column.reason,
                "semantic_role": column.semantic_role,
                "selected": column.selected,
            },
            status="proposed",
            confidence=min(1.0, column.score / 5.0),
            source="relevance",
            locked=column.score >= 4.0,
        )
    top_questions = _maybe_engine_questions(
        store, project_id, source_name, source_snapshot_id, goal_text
    )
    if top_questions is None:
        top_questions = _build_question_proposals(
            source_view,
            semantic_map,
            project_goal,
            relevant_columns,
            selected_tables=selected_table_names,
            project_id=project_id,
            source_name=source_name,
            store=store,
            source_snapshot_id=source_snapshot_id,
        )

    notes: list[str] = []
    if not selected_table_names:
        notes.append("No tables were preselected; relevance inferred the source slice.")
    if any(q.answerability == "cannot_answer" for q in top_questions):
        notes.append("At least one question is intentionally blocked with a concrete reason.")

    return H2RelevanceResult(
        project_id=project_id,
        source_name=source_name,
        source_snapshot_id=source_snapshot_id,
        selected_tables=selected_table_names,
        relevant_columns=relevant_columns[:10],
        proposed_questions=top_questions,
        notes=notes,
    )


def build_discovery_from_store(store: HeadwaterStore, source_name: str) -> DiscoveryResult:
    source = store.get_source(source_name)
    if source is None:
        raise ValueError(f"Source '{source_name}' is not registered in the H2 store.")

    snapshot = store.get_latest_source_snapshot(source_name)
    snapshot_id = snapshot["id"] if snapshot else None
    table_rows = store.get_tables(source_name)
    profile_rows = _load_profile_rows(store, source_name, snapshot_id=snapshot_id)
    relationship_rows = _load_relationship_rows(store, source_name, snapshot_id=snapshot_id)

    tables: list[TableInfo] = []
    for row in table_rows:
        tables.append(
            TableInfo(
                name=row["name"],
                schema_name=row.get("schema_name"),
                row_count=int(row.get("row_count") or 0),
                columns=[
                    ColumnInfo(
                        name=col["name"],
                        dtype=col["dtype"],
                        nullable=bool(col.get("nullable", 1)),
                        is_primary_key=bool(col.get("is_primary_key", 0)),
                        description=col.get("description"),
                        semantic_type=col.get("semantic_type"),
                        role=col.get("semantic_type") or col.get("role"),
                        locked=bool(col.get("locked", 0)),
                    )
                    for col in store.get_columns(source_name, row["name"])
                ],
                description=row.get("description"),
                domain=row.get("domain"),
                locked=bool(row.get("locked", 0)),
            )
        )

    profiles = [ColumnProfile(**_profile_payload(row)) for row in profile_rows]
    relationships = [
        Relationship(
            from_table=row["from_table"],
            from_column=row["from_column"],
            to_table=row["to_table"],
            to_column=row["to_column"],
            type=row["rel_type"],
            confidence=float(row.get("confidence") or 0.0),
            referential_integrity=float(row.get("referential_integrity") or 0.0),
            source="declared" if float(row.get("confidence") or 0.0) >= 0.99 else "inferred_name",
        )
        for row in relationship_rows
    ]

    captured_at = (
        datetime.fromisoformat(snapshot["captured_at"].replace("Z", "+00:00"))
        if snapshot and snapshot.get("captured_at")
        else datetime.now()
    )
    return DiscoveryResult(
        source=SourceConfig(
            name=source["name"],
            type=source["type"],
            path=source.get("path"),
            uri=source.get("uri"),
            mode=source.get("mode", "generate"),
        ),
        tables=tables,
        profiles=profiles,
        relationships=relationships,
        discovered_at=captured_at,
    )


def _goal_from_project(project: dict[str, Any]) -> dict[str, Any]:
    return dict(project.get("goal") or {})


def _goal_text(goal: dict[str, Any]) -> str:
    parts = [
        str(goal.get("statement") or ""),
        str(goal.get("decision") or ""),
        str(goal.get("target_metric") or ""),
        " ".join(str(entity) for entity in goal.get("entities") or []),
        str(goal.get("time_horizon") or ""),
        " ".join(str(note) for note in goal.get("notes") or []),
    ]
    return " ".join(part for part in parts if part).strip().lower()


def _goal_intents(
    goal_text: str,
    *,
    resource_vocabulary: set[str] | None = None,
) -> set[str]:
    """Derive goal intents from the goal text plus any resource vocabulary.

    Resource vocabulary comes from user-provided definitions (ingested via S6).
    Those words flow through the same generic hint sets — no new domain logic
    is added here.
    """
    goal_tokens = set(re.findall(r"[a-z0-9]+", goal_text.lower()))
    # Resource vocabulary extends goal context without hardcoding domain terms.
    # e.g. if a data dictionary defines "workflow step" or "duration", those
    # words may trigger the same generic intent detection as if they were in
    # the goal text itself.
    all_tokens = goal_tokens | (resource_vocabulary or set())

    intents: set[str] = set()
    if all_tokens & _GOAL_TIME_HINTS:
        intents.add("time")
    if all_tokens & _GOAL_SEGMENT_HINTS:
        intents.add("segment")
    if all_tokens & _GOAL_WORKFLOW_HINTS:
        intents.add("workflow")
    if all_tokens & _GOAL_QUALITY_HINTS:
        intents.add("quality")
    if all_tokens & {"utilization", "throughput", "capacity"}:
        intents.add("utilization")
    if goal_tokens & {"compare", "breakdown", "longest", "highest", "worst"}:
        intents.add("compare")
    if all_tokens & {"entity", "customer", "subject", "device", "site"}:
        intents.add("entity")
    return intents


def _score_relevance(
    discovery: DiscoveryResult,
    semantic_map: dict[str, str],
    goal_intents: set[str],
    selected_tables: list[str],
    *,
    resource_col_keys: set[str] | None = None,
) -> list[H2RelevantColumn]:
    selected = set(selected_tables)
    known_cols = resource_col_keys or set()
    score_rows: list[H2RelevantColumn] = []
    for table in discovery.tables:
        for column in table.columns:
            profile = _profile_lookup(discovery, table.name, column.name)
            col_key = f"{table.name}.{column.name}".lower()
            role = semantic_map.get(col_key)
            score, reason = _score_column(
                table.name,
                column.name,
                role,
                profile,
                goal_intents,
                selected_tables=selected,
                resource_defined=col_key in known_cols,
            )
            score_rows.append(
                H2RelevantColumn(
                    table_name=table.name,
                    column_name=column.name,
                    semantic_role=role,
                    score=round(score, 3),
                    reason=reason,
                    selected=table.name in selected,
                )
            )
    return score_rows


def _score_column(
    table_name: str,
    column_name: str,
    role: str | None,
    profile: ColumnProfile | None,
    goal_intents: set[str],
    *,
    selected_tables: set[str],
    resource_defined: bool = False,
) -> tuple[float, str]:
    score = 0.0
    reasons: list[str] = []
    name = f"{table_name}.{column_name}".lower()
    normalized_role = (role or "").lower()

    if table_name in selected_tables:
        score += 2.0
        reasons.append("preselected")

    # Boost columns that the user has defined in a resource file.
    # This replaces hardcoded domain-specific column name hints —
    # the score comes from what the user told us, not from the engine.
    if resource_defined:
        score += 0.6
        reasons.append("resource-defined")

    if any(hint in name for hint in _GEOGRAPHIC_HINTS):
        score += 0.3
        reasons.append("geographic")
        if "location" in goal_intents or "entity" in goal_intents:
            score += 0.4
            reasons.append("geo-intent")
        return max(score, 0.0), ", ".join(reasons)

    if normalized_role in _TEMPORAL_ROLE_PREFIXES:
        score += 3.0
        reasons.append("temporal")
    elif normalized_role in _MEASURE_ROLES:
        score += 2.4
        reasons.append("measure")
    elif normalized_role in _CATEGORY_ROLES:
        score += 1.6
        reasons.append("category")
    elif normalized_role in {"identifier", "foreign_key", "entity"}:
        score += 1.3
        reasons.append("identity")
    elif normalized_role:
        score += 1.0
        reasons.append(normalized_role)

    if any(token in name for token in _WORKFLOW_HINTS) and "workflow" in goal_intents:
        score += 1.5
        reasons.append("workflow-term")
    if any(token in name for token in _RESOURCE_HINTS) and "utilization" in goal_intents:
        score += 1.4
        reasons.append("resource-term")
    if any(token in name for token in _GOAL_TIME_HINTS) and "time" in goal_intents:
        score += 1.1
        reasons.append("time-term")
    if any(token in name for token in _GOAL_SEGMENT_HINTS) and "segment" in goal_intents:
        score += 1.0
        reasons.append("segment-term")
    if any(token in name for token in _GOAL_QUALITY_HINTS) and "quality" in goal_intents:
        score += 1.0
        reasons.append("quality-term")

    if profile is not None:
        if profile.null_rate and profile.null_rate > 0.25:
            score -= 0.5
            reasons.append("high-null")
        if profile.uniqueness_ratio and profile.uniqueness_ratio >= 0.95:
            score += 0.4
            reasons.append("high-uniqueness")
        if profile.min_date and profile.max_date:
            score += 0.4
            reasons.append("temporal-span")
        if profile.distinct_count and profile.distinct_count <= 8:
            score += 0.2
            reasons.append("low-cardinality")

    if not reasons:
        reasons.append("weak-signal")
    return max(score, 0.0), ", ".join(reasons)


def _build_question_proposals(
    discovery: DiscoveryResult,
    semantic_map: dict[str, str],
    goal: dict[str, Any],
    relevant_columns: list[H2RelevantColumn],
    *,
    selected_tables: list[str],
    project_id: str,
    source_name: str,
    store: HeadwaterStore,
    source_snapshot_id: str | None,
) -> list[H2QuestionProposal]:
    all_profiles = {(p.table_name, p.column_name): p for p in discovery.profiles}
    top_by_table = _top_columns_by_table(relevant_columns)
    goal_text = _goal_text(goal)
    target_metric = str(goal.get("target_metric") or "").strip() or _infer_metric_label(
        relevant_columns,
        all_profiles,
    )
    focus_table = _select_focus_table(selected_tables, relevant_columns, goal_text=goal_text)
    proposals: list[H2QuestionProposal] = []

    time_candidate = (
        _find_named_column_in_table(discovery, focus_table, _TIME_NAME_HINTS)
        or _find_named_column(discovery, _TIME_NAME_HINTS)
        or _first_column_in_table(top_by_table, focus_table, _TEMPORAL_ROLE_PREFIXES)
        or _first_column(top_by_table, _TEMPORAL_ROLE_PREFIXES)
    )
    measure_candidate = _find_metric_column(
        discovery,
        relevant_columns,
        focus_table=focus_table,
        target_metric=target_metric,
    ) or _first_measure_candidate(top_by_table, preferred_table=focus_table)
    # ``target_metric`` (from the goal / first numeric column) seeded the search,
    # but the title must name the column we actually measure — otherwise the
    # question and its SQL diverge.  Derive the label from the chosen measure.
    metric_label = _metric_label(measure_candidate, target_metric)
    # When the user explicitly named the metric AND it refers to the column we
    # chose, keep their richer wording (e.g. a two-word label) over the column's
    # bare name.  It still names the same measure, so they can't diverge.
    goal_metric = str(goal.get("target_metric") or "").strip()
    if goal_metric and measure_candidate and _label_matches_column(goal_metric, measure_candidate):
        metric_label = goal_metric
    category_candidate = (
        _find_named_column_in_table(
            discovery,
            focus_table,
            _CATEGORY_NAME_HINTS,
        )
        or _find_named_column(discovery, _CATEGORY_NAME_HINTS)
        or _first_column_in_table(top_by_table, focus_table, _CATEGORY_ROLES)
        or _first_column(top_by_table, _CATEGORY_ROLES)
    )
    workflow_candidate = _find_named_column_in_table(
        discovery,
        focus_table,
        _WORKFLOW_NAME_HINTS,
    )
    if workflow_candidate is None and focus_table is None:
        workflow_candidate = _find_named_column(
            discovery, _WORKFLOW_NAME_HINTS
        ) or _first_workflow_candidate(top_by_table, preferred_table=focus_table)
    resource_candidate = (
        _find_named_column_in_table(
            discovery,
            focus_table,
            _RESOURCE_HINTS,
        )
        or _find_named_column(discovery, _RESOURCE_HINTS)
        or _first_resource_candidate(
            top_by_table,
            preferred_table=focus_table,
        )
    )
    entity_candidate = _find_entity_candidate(
        discovery,
        focus_table=focus_table,
        goal_text=goal_text,
    )
    coverage_days = _coverage_days(discovery)

    if time_candidate and measure_candidate:
        proposals.append(
            _persist_question(
                store,
                project_id,
                source_name,
                question_id="when-worst",
                title=f"How does {metric_label} change over time?",
                answerability="answerable",
                reason="Temporal columns and a measurable signal are available.",
                needed_columns=[time_candidate, measure_candidate],
                confidence=0.9,
                col_roles={_cr(time_candidate): "event_ts", _cr(measure_candidate): "measure"},
            )
        )

    if category_candidate and measure_candidate:
        _, category_name = _candidate_parts(category_candidate)
        category_label = _friendly_name(category_name)
        answerability = (
            "answerable_with_caveat" if _looks_code_like(category_name) else "answerable"
        )
        caveat = (
            "Category appears code-like; mapping may need review."
            if answerability == "answerable_with_caveat"
            else "Segmentation is supported by category and measure columns."
        )
        proposals.append(
            _persist_question(
                store,
                project_id,
                source_name,
                question_id="which-segment",
                title=f"Which {category_label} has the highest {metric_label}?",
                answerability=answerability,
                reason=caveat,
                needed_columns=[category_candidate, measure_candidate],
                confidence=0.84 if answerability == "answerable" else 0.74,
                col_roles={
                    _cr(category_candidate): "categorical",
                    _cr(measure_candidate): "measure",
                },
            )
        )

    if entity_candidate and measure_candidate:
        _, entity_name = _candidate_parts(entity_candidate)
        proposals.append(
            _persist_question(
                store,
                project_id,
                source_name,
                question_id="entity-ranking",
                title=f"Which {entity_name.replace('_id', '')} has the lowest {metric_label}?",
                answerability="answerable",
                reason="The source includes a stable entity key and a measurable outcome.",
                needed_columns=[entity_candidate, measure_candidate],
                confidence=0.81,
                col_roles={
                    _cr(entity_candidate): "identifier",
                    _cr(measure_candidate): "measure",
                },
            )
        )

    cross = _cross_table_question(
        store,
        project_id,
        source_name,
        top_by_table,
        source_snapshot_id,
        existing=[set(p.needed_columns) for p in proposals],
    )
    if cross is not None:
        proposals.append(cross)

    if coverage_days is not None:
        if coverage_days >= 14 and time_candidate:
            proposals.append(
                _persist_question(
                    store,
                    project_id,
                    source_name,
                    question_id="week-over-week",
                    title=f"Has {metric_label} changed week-over-week?",
                    answerability="answerable",
                    reason="At least two weeks of temporal coverage are present.",
                    needed_columns=(
                        [time_candidate, measure_candidate]
                        if measure_candidate
                        else [time_candidate]
                    ),
                    confidence=0.7,
                    col_roles={
                        _cr(time_candidate): "event_ts",
                        **({_cr(measure_candidate): "measure"} if measure_candidate else {}),
                    },
                )
            )
        else:
            proposals.append(
                _persist_question(
                    store,
                    project_id,
                    source_name,
                    question_id="week-over-week",
                    title=f"Has {metric_label} changed week-over-week?",
                    answerability="cannot_answer",
                    reason=(
                        "Week-over-week needs at least 14 days of coverage; "
                        "the source is shorter than that."
                    ),
                    needed_columns=[time_candidate] if time_candidate else [],
                    confidence=0.2,
                    snapshot_id=source_snapshot_id,
                    is_gap=True,
                    col_roles={_cr(time_candidate): "event_ts"} if time_candidate else {},
                )
            )

    if resource_candidate and measure_candidate and "utilization" in goal_text:
        _, resource_name = _candidate_parts(resource_candidate)
        resource_label = _friendly_name(resource_name)
        proposals.append(
            _persist_question(
                store,
                project_id,
                source_name,
                question_id="bottleneck",
                title=f"Which {resource_label} is the bottleneck?",
                answerability="answerable_with_caveat",
                reason=(
                    "Resource-like columns exist, but bottleneck framing may need capacity context."
                ),
                needed_columns=[resource_candidate, measure_candidate],
                confidence=0.72,
                col_roles={
                    _cr(resource_candidate): "categorical",
                    _cr(measure_candidate): "measure",
                },
            )
        )

    if (
        workflow_candidate
        and measure_candidate
        and not any(q.question_id.endswith("workflow-step") for q in proposals)
    ):
        proposals.append(
            _persist_question(
                store,
                project_id,
                source_name,
                question_id="workflow-step",
                title=f"Which workflow step contributes most to {metric_label}?",
                answerability="answerable_with_caveat",
                reason=(
                    "Workflow-like columns exist, but the frame may still "
                    "need business confirmation."
                ),
                needed_columns=[workflow_candidate, measure_candidate],
                confidence=0.64,
                col_roles={
                    _cr(workflow_candidate): "categorical",
                    _cr(measure_candidate): "measure",
                },
            )
        )

    if not proposals and relevant_columns:
        best = relevant_columns[0]
        fallback_label = target_metric or _friendly_name(best.column_name)
        proposals.append(
            _persist_question(
                store,
                project_id,
                source_name,
                question_id="fallback",
                title=f"What drives {fallback_label} in this source?",
                answerability="answerable_with_caveat",
                reason=(
                    "The source is relevant, but the goal needs a more "
                    "specific framing to ask a sharper question."
                ),
                needed_columns=[best],
                confidence=0.5,
            )
        )

    return proposals[:5]


def _table_measure(top_by_table: dict[str, list[H2RelevantColumn]], table: str) -> str | None:
    """A measure column strictly within ``table`` (no cross-table fallback)."""
    for column in top_by_table.get(table, []):
        role = (column.semantic_role or "").lower()
        name = column.column_name.lower()
        if role in _MEASURE_ROLES and not any(h in name for h in _GEOGRAPHIC_HINTS):
            return f"{column.table_name}.{column.column_name}"
    return None


def _cross_table_question(
    store: HeadwaterStore,
    project_id: str,
    source_name: str,
    top_by_table: dict[str, list[H2RelevantColumn]],
    snapshot_id: str | None,
    *,
    existing: list[set[str]] | None = None,
) -> H2QuestionProposal | None:
    """Propose a JOIN-based question when two related tables let us segment a
    measure in one by a dimension in the other.

    Entirely relationship- and role-driven — no dataset-specific assumptions: it
    only fires when a detected relationship (confidence >= 0.80) links a table
    holding a measure to a table holding a categorical dimension.  The answer
    builder emits the JOIN from the same relationship.  Skips a pair that another
    proposal already covers (so it never duplicates an existing question).
    """
    existing = existing or []
    relationships = sorted(
        store.get_relationships(source_name),
        key=lambda r: float(r.get("confidence") or 0.0),
        reverse=True,
    )
    for rel in relationships:
        conf = float(rel.get("confidence") or 0.0)
        if conf < 0.80:
            continue
        ta, tb = rel.get("from_table"), rel.get("to_table")
        if not ta or not tb or ta == tb:
            continue
        # A measure in one table, a dimension in the other (try both directions).
        for measure_tbl, dim_tbl in ((ta, tb), (tb, ta)):
            measure = _table_measure(top_by_table, measure_tbl)
            dim = _first_column_in_table(top_by_table, dim_tbl, _CATEGORY_ROLES)
            if not measure or dim is None:
                continue
            if {_cr(dim), measure} in existing:
                continue  # another question already covers this exact pair
            measure_label = _friendly_name(_candidate_parts(measure)[1])
            dim_label = _friendly_name(dim.column_name)
            return _persist_question(
                store,
                project_id,
                source_name,
                question_id="cross-segment",
                title=f"How does {measure_label} vary by {dim_label}?",
                answerability="answerable",
                reason=(
                    f"'{measure_tbl}' and '{dim_tbl}' are linked by a "
                    f"{int(conf * 100)}%-confidence relationship, so the measure "
                    "can be segmented across the related dimension via a join."
                ),
                needed_columns=[_cr(dim), measure],
                confidence=round(min(0.8, conf), 2),
                snapshot_id=snapshot_id,
                col_roles={_cr(dim): "categorical", _cr(measure): "measure"},
            )
    return None


def _metric_label(measure_candidate: str | None, fallback: str) -> str:
    """The label a question title uses for its measure.

    It MUST name the column the answer actually aggregates, or the question asks
    about one thing while the SQL computes another (observed bug: a title saying
    "hour day of arrival" while the query averaged ``total_duration``).  When no
    measure column is chosen, fall back to the inferred/goal label.
    """
    if not measure_candidate:
        return fallback
    _, name = _candidate_parts(measure_candidate)
    return _friendly_name(name)


def _label_matches_column(label: str, measure_candidate: str) -> bool:
    """True when an explicit metric label shares a word with the measure column.

    Used to decide whether the user's wording names the same thing as the chosen
    column — if so we keep their wording; if not we fall back to the column's own
    name so the title never misnames the measure.
    """
    _, col = _candidate_parts(measure_candidate)
    label_tokens = set(re.findall(r"[a-z0-9]+", label.lower()))
    col_tokens = set(re.findall(r"[a-z0-9]+", col.lower()))
    return bool(label_tokens & col_tokens)


def _cr(col: H2RelevantColumn | str | None) -> str:
    """Return a 'table.column' string from either a column object or a ref string."""
    if col is None:
        return ""
    if isinstance(col, H2RelevantColumn):
        return f"{col.table_name}.{col.column_name}"
    return str(col)


def _proposal_from_row(q: dict[str, Any]) -> H2QuestionProposal:
    """Rebuild a proposal from a stored question row (the keep-stable path)."""
    payload = q.get("question") or {}
    return H2QuestionProposal(
        question_id=str(q["id"]),
        title=str(q.get("title") or payload.get("title") or ""),
        answerability=str(q.get("answerability") or payload.get("answerability") or "answerable"),
        reason=str(payload.get("reason") or ""),
        needed_columns=list(payload.get("needed_columns") or []),
        confidence=float(q.get("confidence") or 0.0),
    )


def _maybe_engine_questions(
    store: HeadwaterStore,
    project_id: str,
    source_name: str,
    snapshot_id: str | None,
    goal_text: str,
) -> list[H2QuestionProposal] | None:
    """Goal-aware questions from the reasoning engine, when enabled.

    Stability is the contract: the engine question set is generated ONCE per goal
    and then left alone. Answering a question, defining a term, resolving an item,
    or any recompute that does not change the goal returns the EXACT same questions
    (and keeps their verdicts/answers). Only a new or edited goal regenerates them.
    This prevents the confusing "questions vanish as I work" behavior — a recompute
    must never silently rewrite the question set the user is acting on.

    Returns ``None`` (callers fall back to heuristic templates) only when the engine
    is off, or when there are no questions yet and the engine produces none.
    """
    from headwater.core.config import get_settings

    settings = get_settings()
    if not getattr(settings, "reasoning_engine", False):
        return None

    from headwater.reasoning.cache import NodeCache
    from headwater.reasoning.types import stable_hash

    cache = NodeCache(store)
    goal_sig = stable_hash(goal_text or "")
    existing = [
        q
        for q in store.list_questions(project_id)
        if str(q["id"]).startswith(f"{project_id}:rq")
    ]
    prev_sig = cache.get("engine.goalsig", project_id)

    # Already generated for this goal -> keep them exactly as they are.
    if existing and prev_sig == goal_sig:
        return [_proposal_from_row(q) for q in existing]

    from headwater.reasoning.nodes import run_question_vertical

    specs = run_question_vertical(store, project_id, settings=settings)
    if not specs:
        # Engine produced nothing this run; never wipe an existing set to templates.
        return [_proposal_from_row(q) for q in existing] if existing else None

    # New or changed goal: (re)generate the engine question set.
    desired_ids = {f"{project_id}:rq{i}" for i in range(len(specs))}
    stale = [q["id"] for q in existing if q["id"] not in desired_ids]
    store.delete_questions(stale)

    proposals: list[H2QuestionProposal] = []
    for i, spec in enumerate(specs):
        proposals.append(
            _persist_question(
                store,
                project_id,
                source_name,
                question_id=f"rq{i}",
                title=str(spec.get("title") or f"Question {i + 1}"),
                answerability="answerable",
                reason=str(spec.get("reason") or "Goal-aware question from the reasoning engine."),
                needed_columns=list(spec.get("needed_columns") or []),
                confidence=float(spec.get("score") or 0.8),
                snapshot_id=snapshot_id,
                col_roles=dict(spec.get("col_roles") or {}),
            )
        )
    cache.put("engine.goalsig", project_id, goal_sig)
    return proposals


def _persist_question(
    store: HeadwaterStore,
    project_id: str,
    source_name: str,
    *,
    question_id: str,
    title: str,
    answerability: str,
    reason: str,
    needed_columns: list[H2RelevantColumn | str],
    confidence: float,
    snapshot_id: str | None = None,
    is_gap: bool = False,
    col_roles: dict[str, str] | None = None,
) -> H2QuestionProposal:
    full_id = f"{project_id}:{question_id}"
    needed = [
        f"{col.table_name}.{col.column_name}" if isinstance(col, H2RelevantColumn) else str(col)
        for col in needed_columns
    ]
    store.upsert_question(
        full_id,
        project_id=project_id,
        title=title,
        question={
            "title": title,
            "reason": reason,
            "needed_columns": needed,
            "col_roles": col_roles or {},
            "answerability": answerability,
            "source_snapshot_id": snapshot_id,
        },
        source_name=source_name,
        status="cannot_answer" if answerability == "cannot_answer" else "draft",
        answerability=answerability,
        confidence=confidence,
    )
    if is_gap or answerability == "cannot_answer":
        store.upsert_resolve_item(
            f"{project_id}:{question_id}:gap",
            project_id=project_id,
            question_id=full_id,
            issue_kind="insufficient_coverage",
            title=title,
            body=reason,
            # A coverage/data limitation is informational — the analyst can't fix
            # it by defining a term, so it never ranks as a high-priority ask.
            priority="low",
            status="open",
            payload={
                "needed_columns": needed,
                "answerability": answerability,
                "category": "limitation",
            },
        )
    return H2QuestionProposal(
        question_id=full_id,
        title=title,
        answerability=answerability,
        reason=reason,
        needed_columns=needed,
        confidence=confidence,
    )


def _top_columns_by_table(
    relevant_columns: list[H2RelevantColumn],
) -> dict[str, list[H2RelevantColumn]]:
    grouped: dict[str, list[H2RelevantColumn]] = defaultdict(list)
    for column in sorted(
        relevant_columns,
        key=lambda item: (-item.score, item.table_name, item.column_name),
    ):
        grouped[column.table_name].append(column)
    return dict(grouped)


def _select_focus_table(
    selected_tables: list[str],
    relevant_columns: list[H2RelevantColumn],
    *,
    goal_text: str,
) -> str | None:
    if selected_tables:
        return selected_tables[0]
    if not relevant_columns:
        return None

    goal_tokens = {token for token in re.findall(r"[a-z0-9]+", goal_text.lower()) if len(token) > 2}
    by_table: dict[str, float] = defaultdict(float)
    for column in relevant_columns:
        table_name = column.table_name.lower()
        column_name = column.column_name.lower()
        column_score = column.score
        if _metric_name_match(goal_text, column.table_name, column.column_name):
            column_score += 3.0
        if any(token in table_name for token in goal_tokens):
            column_score += 4.0
        if any(token in column_name for token in goal_tokens):
            column_score += 1.5
        if any(hint in column_name for hint in _TIME_NAME_HINTS) and "time" in goal_tokens:
            column_score += 1.5
        if column_score > by_table[column.table_name]:
            by_table[column.table_name] = column_score
    return max(by_table.items(), key=lambda item: (item[1], item[0]))[0]


def _metric_name_match(goal_text: str, table_name: str, column_name: str) -> bool:
    if not goal_text:
        return False
    name = f"{table_name}.{column_name}".lower()
    goal_tokens = set(re.findall(r"[a-z0-9]+", goal_text.lower()))
    name_tokens = set(re.findall(r"[a-z0-9]+", name))
    return bool(goal_tokens & name_tokens) or any(
        phrase in name for phrase in ("score", "rate", "count", "duration", "risk", "quality")
    )


def _first_column(
    grouped: dict[str, list[H2RelevantColumn]],
    role_prefixes: set[str],
) -> H2RelevantColumn | None:
    for columns in grouped.values():
        for column in columns:
            if column.semantic_role and column.semantic_role.lower() in role_prefixes:
                return column
    return None


def _first_column_in_table(
    grouped: dict[str, list[H2RelevantColumn]],
    table_name: str | None,
    role_prefixes: set[str],
) -> H2RelevantColumn | None:
    if table_name is None:
        return None
    for column in grouped.get(table_name, []):
        if column.semantic_role and column.semantic_role.lower() in role_prefixes:
            return column
    return None


def _first_workflow_candidate(
    grouped: dict[str, list[H2RelevantColumn]],
    *,
    preferred_table: str | None = None,
) -> H2RelevantColumn | None:
    if preferred_table is not None:
        for column in grouped.get(preferred_table, []):
            name = column.column_name.lower()
            if any(token in name for token in _WORKFLOW_HINTS):
                return column
    for columns in grouped.values():
        for column in columns:
            name = column.column_name.lower()
            if any(token in name for token in _WORKFLOW_HINTS):
                return column
    return None


def _first_resource_candidate(
    grouped: dict[str, list[H2RelevantColumn]],
    *,
    preferred_table: str | None = None,
) -> H2RelevantColumn | None:
    if preferred_table is not None:
        for column in grouped.get(preferred_table, []):
            name = column.column_name.lower()
            if any(token in name for token in _RESOURCE_HINTS):
                return column
    for columns in grouped.values():
        for column in columns:
            name = column.column_name.lower()
            if any(token in name for token in _RESOURCE_HINTS):
                return column
    return None


def _first_measure_candidate(
    grouped: dict[str, list[H2RelevantColumn]],
    *,
    preferred_table: str | None = None,
) -> str | None:
    if preferred_table is not None:
        for column in grouped.get(preferred_table, []):
            if column.semantic_role and column.semantic_role.lower() in _MEASURE_ROLES:
                name = column.column_name.lower()
                if not any(hint in name for hint in _GEOGRAPHIC_HINTS):
                    return f"{column.table_name}.{column.column_name}"
    for columns in grouped.values():
        for column in columns:
            if column.semantic_role and column.semantic_role.lower() in _MEASURE_ROLES:
                name = column.column_name.lower()
                if not any(hint in name for hint in _GEOGRAPHIC_HINTS):
                    return f"{column.table_name}.{column.column_name}"
    for columns in grouped.values():
        for column in columns:
            name = column.column_name.lower()
            if any(
                token in name
                for token in {
                    "score",
                    "count",
                    "amount",
                    "value",
                    "duration",
                    "minutes",
                    "minute",
                    "hours",
                    "rate",
                    "ratio",
                    "total",
                }
            ) and not any(hint in name for hint in _GEOGRAPHIC_HINTS):
                return f"{column.table_name}.{column.column_name}"
    return None


def _find_metric_column(
    discovery: DiscoveryResult,
    relevant_columns: list[H2RelevantColumn],
    *,
    focus_table: str | None,
    target_metric: str,
) -> str | None:
    search_terms = {
        token
        for token in re.findall(r"[a-z0-9]+", target_metric.lower())
        if token not in {"project", "source", "data", "table", "record", "row", "value"}
    }
    if not search_terms:
        return None
    measure_tokens = {
        "score",
        "count",
        "duration",
        "rate",
        "value",
        "amount",
        "total",
        "risk",
        "quality",
        "volume",
        "percent",
        "ratio",
        "throughput",
        "utilization",
        "minutes",
        "minute",
        "hours",
        "hour",
    }

    candidates: list[tuple[float, str]] = []
    for candidate in relevant_columns:
        if candidate.semantic_role and candidate.semantic_role.lower() not in _MEASURE_ROLES:
            continue
        name = f"{candidate.table_name}.{candidate.column_name}".lower()
        tokens = set(re.findall(r"[a-z0-9]+", name))
        if any(hint in name for hint in _GEOGRAPHIC_HINTS):
            continue
        score = 0.0
        if focus_table is not None and candidate.table_name == focus_table:
            score += 2.5
        if tokens & measure_tokens:
            score += 3.5
        if search_terms & tokens:
            score += 2.0
        if any(
            token in name
            for token in ("score", "count", "duration", "rate", "value", "amount", "total")
        ):
            score += 1.5
        if "id" in candidate.column_name.lower():
            score -= 2.5
        if _looks_code_like(candidate.column_name):
            score -= 1.0
        candidates.append((score, f"{candidate.table_name}.{candidate.column_name}"))

    if candidates:
        return max(candidates, key=lambda item: (item[0], item[1]))[1]

    fallback_names: list[str] = []
    if focus_table is not None:
        fallback_names.extend(
            f"{table.name}.{column.name}"
            for table in discovery.tables
            if table.name == focus_table
            for column in table.columns
        )
    fallback_names.extend(
        f"{table.name}.{column.name}" for table in discovery.tables for column in table.columns
    )
    for candidate in fallback_names:
        lower = candidate.lower()
        if any(token in lower for token in measure_tokens) and not any(
            hint in lower for hint in _GEOGRAPHIC_HINTS
        ):
            return candidate
    return None


def _find_entity_candidate(
    discovery: DiscoveryResult,
    *,
    focus_table: str | None,
    goal_text: str,
) -> H2RelevantColumn | None:
    hints = {
        "site",
        "zone",
        "location",
        "sensor",
        "subject",
        "customer",
        "device",
        "program",
        "project",
        "entity",
    }
    if any(token in goal_text for token in ("site", "zone", "sensor", "asset", "unit")):
        hints |= {"site", "zone", "sensor"}
    if focus_table is not None:
        candidate = _find_named_column_in_table(discovery, focus_table, hints)
        if candidate is not None:
            return _column_ref(discovery, candidate)
    candidate = _find_named_column(discovery, hints)
    if candidate is not None:
        return _column_ref(discovery, candidate)
    return None


def _find_named_column_in_table(
    discovery: DiscoveryResult,
    table_name: str | None,
    hints: set[str],
) -> str | None:
    if table_name is None:
        return None
    for table in discovery.tables:
        if table.name != table_name:
            continue
        for column in table.columns:
            name = column.name.lower()
            if any(hint in name for hint in hints):
                if any(geo in name for geo in _GEOGRAPHIC_HINTS):
                    continue
                return f"{table.name}.{column.name}"
    return None


def _column_ref(discovery: DiscoveryResult, dotted_name: str) -> H2RelevantColumn | None:
    table_name, column_name = dotted_name.split(".", 1)
    for table in discovery.tables:
        if table.name != table_name:
            continue
        for column in table.columns:
            if column.name == column_name:
                return H2RelevantColumn(
                    table_name=table_name,
                    column_name=column_name,
                    semantic_role=column.role,
                    score=0.0,
                    reason="named candidate",
                    selected=False,
                )
    return None


def _infer_metric_label(
    relevant_columns: list[H2RelevantColumn],
    profiles: dict[tuple[str, str], ColumnProfile],
) -> str:
    for column in relevant_columns:
        profile = profiles.get((column.table_name, column.column_name))
        if profile and profile.mean is not None:
            return _friendly_name(column.column_name)
    return "metric"


def _coverage_days(discovery: DiscoveryResult) -> int | None:
    spans = []
    for profile in discovery.profiles:
        if not profile.min_date or not profile.max_date:
            continue
        try:
            start = datetime.fromisoformat(str(profile.min_date).replace("Z", "+00:00"))
            end = datetime.fromisoformat(str(profile.max_date).replace("Z", "+00:00"))
        except ValueError:
            continue
        span = max(0, (end - start).days + 1)
        spans.append(span)
    return max(spans) if spans else None


def _profile_lookup(
    discovery: DiscoveryResult,
    table_name: str,
    column_name: str,
) -> ColumnProfile | None:
    for profile in discovery.profiles:
        if profile.table_name == table_name and profile.column_name == column_name:
            return profile
    return None


def _load_profile_rows(
    store: HeadwaterStore,
    source_name: str,
    *,
    snapshot_id: str | None,
) -> list[dict[str, Any]]:
    if snapshot_id is None:
        rows = store.con.execute(
            "SELECT * FROM profiles WHERE source_name = ? ORDER BY table_name, column_name",
            (source_name,),
        ).fetchall()
    else:
        rows = store.con.execute(
            """
            SELECT *
              FROM profiles
             WHERE source_name = ? AND snapshot_id = ?
             ORDER BY table_name, column_name
            """,
            (source_name, snapshot_id),
        ).fetchall()
    items = [dict(row) for row in rows]
    for item in items:
        item["profile"] = json.loads(item.pop("profile_json") or "{}")
    return items


def _load_relationship_rows(
    store: HeadwaterStore,
    source_name: str,
    *,
    snapshot_id: str | None,
) -> list[dict[str, Any]]:
    if snapshot_id is None:
        rows = store.con.execute(
            "SELECT * FROM relationships WHERE source_name = ? ORDER BY id",
            (source_name,),
        ).fetchall()
    else:
        rows = store.con.execute(
            """
            SELECT *
              FROM relationships
             WHERE source_name = ? AND snapshot_id = ?
             ORDER BY id
            """,
            (source_name, snapshot_id),
        ).fetchall()
    return [dict(row) for row in rows]


def _profile_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row["profile"])
    payload.setdefault("table_name", row["table_name"])
    payload.setdefault("column_name", row["column_name"])
    payload.setdefault("dtype", row["dtype"])
    return payload


def _find_named_column(discovery: DiscoveryResult, hints: set[str]) -> str | None:
    for table in discovery.tables:
        for column in table.columns:
            name = column.name.lower()
            if any(hint in name for hint in hints):
                if any(geo in name for geo in _GEOGRAPHIC_HINTS):
                    continue
                return f"{table.name}.{column.name}"
    return None


# ── Resource context helpers ──────────────────────────────────────────────────

_DEFINITION_STOP_WORDS = frozenset(
    {
        "the",
        "and",
        "for",
        "are",
        "was",
        "were",
        "has",
        "have",
        "had",
        "its",
        "with",
        "from",
        "this",
        "that",
        "which",
        "each",
        "all",
        "can",
        "may",
        "will",
        "used",
        "been",
        "per",
        "not",
        "but",
        "also",
        "into",
        "than",
        "more",
        "over",
        "when",
        "where",
        "how",
        "any",
    }
)


def _resource_context_from_claims(
    claims: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    """Extract resource-backed column keys and domain vocabulary from project claims.

    Returns:
        resource_col_keys: set of "table.column" strings that have at least one
            claim sourced from a user-provided resource file.
        resource_vocabulary: set of meaningful words extracted from the text of
            resource-backed definition claims.  These words flow through the same
            generic intent hint sets as the goal text — no new domain logic is added.

    This is the bridge that lets a user's data dictionary improve relevance and intent
    detection without any domain-specific code in the engine.
    """
    resource_col_keys: set[str] = set()
    vocab_words: set[str] = set()

    for claim in claims:
        if not str(claim.get("source", "")).startswith("resource:"):
            continue

        table = claim.get("table_name")
        col = claim.get("column_name")
        if table and col:
            resource_col_keys.add(f"{table}.{col}".lower())

        # Extract vocabulary from definition text so the goal intent engine
        # can pick up domain-specific workflow/time/segment signals that the
        # user documented but did not spell out in the goal statement.
        if claim.get("claim_type") == "definition":
            value = claim.get("claim", {}).get("value")
            if isinstance(value, str):
                words = {
                    w
                    for w in re.findall(r"[a-z][a-z0-9]+", value.lower())
                    if w not in _DEFINITION_STOP_WORDS and len(w) >= 3
                }
                vocab_words |= words

    return resource_col_keys, vocab_words
