"""Headwater 2 resolve card engine.

Converts unresolved semantic ambiguity, data quality risks, and cannot-answer gaps
into ranked Resolve cards that tell a data professional exactly what human knowledge
is needed to move a question toward certification.

Cards are ranked by: affected question count, contract impact, user-only-knowability.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from headwater.core.store import HeadwaterStore

ResolveIssueKind = Literal[
    "enum_mapping_needed",
    "ambiguous_code",
    "missing_definition",
    "data_quality_risk",
    "cannot_answer_gap",
    "structural_ambiguity",
]

ResolvePriority = Literal["high", "medium", "low"]

_PRIORITY_RANK = {"high": 0, "medium": 1, "low": 2}

# Code-like heuristics: short varchar with few distinct values and short average length
_CODE_MAX_DISTINCT = 30
_CODE_MAX_AVG_LEN = 4.0
_CODE_MAX_UNIQUENESS_RATIO = 0.05
_HIGH_NULL_RATE = 0.20

# Contracts that a resolve card can clear
_CONTRACT_IMPACTS: dict[ResolveIssueKind, list[str]] = {
    "enum_mapping_needed": ["definition_consistent", "no_misleading"],
    "ambiguous_code": ["definition_consistent"],
    "missing_definition": ["definition_consistent"],
    "data_quality_risk": ["structural_integrity", "no_blocking_gaps"],
    "cannot_answer_gap": ["no_blocking_gaps"],
    "structural_ambiguity": ["structural_integrity"],
}


@dataclass(slots=True)
class ResolveCard:
    card_id: str
    issue_kind: ResolveIssueKind
    priority: ResolvePriority
    title: str
    body: str
    affected_questions: list[str] = field(default_factory=list)
    contract_impacts: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)


def build_resolve_cards(
    store: HeadwaterStore,
    project_id: str,
) -> list[ResolveCard]:
    """Build ranked resolve cards from all project state.

    The result is persisted as resolve_items in the store and also returned
    for immediate use by callers.
    """
    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")

    source_name = project_sources[0]["source_name"]
    questions = store.list_questions(project_id)
    profiles = store.get_profiles(source_name)
    columns = _load_all_columns(store, source_name)
    claims = store.list_semantic_claims(project_id)

    profile_map: dict[str, dict[str, Any]] = {
        f"{p['table_name']}.{p['column_name']}": p["profile"] for p in profiles
    }
    question_columns: dict[str, list[str]] = {
        q["id"]: list(q["question"].get("needed_columns") or []) for q in questions
    }
    cards: list[ResolveCard] = []

    # Enum mapping cards: short-code varchar columns
    cards.extend(
        _enum_mapping_cards(profile_map, columns, question_columns, project_id)
    )

    # Data quality risk cards: high null rates
    cards.extend(
        _data_quality_cards(profile_map, columns, question_columns, project_id)
    )

    # Cannot-answer gap cards
    cards.extend(
        _cannot_answer_cards(questions, project_id)
    )

    # Missing definition cards: no description, affects multiple questions
    cards.extend(
        _missing_definition_cards(columns, question_columns, claims, profile_map, project_id)
    )

    cards = _deduplicate_and_rank(cards)
    _persist_cards(store, project_id, cards)
    return cards


def _load_all_columns(
    store: HeadwaterStore,
    source_name: str,
) -> list[dict[str, Any]]:
    tables = store.get_tables(source_name)
    columns = []
    for table in tables:
        for col in store.get_columns(source_name, table["name"]):
            col["table_name"] = table["name"]
            columns.append(col)
    return columns


def _enum_mapping_cards(
    profile_map: dict[str, dict[str, Any]],
    columns: list[dict[str, Any]],
    question_columns: dict[str, list[str]],
    project_id: str,
) -> list[ResolveCard]:
    cards = []
    for col in columns:
        if col.get("locked"):
            continue
        key = f"{col['table_name']}.{col['name']}"
        profile = profile_map.get(key, {})
        if not _is_code_like(col, profile):
            continue
        top_values = profile.get("top_values") or []
        value_list = [str(v[0]) for v in top_values[:8] if v] if top_values else []
        affected = _affected_questions(key, question_columns)
        priority: ResolvePriority = (
            "high" if len(affected) >= 2 else "medium" if affected else "low"
        )
        label = col["name"].replace("_", " ")
        card = ResolveCard(
            card_id=f"{project_id}:enum:{key}",
            issue_kind="enum_mapping_needed",
            priority=priority,
            title=f'What do the "{col["table_name"]}.{col["name"]}" codes mean?',
            body=(
                f"Column `{col['table_name']}.{col['name']}` contains short codes "
                f"({', '.join(value_list) or 'unknown'}) with no business definition. "
                f"Add a mapping so Headwater can correctly segment and classify {label} values."
            ),
            affected_questions=affected,
            contract_impacts=list(_CONTRACT_IMPACTS["enum_mapping_needed"]),
            payload={"table": col["table_name"], "column": col["name"], "top_values": value_list},
        )
        cards.append(card)
    return cards


def _data_quality_cards(
    profile_map: dict[str, dict[str, Any]],
    columns: list[dict[str, Any]],
    question_columns: dict[str, list[str]],
    project_id: str,
) -> list[ResolveCard]:
    cards = []
    for col in columns:
        key = f"{col['table_name']}.{col['name']}"
        profile = profile_map.get(key, {})
        null_rate = profile.get("null_rate")
        if null_rate is None or float(null_rate) < _HIGH_NULL_RATE:
            continue
        affected = _affected_questions(key, question_columns)
        if not affected:
            continue
        pct = int(float(null_rate) * 100)
        card = ResolveCard(
            card_id=f"{project_id}:null:{key}",
            issue_kind="data_quality_risk",
            priority="high" if float(null_rate) >= 0.50 else "medium",
            title=f'High null rate in "{col["name"]}" ({pct}%)',
            body=(
                f"Column `{col['table_name']}.{col['name']}` has {pct}% missing values. "
                "Verify whether nulls are expected (e.g. not applicable) or represent "
                "data collection gaps that could skew results."
            ),
            affected_questions=affected,
            contract_impacts=list(_CONTRACT_IMPACTS["data_quality_risk"]),
            payload={"table": col["table_name"], "column": col["name"], "null_rate": null_rate},
        )
        cards.append(card)
    return cards


def _cannot_answer_cards(
    questions: list[dict[str, Any]],
    project_id: str,
) -> list[ResolveCard]:
    cards = []
    for q in questions:
        if q.get("answerability") != "cannot_answer":
            continue
        reason = q["question"].get("reason") or q.get("title", "")
        card = ResolveCard(
            card_id=f"{project_id}:gap:{q['id']}",
            issue_kind="cannot_answer_gap",
            priority="medium",
            title=f'Gap: "{q["title"]}"',
            body=(
                f"This question cannot be answered with the current data: {reason}. "
                "Review whether additional data sources or time coverage could resolve this gap."
            ),
            affected_questions=[q["id"]],
            contract_impacts=list(_CONTRACT_IMPACTS["cannot_answer_gap"]),
            payload={"question_id": q["id"], "reason": reason},
        )
        cards.append(card)
    return cards


def _missing_definition_cards(
    columns: list[dict[str, Any]],
    question_columns: dict[str, list[str]],
    claims: list[dict[str, Any]],
    profile_map: dict[str, dict[str, Any]],
    project_id: str,
) -> list[ResolveCard]:
    # Only flag columns that (a) have no description or semantic type, (b) affect >= 2 questions,
    # (c) are not already covered by an enum_mapping card (not code-like)
    claimed_keys = {
        f"{c['table_name']}.{c['column_name']}"
        for c in claims
        if c.get("status") == "locked"
    }
    cards = []
    for col in columns:
        if col.get("description") or col.get("semantic_type") or col.get("locked"):
            continue
        key = f"{col['table_name']}.{col['name']}"
        if key in claimed_keys:
            continue
        profile = profile_map.get(key, {})
        if _is_code_like(col, profile):
            continue
        affected = _affected_questions(key, question_columns)
        if len(affected) < 2:
            continue
        card = ResolveCard(
            card_id=f"{project_id}:def:{key}",
            issue_kind="missing_definition",
            priority="low",
            title=f'No definition for "{col["name"]}"',
            body=(
                f"Column `{col['table_name']}.{col['name']}` affects {len(affected)} question(s) "
                "but has no business definition or semantic lock. "
                "Add a description so Headwater can consistently apply it across projects."
            ),
            affected_questions=affected,
            contract_impacts=list(_CONTRACT_IMPACTS["missing_definition"]),
            payload={"table": col["table_name"], "column": col["name"]},
        )
        cards.append(card)
    return cards


def _is_code_like(col: dict[str, Any], profile: dict[str, Any]) -> bool:
    if col.get("dtype", "").lower() not in ("varchar", "text", "string", "category"):
        return False
    distinct_count = profile.get("distinct_count") or 0
    avg_len = profile.get("avg_length") or 0.0
    uniqueness = profile.get("uniqueness_ratio") or 0.0
    if distinct_count < 2:
        return False
    return (
        int(distinct_count) <= _CODE_MAX_DISTINCT
        and float(avg_len) <= _CODE_MAX_AVG_LEN
        and float(uniqueness) <= _CODE_MAX_UNIQUENESS_RATIO
    )


def _affected_questions(col_key: str, question_columns: dict[str, list[str]]) -> list[str]:
    return [
        qid
        for qid, needed in question_columns.items()
        if any(
            (col_key in [f"{_table}.{_col}" for _table, _col in (_split(c) for c in needed)])
            or col_key.endswith(f".{needed[i]}")
            for i in range(len(needed))
        )
    ]


def _split(col_ref: str) -> tuple[str, str]:
    parts = col_ref.split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("", col_ref)


def _deduplicate_and_rank(cards: list[ResolveCard]) -> list[ResolveCard]:
    seen: set[str] = set()
    unique: list[ResolveCard] = []
    for card in cards:
        if card.card_id not in seen:
            seen.add(card.card_id)
            unique.append(card)
    return sorted(
        unique,
        key=lambda c: (
            _PRIORITY_RANK[c.priority],
            -len(c.affected_questions),
            -len(c.contract_impacts),
            c.card_id,
        ),
    )


def _persist_cards(
    store: HeadwaterStore,
    project_id: str,
    cards: list[ResolveCard],
) -> None:
    for card in cards:
        question_id = card.affected_questions[0] if card.affected_questions else None
        store.upsert_resolve_item(
            card.card_id,
            project_id=project_id,
            issue_kind=card.issue_kind,
            title=card.title,
            body=card.body,
            question_id=question_id,
            priority=card.priority,
            status="open",
            payload={
                **card.payload,
                "affected_questions": card.affected_questions,
                "contract_impacts": card.contract_impacts,
            },
        )
