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
from headwater.services.h2_column_kinds import is_numeric_dtype, measure_column_ref
from headwater.services.h2_duration import (
    FORMATS,
    detect_duration,
    is_temporal_duration_dtype,
)
from headwater.services.h2_readiness import _columns_with_satisfying_claim

ResolveIssueKind = Literal[
    "enum_mapping_needed",
    "ambiguous_code",
    "missing_definition",
    "unusable_measure",
    "data_quality_risk",
    "cannot_answer_gap",
    "structural_ambiguity",
]

ResolvePriority = Literal["high", "medium", "low"]

_PRIORITY_RANK = {"high": 0, "medium": 1, "low": 2}

# Kinds this builder regenerates each run (so it may purge stale ones), plus the
# legacy kinds it now supersedes — the verbose judge "answer_gap" cards and the
# framing-time "insufficient_coverage" cards (replaced by cannot_answer_gap).
# Kinds created by other flows (e.g. resource-ingestion structural_ambiguity) are
# deliberately NOT in this set, so the purge never deletes them.
_BUILD_OWNED_KINDS = frozenset(
    {
        "enum_mapping_needed",
        "data_quality_risk",
        "unusable_measure",
        "cannot_answer_gap",
        "missing_definition",
        "answer_gap",
        "insufficient_coverage",
    }
)

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
    "unusable_measure": ["no_blocking_gaps"],
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
    # Columns the analyst has already defined (locked / filled enum / definition)
    # are no longer surfaced as resolve work — the evidence clears them.
    satisfied_cols = _columns_with_satisfying_claim(claims)
    # Columns with a confirmed numeric derivation (e.g. a parsed duration). A
    # meaning-definition does NOT make a text measure aggregatable — only a
    # derivation does — so unusable-measure cards key off this set, not satisfaction.
    derived_cols = {
        f"{c['table_name']}.{c['column_name']}"
        for c in claims
        if c.get("claim_type") == "derivation"
        and c.get("table_name")
        and c.get("column_name")
    }

    profile_map: dict[str, dict[str, Any]] = {
        f"{p['table_name']}.{p['column_name']}": p["profile"] for p in profiles
    }
    question_columns: dict[str, list[str]] = {
        q["id"]: list(q["question"].get("needed_columns") or []) for q in questions
    }
    cards: list[ResolveCard] = []

    # Enum mapping cards: short-code varchar columns
    cards.extend(
        _enum_mapping_cards(
            profile_map, columns, question_columns, project_id, satisfied_cols
        )
    )

    # Data quality risk cards: high null rates
    cards.extend(
        _data_quality_cards(profile_map, columns, question_columns, project_id)
    )

    # Unusable-measure cards: a question's measure column is stored as text
    # (e.g. a duration) and can't be aggregated until it's defined/derived.
    cards.extend(
        _unusable_measure_cards(
            questions,
            columns,
            profile_map,
            project_id,
            derived_cols,
            store=store,
            source_name=source_name,
        )
    )

    # Cannot-answer gap cards
    cards.extend(
        _cannot_answer_cards(questions, project_id)
    )

    # Missing definition cards: no description, affects multiple questions
    cards.extend(
        _missing_definition_cards(
            columns, question_columns, claims, profile_map, project_id, satisfied_cols
        )
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
    satisfied_cols: set[str],
) -> list[ResolveCard]:
    cards = []
    for col in columns:
        if col.get("locked"):
            continue
        key = f"{col['table_name']}.{col['name']}"
        if key in satisfied_cols:
            continue
        profile = profile_map.get(key, {})
        if not _is_code_like(col, profile):
            continue
        top_values = profile.get("top_values") or []
        value_list = [str(v[0]) for v in top_values[:8] if v] if top_values else []
        affected = _affected_questions(key, question_columns)
        priority: ResolvePriority = (
            "high" if len(affected) >= 2 else "medium" if affected else "low"
        )
        n = len(value_list)
        card = ResolveCard(
            card_id=f"{project_id}:enum:{key}",
            issue_kind="enum_mapping_needed",
            priority=priority,
            title=f'Define the {col["name"]} codes',
            # Lean: the concrete codes are shown as chips in the UI (payload.values).
            body=(
                f"{n} code{'s' if n != 1 else ''} with no defined meaning. "
                "Add what each one stands for."
            ),
            affected_questions=affected,
            contract_impacts=list(_CONTRACT_IMPACTS["enum_mapping_needed"]),
            payload={
                "table": col["table_name"],
                "column": col["name"],
                "values": value_list,
                "category": "input",
            },
        )
        cards.append(card)
    return cards


def _unusable_measure_cards(
    questions: list[dict[str, Any]],
    columns: list[dict[str, Any]],
    profile_map: dict[str, dict[str, Any]],
    project_id: str,
    derived_cols: set[str],
    *,
    store: HeadwaterStore | None = None,
    source_name: str | None = None,
) -> list[ResolveCard]:
    """One lean card per text measure that blocks questions, grouped by column.

    Surfaces the concrete root cause — "this column is stored as text, so it
    can't be averaged" — instead of a judge's prose about the SQL.  Several
    questions that all need the same unusable measure collapse into one card.
    Cleared by a confirmed derivation (``derived_cols``), not by a mere
    definition: a meaning doesn't make text aggregatable.
    """
    dtype_by_key = {
        f"{c['table_name']}.{c['name']}".lower(): (c.get("dtype") or "").lower()
        for c in columns
    }
    groups: dict[str, list[dict[str, Any]]] = {}
    for q in questions:
        if q.get("answerability") == "cannot_answer":
            continue
        measure = measure_column_ref(q)
        if not measure or measure in derived_cols:
            continue
        dtype = dtype_by_key.get(measure.lower())
        # Unknown dtype is left alone; only flag a known non-numeric measure.
        if dtype is None or is_numeric_dtype(dtype):
            continue
        groups.setdefault(measure, []).append(q)

    def _profile_samples(measure: str) -> list[str]:
        # Cached materialized samples first, then top values, then min/max (the
        # latter two may be null for a high-cardinality text column).
        profile = profile_map.get(measure, {})
        pool = [str(v) for v in (profile.get("sample_values") or []) if str(v).strip()]
        pool += [str(v[0]) for v in (profile.get("top_values") or []) if v]
        for k in ("min_value", "max_value"):
            val = profile.get(k)
            if val is not None and str(val).strip():
                pool.append(str(val))
        return pool

    # Fallback: for a TEXT measure with no profile samples (and not a temporal
    # dtype, which detects from the type alone), materialize a few real values so
    # the duration shape can still be recognized. Bounded and materialize-once.
    need = [
        _split(m)
        for m in groups
        if not is_temporal_duration_dtype(dtype_by_key.get(m.lower()))
        and not _profile_samples(m)
    ]
    materialized: dict[str, list[str]] = {}
    if need and store is not None and source_name:
        from headwater.services.h2_execute import sample_text_columns

        materialized = sample_text_columns(store, source_name, need)

    cards: list[ResolveCard] = []
    for measure, qs in groups.items():
        table, col = _split(measure)
        label = col.replace("_", " ")
        dtype = dtype_by_key.get(measure.lower())
        sample_pool = _profile_samples(measure) or materialized.get(measure, [])
        example = sample_pool[0] if sample_pool else ""
        # If it's a duration (a TIME/INTERVAL dtype, or duration-shaped text),
        # propose a one-click convert-to-minutes (user confirms or picks another).
        proposal = detect_duration(sample_pool, dtype=dtype)
        if proposal:
            body = (
                f"This is a duration ({proposal.detected.label}); convert it to "
                f"{proposal.unit} so it can be totaled or averaged"
                + (" — or pick another interpretation." if proposal.alternatives else ".")
            )
        else:
            body = (
                "This value can't be totaled or averaged as-is"
                + (f' (e.g. "{example}")' if example else "")
                + ". Define how to turn it into a number."
            )
        derivation = (
            {
                "kind": "duration",
                "unit": proposal.unit,
                "detected": {"id": proposal.detected.id, "label": proposal.detected.label},
                "options": [
                    {"id": f.id, "label": f.label} for f in proposal.all_formats
                ],
                "samples": proposal.samples,
            }
            if proposal
            else None
        )
        n = len(qs)
        cards.append(
            ResolveCard(
                card_id=f"{project_id}:measure:{measure}",
                issue_kind="unusable_measure",
                priority="high" if n >= 2 else "medium",
                title=f'Make "{label}" measurable',
                body=body,
                affected_questions=[q["id"] for q in qs],
                contract_impacts=list(_CONTRACT_IMPACTS["unusable_measure"]),
                payload={
                    "table": table,
                    "column": col,
                    "affected_titles": [q.get("title") for q in qs],
                    "category": "input",
                    "derivation": derivation,
                },
            )
        )
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
            # Informational: a data/coverage limitation, not a defining task.
            priority="low",
            title=f'Gap: "{q["title"]}"',
            body=(
                f"This question cannot be answered with the current data: {reason}. "
                "Review whether additional data sources or time coverage could resolve this gap."
            ),
            affected_questions=[q["id"]],
            contract_impacts=list(_CONTRACT_IMPACTS["cannot_answer_gap"]),
            payload={"question_id": q["id"], "reason": reason, "category": "limitation"},
        )
        cards.append(card)
    return cards


def _missing_definition_cards(
    columns: list[dict[str, Any]],
    question_columns: dict[str, list[str]],
    claims: list[dict[str, Any]],
    profile_map: dict[str, dict[str, Any]],
    project_id: str,
    satisfied_cols: set[str],
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
        if key in claimed_keys or key in satisfied_cols:
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
    """Persist the freshly-built card set as the single source of truth.

    The builder OWNS the project's resolve items: a user's 'deferred' disposition
    on a card that still applies is preserved, and any item no longer generated
    (a resolved gap, a stale judge-prose card from an earlier version) is removed
    so the screen always reflects the current data — never accumulates cruft.
    """
    existing = {it["id"]: it for it in store.list_resolve_items(project_id)}
    desired_ids = {c.card_id for c in cards}

    for card in cards:
        question_id = card.affected_questions[0] if card.affected_questions else None
        prev = existing.get(card.card_id)
        status = "deferred" if prev and prev.get("status") == "deferred" else "open"
        store.upsert_resolve_item(
            card.card_id,
            project_id=project_id,
            issue_kind=card.issue_kind,
            title=card.title,
            body=card.body,
            question_id=question_id,
            priority=card.priority,
            status=status,
            payload={
                **card.payload,
                "affected_questions": card.affected_questions,
                "contract_impacts": card.contract_impacts,
            },
        )

    for item_id, item in existing.items():
        if item_id not in desired_ids and item.get("issue_kind") in _BUILD_OWNED_KINDS:
            store.delete_resolve_item(item_id)


def define_card(
    store: HeadwaterStore,
    project_id: str,
    card_id: str,
    markdown: str,
) -> dict[str, Any]:
    """Bind a Resolve card's human-supplied definition directly to its column.

    This is the S-BIND path. The card already knows which ``{table, column}`` it
    is about, so the analyst's text or code table is written as a column-scoped,
    locked semantic claim -- ground truth the fast recompute reads to clear the
    gap and feed answers. Cards without a column (e.g. cannot-answer gaps) are
    left to the resource-ingest path; this returns ``bound=False`` for them.
    """
    text = (markdown or "").strip()
    if not text:
        raise ValueError("No definition text provided.")

    card = next(
        (r for r in store.list_resolve_items(project_id) if r["id"] == card_id), None
    )
    if card is None:
        raise ValueError(f"Resolve card '{card_id}' not found.")

    payload = card.get("payload") or {}
    table = payload.get("table")
    column = payload.get("column")
    if not table or not column:
        return {"bound": False, "reason": "Card has no column to bind."}

    enum_map = _parse_enum_table(text)
    if enum_map:
        claim_type = "enum_mapping"
        # Keep the analyst's original markdown alongside the parsed map so the
        # Resolve card can rehydrate the exact text on a later visit.
        claim = {"value": enum_map, "text": text}
    else:
        claim_type = "definition"
        claim = {"value": text, "text": text}

    project_sources = store.get_project_sources(project_id)
    source_name = project_sources[0]["source_name"] if project_sources else None

    store.upsert_semantic_claim(
        f"{project_id}:define:{table}.{column}",
        project_id=project_id,
        source_name=source_name,
        scope_type="column",
        claim_type=claim_type,
        claim=claim,
        table_name=table,
        column_name=column,
        status="locked",
        confidence=1.0,
        source="user",
        locked=True,
    )
    store.set_resolve_item_status(card_id, "resolved")
    return {"bound": True, "claim_type": claim_type, "table": table, "column": column}


def _parse_enum_table(markdown: str) -> dict[str, str]:
    """Extract a ``{code: meaning}`` map from a markdown 2-column table, if any.

    Header rows (``code``/``value``/...) and separator rows (``---``) are skipped.
    Returns an empty dict when the text is not a code table -- the caller then
    treats the whole text as a free-text definition.
    """
    enum_map: dict[str, str] = {}
    for raw in markdown.splitlines():
        line = raw.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 2:
            continue
        code, meaning = cells[0], cells[1]
        if not code or not meaning:
            continue
        if set(code) <= {"-", ":"} or set(meaning) <= {"-", ":"}:
            continue
        if code.lower() in ("code", "value", "key", "abbr", "abbreviation", "column"):
            continue
        enum_map[code] = meaning
    return enum_map


def confirm_duration_derivation(
    store: HeadwaterStore,
    project_id: str,
    card_id: str,
    format_id: str,
) -> dict[str, Any]:
    """Confirm a parse-to-minutes derivation for an unusable-measure card.

    Writes a locked ``derivation`` semantic claim binding the card's column to a
    duration format. The claim flows through the recompute fingerprint so the
    answer generator then aggregates the parsed minutes (see h2_answer), and the
    column counts as satisfied so the card clears. Advisory: only applied on the
    user's explicit confirmation of a format.
    """
    fmt = FORMATS.get(format_id)
    if fmt is None:
        raise ValueError(f"Unknown duration format '{format_id}'.")

    card = next(
        (r for r in store.list_resolve_items(project_id) if r["id"] == card_id), None
    )
    if card is None:
        raise ValueError(f"Resolve card '{card_id}' not found.")
    payload = card.get("payload") or {}
    table = payload.get("table")
    column = payload.get("column")
    if not table or not column:
        return {"applied": False, "reason": "Card has no column to derive."}

    project_sources = store.get_project_sources(project_id)
    source_name = project_sources[0]["source_name"] if project_sources else None

    store.upsert_semantic_claim(
        f"{project_id}:derive:{table}.{column}",
        project_id=project_id,
        source_name=source_name,
        scope_type="column",
        claim_type="derivation",
        claim={
            "value": f"{fmt.label} parsed to minutes",
            "kind": "duration",
            "format": format_id,
            "unit": "minutes",
        },
        table_name=table,
        column_name=column,
        status="locked",
        confidence=1.0,
        source="user",
        locked=True,
    )
    store.set_resolve_item_status(card_id, "resolved")
    return {"applied": True, "format": format_id, "unit": "minutes"}
