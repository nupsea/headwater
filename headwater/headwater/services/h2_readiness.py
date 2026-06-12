"""Headwater 2 readiness contract evaluation.

Evaluates per-question evidence contracts and computes a derived question state.
Certification is NEVER set directly — it is derived only when all contracts pass.
A question demotes automatically when a contract it once relied on fails.

Certification FAILS CLOSED: a contract whose evidence has not been computed is
``unknown``, and unknown can never certify — missing evidence is not a pass.
(Reasoning-engine plan §4.3: the badge is sacred.)

Contract types:
  columns_profiled      - all needed columns exist in the profile store
  no_blocking_gaps      - no high-priority open resolve items for needed columns
  structural_integrity  - needed columns are present and have acceptable null rates
  no_misleading         - no misleading quality patterns: elevated nulls, code-like
                          columns without meanings, join fan-out risk
  definition_consistent - no conflicting locked semantic claims for needed columns
  insight_confidence    - EDA evidence supports the answer (unknown until EDA runs)

Question states:
  certified    - all contracts pass
  draft        - at least one contract failed or is unknown
  cannot_answer - question was flagged cannot_answer (no certification path)
  demoted      - was certified but a contract now fails (not yet tracked — placeholder)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from headwater.core.store import HeadwaterStore

ContractType = Literal[
    "columns_profiled",
    "no_blocking_gaps",
    "structural_integrity",
    "no_misleading",
    "definition_consistent",
    "insight_confidence",
]

QuestionState = Literal["certified", "draft", "cannot_answer", "demoted"]
ContractStatus = Literal["pass", "fail", "unknown"]

_HIGH_NULL_THRESHOLD = 0.50
# Nulls below the structural-failure bar but high enough to skew an aggregate
# silently — surfaced by no_misleading rather than structural_integrity.
_ELEVATED_NULL_THRESHOLD = 0.25
# A join whose referential integrity is below this fans out / drops rows enough
# to mislead an aggregate built across it.
_FANOUT_RI_THRESHOLD = 0.90
_CONTRACT_WEIGHT: dict[ContractType, int] = {
    "columns_profiled": 20,
    "no_blocking_gaps": 20,
    "structural_integrity": 20,
    "no_misleading": 15,
    "definition_consistent": 15,
    "insight_confidence": 10,
}


@dataclass(slots=True)
class ContractResult:
    contract_type: ContractType
    passed: bool
    note: str
    evidence: dict[str, Any] = field(default_factory=dict)
    # Tri-state: "unknown" = evidence not computed yet. passed is always False
    # for unknown (fail closed); status preserves the distinction for the UI.
    status: ContractStatus = "pass"

    def __post_init__(self) -> None:
        if self.status == "unknown":
            self.passed = False  # unknown can never certify
        else:
            self.status = "pass" if self.passed else "fail"


@dataclass(slots=True)
class QuestionReadiness:
    question_id: str
    state: QuestionState
    readiness_pct: int
    contracts: list[ContractResult] = field(default_factory=list)
    summary: str = ""
    title: str = ""
    needed_columns: list[str] = field(default_factory=list)
    source_snapshot_id: str | None = None


@dataclass(slots=True)
class ProjectReadinessReport:
    project_id: str
    source_name: str
    source_snapshot_id: str | None
    questions: list[QuestionReadiness] = field(default_factory=list)

    @property
    def certified_count(self) -> int:
        return sum(1 for q in self.questions if q.state == "certified")

    @property
    def draft_count(self) -> int:
        return sum(1 for q in self.questions if q.state == "draft")

    @property
    def cannot_answer_count(self) -> int:
        return sum(1 for q in self.questions if q.state == "cannot_answer")


def evaluate_project_readiness(
    store: HeadwaterStore,
    project_id: str,
) -> ProjectReadinessReport:
    """Evaluate readiness for all questions in a project."""
    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")

    source_name = project_sources[0]["source_name"]
    snapshot = store.get_latest_source_snapshot(source_name)
    snapshot_id = snapshot["id"] if snapshot else None

    questions = [
        q for q in store.list_questions(project_id) if q.get("status") != "dropped"
    ]
    profiles = store.get_profiles(source_name)
    resolve_items = store.list_resolve_items(project_id)
    claims = store.list_semantic_claims(project_id)

    profile_map = {
        f"{p['table_name']}.{p['column_name']}": p["profile"] for p in profiles
    }
    # Joins whose referential integrity is weak enough to fan out / drop rows —
    # an aggregate built across one of these can silently mislead.
    weak_joins: dict[tuple[str, str], float] = {}
    for rel in store.get_relationships(source_name):
        ri = rel.get("referential_integrity")
        if ri is not None and float(ri) < _FANOUT_RI_THRESHOLD:
            weak_joins[(rel["from_table"], rel["to_table"])] = float(ri)
    # Tables VERIFIED empty (profiled, zero rows): any question needing one has
    # no data to answer from. An unprofiled row_count of 0 means "unknown" and
    # is not flagged.
    profiled_tables = {p["table_name"] for p in profiles}
    empty_tables: set[str] = {
        t["name"]
        for t in store.get_tables(source_name)
        if int(t.get("row_count") or 0) == 0 and t["name"] in profiled_tables
    }
    # Columns the user has now defined (a filled enum mapping, a non-empty
    # definition, or a locked claim) no longer count as blocking gaps, even if
    # the originating resolve card still reads "open".  This makes gap-clearing
    # evidence-derived: providing a proper column definition (e.g. via Schema &
    # meaning) clears no_blocking_gaps on the next recompute.
    satisfied_cols: set[str] = _columns_with_satisfying_claim(claims)
    high_priority_open: set[str] = {
        f"{r['payload'].get('table','')}.{r['payload'].get('column','')}"
        for r in resolve_items
        if r.get("priority") == "high" and r.get("status") == "open"
        if r.get("payload", {}).get("column")
    } - satisfied_cols
    conflicting_cols: set[str] = _find_conflicting_claims(claims)
    # Code-like columns (an open enum-mapping card) with no meanings supplied:
    # an aggregate grouped by raw codes reads fine and means nothing.
    unmapped_code_cols: set[str] = {
        f"{r['payload'].get('table','')}.{r['payload'].get('column','')}"
        for r in resolve_items
        if r.get("issue_kind") == "enum_mapping_needed" and r.get("status") == "open"
        if r.get("payload", {}).get("column")
    } - satisfied_cols

    # Load stored EDA insight_confidence contracts (written by the EDA runner).
    # A persisted "unknown" placeholder is NOT evidence — skip it, else the
    # round-trip would launder uncomputed evidence into a computed-looking fail.
    eda_contracts: dict[str, dict[str, Any]] = {}
    for q in questions:
        stored = store.list_readiness_contracts(q["id"])
        eda = next(
            (
                c
                for c in stored
                if c["contract_type"] == "insight_confidence"
                and (c.get("evidence") or {}).get("status") != "unknown"
            ),
            None,
        )
        if eda:
            eda_contracts[q["id"]] = eda

    question_results: list[QuestionReadiness] = []
    for q in questions:
        result = evaluate_question(
            question=q,
            profile_map=profile_map,
            high_priority_open=high_priority_open,
            conflicting_cols=conflicting_cols,
            snapshot_id=snapshot_id,
            eda_contract=eda_contracts.get(q["id"]),
            unmapped_code_cols=unmapped_code_cols,
            weak_joins=weak_joins,
            empty_tables=empty_tables,
        )
        _persist_verdict(store, result)
        question_results.append(result)

    return ProjectReadinessReport(
        project_id=project_id,
        source_name=source_name,
        source_snapshot_id=snapshot_id,
        questions=question_results,
    )


def evaluate_question(
    question: dict[str, Any],
    *,
    profile_map: dict[str, dict[str, Any]],
    high_priority_open: set[str],
    conflicting_cols: set[str],
    snapshot_id: str | None,
    eda_contract: dict[str, Any] | None = None,
    unmapped_code_cols: set[str] | None = None,
    weak_joins: dict[tuple[str, str], float] | None = None,
    empty_tables: set[str] | None = None,
) -> QuestionReadiness:
    question_id = question["id"]
    answerability = question.get("answerability", "answerable")
    title = question.get("title", "")
    needed = list(question.get("question", {}).get("needed_columns") or [])

    if answerability == "cannot_answer":
        reason = (question.get("question", {}).get("reason") or "").strip()
        return QuestionReadiness(
            question_id=question_id,
            state="cannot_answer",
            readiness_pct=0,
            contracts=[],
            summary=reason or "This question cannot be answered with the current data.",
            title=title,
            needed_columns=needed,
            source_snapshot_id=snapshot_id,
        )
    contracts = _evaluate_contracts(
        question_id=question_id,
        needed_columns=needed,
        profile_map=profile_map,
        high_priority_open=high_priority_open,
        conflicting_cols=conflicting_cols,
        eda_contract=eda_contract,
        unmapped_code_cols=unmapped_code_cols or set(),
        weak_joins=weak_joins or {},
        empty_tables=empty_tables or set(),
    )

    readiness_pct = _compute_readiness_pct(contracts)
    state: QuestionState = "certified" if all(c.passed for c in contracts) else "draft"
    summary = _build_summary(state, contracts)

    return QuestionReadiness(
        question_id=question_id,
        state=state,
        readiness_pct=readiness_pct,
        contracts=contracts,
        summary=summary,
        title=title,
        needed_columns=needed,
        source_snapshot_id=snapshot_id,
    )


def _evaluate_contracts(
    question_id: str,
    needed_columns: list[str],
    profile_map: dict[str, dict[str, Any]],
    high_priority_open: set[str],
    conflicting_cols: set[str],
    eda_contract: dict[str, Any] | None = None,
    unmapped_code_cols: set[str] | None = None,
    weak_joins: dict[tuple[str, str], float] | None = None,
    empty_tables: set[str] | None = None,
) -> list[ContractResult]:
    unmapped_code_cols = unmapped_code_cols or set()
    weak_joins = weak_joins or {}
    empty_tables = empty_tables or set()
    results: list[ContractResult] = []

    # 1. columns_profiled
    profiled = [c for c in needed_columns if c in profile_map]
    missing = [c for c in needed_columns if c not in profile_map]
    results.append(
        ContractResult(
            contract_type="columns_profiled",
            passed=not missing,
            note=(
                f"All {len(needed_columns)} needed columns profiled."
                if not missing
                else f"{len(missing)} needed column(s) not profiled: {', '.join(missing[:3])}"
            ),
            evidence={"profiled": profiled, "missing": missing},
        )
    )

    # 2. no_blocking_gaps
    blocking = [c for c in needed_columns if c in high_priority_open]
    results.append(
        ContractResult(
            contract_type="no_blocking_gaps",
            passed=not blocking,
            note=(
                "No high-priority open gaps affecting this question."
                if not blocking
                else f"High-priority gaps on: {', '.join(blocking[:3])}"
            ),
            evidence={"blocking_columns": blocking},
        )
    )

    # 3. structural_integrity
    bad_null = [
        c
        for c in needed_columns
        if (profile_map.get(c, {}).get("null_rate") or 0.0) >= _HIGH_NULL_THRESHOLD
    ]
    results.append(
        ContractResult(
            contract_type="structural_integrity",
            passed=not bad_null,
            note=(
                "All needed columns have acceptable null rates."
                if not bad_null
                else f"High null rate (>={int(_HIGH_NULL_THRESHOLD*100)}%) in: {', '.join(bad_null[:3])}"  # noqa: E501
            ),
            evidence={"high_null_columns": bad_null},
        )
    )

    # 4. no_misleading — evidence-derived (plan §4.3: implemented for real).
    # Scans the answer's lineage for patterns that read fine but lie:
    # elevated nulls below the structural bar, code-like dimensions with no
    # meanings, and join paths weak enough to fan out or drop rows.
    misleading_reasons: list[str] = []
    needed_tables_all = {c.rsplit(".", 1)[0] for c in needed_columns if "." in c}
    empty_needed = sorted(needed_tables_all & empty_tables)
    if empty_needed:
        misleading_reasons.append(
            f"table(s) verified EMPTY — no data to answer from: "
            f"{', '.join(empty_needed[:3])}"
        )
    elevated_null = [
        c
        for c in needed_columns
        if _ELEVATED_NULL_THRESHOLD
        <= (profile_map.get(c, {}).get("null_rate") or 0.0)
        < _HIGH_NULL_THRESHOLD
    ]
    if elevated_null:
        misleading_reasons.append(
            f"elevated null rate (>= {int(_ELEVATED_NULL_THRESHOLD * 100)}%) in: "
            f"{', '.join(elevated_null[:3])}"
        )
    unmapped = [c for c in needed_columns if c in unmapped_code_cols]
    if unmapped:
        misleading_reasons.append(
            f"code-like column(s) without meanings: {', '.join(unmapped[:3])}"
        )
    needed_tables = {c.rsplit(".", 1)[0] for c in needed_columns if "." in c}
    fanout = [
        (a, b, ri)
        for (a, b), ri in weak_joins.items()
        if a in needed_tables and b in needed_tables
    ]
    if fanout:
        worst = min(fanout, key=lambda x: x[2])
        misleading_reasons.append(
            f"join fan-out risk {worst[0]} -> {worst[1]} "
            f"(referential integrity {worst[2]:.0%})"
        )
    results.append(
        ContractResult(
            contract_type="no_misleading",
            passed=not misleading_reasons,
            note=(
                "No misleading quality patterns detected in needed columns."
                if not misleading_reasons
                else "Misleading pattern(s): " + "; ".join(misleading_reasons)
            ),
            evidence={
                "empty_tables": empty_needed,
                "elevated_null_columns": elevated_null,
                "unmapped_code_columns": unmapped,
                "weak_joins": [
                    {"from": a, "to": b, "referential_integrity": ri}
                    for a, b, ri in fanout
                ],
            },
        )
    )

    # 5. definition_consistent
    conflicting = [c for c in needed_columns if c in conflicting_cols]
    results.append(
        ContractResult(
            contract_type="definition_consistent",
            passed=not conflicting,
            note=(
                "No conflicting semantic definitions for needed columns."
                if not conflicting
                else f"Conflicting definitions on: {', '.join(conflicting[:3])}"
            ),
            evidence={"conflicting_columns": conflicting},
        )
    )

    # 6. insight_confidence — populated by the EDA runner when it has been
    # executed. Uncomputed evidence is UNKNOWN and fails closed (plan §4.3):
    # it never blocks drafting or exploration, only the credential.
    if eda_contract:
        results.append(
            ContractResult(
                contract_type="insight_confidence",
                passed=bool(eda_contract.get("passed")),
                note=str(eda_contract.get("note") or "EDA confidence score available."),
                evidence=dict(eda_contract.get("evidence") or {}),
            )
        )
    else:
        results.append(
            ContractResult(
                contract_type="insight_confidence",
                passed=False,
                status="unknown",
                note=(
                    "Insight evidence not computed yet — certification is capped "
                    "at Draft until the EDA battery runs (recompute runs it)."
                ),
                evidence={},
            )
        )

    return results


def _columns_with_satisfying_claim(claims: list[dict[str, Any]]) -> set[str]:
    """Return 'table.column' keys whose meaning the user has now supplied.

    A column counts as satisfied when it has a claim that is either locked, a
    non-empty free-text definition, or an enum mapping with at least one
    non-empty meaning.  Such a column should no longer be treated as a blocking
    gap.
    """
    satisfied: set[str] = set()
    for c in claims:
        table = c.get("table_name")
        column = c.get("column_name")
        if not table or not column:
            continue
        if c.get("locked"):
            satisfied.add(f"{table}.{column}")
            continue
        value = (c.get("claim") or {}).get("value")
        if isinstance(value, dict):
            # enum mapping: satisfied once any code has a non-empty meaning
            if any(str(v).strip() for v in value.values()):
                satisfied.add(f"{table}.{column}")
        elif isinstance(value, str) and value.strip():
            satisfied.add(f"{table}.{column}")
    return satisfied


# Claim types that assert a *meaning* for a column. Only these can conflict on
# definition. Relevance/EDA/role claims describe a column but never define it, so
# they must not trigger definition_consistent failures.
_DEFINITIONAL_CLAIM_TYPES = {"definition", "enum_mapping", "semantic_type"}


def _find_conflicting_claims(claims: list[dict[str, Any]]) -> set[str]:
    """Columns with genuinely conflicting *definitions*.

    A locked definition is ground truth: it supersedes lower-status placeholders
    (e.g. the empty bootstrap enum map, or a proposed suggestion) rather than
    conflicting with them. A column is conflicting only when:

      * the resource ingester explicitly flagged it ``needs_review`` (it found
        two sources asserting different values), or
      * two or more *active* (locked/accepted) definitions disagree on value.
    """
    from collections import defaultdict

    by_col: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for c in claims:
        if not (c.get("table_name") and c.get("column_name")):
            continue
        if c.get("claim_type") not in _DEFINITIONAL_CLAIM_TYPES:
            continue
        key = f"{c['table_name']}.{c['column_name']}"
        by_col[key].append(c)

    conflicting: set[str] = set()
    for key, col_claims in by_col.items():
        if any(c.get("status") == "needs_review" for c in col_claims):
            conflicting.add(key)
            continue
        active_values = {
            _claim_value_signature(c)
            for c in col_claims
            if c.get("status") in ("locked", "accepted")
        }
        active_values.discard("")  # ignore empty placeholders
        if len(active_values) > 1:
            conflicting.add(key)
    return conflicting


def _claim_value_signature(claim: dict[str, Any]) -> str:
    """A comparable signature of a claim's asserted value (empty if blank)."""
    import json

    value = (claim.get("claim") or {}).get("value")
    if isinstance(value, dict):
        filled = {str(k): str(v).strip() for k, v in value.items() if str(v).strip()}
        return json.dumps(filled, sort_keys=True) if filled else ""
    if value is None:
        return ""
    return str(value).strip()


def _compute_readiness_pct(contracts: list[ContractResult]) -> int:
    total_weight = sum(_CONTRACT_WEIGHT.get(c.contract_type, 0) for c in contracts)  # type: ignore[arg-type]
    if total_weight == 0:
        return 0
    earned = sum(
        _CONTRACT_WEIGHT.get(c.contract_type, 0)  # type: ignore[arg-type]
        for c in contracts
        if c.passed
    )
    return int(earned * 100 / total_weight)


def _build_summary(state: QuestionState, contracts: list[ContractResult]) -> str:
    if state == "certified":
        return "All evidence contracts pass. This question is certified."
    # Genuine failures explain the draft better than uncomputed evidence does;
    # mention unknown evidence only when it is the sole blocker.
    failed = [c for c in contracts if c.status == "fail"]
    unknown = [c for c in contracts if c.status == "unknown"]
    if failed:
        reasons = "; ".join(c.note for c in failed[:2])
        return f"Draft — {reasons}."
    reasons = "; ".join(c.note for c in unknown[:2])
    return f"Draft (evidence pending) — {reasons}."


def _persist_verdict(store: HeadwaterStore, result: QuestionReadiness) -> None:
    verdict_id = f"{result.question_id}:verdict:latest"
    trust_bucket = _trust_bucket(result.state, result.readiness_pct)
    store.upsert_readiness_verdict(
        verdict_id,
        question_id=result.question_id,
        state=result.state,
        readiness_pct=result.readiness_pct,
        trust_bucket=trust_bucket,
        summary=result.summary,
        source_snapshot_id=result.source_snapshot_id,
    )
    for contract in result.contracts:
        contract_id = f"{result.question_id}:contract:{contract.contract_type}"
        store.upsert_readiness_contract(
            contract_id,
            question_id=result.question_id,
            contract_type=contract.contract_type,
            passed=contract.passed,
            note=contract.note,
            # status rides inside evidence so the tri-state survives storage
            # without a schema change ("unknown" = evidence not computed).
            evidence={**contract.evidence, "status": contract.status},
            snapshot_id=result.source_snapshot_id,
        )


def _trust_bucket(state: QuestionState, readiness_pct: int) -> str:
    if state == "certified":
        return "trustworthy"
    if state == "cannot_answer":
        return "gaps"
    if readiness_pct >= 60:
        return "risky"
    return "not_started"
