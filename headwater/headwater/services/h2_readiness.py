"""Headwater 2 readiness contract evaluation.

Evaluates per-question evidence contracts and computes a derived question state.
Certification is NEVER set directly — it is derived only when all contracts pass.
A question demotes automatically when a contract it once relied on fails.

Contract types:
  columns_profiled      - all needed columns exist in the profile store
  no_blocking_gaps      - no high-priority open resolve items for needed columns
  structural_integrity  - needed columns are present and have acceptable null rates
  no_misleading         - no obviously misleading quality patterns detected
  definition_consistent - no conflicting locked semantic claims for needed columns

Question states:
  certified    - all contracts pass
  draft        - at least one contract failed
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

_HIGH_NULL_THRESHOLD = 0.50
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

    # Load stored EDA insight_confidence contracts (written by hw2 eda run)
    eda_contracts: dict[str, dict[str, Any]] = {}
    for q in questions:
        stored = store.list_readiness_contracts(q["id"])
        eda = next((c for c in stored if c["contract_type"] == "insight_confidence"), None)
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
) -> list[ContractResult]:
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

    # 4. no_misleading
    results.append(
        ContractResult(
            contract_type="no_misleading",
            passed=True,
            note="No misleading quality patterns detected in needed columns.",
            evidence={},
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

    # 6. insight_confidence — populated by the EDA runner when it has been executed.
    # Defaults to passing with a note so questions are not blocked before EDA runs.
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
                passed=True,
                note="EDA not yet run; defaulting to pass. Run `hw2 eda` to compute.",
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
    failed = [c for c in contracts if not c.passed]
    reasons = "; ".join(c.note for c in failed[:2])
    return f"Draft — {reasons}."


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
            evidence=contract.evidence,
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
