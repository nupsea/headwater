"""Headwater 2 pipeline orchestration — the recompute spine.

This is the single place where two-factor certification happens.  An answer is
``certified`` only when BOTH gates pass:

  1. Statistical readiness contracts (see ``h2_readiness``) — the data *can*
     support the answer (columns profiled, no blocking gaps, acceptable nulls,
     consistent definitions, EDA insight confidence).
  2. The LLM judge (see ``analyzer.judge``) — the generated SQL and its executed
     result *do* answer the question without misinterpretation.

Neither gate is weakened by the other.  If the judge is unavailable (no local
model running) or unsure, the answer holds at ``doubtful`` — it never certifies
without an explicit judge approval.

Final answer states (the three buckets the UI shows):
  - ``certified``     statistics pass AND judge approves; shows real data + chart
  - ``doubtful``      executed, but a gate did not clear; shows SQL + reasons
  - ``cannot_answer`` the question was flagged unanswerable; shows guidance only

Raw result rows are returned for display only and are never persisted to the
metadata store nor sent to the LLM (invariant I-3).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from headwater.analyzer.judge import JudgeResult, judge_answer
from headwater.analyzer.llm import LLMProvider, get_provider
from headwater.core.config import HeadwaterSettings, get_settings
from headwater.core.store import HeadwaterStore
from headwater.services.h2_execute import ExecutedResult, execute_answers
from headwater.services.h2_readiness import (
    QuestionReadiness,
    evaluate_project_readiness,
)

FinalState = str  # "certified" | "doubtful" | "cannot_answer"


@dataclass(slots=True)
class FinalizedAnswer:
    question_id: str
    question_title: str
    state: FinalState
    sql_text: str | None
    chart_spec: dict[str, Any]
    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    result_stats: dict[str, Any] = field(default_factory=dict)
    readiness_pct: int = 0
    statistical_pass: bool = False
    judge_verdict: str = "unavailable"
    judge_confidence: float = 0.0
    judge_reasons: list[str] = field(default_factory=list)
    caveats: list[str] = field(default_factory=list)
    execution_error: str | None = None
    # Plain-English takeaway derived from the executed result (h2_insight).
    finding_headline: str = ""
    finding_support: str = ""
    source_snapshot_id: str | None = None
    # The input fingerprint the judge verdict was produced against.  Persisted in
    # the judge contract so a later fast-path load can tell whether the verdict
    # still applies (same inputs -> honor it) or is stale (re-run certification).
    # None means "do not (re)write the judge contract" — preserve what's stored.
    judged_fingerprint: str | None = None
    # Calculated confidence (blended evidence) + its component breakdown.
    display_confidence: float = 0.0
    confidence_breakdown: dict[str, float] = field(default_factory=dict)
    # column -> {raw code: human meaning}, from locked enum_mapping claims.
    # Display-only: the SQL still groups on raw codes; the UI relabels output.
    value_labels: dict[str, dict[str, str]] = field(default_factory=dict)


@dataclass(slots=True)
class FinalizedProject:
    project_id: str
    source_name: str
    source_snapshot_id: str | None
    answers: list[FinalizedAnswer] = field(default_factory=list)

    @property
    def certified_count(self) -> int:
        return sum(1 for a in self.answers if a.state == "certified")

    @property
    def doubtful_count(self) -> int:
        return sum(1 for a in self.answers if a.state == "doubtful")

    @property
    def cannot_answer_count(self) -> int:
        return sum(1 for a in self.answers if a.state == "cannot_answer")

    @property
    def pending_count(self) -> int:
        return sum(1 for a in self.answers if a.state == "pending")


def finalize_project_answers(
    store: HeadwaterStore,
    project_id: str,
    *,
    settings: HeadwaterSettings | None = None,
    provider: LLMProvider | None = None,
    run_judge: bool = True,
) -> FinalizedProject:
    """Draft, execute, and (optionally) judge every answerable question.

    With ``run_judge=False`` this is the fast path: it drafts SQL, executes it,
    and evaluates statistical readiness, leaving stat-ready answers in the
    ``pending`` state (awaiting certification).  With ``run_judge=True`` it also
    runs the LLM judge and applies the two-factor certification gate.

    ``provider`` may be injected (tests, or to share one provider); otherwise it
    is built from settings (Ollama by default) only when judging.
    """
    from headwater.services.h2_answer import (
        _build_col_role_map,
        draft_project_answers,
    )

    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")
    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")
    source_name = project_sources[0]["source_name"]

    settings = settings or get_settings()
    if run_judge and provider is None:
        # H2's HeadwaterStore lacks the LLM audit/cache methods, so pass store=None.
        provider = get_provider(settings, store=None)

    # 1. Draft SQL + chart specs (also persists answer artifacts).
    drafts = draft_project_answers(store, project_id)
    draft_by_q = {d.question_id: d for d in drafts.answers}

    # 2. Statistical readiness (persists verdicts/contracts).
    readiness = evaluate_project_readiness(store, project_id)
    readiness_by_q = {q.question_id: q for q in readiness.questions}
    snapshot_id = readiness.source_snapshot_id

    # 3. Execute every answerable draft once against a freshly materialized source.
    exec_items: list[tuple[str, str | None]] = [
        (d.question_id, d.sql_text)
        for d in drafts.answers
        if d.state != "cannot_answer" and d.sql_text
    ]
    executed: dict[str, ExecutedResult] = (
        execute_answers(store, source_name, exec_items) if exec_items else {}
    )

    # Column metadata for the judge (I-3-safe: names/roles/dtypes only).
    claims = store.list_semantic_claims(project_id)
    role_map = _build_col_role_map(claims, store, source_name)
    # Resolved enum meanings flow into answer display (codes -> human labels).
    value_labels = _build_value_labels(claims)

    result = FinalizedProject(
        project_id=project_id,
        source_name=source_name,
        source_snapshot_id=snapshot_id,
    )

    # One fingerprint for the whole pass: stamps fresh judge verdicts and decides
    # whether a previously-stored verdict still applies on the fast path.  Inputs
    # don't change during finalize, so the start value also reconciles the state.
    current_fp = project_input_fingerprint(store, project_id)

    for question in store.list_questions(project_id):
        if question.get("status") == "dropped":
            continue  # user-curated out — excluded from answers
        qid = question["id"]
        draft = draft_by_q.get(qid)
        readiness_q = readiness_by_q.get(qid)
        finalized = _finalize_one(
            store=store,
            question=question,
            draft=draft,
            readiness_q=readiness_q,
            executed=executed.get(qid),
            role_map=role_map,
            provider=provider,
            snapshot_id=snapshot_id,
            run_judge=run_judge,
            value_labels=value_labels,
            current_fingerprint=current_fp,
        )
        result.answers.append(finalized)

    # Resolve cards are derived structurally from the data (see h2_resolve) — the
    # judge is a certification gate here, not a card generator, so it never dumps
    # prose onto the Resolve screen.

    # Derived state now reflects the current inputs — reconcile the fingerprint
    # so the recompute banner only re-appears after a genuine input change.
    store.set_pipeline_state(
        project_id,
        last_input_hash=current_fp,
        impacted_count=_impacted_count(store, project_id),
    )
    return result


def _build_value_labels(
    claims: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    """Map column -> {raw code: meaning} from locked enum_mapping claims.

    Newest claim wins (``list_semantic_claims`` is ordered newest-first), so an
    edited interpretation supersedes an earlier one without duplication.
    """
    labels: dict[str, dict[str, str]] = {}
    for c in claims:
        if c.get("claim_type") != "enum_mapping":
            continue
        col = c.get("column_name") or ""
        value = (c.get("claim") or {}).get("value")
        if not col or col in labels or not isinstance(value, dict):
            continue
        mapping = {str(k): str(v) for k, v in value.items() if str(v).strip()}
        if mapping:
            labels[col] = mapping
    return labels


def _finalize_one(
    *,
    store: HeadwaterStore,
    question: dict[str, Any],
    draft: Any,
    readiness_q: QuestionReadiness | None,
    executed: ExecutedResult | None,
    role_map: dict[str, dict[str, Any]],
    provider: LLMProvider | None,
    snapshot_id: str | None,
    run_judge: bool = True,
    value_labels: dict[str, dict[str, str]] | None = None,
    current_fingerprint: str | None = None,
) -> FinalizedAnswer:
    qid = question["id"]
    title = question.get("title", qid)
    q_payload = question.get("question") or {}
    answerability = question.get("answerability", "answerable")

    # Unanswerable questions never reach the two-factor gate.
    if answerability == "cannot_answer" or (draft and draft.state == "cannot_answer"):
        fa = FinalizedAnswer(
            question_id=qid,
            question_title=title,
            state="cannot_answer",
            sql_text=None,
            chart_spec={},
            caveats=[q_payload.get("reason") or ""],
            source_snapshot_id=snapshot_id,
        )
        _persist(store, fa)
        return fa

    sql_text = draft.sql_text if draft else None
    chart_spec = dict(draft.chart_spec) if draft else {}
    caveats = list(draft.caveats) if draft else []
    readiness_pct = readiness_q.readiness_pct if readiness_q else 0
    is_user_query = bool(q_payload.get("user_sql"))
    statistical_pass = bool(
        readiness_q and all(c.passed for c in readiness_q.contracts) and readiness_q.contracts
    )

    columns: list[str] = []
    rows: list[dict[str, Any]] = []
    row_count = 0
    truncated = False
    result_stats: dict[str, Any] = {}
    execution_error: str | None = None
    if executed is not None:
        columns = executed.columns
        rows = executed.rows
        row_count = executed.row_count
        truncated = executed.truncated
        result_stats = executed.stats
        execution_error = executed.error

    # Carry only the enum maps for columns this answer actually returns.
    answer_labels = {
        col: (value_labels or {})[col]
        for col in columns
        if value_labels and col in value_labels
    }

    executed_ok = bool(executed is not None and executed.ok)
    # A promoted console query has no template-derived contracts, so its
    # statistical factor is simply "did the analyst's SQL execute cleanly". The
    # judge still independently gates whether it answers the question. We also
    # infer a chart from the shape of the result so it reads as an answer.
    if is_user_query:
        from headwater.services.h2_insight import infer_chart_spec

        statistical_pass = executed_ok
        if executed_ok and not chart_spec:
            chart_spec = infer_chart_spec(columns, rows)
    # The judge can evaluate any answer that actually executed — it assesses
    # whether the SQL answers the question, independent of statistical readiness.
    can_judge = bool(executed_ok and sql_text)
    # Eligible to certify on the statistics factor (the fast path's "pending").
    stat_ready = bool(statistical_pass and can_judge)

    # Gate 2: the judge.  On the fast path (run_judge=False) we do NOT re-run the
    # model; instead we rehydrate any verdict already stored for this answer so it
    # persists across navigation/recompute.  A stored verdict produced against the
    # current inputs is honored (and can certify); one produced against older
    # inputs comes back "stale" so the UI prompts a re-run.  Neither path ever
    # certifies without a real judge approval — the two-factor invariant holds.
    judged_fingerprint: str | None = None
    # A verdict already produced against the *current* inputs is the source of
    # truth either way: the fast path rehydrates it, and recertify honors it
    # rather than re-judging.  This keeps certification stable — re-running the
    # judge never re-litigates (and a non-deterministic model never flips) an
    # answer already decided for these inputs.  The model is invoked only for
    # answers that lack a fresh verdict (never judged, or stale after an input
    # change), and only when run_judge is set.
    stored = _rehydrate_judge(store, qid, current_fingerprint)
    if stored.available:
        judge_result = stored
        judged_fingerprint = current_fingerprint
        state = _final_state(
            statistical_pass=statistical_pass,
            executed_ok=executed_ok,
            judge=judge_result,
        )
    elif not run_judge:
        # Fast path, no fresh verdict: pending (never run) or stale (inputs moved).
        judge_result = stored
        state = "pending" if stat_ready else "doubtful"
    else:
        judge_result = JudgeResult(
            verdict="unavailable", confidence=0.0, reasons=[], available=False
        )
        if can_judge and provider is not None:
            judge_cols = _judge_columns(q_payload.get("needed_columns") or [], role_map)
            judge_result = judge_answer(
                provider,
                question_title=title,
                question_reason=q_payload.get("reason") or "",
                sql_text=sql_text,
                columns=judge_cols,
                result_stats=result_stats,
            )
        if judge_result.available:
            # A real, fresh verdict — stamp the fingerprint and apply the gate.
            judged_fingerprint = current_fingerprint
            state = _final_state(
                statistical_pass=statistical_pass,
                executed_ok=executed_ok,
                judge=judge_result,
            )
        else:
            # The model could not produce a verdict (outage / nothing to judge).
            # Never demote: hold pending if stat-ready, else doubtful.  A prior
            # real verdict, if any, was already honored above.
            state = "pending" if stat_ready else "doubtful"

    if state == "doubtful":
        caveats = caveats + _doubt_reasons(
            statistical_pass=statistical_pass,
            readiness_q=readiness_q,
            executed=executed,
            judge=judge_result,
        )

    # State the takeaway in plain English (deterministic, from the executed rows).
    from headwater.services.h2_insight import summarize_answer

    finding = summarize_answer(
        chart_spec=chart_spec,
        columns=columns,
        rows=rows,
        value_labels=answer_labels,
        title=title,
    )

    confidence, confidence_breakdown = _answer_confidence(
        executed_ok=executed_ok,
        readiness_q=readiness_q,
        result_stats=result_stats,
        judge=judge_result,
    )

    fa = FinalizedAnswer(
        question_id=qid,
        question_title=title,
        state=state,
        sql_text=sql_text,
        chart_spec=chart_spec,
        columns=columns,
        rows=rows,
        finding_headline=finding.headline if finding else "",
        finding_support=finding.support if finding else "",
        row_count=row_count,
        truncated=truncated,
        result_stats=result_stats,
        readiness_pct=readiness_pct,
        statistical_pass=statistical_pass,
        judge_verdict=judge_result.verdict,
        judge_confidence=judge_result.confidence,
        judge_reasons=judge_result.reasons,
        caveats=[c for c in caveats if c],
        execution_error=execution_error,
        value_labels=answer_labels,
        source_snapshot_id=snapshot_id,
        judged_fingerprint=judged_fingerprint,
        display_confidence=confidence,
        confidence_breakdown=confidence_breakdown,
    )
    _persist(store, fa)
    return fa


def _rehydrate_judge(
    store: HeadwaterStore, question_id: str, current_fingerprint: str | None
) -> JudgeResult:
    """Reconstruct the last judge verdict for an answer from the stored contract.

    The fast path never calls the model; this is how a verdict the judge already
    produced survives navigation and recompute (the central-truth principle: a
    computed verdict lives in the store and every view reflects it).

      - no stored verdict        -> "pending"  (genuinely not run yet)
      - stored, inputs unchanged -> the verdict itself, available=True (honored)
      - stored, inputs changed   -> "stale"    (UI offers "Re-run certification")
    """
    contract = next(
        (
            c
            for c in store.list_readiness_contracts(question_id)
            if c.get("contract_type") == "judge_verdict"
        ),
        None,
    )
    if contract is None:
        return JudgeResult(verdict="pending", confidence=0.0, reasons=[], available=False)
    ev = contract.get("evidence") or {}
    verdict = ev.get("verdict") or "pending"
    if verdict in ("pending", "unavailable", "stale"):
        return JudgeResult(verdict="pending", confidence=0.0, reasons=[], available=False)
    if ev.get("judged_fingerprint") != current_fingerprint:
        return JudgeResult(verdict="stale", confidence=0.0, reasons=[], available=False)
    return JudgeResult(
        verdict=verdict,
        confidence=float(ev.get("confidence") or 0.0),
        reasons=list(ev.get("reasons") or []),
        available=True,
    )


def _final_state(
    *, statistical_pass: bool, executed_ok: bool, judge: JudgeResult
) -> FinalState:
    """Two-factor gate. Certified requires statistics AND judge approval."""
    if not statistical_pass or not executed_ok:
        return "doubtful"
    if judge.approves:
        return "certified"
    return "doubtful"


def _result_completeness(result_stats: dict[str, Any]) -> float | None:
    """Mean non-null fraction across the result's numeric columns (else all).

    A clean aggregate trends to ~1.0; a degenerate / all-null measure trends to
    0, which correctly drags confidence down.  Reads only aggregate stats, so it
    is fully domain-agnostic.
    """
    cols = result_stats.get("columns") or {}
    row_count = int(result_stats.get("row_count") or 0)
    if row_count <= 0 or not cols:
        return None

    def _nn(info: dict[str, Any]) -> float:
        return max(0.0, min(1.0, (row_count - int(info.get("null_count") or 0)) / row_count))

    numeric = [
        _nn(info)
        for info in cols.values()
        if any(t in str(info.get("dtype", "")).lower() for t in ("int", "float", "dec", "double"))
    ]
    fracs = numeric or [_nn(info) for info in cols.values()]
    return sum(fracs) / len(fracs) if fracs else None


def _answer_confidence(
    *,
    executed_ok: bool,
    readiness_q: QuestionReadiness | None,
    result_stats: dict[str, Any],
    judge: JudgeResult,
) -> tuple[float, dict[str, float]]:
    """A *calculated* 0..1 confidence, blended from real, domain-agnostic signals.

    Components (each in 0..1, only counted when actually available):
      - ``readiness``: weighted fraction of evidence contracts that pass
        (``readiness_pct``) — included only when contracts were evaluated.
      - ``completeness``: non-null fraction of the result's measure cells.
      - ``verification``: the judge's own confidence when it certified, else 0 —
        weighted highest, as it is the certification gate (so an unverified but
        data-ready answer lands meaningfully below a certified one).

    Returns ``(confidence, components)`` so the UI can show how it was derived —
    it is a derived number, never a constant.
    """
    if not executed_ok:
        return 0.0, {}
    comps: dict[str, float] = {}
    weights: dict[str, float] = {}
    if readiness_q is not None and readiness_q.contracts:
        comps["readiness"] = round(readiness_q.readiness_pct / 100.0, 2)
        weights["readiness"] = 1.0
    completeness = _result_completeness(result_stats)
    if completeness is not None:
        comps["completeness"] = round(completeness, 2)
        weights["completeness"] = 1.0
    comps["verification"] = (
        round(max(0.0, min(1.0, judge.confidence)), 2) if judge.approves else 0.0
    )
    weights["verification"] = 2.0
    total = sum(weights.values())
    score = sum(comps[k] * weights[k] for k in weights) / total if total else 0.0
    return round(score, 2), comps


def _doubt_reasons(
    *,
    statistical_pass: bool,
    readiness_q: QuestionReadiness | None,
    executed: ExecutedResult | None,
    judge: JudgeResult,
) -> list[str]:
    reasons: list[str] = []
    if not statistical_pass and readiness_q:
        failed = [c.note for c in readiness_q.contracts if not c.passed]
        reasons.extend(failed[:2])
    if executed is not None and executed.error:
        reasons.append(f"Query failed to execute: {executed.error}")
    if not judge.approves:
        if judge.available:
            reasons.append(f"Judge withheld certification ({judge.verdict}).")
            reasons.extend(judge.reasons[:2])
        elif judge.verdict == "unavailable":
            # A genuine provider failure (only set after run_judge attempted it);
            # the fast path leaves the judge "pending" and says nothing here.
            reasons.append(
                "LLM judge unavailable — start a local model "
                "(e.g. `ollama pull qwen2.5:14b-instruct`) to certify."
            )
    return reasons


def _judge_columns(
    needed_columns: list[str], role_map: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    cols: list[dict[str, Any]] = []
    for ref in needed_columns:
        info = role_map.get(ref.lower(), {})
        cols.append(
            {
                "ref": ref,
                "dtype": info.get("dtype", "?"),
                "role": info.get("semantic_role") or "?",
            }
        )
    return cols


def _persist(store: HeadwaterStore, fa: FinalizedAnswer) -> None:
    """Persist the final verdict, the judge contract, and the answer state.

    Only summaries and verdicts are stored — never raw rows.
    """
    from headwater.services.h2_readiness import _trust_bucket

    verdict_id = f"{fa.question_id}:verdict:latest"
    store.upsert_readiness_verdict(
        verdict_id,
        question_id=fa.question_id,
        state=fa.state,
        readiness_pct=fa.readiness_pct,
        trust_bucket=_trust_bucket(fa.state, fa.readiness_pct),  # type: ignore[arg-type]
        summary=_verdict_summary(fa),
        source_snapshot_id=fa.source_snapshot_id,
    )

    # Persist the judge result as a contract for audit/display — but only when we
    # hold an authoritative verdict (a fresh run, or a rehydrated valid one).
    # On the fast path with a pending/stale judge, leave the stored verdict alone
    # so a real prior verdict is never clobbered by "not run / stale".
    if fa.judged_fingerprint is not None:
        store.upsert_readiness_contract(
            f"{fa.question_id}:contract:judge_verdict",
            question_id=fa.question_id,
            contract_type="judge_verdict",
            passed=(fa.judge_verdict == "certified"),
            note=f"{fa.judge_verdict}: {'; '.join(fa.judge_reasons)}"[:500],
            evidence={
                "verdict": fa.judge_verdict,
                "confidence": fa.judge_confidence,
                "reasons": fa.judge_reasons,
                "judged_fingerprint": fa.judged_fingerprint,
            },
            snapshot_id=fa.source_snapshot_id,
        )

    certified_at = (
        datetime.now(UTC).isoformat() if fa.state == "certified" else None
    )
    store.upsert_answer_artifact(
        f"{fa.question_id}:answer:latest",
        question_id=fa.question_id,
        sql_text=fa.sql_text,
        chart_spec=fa.chart_spec,
        state=fa.state,
        certified_at=certified_at,
        source_snapshot_id=fa.source_snapshot_id,
    )


def _verdict_summary(fa: FinalizedAnswer) -> str:
    if fa.state == "certified":
        return (
            f"Certified — statistics pass and the judge approved "
            f"({fa.judge_confidence:.0%} confidence)."
        )
    if fa.state == "cannot_answer":
        return "Cannot answer with the current data."
    if fa.state == "pending":
        return "Statistics pass — awaiting LLM judge. Run certification to verify."
    reasons = "; ".join(fa.caveats[:2]) if fa.caveats else "a verification gate did not clear"
    return f"Doubtful — {reasons}."


# ── Recompute spine (staged) ──────────────────────────────────────────────────


def project_input_fingerprint(store: HeadwaterStore, project_id: str) -> str:
    """Stable hash over the inputs that drive derived state.

    Changes to the project goal, selected scope, column metadata
    (description/semantic_type/lock), semantic claims, or resolve dispositions
    flip this hash — which is how the UI knows downstream relevance/questions/
    answers are stale and offers a complete recompute.
    """
    project = store.get_project(project_id) or {}
    payload: dict[str, Any] = {"goal": project.get("goal") or {}, "sources": []}
    for ps in store.get_project_sources(project_id):
        source_name = ps["source_name"]
        selected = sorted(ps.get("selected_tables") or [])
        if not selected:
            # Empty scope means "all tables" — fingerprint them all.
            selected = sorted(t["name"] for t in store.get_tables(source_name))
        tables_meta = []
        for tname in selected:
            cols = store.get_columns(source_name, tname)
            tables_meta.append(
                {
                    "table": tname,
                    "columns": [
                        {
                            "name": c["name"],
                            "description": c.get("description"),
                            "semantic_type": c.get("semantic_type"),
                            "locked": c.get("locked"),
                        }
                        for c in cols
                    ],
                }
            )
        payload["sources"].append(
            {"source": source_name, "selected": selected, "tables": tables_meta}
        )

    payload["claims"] = sorted(
        (
            {
                "id": c["id"],
                "status": c.get("status"),
                "locked": c.get("locked"),
                "claim": c.get("claim") or c.get("claim_json"),
            }
            for c in store.list_semantic_claims(project_id)
        ),
        key=lambda x: str(x["id"]),
    )
    # Only *user-facing* resolve items count as inputs.  ``answer_gap`` cards are
    # derived — finalize opens/closes them as a side effect of judging — so
    # including them here would feed derived state back into the staleness hash,
    # making a just-computed judge verdict look stale on the very next load.
    payload["resolve"] = sorted(
        (
            {"id": r["id"], "status": r.get("status")}
            for r in store.list_resolve_items(project_id)
            if r.get("issue_kind") != "answer_gap"
        ),
        key=lambda x: str(x["id"]),
    )

    blob = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _impacted_count(store: HeadwaterStore, project_id: str) -> int:
    """How many answers a recompute would re-verify (answerable questions)."""
    return sum(
        1
        for q in store.list_questions(project_id)
        if q.get("answerability") != "cannot_answer"
    )


def get_project_state(store: HeadwaterStore, project_id: str) -> dict[str, Any]:
    """Whether derived state is stale relative to the current inputs."""
    if store.get_project(project_id) is None:
        raise ValueError(f"Project '{project_id}' is not registered.")
    current = project_input_fingerprint(store, project_id)
    row = store.get_pipeline_state(project_id)
    last_hash = row.get("last_input_hash") if row else None
    return {
        "project_id": project_id,
        "stale": last_hash != current,
        "never_computed": row is None,
        "impacted_count": _impacted_count(store, project_id),
        "last_recomputed_at": row.get("last_recomputed_at") if row else None,
    }


def recompute_project(
    store: HeadwaterStore,
    project_id: str,
    *,
    settings: HeadwaterSettings | None = None,
    run_judge: bool = False,
) -> dict[str, Any]:
    """Recompute derived state and mark it fresh.

    Dispatches to the reasoning-graph runner when ``settings.reasoning_engine`` is
    on (surgical: a run with no input change skips the stages), else to the legacy
    linear refresh. Both produce identical store side effects and return summary.
    """
    settings = settings or get_settings()
    if getattr(settings, "reasoning_engine", False):
        return _recompute_project_via_engine(
            store, project_id, settings=settings, run_judge=run_judge
        )
    return _legacy_recompute_project(
        store, project_id, settings=settings, run_judge=run_judge
    )


def regenerate_engine_questions(
    store: HeadwaterStore,
    project_id: str,
    *,
    settings: HeadwaterSettings | None = None,
) -> dict[str, Any]:
    """Explicitly re-run the goal-aware analysis and replace the question set.

    Engine questions are normally generated once per goal and kept stable, so this
    is the deliberate "give me a fresh set" action (e.g. after adding tables, or to
    retry the model). It clears the engine question set + the caches that pin it,
    then recomputes — producing a fresh LLM-proposed set and its verdicts.
    """
    settings = settings or get_settings()
    if not getattr(settings, "reasoning_engine", False):
        # Engine off: just re-run the normal recompute (templates).
        return recompute_project(store, project_id, settings=settings)

    from headwater.reasoning.cache import NodeCache

    rq_ids = [
        q["id"]
        for q in store.list_questions(project_id)
        if str(q["id"]).startswith(f"{project_id}:rq")
    ]
    store.delete_questions(rq_ids)

    cache = NodeCache(store)
    cache.invalidate("engine.goalsig", project_id)  # force regeneration for this project
    # Clear the LLM result + the deterministic recompute nodes so the next run is
    # genuinely fresh. Other projects keep their questions (their goalsig stands),
    # so a global clear only forces a cheap recompute for them, never a re-LLM.
    for node_id in ("question.vertical", "relevance", "answers"):
        cache.invalidate(node_id)

    return recompute_project(store, project_id, settings=settings)


def _recompute_project_via_engine(
    store: HeadwaterStore,
    project_id: str,
    *,
    settings: HeadwaterSettings,
    run_judge: bool,
) -> dict[str, Any]:
    """Route recompute through the typed-node graph (PR2 parity wrapping)."""
    from headwater.knowledge import make_projection
    from headwater.reasoning import NodeCache, NodeCtx, NodeRunner, ProjectState
    from headwater.reasoning.ledger import ProvenanceLedger
    from headwater.reasoning.nodes import build_recompute_graph

    if store.get_project(project_id) is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    projection = make_projection(settings, store)
    state = ProjectState(project_id, store, projection)
    ctx = NodeCtx(settings=settings, llm=None, run_slow=run_judge)
    runner = NodeRunner(NodeCache(store), projection, ProvenanceLedger(store))
    runner.run(build_recompute_graph(run_judge=run_judge), state, ctx)

    # The runner adopts cached outputs into state on a skip, so the answer counts
    # are available whether or not the node re-ran this pass.
    counts = state.output_of("answers") or {}
    fingerprint = project_input_fingerprint(store, project_id)
    store.set_pipeline_state(
        project_id,
        last_input_hash=fingerprint,
        impacted_count=_impacted_count(store, project_id),
    )
    return {
        "project_id": project_id,
        "certified_count": counts.get("certified_count", 0),
        "doubtful_count": counts.get("doubtful_count", 0),
        "pending_count": counts.get("pending_count", 0),
        "cannot_answer_count": counts.get("cannot_answer_count", 0),
        "recomputed_at": datetime.now(UTC).isoformat(),
    }


def _legacy_recompute_project(
    store: HeadwaterStore,
    project_id: str,
    *,
    settings: HeadwaterSettings | None = None,
    run_judge: bool = False,
) -> dict[str, Any]:
    """Re-run the derived state *from the beginning* and mark it fresh.

    This is the complete fast refresh that enforces the cross-cutting rule: any
    input change (a column meaning, a semantic claim, an added resource, a
    selected-scope or disposition edit) re-runs every derived stage in order so
    the whole workflow reflects the new truth:

        relevance (relevant columns + proposed questions)
          -> statistical readiness
            -> drafted SQL
              -> executed data

    The LLM judge (certification) is deliberately excluded — it stays a separate
    explicit action because the local model is slow (run_judge defaults to
    False).  ``propose_relevance`` itself is heuristic and never calls the LLM,
    so the fast path holds no model in its critical path.
    """
    # Stage 1 — re-propose relevance and questions from the *current* inputs.
    # Without this the refresh would skip the earliest stages and a corrected
    # meaning or added dictionary would never change which questions are
    # answerable.  Idempotent: relevance claims and questions are upserted by
    # deterministic id.
    from headwater.services.h2_project import propose_relevance

    propose_relevance(store=store, project_id=project_id)

    # Stages 2-4 — readiness, draft, execute (and optional judge) over the
    # freshly proposed question set.
    result = finalize_project_answers(
        store, project_id, settings=settings, run_judge=run_judge
    )
    fingerprint = project_input_fingerprint(store, project_id)
    impacted = _impacted_count(store, project_id)
    store.set_pipeline_state(
        project_id, last_input_hash=fingerprint, impacted_count=impacted
    )
    return {
        "project_id": project_id,
        "certified_count": result.certified_count,
        "doubtful_count": result.doubtful_count,
        "pending_count": result.pending_count,
        "cannot_answer_count": result.cannot_answer_count,
        "recomputed_at": datetime.now(UTC).isoformat(),
    }
