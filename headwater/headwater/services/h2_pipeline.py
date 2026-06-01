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
    source_snapshot_id: str | None = None
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
    exec_items = [
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
        )
        result.answers.append(finalized)

    # Derived state now reflects the current inputs — reconcile the fingerprint
    # so the recompute banner only re-appears after a genuine input change.
    store.set_pipeline_state(
        project_id,
        last_input_hash=project_input_fingerprint(store, project_id),
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

    can_judge = bool(
        statistical_pass and executed is not None and executed.ok and sql_text
    )

    # Gate 2: the judge. On the fast path (run_judge=False) we stop after
    # execution and leave stat-ready answers "pending" certification.
    if not run_judge:
        judge_result = JudgeResult(
            verdict="pending", confidence=0.0, reasons=[], available=False
        )
        state = "pending" if can_judge else "doubtful"
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
        state = _final_state(
            statistical_pass=statistical_pass,
            executed_ok=bool(executed is not None and executed.ok),
            judge=judge_result,
        )

    if state == "doubtful":
        caveats = caveats + _doubt_reasons(
            statistical_pass=statistical_pass,
            readiness_q=readiness_q,
            executed=executed,
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
    )
    _persist(store, fa)
    if run_judge:
        _sync_gap_card(store, question.get("project_id", ""), fa)
    return fa


def _sync_gap_card(store: HeadwaterStore, project_id: str, fa: FinalizedAnswer) -> None:
    """Turn a judge verdict into an actionable Resolve card (truth + ask).

    Certified answers close their gap card.  Doubtful/rejected answers open one
    carrying the judge's reasons and the resolution paths the user can take.
    A user's 'defer' disposition is preserved across recomputes.
    """
    if not project_id:
        return
    card_id = f"{project_id}:answer_gap:{fa.question_id}"

    if fa.state == "certified":
        store.set_resolve_item_status(card_id, "resolved")
        return
    if fa.state != "doubtful":
        return

    reasons = fa.judge_reasons or fa.caveats
    body = (
        "The certification gate did not clear for this answer.\n\n"
        + "\n".join(f"- {r}" for r in reasons[:4])
        + "\n\nResolution paths: provide a column or define a derivation, "
        "add context (paste a data dictionary or notes), confirm a column's "
        "meaning, or defer this to the next cycle."
    )
    existing = next(
        (r for r in store.list_resolve_items(project_id) if r["id"] == card_id), None
    )
    # Don't re-open something the user explicitly deferred.
    status = "deferred" if existing and existing.get("status") == "deferred" else "open"
    store.upsert_resolve_item(
        card_id,
        project_id=project_id,
        issue_kind="answer_gap",
        title=f'Resolve to certify: "{fa.question_title}"',
        body=body,
        question_id=fa.question_id,
        priority="high" if fa.judge_verdict == "reject" else "medium",
        status=status,
        payload={
            "affected_questions": [fa.question_id],
            "contract_impacts": [],
            "judge_verdict": fa.judge_verdict,
            "judge_reasons": fa.judge_reasons,
        },
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
    if statistical_pass and not judge.approves:
        if not judge.available:
            reasons.append(
                "LLM judge unavailable — start a local model "
                "(e.g. `ollama pull qwen2.5:14b-instruct`) to certify."
            )
        else:
            reasons.append(f"Judge withheld certification ({judge.verdict}).")
            reasons.extend(judge.reasons[:2])
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
        trust_bucket=_trust_bucket(fa.state, fa.readiness_pct),
        summary=_verdict_summary(fa),
        source_snapshot_id=fa.source_snapshot_id,
    )

    # Persist the judge result as a contract for audit/display.
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
        key=lambda x: x["id"],
    )
    payload["resolve"] = sorted(
        (
            {"id": r["id"], "status": r.get("status")}
            for r in store.list_resolve_items(project_id)
        ),
        key=lambda x: x["id"],
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
