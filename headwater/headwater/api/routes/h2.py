"""Headwater 2 API routes.

Thin wrappers over the H2 service layer — all business logic stays in the
services.  Routes enforce source/project limits from config, never from
hard-coded constants.

Route structure:
  /h2/sources                  discover, list, get
  /h2/sources/{name}/catalog   view and edit column metadata
  /h2/projects                 frame, list, get
  /h2/projects/{id}/relevance  re-run relevance
  /h2/projects/{id}/resolve    build and list resolve cards
  /h2/projects/{id}/readiness  evaluate readiness
  /h2/projects/{id}/eda        run EDA families
  /h2/projects/{id}/answer     draft answers
  /h2/projects/{id}/certify    re-evaluate certification
  /h2/projects/{id}/report     markdown audit report
  /h2/projects/{id}/resources  ingest resource files
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile, status
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore

router = APIRouter(prefix="/h2", tags=["h2"])

_MAX_SOURCES = 3
_MAX_PROJECTS_PER_SOURCE = 5


# ── Dependency ────────────────────────────────────────────────────────────────

def _get_store() -> HeadwaterStore:
    settings = get_settings()
    settings.ensure_dirs()
    db_path = settings.data_dir / "h2_metadata.db"
    store = HeadwaterStore(db_path)
    store.init()
    return store


# ── Request / Response models ─────────────────────────────────────────────────

class DiscoverRequest(BaseModel):
    path: str
    source_type: str | None = None
    name: str | None = None


class FrameProjectRequest(BaseModel):
    project_id: str
    source_name: str
    display_name: str
    goal: str
    decision: str | None = None
    target_metric: str | None = None
    entities: list[str] = []
    time_horizon: str | None = None
    selected_tables: list[str] = []


class UpdateColumnRequest(BaseModel):
    description: str | None = None
    semantic_type: str | None = None
    dtype: str | None = None
    locked: bool | None = None


class IngestResourceRequest(BaseModel):
    content: str
    filename: str
    lock: bool = False


class ResolveDispositionRequest(BaseModel):
    status: str  # open | deferred | resolved


class DefineCardRequest(BaseModel):
    markdown: str


class DeriveCardRequest(BaseModel):
    format_id: str


class QueryRequest(BaseModel):
    source_name: str
    sql: str


# ── Sources ───────────────────────────────────────────────────────────────────

@router.post("/sources", status_code=status.HTTP_201_CREATED)
def discover_source(req: DiscoverRequest) -> dict[str, Any]:
    from headwater.services.h2_source import discover_and_persist

    store = _get_store()
    try:
        existing = store.con.execute("SELECT COUNT(*) FROM sources").fetchone()
        if existing and existing[0] >= _MAX_SOURCES:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Maximum {_MAX_SOURCES} sources allowed. Remove one before adding another.",
            )
        outcome = discover_and_persist(
            req.path,
            store=store,
            source_type=req.source_type,
            name=req.name,
        )
    finally:
        store.close()

    return {
        "snapshot_id": outcome.snapshot_id,
        "table_count": len(outcome.discovery.tables),
        "profile_count": len(outcome.discovery.profiles),
        "relationship_count": len(outcome.discovery.relationships),
    }


@router.get("/sources")
def list_sources() -> list[dict[str, Any]]:
    store = _get_store()
    try:
        rows = store.con.execute("SELECT * FROM sources ORDER BY updated_at DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        store.close()


@router.get("/sources/{source_name}")
def get_source(source_name: str) -> dict[str, Any]:
    store = _get_store()
    try:
        src = store.get_source(source_name)
        if src is None:
            raise HTTPException(status_code=404, detail=f"Source '{source_name}' not found.")
        tables = store.get_tables(source_name)
        snapshot = store.get_latest_source_snapshot(source_name)
        return {**src, "tables": tables, "latest_snapshot": snapshot}
    finally:
        store.close()


# ── Source catalog ────────────────────────────────────────────────────────────

@router.get("/sources/{source_name}/catalog")
def get_catalog(
    source_name: str,
    table: str | None = None,
    project_id: str | None = None,
) -> list[dict[str, Any]]:
    from headwater.services.h2_catalog import get_source_catalog

    store = _get_store()
    try:
        catalog = get_source_catalog(store, source_name, table_name=table,
                                     project_id=project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    result = []
    for tbl in catalog:
        result.append({
            "table_name": tbl.table_name,
            "row_count": tbl.row_count,
            "description": tbl.description,
            "columns": [
                {
                    "column_name": col.column_name,
                    "dtype": col.dtype,
                    "semantic_type": col.semantic_type,
                    "description": col.description,
                    "locked": col.locked,
                    "ordinal": col.ordinal,
                    "profile_summary": col.profile_summary,
                }
                for col in tbl.columns
            ],
        })
    return result


@router.post("/sources/{source_name}/suggest-goal")
def suggest_goal(source_name: str) -> dict[str, Any]:
    """LLM-propose an analysis goal inferred from the source schema (metadata only)."""
    from headwater.services.h2_enrich import suggest_goal as _suggest

    store = _get_store()
    try:
        return _suggest(store, source_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()


@router.post("/sources/{source_name}/generate-descriptions")
def generate_descriptions(source_name: str, overwrite: bool = False) -> dict[str, Any]:
    """LLM-generate column descriptions from names/types (one call per table)."""
    from headwater.services.h2_enrich import generate_descriptions as _gen

    store = _get_store()
    try:
        return _gen(store, source_name, overwrite=overwrite)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()


@router.get("/sources/{source_name}/relationships")
def get_relationships(source_name: str) -> list[dict[str, Any]]:
    """Inferred relationships for a source — feeds the data-model diagram."""
    store = _get_store()
    try:
        rels = store.get_relationships(source_name)
    finally:
        store.close()
    return [
        {
            "from_table": r["from_table"],
            "from_column": r["from_column"],
            "to_table": r["to_table"],
            "to_column": r["to_column"],
            "rel_type": r["rel_type"],
            "confidence": r["confidence"],
            "referential_integrity": r["referential_integrity"],
        }
        for r in rels
    ]


@router.patch("/sources/{source_name}/catalog/{table_name}/{column_name}")
def update_catalog_column(
    source_name: str,
    table_name: str,
    column_name: str,
    req: UpdateColumnRequest,
) -> dict[str, Any]:
    from headwater.services.h2_catalog import update_column

    store = _get_store()
    try:
        update_column(
            store, source_name, table_name, column_name,
            description=req.description,
            semantic_type=req.semantic_type,
            dtype=req.dtype,
            lock=req.locked,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return {"updated": f"{source_name}.{table_name}.{column_name}"}


# ── Projects ──────────────────────────────────────────────────────────────────

@router.post("/projects", status_code=status.HTTP_201_CREATED)
def frame_project(req: FrameProjectRequest) -> dict[str, Any]:
    from headwater.core.config import get_settings as _gs
    from headwater.services.h2_project import frame_project as _frame
    from headwater.services.h2_project import propose_relevance

    store = _get_store()
    try:
        src_projects = store.con.execute(
            "SELECT COUNT(*) FROM project_sources WHERE source_name=?",
            (req.source_name,),
        ).fetchone()
        if src_projects and src_projects[0] >= _MAX_PROJECTS_PER_SOURCE:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Maximum {_MAX_PROJECTS_PER_SOURCE} projects per source.",
            )
        settings = _gs()
        spec = _frame(
            store=store,
            project_id=req.project_id,
            source_name=req.source_name,
            display_name=req.display_name,
            goal_statement=req.goal,
            selected_tables=req.selected_tables,
            decision=req.decision,
            target_metric=req.target_metric,
            entities=req.entities,
            time_horizon=req.time_horizon,
            settings=settings,
        )
        relevance = propose_relevance(store=store, project_id=req.project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return {
        "project_id": spec.project_id,
        "source_name": spec.source_name,
        "source_snapshot_id": spec.source_snapshot_id,
        "selected_tables": spec.selected_tables,
        "relevant_columns": [
            {
                "table_name": c.table_name,
                "column_name": c.column_name,
                "semantic_role": c.semantic_role,
                "score": c.score,
                "reason": c.reason,
            }
            for c in relevance.relevant_columns
        ],
        "proposed_questions": [
            {
                "question_id": q.question_id,
                "title": q.title,
                "answerability": q.answerability,
                "reason": q.reason,
                "needed_columns": q.needed_columns,
                "confidence": q.confidence,
            }
            for q in relevance.proposed_questions
        ],
        "notes": relevance.notes,
    }


@router.get("/projects")
def list_projects() -> list[dict[str, Any]]:
    store = _get_store()
    try:
        return store.list_projects()
    finally:
        store.close()


@router.get("/projects/{project_id}")
def get_project(project_id: str) -> dict[str, Any]:
    store = _get_store()
    try:
        project = store.get_project(project_id)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
        questions = store.list_questions(project_id)
        sources = store.get_project_sources(project_id)
        return {**project, "questions": questions, "sources": sources}
    finally:
        store.close()


class SetGoalRequest(BaseModel):
    goal: str


@router.post("/projects/{project_id}/goal")
def set_goal(project_id: str, req: SetGoalRequest) -> dict[str, Any]:
    """Set/refine a project's goal and re-propose relevant questions."""
    from headwater.services.h2_project import set_project_goal

    if len(req.goal.strip()) < 6:
        raise HTTPException(status_code=422, detail="Goal must be at least 6 characters.")
    store = _get_store()
    try:
        set_project_goal(store, project_id, req.goal.strip())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()
    return {"project_id": project_id, "goal": req.goal.strip()}


@router.post("/projects/{project_id}/relevance")
def rerun_relevance(project_id: str) -> dict[str, Any]:
    from headwater.services.h2_project import propose_relevance

    store = _get_store()
    try:
        result = propose_relevance(store=store, project_id=project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return {
        "source_snapshot_id": result.source_snapshot_id,
        "selected_tables": result.selected_tables,
        "relevant_columns": [
            {"table_name": c.table_name, "column_name": c.column_name,
             "semantic_role": c.semantic_role, "score": c.score, "reason": c.reason}
            for c in result.relevant_columns
        ],
        "proposed_questions": [
            {"question_id": q.question_id, "title": q.title,
             "answerability": q.answerability, "reason": q.reason}
            for q in result.proposed_questions
        ],
        "notes": result.notes,
    }


class QuestionDispositionRequest(BaseModel):
    dropped: bool


@router.post("/projects/{project_id}/questions/{question_id}/disposition")
def set_question_disposition(
    project_id: str, question_id: str, req: QuestionDispositionRequest
) -> dict[str, Any]:
    """Keep or drop a proposed question. Dropped questions are excluded from
    readiness/answers and stay dropped across recomputes (the upsert preserves
    a 'dropped' status). Restoring re-enables it per its answerability."""
    store = _get_store()
    try:
        q = store.get_question(question_id)
        if q is None or q.get("project_id") != project_id:
            raise HTTPException(status_code=404, detail="Question not found.")
        if req.dropped:
            status = "dropped"
        else:
            status = (
                "cannot_answer"
                if q.get("answerability") == "cannot_answer"
                else "draft"
            )
        store.set_question_status(question_id, status)
    finally:
        store.close()
    return {"question_id": question_id, "status": status, "dropped": req.dropped}


# ── Resolve ───────────────────────────────────────────────────────────────────

@router.post("/projects/{project_id}/resolve")
def build_resolve(project_id: str) -> list[dict[str, Any]]:
    """Rebuild resolve cards from current data and return the lean set."""
    return _rebuild_and_format(project_id)


@router.get("/projects/{project_id}/resolve")
def get_resolve(project_id: str) -> list[dict[str, Any]]:
    """Return the current resolve cards, rebuilt fresh from the data.

    Building on read keeps the screen honest: cards are derived structurally
    (undefined codes, unusable measures, coverage gaps), resolved/satisfied ones
    drop off, and stale cards from earlier versions are purged automatically — no
    manual Rebuild needed.
    """
    return _rebuild_and_format(project_id)


def _rebuild_and_format(project_id: str) -> list[dict[str, Any]]:
    from headwater.services.h2_readiness import _columns_with_satisfying_claim
    from headwater.services.h2_resolve import build_resolve_cards

    store = _get_store()
    try:
        build_resolve_cards(store, project_id)  # owns the set; purges stale cards
        items = store.list_resolve_items(project_id)
        claims = store.list_semantic_claims(project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    satisfied = _columns_with_satisfying_claim(claims)
    out: list[dict[str, Any]] = []
    for it in items:
        # Once an item is resolved or its column is satisfied, it's done — keep it
        # off the screen so the list stays lean.
        if it.get("status") == "resolved":
            continue
        payload = it.get("payload") or {}
        key = (
            f"{payload['table']}.{payload['column']}"
            if payload.get("table") and payload.get("column")
            else None
        )
        # A meaning-definition hides enum/definition cards, but an unusable_measure
        # card is cleared only by a derivation (tracked via status=resolved), not
        # by a definition — so don't hide it here.
        if key and key in satisfied and it["issue_kind"] != "unusable_measure":
            continue
        # "limitation" = a data/coverage gap the analyst can't fix by defining a
        # term (informational); everything else is an actionable input.
        category = payload.get("category") or (
            "limitation"
            if it["issue_kind"] in ("insufficient_coverage", "cannot_answer_gap")
            else "input"
        )
        out.append(
            {
                "card_id": it["id"],
                "issue_kind": it["issue_kind"],
                "priority": it["priority"],
                "title": it["title"],
                "body": it["body"],
                "status": it.get("status", "open"),
                "category": category,
                # Concrete code values shown as chips for enum cards.
                "values": payload.get("values", []),
                # Duration parse-to-minutes proposal for unusable-measure cards.
                "derivation": payload.get("derivation"),
                "why": payload.get("why", []),
                "affected_questions": payload.get("affected_questions", []),
                "affected_titles": payload.get("affected_titles", []),
                "contract_impacts": payload.get("contract_impacts", []),
            }
        )
    return out


def _claim_display(claim: dict[str, Any] | None) -> str:
    """Render a saved claim back to the text the analyst would recognize."""
    if not claim:
        return ""
    body = claim.get("claim") or {}
    if body.get("text"):
        return str(body["text"])
    value = body.get("value")
    if isinstance(value, dict):
        lines = ["| code | meaning |", "| --- | --- |"]
        lines += [f"| {k} | {v} |" for k, v in value.items()]
        return "\n".join(lines)
    return str(value or "")


@router.post("/projects/{project_id}/resolve/{card_id}/disposition")
def set_resolve_disposition(
    project_id: str, card_id: str, req: ResolveDispositionRequest
) -> dict[str, Any]:
    """Record a disposition on a resolve card (e.g. defer to next cycle)."""
    if req.status not in {"open", "deferred", "resolved"}:
        raise HTTPException(status_code=422, detail="status must be open|deferred|resolved")
    store = _get_store()
    try:
        store.set_resolve_item_status(card_id, req.status)
    finally:
        store.close()
    return {"card_id": card_id, "status": req.status}


@router.post("/projects/{project_id}/resolve/{card_id}/suggest")
def suggest_resolution(project_id: str, card_id: str) -> dict[str, Any]:
    """Ask the local LLM to draft a resolution for a card (for human review)."""
    from headwater.services.h2_enrich import suggest_resolution as _suggest

    store = _get_store()
    try:
        return _suggest(store, project_id, card_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()


@router.post("/projects/{project_id}/resolve/{card_id}/define")
def define_resolve(
    project_id: str, card_id: str, req: DefineCardRequest
) -> dict[str, Any]:
    """Bind a card's definition to its column as a locked semantic claim (S-BIND)."""
    from headwater.services.h2_resolve import define_card

    store = _get_store()
    try:
        return define_card(store, project_id, card_id, req.markdown)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()


@router.post("/projects/{project_id}/resolve/{card_id}/derive")
def derive_resolve(
    project_id: str, card_id: str, req: DeriveCardRequest
) -> dict[str, Any]:
    """Confirm a parse-to-minutes duration derivation for an unusable-measure card."""
    from headwater.services.h2_resolve import confirm_duration_derivation

    store = _get_store()
    try:
        return confirm_duration_derivation(store, project_id, card_id, req.format_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()


# ── Ad-hoc query (power tool) ─────────────────────────────────────────────────

@router.post("/query")
def run_query(req: QueryRequest) -> dict[str, Any]:
    """Run read-only SQL against a freshly materialized source (sandboxed)."""
    from headwater.services.h2_execute import execute_one, materialize_source

    store = _get_store()
    con = None
    try:
        con, _ = materialize_source(store, req.source_name)
        result = execute_one(con, "adhoc", req.sql)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        if con is not None:
            con.close()
        store.close()

    return {
        "columns": result.columns,
        "rows": result.rows,
        "row_count": result.row_count,
        "truncated": result.truncated,
        "error": result.error,
    }


# ── Readiness ─────────────────────────────────────────────────────────────────

@router.post("/projects/{project_id}/readiness")
def evaluate_readiness(project_id: str) -> dict[str, Any]:
    from headwater.services.h2_readiness import evaluate_project_readiness

    store = _get_store()
    try:
        report = evaluate_project_readiness(store, project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return {
        "project_id": report.project_id,
        "source_name": report.source_name,
        "source_snapshot_id": report.source_snapshot_id,
        "certified_count": report.certified_count,
        "draft_count": report.draft_count,
        "cannot_answer_count": report.cannot_answer_count,
        "questions": [
            {
                "question_id": q.question_id,
                "state": q.state,
                "readiness_pct": q.readiness_pct,
                "summary": q.summary,
                "title": q.title,
                "needed_columns": q.needed_columns,
                "contracts": [
                    {"contract_type": c.contract_type, "passed": c.passed, "note": c.note}
                    for c in q.contracts
                ],
            }
            for q in report.questions
        ],
    }


# ── EDA ───────────────────────────────────────────────────────────────────────

@router.post("/projects/{project_id}/eda")
def run_eda(project_id: str) -> dict[str, Any]:
    from headwater.services.h2_eda import run_eda as _run_eda

    store = _get_store()
    try:
        report = _run_eda(store, project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return {
        "project_id": report.project_id,
        "source_name": report.source_name,
        "findings_count": len(report.findings),
        "critical_count": len(report.critical_findings),
        "insight_confidence_score": report.insight_confidence_score,
        "top_findings": [
            {
                "col_ref": f.col_ref,
                "family": f.family,
                "title": f.title,
                "confidence": f.confidence,
                "effect_size": f.effect_size,
                "flags": f.flags,
            }
            for f in report.findings[:20]
        ],
    }


# ── Answer ────────────────────────────────────────────────────────────────────

def _answers_payload(result: Any) -> dict[str, Any]:
    return {
        "project_id": result.project_id,
        "certified_count": result.certified_count,
        "doubtful_count": result.doubtful_count,
        "pending_count": result.pending_count,
        "cannot_answer_count": result.cannot_answer_count,
        "answers": [
            {
                "question_id": a.question_id,
                "question_title": a.question_title,
                "state": a.state,
                "confidence": a.judge_confidence,
                "sql_text": a.sql_text,
                "chart_spec": a.chart_spec,
                "columns": a.columns,
                "rows": a.rows,
                "row_count": a.row_count,
                "truncated": a.truncated,
                "result_stats": a.result_stats,
                "readiness_pct": a.readiness_pct,
                "statistical_pass": a.statistical_pass,
                "judge_verdict": a.judge_verdict,
                "judge_confidence": a.judge_confidence,
                "judge_reasons": a.judge_reasons,
                "caveats": a.caveats,
                "execution_error": a.execution_error,
                "value_labels": a.value_labels,
            }
            for a in result.answers
        ],
    }


@router.post("/projects/{project_id}/answer")
def draft_answers(project_id: str) -> dict[str, Any]:
    """Fast path: draft SQL, execute it, and return real data (no LLM judge).

    Stat-ready answers come back as ``pending`` certification.  The UI renders
    data + charts immediately and lets the user trigger certification separately
    via /answer/certify (the judge can be slow on a local model).
    """
    from headwater.services.h2_pipeline import finalize_project_answers

    store = _get_store()
    try:
        result = finalize_project_answers(store, project_id, run_judge=False)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return _answers_payload(result)


@router.post("/projects/{project_id}/answer/certify")
def certify_answers(project_id: str) -> dict[str, Any]:
    """Two-factor certification: run the LLM judge over the executed answers.

    User-triggered because the local model can take seconds per question.
    """
    from headwater.services.h2_pipeline import finalize_project_answers

    store = _get_store()
    try:
        result = finalize_project_answers(store, project_id, run_judge=True)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return _answers_payload(result)


# ── Recompute spine (staged) ──────────────────────────────────────────────────

@router.get("/projects/{project_id}/state")
def get_state(project_id: str) -> dict[str, Any]:
    """Is derived state stale relative to current inputs? Drives the UI banner."""
    from headwater.services.h2_pipeline import get_project_state

    store = _get_store()
    try:
        return get_project_state(store, project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()


@router.post("/projects/{project_id}/recompute")
def recompute(project_id: str) -> dict[str, Any]:
    """Re-run derived stages from current inputs (fast; certification separate)."""
    from headwater.services.h2_pipeline import recompute_project

    store = _get_store()
    try:
        return recompute_project(store, project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()


# ── Certify ───────────────────────────────────────────────────────────────────

@router.post("/projects/{project_id}/certify")
def certify_project(project_id: str) -> dict[str, Any]:
    from headwater.services.h2_certify import evaluate_and_certify

    store = _get_store()
    try:
        report = evaluate_and_certify(store, project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return {
        "project_id": report.project_id,
        "source_snapshot_id": report.source_snapshot_id,
        "demotions": [
            {
                "question_id": d.question_id,
                "question_title": d.question_title,
                "prior_snapshot_id": d.prior_snapshot_id,
                "breaking_contracts": d.breaking_contracts,
                "drift_summary": d.drift_summary,
            }
            for d in report.demotions
        ],
        "newly_certified": report.newly_certified,
        "unchanged": report.unchanged,
        "has_drift": bool(report.snapshot_diff and report.snapshot_diff.has_changes),
    }


# ── Report ────────────────────────────────────────────────────────────────────

@router.get("/projects/{project_id}/report")
def get_report(project_id: str) -> PlainTextResponse:
    from headwater.services.h2_report import build_report

    store = _get_store()
    try:
        text = build_report(store, project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return PlainTextResponse(content=text, media_type="text/markdown")


# ── Resources ─────────────────────────────────────────────────────────────────

@router.post("/projects/{project_id}/resources")
async def ingest_resource(
    project_id: str,
    file: UploadFile,
    lock: bool = False,
) -> dict[str, Any]:
    from headwater.services.h2_resource import ingest_resource as _ingest

    settings = get_settings()
    settings.ensure_dirs()
    tmp_dir = settings.data_dir / "resources" / project_id
    tmp_dir.mkdir(parents=True, exist_ok=True)
    filename = file.filename or "resource.md"
    dest = tmp_dir / filename
    content = await file.read()
    dest.write_bytes(content)

    store = _get_store()
    try:
        result = _ingest(store, project_id, dest, lock_on_ingest=lock)
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return {
        "resource_path": result.resource_path,
        "resource_format": result.resource_format,
        "sensitivity": result.sensitivity,
        "sensitivity_notes": result.sensitivity_notes,
        "claims_created": result.claims_created,
        "claims_updated": result.claims_updated,
        "claims_skipped_locked": result.claims_skipped_locked,
        "conflicts_detected": result.conflicts_detected,
        "notes": result.notes,
    }


@router.get("/projects/{project_id}/resources")
def list_resources(project_id: str) -> list[dict[str, Any]]:
    store = _get_store()
    try:
        claim = store.get_semantic_claim(f"{project_id}:resource_registry")
    finally:
        store.close()

    if claim is None:
        return []
    return list(claim.get("claim", {}).get("value") or [])
