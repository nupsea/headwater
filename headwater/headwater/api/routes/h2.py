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
    locked: bool | None = None


class IngestResourceRequest(BaseModel):
    content: str
    filename: str
    lock: bool = False


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


# ── Resolve ───────────────────────────────────────────────────────────────────

@router.post("/projects/{project_id}/resolve")
def build_resolve(project_id: str) -> list[dict[str, Any]]:
    from headwater.services.h2_resolve import build_resolve_cards

    store = _get_store()
    try:
        cards = build_resolve_cards(store, project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return [
        {
            "card_id": c.card_id,
            "issue_kind": c.issue_kind,
            "priority": c.priority,
            "title": c.title,
            "body": c.body,
            "affected_questions": c.affected_questions,
            "contract_impacts": c.contract_impacts,
        }
        for c in cards
    ]


@router.get("/projects/{project_id}/resolve")
def get_resolve(project_id: str) -> list[dict[str, Any]]:
    store = _get_store()
    try:
        return store.list_resolve_items(project_id)
    finally:
        store.close()


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

@router.post("/projects/{project_id}/answer")
def draft_answers(project_id: str) -> dict[str, Any]:
    from headwater.services.h2_answer import draft_project_answers

    store = _get_store()
    try:
        result = draft_project_answers(store, project_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        store.close()

    return {
        "project_id": result.project_id,
        "certified_count": result.certified_count,
        "draft_count": result.draft_count,
        "cannot_answer_count": result.cannot_answer_count,
        "answers": [
            {
                "question_id": a.question_id,
                "question_title": a.question_title,
                "state": a.state,
                "confidence": a.confidence,
                "sql_text": a.sql_text,
                "chart_spec": a.chart_spec,
                "caveats": a.caveats,
            }
            for a in result.answers
        ],
    }


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
