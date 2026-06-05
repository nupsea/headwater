"""Parity nodes for the recompute spine (PR2).

Two deterministic nodes reproduce today's linear refresh exactly:

    relevance  (propose_relevance)      -> questions + relevant columns
       |  node:relevance (a digest of the produced questions)
       v
    answers    (finalize_project_answers) -> readiness, draft, execute, (judge)

Inputs mirror the sub-payloads ``project_input_fingerprint`` already hashes
(goal / scope / columns / claims / resolve), so a node re-runs exactly when the
legacy staleness check would have flagged the project stale — and a run with no
input change skips both nodes (the legacy path always re-ran everything).

Service calls are imported lazily inside ``compute`` to avoid an import cycle
(services import this module's graph; this module must not import services at
load time).
"""

from __future__ import annotations

from typing import Any

from headwater.core.store import HeadwaterStore
from headwater.reasoning.graph import Graph
from headwater.reasoning.node import BaseNode, register_resolver
from headwater.reasoning.types import (
    InputRef,
    NodeCtx,
    NodeResult,
    ProjectState,
    stable_hash,
)

# The InputRef families a project's derived state depends on. Mirrors the parts
# of ``project_input_fingerprint``.
_GOAL = "project:goal"
_SCOPE = "project:scope"
_COLUMNS = "project:columns"
_CLAIMS = "project:claims"
_RESOLVE = "project:resolve"
_RELEVANCE_INPUTS = [_GOAL, _SCOPE, _COLUMNS, _CLAIMS, _RESOLVE]


def _drop_volatile(row: dict[str, Any]) -> dict[str, Any]:
    """Strip timestamp columns so a digest reflects content, not when it was written."""
    return {k: v for k, v in row.items() if not k.endswith("_at")}


def _project_input(state: ProjectState, ref: InputRef) -> object:
    """Resolve a ``project:*`` input to a stable, content-bearing value.

    Reads live store state; the runner's topo order guarantees upstream writes
    (e.g. relevance persisting questions) have landed before a downstream node
    hashes its inputs.
    """
    store = state.store
    pid = state.project_id
    suffix = ref.split(":", 1)[1]

    if suffix == "goal":
        return (store.get_project(pid) or {}).get("goal") or {}

    if suffix in ("scope", "columns"):
        out: list[dict[str, Any]] = []
        for ps in store.get_project_sources(pid):
            source_name = ps["source_name"]
            selected = sorted(ps.get("selected_tables") or [])
            if not selected:
                selected = sorted(t["name"] for t in store.get_tables(source_name))
            if suffix == "scope":
                out.append({"source": source_name, "selected": selected})
                continue
            for tname in selected:
                cols = store.get_columns(source_name, tname)
                out.append(
                    {
                        "table": f"{source_name}.{tname}",
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
        return out

    if suffix == "claims":
        return sorted(
            (
                {
                    "id": c["id"],
                    "status": c.get("status"),
                    "locked": c.get("locked"),
                    "claim": c.get("claim") or c.get("claim_json"),
                }
                for c in store.list_semantic_claims(pid)
            ),
            key=lambda x: str(x["id"]),
        )

    if suffix == "resolve":
        # Only user-facing resolve items count (derived answer_gap cards do not).
        return sorted(
            (
                {"id": r["id"], "status": r.get("status")}
                for r in store.list_resolve_items(pid)
                if r.get("issue_kind") != "answer_gap"
            ),
            key=lambda x: str(x["id"]),
        )

    return ref


def register_project_resolver() -> None:
    """Idempotently register the ``project:*`` input resolver."""
    register_resolver("project", _project_input)


# Register on import so node input hashing works as soon as the graph is built.
register_project_resolver()


class RelevanceNode(BaseNode):
    """Wraps ``propose_relevance``: re-derive relevant columns + questions."""

    id = "relevance"
    lane = "D"

    def inputs(self, state: ProjectState) -> list[InputRef]:
        return list(_RELEVANCE_INPUTS)

    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        from headwater.services.h2_project import propose_relevance

        propose_relevance(store=state.store, project_id=state.project_id)
        digest = _questions_digest(state.store, state.project_id)
        return NodeResult(output=digest)


class AnswersNode(BaseNode):
    """Wraps ``finalize_project_answers``: readiness, draft, execute, (judge).

    ``run_judge`` is part of the node's input identity so a fast-path (no-judge)
    result is never reused for a certification run.
    """

    id = "answers"
    lane = "D"

    def __init__(self, *, run_judge: bool) -> None:
        self._run_judge = run_judge

    def inputs(self, state: ProjectState) -> list[InputRef]:
        return [
            "node:relevance",
            _CLAIMS,
            _RESOLVE,
            _COLUMNS,
            f"param:run_judge:{int(self._run_judge)}",
        ]

    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        from headwater.services.h2_pipeline import finalize_project_answers

        result = finalize_project_answers(
            state.store,
            state.project_id,
            settings=ctx.settings,
            run_judge=self._run_judge,
        )
        return NodeResult(
            output={
                "certified_count": result.certified_count,
                "doubtful_count": result.doubtful_count,
                "pending_count": result.pending_count,
                "cannot_answer_count": result.cannot_answer_count,
            }
        )


def _questions_digest(store: HeadwaterStore, project_id: str) -> dict[str, Any]:
    """A content-sensitive summary of the produced question set.

    Used as the ``relevance`` node output so downstream invalidation triggers on
    any question change — not merely a change in the question count.
    """
    rows = sorted(
        (_drop_volatile(dict(q)) for q in store.list_questions(project_id)),
        key=lambda r: str(r.get("id")),
    )
    return {"count": len(rows), "digest": stable_hash(rows)}


def build_recompute_graph(*, run_judge: bool) -> Graph:
    """Assemble the recompute DAG: relevance -> answers."""
    return Graph().add(RelevanceNode()).add(AnswersNode(run_judge=run_judge))
