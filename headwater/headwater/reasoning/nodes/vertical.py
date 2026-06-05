"""The goal-aware question vertical: ontology.map -> goal.parse -> question.gen.

Runs the three nodes through the runner over the SQLite knowledge projection and
returns the proposed question specs. This is the slice that makes the engine
visibly earn its keep: two different goals on the same schema yield different
questions, because generation is a graph traversal of the goal's intent rather
than a template fill.
"""

from __future__ import annotations

from typing import Any

from headwater.core.store import HeadwaterStore
from headwater.knowledge import make_projection
from headwater.reasoning.cache import NodeCache
from headwater.reasoning.graph import Graph
from headwater.reasoning.ledger import ProvenanceLedger
from headwater.reasoning.nodes.goal_parse import GoalParseNode
from headwater.reasoning.nodes.ontology_map import OntologyMapNode
from headwater.reasoning.nodes.question_gen import QuestionGenNode
from headwater.reasoning.runner import NodeRunner
from headwater.reasoning.types import NodeCtx, ProjectState


def build_question_vertical() -> Graph:
    return Graph().add(OntologyMapNode()).add(GoalParseNode()).add(QuestionGenNode())


def run_question_vertical(
    store: HeadwaterStore, project_id: str, *, settings: Any
) -> list[dict[str, Any]]:
    """Build the ontology, then ask the LLM to propose goal-grounded questions.

    Two tiers: the LLM reads the goal + the ontology-derived schema brief and
    proposes questions mapping the user's vocabulary to real columns (verified
    against the catalog). If no model is available or nothing verifies, fall back
    to the deterministic graph traversal. Returns question specs (title / intent /
    needed_columns / col_roles / reason / unit / score), empty when neither tier
    produces anything — callers then use the heuristic templates.
    """
    projection = make_projection(settings, store)
    state = ProjectState(project_id, store, projection)
    # ontology.map and goal.parse are L-lane; this is an explicit on-demand
    # generation, so the slow lane is allowed to run.
    ctx = NodeCtx(settings=settings, llm=None, run_slow=True)
    runner = NodeRunner(NodeCache(store), projection, ProvenanceLedger(store))
    runner.run(build_question_vertical(), state, ctx)
    deterministic = state.output_of("question.gen") or []

    llm_specs = _llm_proposals(store, project_id, projection, settings)
    return llm_specs or deterministic


def _llm_proposals(
    store: HeadwaterStore, project_id: str, projection: Any, settings: Any
) -> list[dict[str, Any]]:
    """LLM-driven, verified proposals — or [] on any failure (graceful)."""
    try:
        from headwater.analyzer.llm import NoLLMProvider, get_provider
        from headwater.reasoning.nodes.goal_parse import _goal_text
        from headwater.reasoning.nodes.llm_propose import propose_and_verify

        reasoning_model = getattr(settings, "reasoning_model", "") or settings.llm_model
        provider = get_provider(settings.model_copy(update={"llm_model": reasoning_model}))
        if isinstance(provider, NoLLMProvider):
            return []
        goal = (store.get_project(project_id) or {}).get("goal") or {}
        goal_text = _goal_text(goal)
        if not goal_text.strip():
            return []
        return propose_and_verify(
            store,
            project_id,
            projection=projection,
            provider=provider,
            goal_text=goal_text,
        )
    except Exception:
        return []
