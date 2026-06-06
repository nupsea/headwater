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
    from headwater.reasoning.nodes.goal_parse import _goal_text
    from headwater.reasoning.nodes.llm_propose import build_schema_brief
    from headwater.reasoning.types import stable_hash

    projection = make_projection(settings, store)
    state = ProjectState(project_id, store, projection)
    # ontology.map and goal.parse are L-lane; this is an explicit on-demand
    # generation, so the slow lane is allowed to run.
    ctx = NodeCtx(settings=settings, llm=None, run_slow=True)
    runner = NodeRunner(NodeCache(store), projection, ProvenanceLedger(store))
    runner.run(build_question_vertical(), state, ctx)
    deterministic = state.output_of("question.gen") or []

    # Cache the FINAL question set by (goal + schema + model), so the expensive LLM
    # is attempted AT MOST ONCE per goal/schema — even if it times out and we fall
    # back to deterministic. Without this, a slow/timing-out model is re-attempted
    # on every recompute. Only a goal/scope/schema change invalidates the cache.
    goal_text = _goal_text((store.get_project(project_id) or {}).get("goal") or {})
    if not goal_text.strip():
        return deterministic
    reasoning_model = getattr(settings, "reasoning_model", "") or settings.llm_model
    brief = build_schema_brief(store, project_id, projection)
    cache = NodeCache(store)
    key = stable_hash([project_id, goal_text, brief, reasoning_model])
    cached = cache.get("question.vertical", key)
    if cached is not None:
        return cached

    llm_specs = _attempt_llm(store, project_id, projection, settings, goal_text, brief)
    result = llm_specs or deterministic
    cache.put("question.vertical", key, result)  # cache the outcome either way
    return result


def _attempt_llm(
    store: HeadwaterStore,
    project_id: str,
    projection: Any,
    settings: Any,
    goal_text: str,
    brief: str,
) -> list[dict[str, Any]]:
    """One verified LLM attempt — or [] on no model / error / timeout (graceful)."""
    try:
        from headwater.analyzer.llm import NoLLMProvider, get_provider
        from headwater.reasoning.nodes.llm_propose import propose_and_verify

        reasoning_model = getattr(settings, "reasoning_model", "") or settings.llm_model
        provider = get_provider(settings.model_copy(update={"llm_model": reasoning_model}))
        if isinstance(provider, NoLLMProvider):
            return []
        return propose_and_verify(
            store,
            project_id,
            projection=projection,
            provider=provider,
            goal_text=goal_text,
            brief=brief,
        )
    except Exception:
        return []
