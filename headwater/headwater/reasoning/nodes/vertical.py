"""The goal-aware question vertical: ontology.map -> goal.parse -> question.gen.

Runs the three nodes through the runner over the SQLite knowledge projection and
returns the proposed question specs. This is the slice that makes the engine
visibly earn its keep: two different goals on the same schema yield different
questions, because generation is a graph traversal of the goal's intent rather
than a template fill.
"""

from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)


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
    # generation, so the slow lane is allowed to run — with a real provider so
    # the L nodes can propose (they fall back to heuristics without one).
    provider = _provider_or_none(settings)
    logger.info(
        "question.vertical: start project=%s (model %s)",
        project_id,
        "available" if provider is not None else "NOT available — heuristics only",
    )
    ctx = NodeCtx(settings=settings, llm=provider, run_slow=True)
    runner = NodeRunner(NodeCache(store), projection, ProvenanceLedger(store))
    report = runner.run(build_question_vertical(), state, ctx)
    concepts = (state.output_of("ontology.map") or {}).get("concepts") or {}
    logger.info(
        "question.vertical: graph ran=%s skipped=%s; ontology concepts=%s; "
        "parsed intent=%s",
        list(getattr(report, "ran", []) or []),
        list(getattr(report, "skipped", []) or []),
        dict(sorted(concepts.items())),
        state.output_of("goal.parse") or {},
    )
    deterministic = state.output_of("question.gen") or []

    # Cache the FINAL question set by (goal + schema + model), so the expensive LLM
    # is attempted AT MOST ONCE per goal/schema — even if it times out and we fall
    # back to deterministic. Without this, a slow/timing-out model is re-attempted
    # on every recompute. Only a goal/scope/schema change invalidates the cache.
    goal_text = _goal_text((store.get_project(project_id) or {}).get("goal") or {})
    if not goal_text.strip():
        logger.info(
            "question.vertical: no goal text — returning %d deterministic question(s)",
            len(deterministic),
        )
        return deterministic
    reasoning_model = getattr(settings, "reasoning_model", "") or settings.llm_model
    brief = build_schema_brief(store, project_id, projection)
    # Questions the user already has (their own additions, or a prior engine set
    # being regenerated away from): the model is told not to restate them.
    avoid_titles = sorted(
        str(q.get("title") or "").strip()
        for q in store.list_questions(project_id)
        if str(q.get("title") or "").strip()
    )
    cache = NodeCache(store)
    key = stable_hash([project_id, goal_text, brief, reasoning_model, avoid_titles])
    cached = cache.get("question.vertical", key)
    if cached is not None:
        logger.info(
            "question.vertical: cache HIT (goal+schema+model unchanged) — returning "
            "the same %d question(s) WITHOUT calling the model. To force a fresh "
            "set, change the goal or column descriptions, or use Regenerate.",
            len(cached),
        )
        return cached

    llm_specs = _attempt_llm(
        store, project_id, projection, settings, goal_text, brief, avoid_titles
    )
    result = llm_specs or deterministic
    if llm_specs:
        logger.info(
            "question.vertical: using %d model-proposed question(s)", len(llm_specs)
        )
    else:
        logger.info(
            "question.vertical: model produced nothing usable — using %d "
            "deterministic traversal question(s)",
            len(deterministic),
        )
    cache.put("question.vertical", key, result)  # cache the outcome either way
    return result


def _provider_or_none(settings: Any) -> Any:
    """The reasoning-model provider, or None when no model is configured."""
    try:
        from headwater.analyzer.llm import NoLLMProvider, get_provider

        model = getattr(settings, "reasoning_model", "") or settings.llm_model
        provider = get_provider(settings.model_copy(update={"llm_model": model}))
        return None if isinstance(provider, NoLLMProvider) else provider
    except Exception:
        return None


def _attempt_llm(
    store: HeadwaterStore,
    project_id: str,
    projection: Any,
    settings: Any,
    goal_text: str,
    brief: str,
    avoid_titles: list[str] | None = None,
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
            avoid_titles=avoid_titles,
        )
    except Exception:
        return []
