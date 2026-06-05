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
    """Build the ontology, parse the goal, traverse for goal-aware questions.

    Returns the question specs (title / intent / needed_columns / col_roles /
    reason / unit / score). Empty when no measure x dimension pattern satisfies
    the goal — callers should fall back to the heuristic templates then.
    """
    projection = make_projection(settings, store)
    state = ProjectState(project_id, store, projection)
    # ontology.map and goal.parse are L-lane; this is an explicit on-demand
    # generation, so the slow lane is allowed to run.
    ctx = NodeCtx(settings=settings, llm=None, run_slow=True)
    runner = NodeRunner(NodeCache(store), projection, ProvenanceLedger(store))
    runner.run(build_question_vertical(), state, ctx)
    return state.output_of("question.gen") or []
