"""Reasoning nodes — thin wrappers around existing services.

PR2 wraps the current recompute stages as parity nodes so ``recompute_project``
can route through the graph with byte-identical output. Later phases add the
ontology/goal/question vertical and the insight battery.
"""

from headwater.reasoning.nodes.goal_parse import GoalIntent, GoalParseNode, parse_goal
from headwater.reasoning.nodes.ontology_map import OntologyMapNode
from headwater.reasoning.nodes.question_gen import QuestionGenNode, QuestionSpec
from headwater.reasoning.nodes.recompute import (
    AnswersNode,
    RelevanceNode,
    build_recompute_graph,
    register_project_resolver,
)
from headwater.reasoning.nodes.vertical import (
    build_question_vertical,
    run_question_vertical,
)

__all__ = [
    "AnswersNode",
    "GoalIntent",
    "GoalParseNode",
    "OntologyMapNode",
    "QuestionGenNode",
    "QuestionSpec",
    "RelevanceNode",
    "build_question_vertical",
    "build_recompute_graph",
    "parse_goal",
    "register_project_resolver",
    "run_question_vertical",
]
