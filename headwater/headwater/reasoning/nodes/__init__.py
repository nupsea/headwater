"""Reasoning nodes — thin wrappers around existing services.

PR2 wraps the current recompute stages as parity nodes so ``recompute_project``
can route through the graph with byte-identical output. Later phases add the
ontology/goal/question vertical and the insight battery.
"""

from headwater.reasoning.nodes.recompute import (
    AnswersNode,
    RelevanceNode,
    build_recompute_graph,
    register_project_resolver,
)

__all__ = [
    "AnswersNode",
    "RelevanceNode",
    "build_recompute_graph",
    "register_project_resolver",
]
