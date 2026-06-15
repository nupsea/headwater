"""Headwater reasoning graph (control plane).

A thin, in-house, typed-node incremental build system. Borrows LangGraph's
concepts (typed state, conditional edges, checkpointing) without the dependency.
Nodes declare their inputs and are pure w.r.t. them, so the runner re-executes
only the subgraph a change touches.
"""

from headwater.reasoning.cache import NodeCache
from headwater.reasoning.graph import Graph, GraphCycleError
from headwater.reasoning.ledger import ProvenanceLedger
from headwater.reasoning.node import BaseNode, LLMNode, Node, register_resolver
from headwater.reasoning.runner import NodeRunner
from headwater.reasoning.types import (
    InputRef,
    NodeCost,
    NodeCtx,
    NodeResult,
    ProjectState,
    ProvenanceRef,
    RunReport,
    stable_hash,
)

__all__ = [
    "BaseNode",
    "Graph",
    "GraphCycleError",
    "InputRef",
    "LLMNode",
    "Node",
    "NodeCache",
    "NodeCost",
    "NodeCtx",
    "NodeResult",
    "NodeRunner",
    "ProjectState",
    "ProvenanceLedger",
    "ProvenanceRef",
    "RunReport",
    "register_resolver",
    "stable_hash",
]
