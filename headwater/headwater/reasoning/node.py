"""Node contract and base classes.

A node is pure with respect to its declared inputs, so its output is hashable and
cacheable. L nodes never emit facts directly — they ``propose`` (the only place a
model is called) and a paired deterministic ``verify`` grounds the proposal
against the projection and computed stats before any fact is written. This is the
structural form of invariant I-3 ("never raw rows / unverified LLM output").
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from headwater.reasoning.types import (
    InputRef,
    Lane,
    NodeCtx,
    NodeResult,
    ProjectState,
    stable_hash,
)


@runtime_checkable
class Node(Protocol):
    id: str
    lane: Lane

    def inputs(self, state: ProjectState) -> list[InputRef]: ...

    def input_hash(self, state: ProjectState) -> str: ...

    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult: ...


class BaseNode:
    """Default node: hashes the resolved values of its declared inputs."""

    id: str = ""
    lane: Lane = "D"

    def inputs(self, state: ProjectState) -> list[InputRef]:
        raise NotImplementedError

    def input_hash(self, state: ProjectState) -> str:
        return stable_hash([(ref, _resolve(state, ref)) for ref in sorted(self.inputs(state))])

    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        raise NotImplementedError


class LLMNode(BaseNode):
    """L node: propose via the model, then verify deterministically.

    With no provider (NoLLMProvider / None) the proposal is empty and ``verify``
    falls back to a deterministic result — so the engine is always model-optional.
    """

    lane: Lane = "L"

    def propose(self, state: ProjectState, ctx: NodeCtx) -> dict:
        raise NotImplementedError

    def verify(self, proposal: dict, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        raise NotImplementedError

    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        proposal = self.propose(state, ctx) if _has_model(ctx) else {}
        return self.verify(proposal, state, ctx)


def _has_model(ctx: NodeCtx) -> bool:
    llm = ctx.llm
    if llm is None:
        return False
    # Avoid importing analyzer at module load (keeps the import edge clean).
    return type(llm).__name__ != "NoLLMProvider"


def _resolve(state: ProjectState, ref: InputRef) -> object:
    """Resolve an InputRef to its current value for hashing.

    Node outputs (``node:<id>``) are read from this run's state. Store-backed refs
    are registered lazily as nodes are implemented; an unknown ref hashes to its
    own name so a not-yet-wired input is stable (never silently collides).
    """
    if ref.startswith("node:"):
        return state.output_of(ref[len("node:") :])
    resolver = _RESOLVERS.get(ref.split(":", 1)[0])
    if resolver is not None:
        return resolver(state, ref)
    return ref


# Registry of store-backed input resolvers, keyed by the ref prefix.
# Populated as nodes that wrap existing services are added (PR2+).
Resolver = Callable[[ProjectState, InputRef], object]
_RESOLVERS: dict[str, Resolver] = {}


def register_resolver(prefix: str, fn: Resolver) -> None:
    """Register how to read an InputRef family (e.g. 'project', 'source')."""
    _RESOLVERS[prefix] = fn
