"""Core types for the reasoning graph (control plane).

A reasoning run is a topo-ordered pass over typed nodes. Each node declares the
inputs it reads; the runner hashes those inputs and re-executes only the nodes
whose inputs changed (or whose upstream re-ran) — surgical recompute that
replaces the single project fingerprint.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from headwater.knowledge.projection import GraphFact

if TYPE_CHECKING:
    from headwater.core.store import HeadwaterStore
    from headwater.knowledge.projection import KnowledgeProjection

# A canonical, hashable key naming an input a node reads, e.g.
#   "project.goal", "source:<src>.<table>.columns", "claim:locked",
#   "node:profile.stats" (an upstream node output).
InputRef = str

Lane = str  # "D" deterministic | "M" ml | "L" llm


def stable_hash(payload: Any) -> str:
    """Deterministic SHA-256 over an arbitrary JSON-able payload."""
    blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ProvenanceRef:
    """How a fact was produced — the certification ledger's unit."""

    produced_by: str
    input_hash: str
    lane: Lane
    model_id: str | None = None
    fact_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class NodeCost:
    lane: Lane
    wall_ms: int = 0
    model_id: str | None = None


@dataclass(slots=True)
class NodeResult:
    """What a node returns: its output plus facts/provenance to persist."""

    output: Any
    facts: list[GraphFact] = field(default_factory=list)
    provenance: ProvenanceRef | None = None
    cost: NodeCost | None = None


@dataclass(slots=True)
class ProjectState:
    """Read-mostly view a node computes against, plus this run's node outputs."""

    project_id: str
    store: HeadwaterStore
    projection: KnowledgeProjection
    outputs: dict[str, Any] = field(default_factory=dict)

    def adopt(self, node_id: str, output: Any) -> None:
        self.outputs[node_id] = output

    def output_of(self, node_id: str) -> Any:
        return self.outputs.get(node_id)


@dataclass(slots=True)
class NodeCtx:
    """Run-scoped context handed to ``compute``."""

    settings: Any  # HeadwaterSettings (kept loose to avoid import coupling)
    llm: Any = None  # LLMProvider; None/NoLLMProvider => deterministic fallback
    run_slow: bool = False  # gate for the M/L slow lane (e.g. the judge)


@dataclass(slots=True)
class RunReport:
    """What a single run did — the basis for the surgical-recompute proof."""

    ran: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    facts_written: int = 0

    def __str__(self) -> str:  # pragma: no cover - convenience
        return (
            f"RunReport(ran={self.ran}, skipped={self.skipped}, facts_written={self.facts_written})"
        )
