"""goal.parse — turn a free-text business goal into a structured intent.

The intent is expressed over the ontology's concept kinds (measure unit, dimension
kind, unit coercion, comparison), so question generation becomes a graph traversal
rather than a template fill. Deterministic keyword parsing is the fallback and the
verifier; an LLM may sharpen it later (model-optional).

Keyword cues are generic measurement/structure English — never domain vocabulary
(no dataset entity names), per the no-domain-hardcoding invariant.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from headwater.reasoning.node import LLMNode
from headwater.reasoning.types import NodeCtx, NodeResult, ProjectState, stable_hash

_MEASURE_CUES: tuple[tuple[str, str], ...] = (
    ("duration", r"(delay|wait|duration|elapsed|how long|slow|speed|latency|turnaround|time to)"),
    ("count", r"(count|number of|how many|volume|frequency|throughput|cases|records)"),
    ("amount", r"(amount|revenue|cost|price|spend|sales|value|budget)"),
    ("rate", r"(rate|ratio|percent|percentage|share|proportion)"),
)

_DIM_CUES: tuple[tuple[str, str], ...] = (
    ("location", r"(where|location|site|zone|region|area|place|room|geograph)"),
    ("step", r"(step|stage|phase|activity|process|workflow|task)"),
    ("status", r"(status|state|outcome|result|condition)"),
    ("category", r"(which|by |per |across|segment|category|type|group|kind|class)"),
)

_UNIT_CUES: tuple[tuple[str, str], ...] = (
    ("hours", r"(in hours|hourly|hours|hrs)"),
    ("minutes", r"(in minutes|minutes|mins)"),
    ("days", r"(in days|daily|days)"),
)

_TREND_RE = re.compile(
    r"(over time|trend|change|growth|season|month over month|week over week)", re.I
)
_RANK_RE = re.compile(r"(highest|lowest|most|least|top|worst|best|rank|max|min)", re.I)


@dataclass(frozen=True, slots=True)
class GoalIntent:
    """Structured goal over the ontology. Lists (not sets) so it is JSON-cacheable."""

    target_measure_kinds: list[str] = field(default_factory=list)
    by_dimension_kinds: list[str] = field(default_factory=list)
    unit: str | None = None
    comparison: str = "segment"  # segment | rank | trend | coverage

    def to_dict(self) -> dict:
        return {
            "target_measure_kinds": self.target_measure_kinds,
            "by_dimension_kinds": self.by_dimension_kinds,
            "unit": self.unit,
            "comparison": self.comparison,
        }


def parse_goal(text: str) -> GoalIntent:
    """Deterministically derive a GoalIntent from goal text (the D verifier)."""
    t = (text or "").lower()

    measures = [kind for kind, pat in _MEASURE_CUES if re.search(pat, t)]
    if not measures:
        measures = ["quantity"]  # generic fallback so a measure can still match

    dims = [kind for kind, pat in _DIM_CUES if re.search(pat, t)]
    if not dims:
        dims = ["category"]

    unit = next((u for u, pat in _UNIT_CUES if re.search(pat, t)), None)

    if _TREND_RE.search(t):
        comparison = "trend"
    elif _RANK_RE.search(t):
        comparison = "rank"
    else:
        comparison = "segment"

    return GoalIntent(
        target_measure_kinds=measures,
        by_dimension_kinds=dims,
        unit=unit,
        comparison=comparison,
    )


def _goal_text(goal: dict) -> str:
    """Flatten a goal dict to searchable text (statement + any string fields)."""
    if not goal:
        return ""
    parts: list[str] = []
    for v in goal.values():
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, (list, tuple)):
            parts.extend(str(x) for x in v)
    return " ".join(parts)


class GoalParseNode(LLMNode):
    """L proposes (later), D verifies/falls back via :func:`parse_goal`."""

    id = "goal.parse"
    lane = "L"

    def inputs(self, state: ProjectState):
        return ["project:goal", "node:ontology.map"]

    def propose(self, state: ProjectState, ctx: NodeCtx) -> dict:
        # Model hook lands in a later pass; the deterministic verifier stands today.
        return {}

    def verify(self, proposal: dict, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        goal = (state.store.get_project(state.project_id) or {}).get("goal") or {}
        intent = parse_goal(_goal_text(goal))
        return NodeResult(
            output=intent.to_dict(),
            provenance=self._prov(state),
        )

    def _prov(self, state: ProjectState):
        from headwater.reasoning.types import ProvenanceRef

        return ProvenanceRef(
            produced_by=self.id,
            input_hash=stable_hash(self.inputs(state)),
            lane=self.lane,
        )
