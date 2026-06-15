"""question.gen — generate goal-aware questions by graph traversal.

Reads the parsed GoalIntent and the ontology projection, then finds valid
(Measure x Dimension x join-path) patterns that satisfy the goal — ranked by the
projection's score — and a trend pattern (Measure x TimeAnchor) when the goal asks
for change over time. Each question is emitted as a ``Question`` node with
``DERIVED_FROM`` edges to the columns it depends on (the provenance behind
"why this question" and "what this answer depends on").

This node only *proposes* question specs (and writes graph provenance); persisting
them into the questions table the UI reads is the integration layer's job, so the
node stays free of the services dependency and is hermetically testable.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

from headwater.knowledge.projection import GraphEdge, GraphFact, GraphNode
from headwater.reasoning.node import BaseNode
from headwater.reasoning.types import NodeCtx, NodeResult, ProjectState, stable_hash

_MAX_QUESTIONS = 6


@dataclass(frozen=True, slots=True)
class QuestionSpec:
    title: str
    intent: str  # ranking | segment | trend
    needed_columns: list[str]
    col_roles: dict[str, str] = field(default_factory=dict)
    reason: str = ""
    unit: str | None = None
    score: float = 0.0


def _col(ref_node_id: str) -> str:
    """'col:events.total_duration' -> 'events.total_duration'."""
    return ref_node_id[len("col:") :] if ref_node_id.startswith("col:") else ref_node_id


def _label(ref: str) -> str:
    name = ref.rsplit(".", 1)[-1]
    return re.sub(r"_id$|_key$", "", name).replace("_", " ").strip() or name


def _qid(measure_ref: str, dim_ref: str, intent: str) -> str:
    raw = f"{intent}:{measure_ref}:{dim_ref}"
    return "q:" + re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")


class QuestionGenNode(BaseNode):
    id = "question.gen"
    lane = "D"

    def inputs(self, state: ProjectState):
        return ["node:goal.parse", "node:ontology.map"]

    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        intent = state.output_of("goal.parse") or {}
        measure_kinds = set(intent.get("target_measure_kinds") or [])
        dim_kinds = set(intent.get("by_dimension_kinds") or [])
        unit = intent.get("unit")
        comparison = intent.get("comparison") or "segment"
        proj = state.projection

        specs: list[QuestionSpec] = []
        facts: list[GraphFact] = []

        if comparison == "trend":
            specs, facts = self._trend(proj, measure_kinds, unit)
        if not specs:  # segment/rank, or trend with no time anchor
            specs, facts = self._by_dimension(proj, measure_kinds, dim_kinds, unit, comparison)
        if not specs:
            # Relax: the parsed kinds matched nothing — fall back to any measure
            # against any dimension/location so the engine is never silently empty.
            broad_m = measure_kinds | {"quantity", "count", "amount", "duration", "rate"}
            broad_d = dim_kinds | {"category", "step", "location", "status"}
            specs, facts = self._by_dimension(proj, broad_m, broad_d, unit, comparison)
        # A temporal view is part of any complete question set: when the data
        # has a time anchor and no trend made it in, add one (the top-up source
        # for the LLM path, and breadth for the deterministic path).
        if specs and not any(s.intent == "trend" for s in specs):
            trend_specs, trend_facts = self._trend(proj, measure_kinds, unit)
            for ts in trend_specs:
                if len(specs) >= _MAX_QUESTIONS + 1:
                    break
                specs.append(ts)
                facts.extend(trend_facts)

        out = [asdict(s) for s in specs]
        prov = self._prov(state, facts)
        return NodeResult(output=out, facts=facts, provenance=prov)

    def _by_dimension(self, proj, measure_kinds, dim_kinds, unit, comparison):
        matches = proj.match_measure_dimension(
            measure_kinds=measure_kinds, dim_kinds=dim_kinds, max_hops=2
        )
        specs: list[QuestionSpec] = []
        facts: list[GraphFact] = []
        # Diversity: at most 2 questions per measure and per dimension — six
        # near-identical "which X has the highest <same measure>" rows is one
        # question asked six times, not six questions.
        measure_uses: dict[str, int] = {}
        dim_uses: dict[str, int] = {}
        for m in matches:
            if len(specs) >= _MAX_QUESTIONS:
                break
            if measure_uses.get(m.measure, 0) >= 2 or dim_uses.get(m.dimension, 0) >= 2:
                continue
            measure_uses[m.measure] = measure_uses.get(m.measure, 0) + 1
            dim_uses[m.dimension] = dim_uses.get(m.dimension, 0) + 1
            measure_ref, dim_ref = _col(m.measure), _col(m.dimension)
            mlabel, dlabel = _label(measure_ref), _label(dim_ref)
            usuffix = f" in {unit}" if unit else ""
            intent = "ranking" if comparison == "rank" else "segment"
            title = f"Which {dlabel} has the highest {mlabel}{usuffix}?"
            reason = (
                f"The goal asks about {mlabel} by {dlabel}; ranking the dimension by "
                f"the measure answers it directly."
            )
            specs.append(
                QuestionSpec(
                    title=title,
                    intent=intent,
                    needed_columns=[measure_ref, dim_ref],
                    col_roles={measure_ref: "measure", dim_ref: "categorical"},
                    reason=reason,
                    unit=unit,
                    score=m.score,
                )
            )
            facts.extend(self._question_facts(measure_ref, dim_ref, intent, title, m))
        return specs, facts

    def _trend(self, proj, measure_kinds, unit):
        measures = [
            n for n in proj.nodes_of_type("Measure") if n.props.get("unit") in measure_kinds
        ] or proj.nodes_of_type("Measure")
        times = proj.nodes_of_type("TimeAnchor")
        if not measures or not times:
            return [], []
        # The measure and the time anchor must live in the SAME table — the
        # temporal SQL builder does not join, so a cross-table pair would draft
        # a query that cannot bind.
        pair = next(
            (
                (m, ts)
                for m in measures
                for ts in times
                if m.props.get("table") == ts.props.get("table")
            ),
            None,
        )
        if pair is None:
            return [], []
        m, ts = pair
        measure_ref, time_ref = _col(m.id), _col(ts.id)
        mlabel = _label(measure_ref)
        usuffix = f" in {unit}" if unit else ""
        title = f"How does {mlabel}{usuffix} change over time?"
        spec = QuestionSpec(
            title=title,
            intent="trend",
            needed_columns=[time_ref, measure_ref],
            col_roles={time_ref: "event_ts", measure_ref: "measure"},
            reason="The goal asks how the measure changes over time; a temporal trend answers it.",
            unit=unit,
            score=0.9,
        )
        facts = self._question_facts(measure_ref, time_ref, "trend", title, None)
        return [spec], facts

    def _question_facts(self, measure_ref, dim_ref, intent, title, match):
        qid = _qid(measure_ref, dim_ref, intent)
        facts: list[GraphFact] = [
            GraphNode(qid, "Question", {"title": title, "intent": intent}),
            GraphEdge(qid, "DERIVED_FROM", f"col:{measure_ref}"),
            GraphEdge(qid, "DERIVED_FROM", f"col:{dim_ref}"),
        ]
        if match is not None and match.join_path is not None:
            for edge in match.join_path.edges:
                facts.append(GraphEdge(qid, "DERIVED_FROM", edge.dst, {"via": "join"}))
        return facts

    def _prov(self, state: ProjectState, facts):
        from headwater.reasoning.types import ProvenanceRef

        qids = tuple(f.id for f in facts if isinstance(f, GraphNode))
        return ProvenanceRef(
            produced_by=self.id,
            input_hash=stable_hash(self.inputs(state)),
            lane=self.lane,
            fact_ids=qids,
        )
