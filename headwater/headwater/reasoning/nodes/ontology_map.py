"""ontology.map — assign every selected column a concept in the upper ontology.

Builds the knowledge projection from the store's columns/profiles/relationships:
a concept node per column (Measure/Dimension/Location/TimeAnchor/Identifier/...)
plus REFERENCES edges from discovered relationships.

L proposes, D verifies: where the deterministic heuristic is UNCERTAIN, the LLM
reads the column's name, dtype, and data-dictionary description and proposes a
concept; ``compatible_concept`` gates each proposal against the column's
observable shape (a Measure must be numeric, a TimeAnchor temporal) before it is
written. With no model the heuristic stands alone — the engine is model-optional.

Reads only the I-3-safe summary (names, dtypes, cardinality, top-N values,
descriptions) — never raw rows.
"""

from __future__ import annotations

import logging
from typing import Any

from headwater.knowledge.ontology import (
    ColumnStats,
    ConceptAssignment,
    classify_column,
    compatible_concept,
)
from headwater.knowledge.projection import GraphEdge, GraphFact, GraphNode
from headwater.reasoning.node import LLMNode
from headwater.reasoning.types import NodeCtx, NodeResult, ProjectState, stable_hash

logger = logging.getLogger(__name__)

# Heuristic assignments at or above this confidence are not second-guessed by
# the model; below it, the LLM's reading of name+description is worth asking for.
_CONFIDENT = 0.75

# Columns per proposal call — bounded so a local model answers within budget.
_PROPOSE_CHUNK = 40

_PROPOSE_SYSTEM = (
    "You classify database columns into a fixed vocabulary of analytical roles. "
    "Given columns (name, type, description), assign each ONE role:\n"
    "- Measure: a numeric quantity worth aggregating (also give unit: "
    "duration|count|amount|rate|quantity)\n"
    "- Dimension: a categorical grouping axis (also give kind: "
    "category|status|step|location)\n"
    "- Code: a short coded value whose meaning needs a mapping\n"
    "- Identifier: a key or join column\n"
    "- TimeAnchor: a date/time column\n"
    "- Location: a place (site, city, region, room)\n"
    "Respond with STRICT JSON, no prose:\n"
    '{"assignments": {"table.column": {"concept": "...", "unit": "...", "kind": "..."}}}'
)

_VALID_UNITS = {"duration", "count", "amount", "rate", "quantity"}
_VALID_KINDS = {"category", "status", "step", "location"}


class OntologyMapNode(LLMNode):
    id = "ontology.map"
    lane = "L"

    def inputs(self, state: ProjectState):
        # The version param salts the input hash so cached pre-LLM assignments
        # are not reused after this node's logic changed.
        return ["project:scope", "project:columns", "project:claims", "param:ontomap:v2"]

    # ── L: propose concepts for the columns the heuristic is unsure about ────

    def propose(self, state: ProjectState, ctx: NodeCtx) -> dict:
        all_stats = self._column_stats(state)
        uncertain = [
            (stats, col)
            for stats, col in all_stats
            if classify_column(stats).confidence < _CONFIDENT
        ]
        logger.info(
            "ontology.map: %d column(s), %d heuristically uncertain — asking the "
            "model to classify those (name+dtype+description)",
            len(all_stats),
            len(uncertain),
        )
        if not uncertain:
            return {}

        from headwater.reasoning.nodes.llm_propose import _invoke

        assignments: dict[str, Any] = {}
        for i in range(0, len(uncertain), _PROPOSE_CHUNK):
            chunk = uncertain[i : i + _PROPOSE_CHUNK]
            lines = []
            for stats, col in chunk:
                desc = str(col.get("description") or "").strip()
                tail = f" — {desc[:90]}" if desc else ""
                lines.append(f"- {stats.ref}: {stats.dtype}{tail}")
            prompt = "COLUMNS:\n" + "\n".join(lines) + "\n\nReturn JSON only."
            try:
                result = _invoke(ctx.llm, prompt, _PROPOSE_SYSTEM)
            except Exception as exc:
                logger.warning(
                    "ontology.map: chunk %d (%d cols) FAILED (%s) — those columns "
                    "keep their heuristic concepts",
                    i // _PROPOSE_CHUNK + 1,
                    len(chunk),
                    exc.__class__.__name__,
                )
                continue  # a failed chunk falls back to heuristics for its columns
            got = result.get("assignments") if isinstance(result, dict) else None
            if isinstance(got, dict):
                assignments.update(got)
        logger.info(
            "ontology.map: model proposed concepts for %d of %d uncertain column(s)",
            len(assignments),
            len(uncertain),
        )
        return {"assignments": assignments}

    # ── D: verify proposals against observable shape; heuristic fallback ─────

    def verify(self, proposal: dict, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        store = state.store
        pid = state.project_id
        sources = store.get_project_sources(pid)
        if not sources:
            return NodeResult(output={"concepts": {}})
        source = sources[0]["source_name"]

        proposed: dict[str, Any] = (
            proposal.get("assignments") if isinstance(proposal, dict) else None
        ) or {}

        facts: list[GraphFact] = []
        counts: dict[str, int] = {}
        accepted = 0
        rejected: list[str] = []
        for stats, _col in self._column_stats(state):
            assign = classify_column(stats)
            llm = proposed.get(stats.ref)
            if assign.confidence < _CONFIDENT and isinstance(llm, dict):
                wanted = str(llm.get("concept") or "")
                if not compatible_concept(wanted, stats):
                    # Shape-incompatible proposal (e.g. text column as Measure):
                    # the heuristic concept stands.
                    rejected.append(f"{stats.ref} ({wanted}/{stats.dtype})")
                    llm = None
            if (
                assign.confidence < _CONFIDENT
                and isinstance(llm, dict)
                and compatible_concept(str(llm.get("concept") or ""), stats)
            ):
                accepted += 1
                concept = str(llm["concept"])
                props: dict[str, str] = {}
                if concept == "Measure":
                    unit = str(llm.get("unit") or "").strip().lower()
                    props["unit"] = unit if unit in _VALID_UNITS else "quantity"
                elif concept == "Dimension":
                    kind = str(llm.get("kind") or "").strip().lower()
                    props["kind"] = kind if kind in _VALID_KINDS else "category"
                elif concept == "Code":
                    props["needs_mapping"] = "1"
                elif concept == "Location":
                    props["kind"] = "location"
                assign = ConceptAssignment(
                    stats.ref, concept, props, 0.75, source="llm"  # type: ignore[arg-type]
                )
            facts.append(assign.to_node())
            counts[assign.concept] = counts.get(assign.concept, 0) + 1

        for r in store.get_relationships(source):
            facts.append(
                GraphEdge(
                    f"col:{r['from_table']}.{r['from_column']}",
                    "REFERENCES",
                    f"col:{r['to_table']}.{r['to_column']}",
                    {"confidence": str(r.get("confidence") or "")},
                )
            )

        if proposed:
            logger.info(
                "ontology.map: accepted %d model concept(s); rejected %d as "
                "shape-incompatible%s",
                accepted,
                len(rejected),
                f" ({', '.join(rejected[:5])})" if rejected else "",
            )

        node_ids = tuple(f.id for f in facts if isinstance(f, GraphNode))
        from headwater.reasoning.types import ProvenanceRef

        prov = ProvenanceRef(
            produced_by=self.id,
            input_hash=stable_hash(self.inputs(state)),
            lane=self.lane,
            fact_ids=node_ids,
        )
        return NodeResult(output={"concepts": counts}, facts=facts, provenance=prov)

    # ── Shared I-3-safe column summaries ──────────────────────────────────────

    def _column_stats(
        self, state: ProjectState
    ) -> list[tuple[ColumnStats, dict[str, Any]]]:
        store = state.store
        sources = store.get_project_sources(state.project_id)
        if not sources:
            return []
        source = sources[0]["source_name"]
        tables = {t["name"]: t for t in store.get_tables(source)}
        selected = sources[0].get("selected_tables") or list(tables)
        profiles = {
            (p["table_name"], p["column_name"]): (p.get("profile") or {})
            for p in store.get_profiles(source)
        }
        rels = store.get_relationships(source)
        fk_cols = {(r["from_table"], r["from_column"]) for r in rels} | {
            (r["to_table"], r["to_column"]) for r in rels
        }

        out: list[tuple[ColumnStats, dict[str, Any]]] = []
        for tname in selected:
            total = int((tables.get(tname) or {}).get("row_count") or 0) or 1
            for col in store.get_columns(source, tname):
                prof = profiles.get((tname, col["name"]), {})
                top = prof.get("top_values") or []
                stats = ColumnStats(
                    ref=f"{tname}.{col['name']}",
                    dtype=str(col.get("dtype") or ""),
                    distinct=int(prof.get("distinct_count") or 0),
                    total=total,
                    top_values=tuple(str(v[0]) for v in top[:6] if v),
                    is_key=bool(col.get("is_primary_key")),
                    in_fk=(tname, col["name"]) in fk_cols,
                )
                out.append((stats, dict(col)))
        return out
