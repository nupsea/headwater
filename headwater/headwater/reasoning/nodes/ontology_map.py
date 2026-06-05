"""ontology.map — assign every selected column a concept in the upper ontology.

Builds the knowledge projection from the store's columns/profiles/relationships:
a concept node per column (Measure/Dimension/Location/TimeAnchor/Identifier/...)
plus REFERENCES edges from discovered relationships. ``classify_column`` is the
deterministic verifier and heuristic fallback (an LLM may propose first, later);
locked claims are respected as ground truth (I-6).

Reads only the I-3-safe summary (names, dtypes, cardinality, top-N values) — never
raw rows.
"""

from __future__ import annotations

from headwater.knowledge.ontology import ColumnStats, classify_column
from headwater.knowledge.projection import GraphEdge, GraphFact, GraphNode
from headwater.reasoning.node import LLMNode
from headwater.reasoning.types import NodeCtx, NodeResult, ProjectState, stable_hash


class OntologyMapNode(LLMNode):
    id = "ontology.map"
    lane = "L"

    def inputs(self, state: ProjectState):
        return ["project:scope", "project:columns", "project:claims"]

    def propose(self, state: ProjectState, ctx: NodeCtx) -> dict:
        # Model proposal hook lands later; classify_column is the verifier/fallback.
        return {}

    def verify(self, proposal: dict, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        store = state.store
        pid = state.project_id
        sources = store.get_project_sources(pid)
        if not sources:
            return NodeResult(output={"concepts": {}})
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

        facts: list[GraphFact] = []
        counts: dict[str, int] = {}
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
                assign = classify_column(stats)
                facts.append(assign.to_node())
                counts[assign.concept] = counts.get(assign.concept, 0) + 1

        for r in rels:
            facts.append(
                GraphEdge(
                    f"col:{r['from_table']}.{r['from_column']}",
                    "REFERENCES",
                    f"col:{r['to_table']}.{r['to_column']}",
                    {"confidence": str(r.get("confidence") or "")},
                )
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
