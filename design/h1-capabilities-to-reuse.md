# H1 Capabilities to Reuse in H2

H1's workflow and UI are being replaced, but several inspection capabilities are
genuinely useful. Kept as on-demand power tools at the SOURCE level (catalog), not in
the guided project flow — so they help without re-creating H1's overload.

## In the prototype now (minimal)

| Capability | H2 placement | H1 origin to mine |
|---|---|---|
| Editable table structure + modifiable generated metadata | Table detail: editable description, semantic type, column descriptions | data_dictionary / table_semantic_details |
| Semantic locks (edits = ground truth, reused across the source's projects) | Lock toggle per column (invariant I-6) | semantic locks |
| Browse the catalog of tables | Source detail → Catalog tab → table list | catalog / data routes |
| Query tables for quick inspection without leaving | Source-level Query console + per-table Inspect | explorer query (NOT the NL-to-SQL engine) |
| Regenerate metadata (preserving locked edits) | Table detail button | analyzer re-run |

## Deferred (useful, add after the wedge proves out)

| Capability | Why deferred |
|---|---|
| Relationship / lineage graph view | Understand screen shows a simple flow for now; mine schema_graph later |
| Column profile charts (distributions, cardinality) | null% shown; deeper charts add density — defer |
| Decisions / provenance audit log | Valuable for trust provenance; mine H1 decisions / llm_audit later |
| Drift / monitoring over time (living verdict) | The vision's monitoring; mine snapshot-diff later, keep simple |

## Principle

These are inspection/correction tools, available on demand from the catalog — never
pushed into the guided flow. The guided flow stays lean; power users dive into the
catalog when they want to.
