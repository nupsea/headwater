# Headwater — Reasoning Engine: Architecture & Implementation Proposal

Status: PROPOSAL / build plan. Reconciles `design/HEADWATER_NEXT_DIRECTION.md` with the
technical review of it. Supersedes the *sequencing and dependency* choices in the
direction doc where they conflict; keeps its diagnosis and target architecture.
Date: 2026-06-04.

> One-line framing (unchanged from the direction doc): Headwater becomes a **stateful
> reasoning graph** (control plane) that runs over a **persistent knowledge projection**
> (data plane). Every fact — a relationship, a meaning, an insight, a verdict — is a node
> with provenance, produced by a deterministic/ML node or an LLM node, and recomputed
> **surgically** by routing only through the parts a change touches.

The review's verdict: the direction is right because it attacks the *class* of failure
(no semantic intent, no provenance, no incremental recompute, no grounded insight). It is
**not** green-lit as written. This document encodes the four corrections the review
required and turns the whole thing into a buildable plan with acceptance proofs.

---

## 0. What changed because of the review (the deltas)

| # | Direction doc said | Review required | Locked decision here |
|---|---|---|---|
| D1 | Knowledge graph = **Kuzu** (P2) | Do **not** commit to Kuzu — `kuzudb/kuzu` was **archived read-only on 2025-10-10**. Build an abstraction first. | Define a `KnowledgeProjection` **interface**. Default backend = SQLite-adjacency + Python traversal (already our SoR). Kuzu only ever a *frozen, optional* backend behind the interface. DuckPGQ evaluated as a time-boxed spike. |
| D2 | Roadmap starts with **P1 pure invisible refactor** | A pure refactor risks "another architecture pass with no product proof." | First slice = **P1 keystone refactor + one thin P2 vertical proof** in the same arc. Acceptance is a *product* demo, not a green test suite. |
| D3 | Certification components "shown / principled" | Certification must **fail closed**. `insight_confidence` defaults to *pass* and `no_misleading` is *always true* — incompatible with "the badge is sacred." | Missing/uncomputed evidence caps an answer at **Draft**, never Certified. Implement `no_misleading` for real. (`h2_readiness.py:271`, `:296`.) |
| D4 | Safe-aggregate learning = **P5, in this arc** | **Park it.** NIST DP guidance: aggregation/de-id alone is not a strong guarantee; privacy budgets compose across releases. | P5 is **out of this arc.** Build the trust engine first. Keep the door open by storing ontology-level provenance now (so telemetry is a later read-only projection, not a re-architecture). |
| D5 | PII = P4 (after the engine) | Pull PII forward **if target data contains PII** — it is structural trust, not a late compliance feature. | PII detect/de-id/trust-boundary is built as front-of-graph nodes; its **scheduling** is gated on one fact only the user knows (see §6 decision gate). Default position: right after P2. |
| D6 | "Knowledge graph" is the model | Never expose "knowledge graph" as a user concept. | UI vocabulary is fixed (§9): *"what I think this column means," "why this question," "what this answer depends on," "what would break certification."* The graph is plumbing. |

Everything else in the direction doc stands: the two-graph split, the node taxonomy, the
domain-agnostic ontology, the "L proposes / D·M verify" rule, and the invariant-preservation
table.

---

## 1. Architecture at a glance

```
                         CONTROL PLANE  (Reasoning Graph)
        a thin, in-house, typed-node incremental build system
   ┌───────────────────────────────────────────────────────────────┐
   │  ingest → profile → ontology.map → goal.parse → question.gen   │
   │     │        │           │             │            │          │
   │     │        │           └──── reads/writes ────┐    │          │
   │     ▼        ▼                                  ▼    ▼          │
   │  pii.detect/deid   stat battery (M)        readiness/certify(D) │
   │     │                  │                        │              │
   │  trust.boundary ───────┴──── provenance ────────┘              │
   └───────────────┬───────────────────────────────┬───────────────┘
                   │ per-node input hashes          │ writes facts + provenance
                   ▼                                ▼
        ┌────────────────────┐         DATA PLANE  (Knowledge Projection)
        │ SQLite (system of  │   ┌──────────────────────────────────────┐
        │ record: metadata,  │──▶│  KnowledgeProjection  (interface)     │
        │ node cache,        │   │  default: SQLite nodes/edges + BFS    │
        │ provenance ledger) │   │  optional: DuckPGQ spike / frozen Kuzu│
        └────────────────────┘   │  holds: ontology, columns, rels,      │
        ┌────────────────────┐   │  claims, questions, insights, PII,    │
        │ DuckDB (analytical │   │  DERIVED_FROM provenance edges        │
        │ data only — I-1)   │   └──────────────────────────────────────┘
        └────────────────────┘
```

Two graphs, kept separate in code and in our heads (the review's "two-graph confusion"
risk): the **Reasoning Graph** answers *"what to run and what to re-run."* The
**Knowledge Projection** answers *"what we know and how we know it."* SQLite stays the
system of record (I-1); the projection is **derived and droppable**.

---

## 2. Dimension A — the Reasoning Graph (in-house, ~few hundred lines)

### 2.1 Decision: build, don't adopt (LangGraph-compatible, not LangGraph-dependent)

What we actually need is an **incremental build system** (Dagster/Bazel semantics) with a
few LLM nodes — not an agent chat loop. We build a thin typed-node graph aligned to the
*existing* recompute/fingerprint model, and borrow LangGraph's concepts (typed state,
conditional edges, checkpointer) without the dependency. The node interface stays
LangGraph-shaped so adoption later is a wrapper, not a rewrite.

### 2.2 The node contract

```python
# headwater/reasoning/node.py  (new package)
class NodeResult(BaseModel):
    output: Any                       # the node's product (claims, questions, stats…)
    facts: list[GraphFact] = []       # nodes/edges to upsert into the projection
    provenance: ProvenanceRef         # what produced this, from which inputs
    cost: NodeCost                    # lane (D/M/L), wall-time, model id if any

class Node(Protocol):
    id: str
    lane: Literal["D", "M", "L"]      # deterministic / ML / LLM
    def inputs(self, state: ProjectState) -> list[InputRef]: ...
    def input_hash(self, state: ProjectState) -> str:        # default: hash(inputs())
        ...
    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult: ...
```

- **`inputs()` is explicit.** A node names the store keys / upstream node outputs it
  reads. This is what makes recompute surgical (see 2.4) and what makes a node *pure
  w.r.t. its inputs*, so its output is hashable and cacheable.
- **`compute()` is the only place work happens.** It receives a read view of state and a
  context (settings, store, projection handle, LLM client for L nodes).
- **L nodes may only *propose*.** They emit candidate facts that a paired D/M node grounds
  against the projection + computed stats before anything is written as truth (the
  I-3-strengthening rule, §7).

### 2.3 The runner and the cache

```python
class NodeRunner:
    def run(self, graph: Graph, state: ProjectState, *, changed: set[InputRef]) -> RunReport:
        order = graph.topo_sort()
        for node in order:
            key = (node.id, node.input_hash(state))
            if cached := self.cache.get(key):          # node-level memoization
                state.adopt(cached); continue
            if not self._dirty(node, changed, state):  # inputs unchanged → skip
                continue
            result = node.compute(state, self.ctx)
            self.cache.put(key, result)
            self.projection.apply(result.facts)        # write facts + provenance
            self.ledger.record(result.provenance)      # the certification ledger
            state.adopt(result)
```

Cache + ledger live in **SQLite** (system of record, I-1): tables `node_cache(node_id,
input_hash, output_json, computed_at)` and `provenance(fact_id, produced_by, input_hash,
node_lane, model_id, ts)`. The provenance ledger *is* the certification ledger the vision
doc demands ("recomputable from facts, re-checked on every trigger") and the audit-report
substrate.

### 2.4 Per-node hashing replaces the single project fingerprint

Today `project_input_fingerprint` (`h2_pipeline.py:650`) hashes the **whole** project into
one digest; any change ⇒ `recompute_project` re-runs **every** stage linearly
(`:741` — relevance → readiness → draft → execute). That is the "all-or-nothing" tax the
direction doc calls out and the reason expensive M/L nodes are unaffordable.

Replacement: a **change event** (new source, edited definition, locked claim, new goal,
ingested doc, confirmed derivation) produces a `changed: set[InputRef]`. The runner
invalidates only nodes whose `inputs()` intersect `changed`, then re-runs that subgraph
**and its descendants**. The central-truth axiom is *preserved, not weakened*: recompute
is still the single update path — it is now surgical.

Concrete payoff (the P1 acceptance proof): **editing the goal re-runs `goal.parse →
question.gen` only** — not `profile`, not the relationship battery, not the shared-source
nodes.

### 2.5 Two-speed execution

- **Fast lane (D, every change):** profiling deltas, stat integrity, relationship/grain
  re-check, staleness propagation, certification re-check. Milliseconds. This is what keeps
  the "living credential" alive on every trigger without paying for the slow lane.
- **Slow lane (M/L, debounced / on-demand / threshold-triggered):** ontology mapping,
  question proposal, the insight battery, the judge. Routed **only** when the fast lane
  signals it is worth it (a new measure column appeared; coverage crossed a threshold; the
  user pressed "certify"). This matches the already-shipped split (memory: *answer split —
  fast data path / user-triggered certify*).

### 2.6 Node taxonomy mapped onto the current code (so this is a refactor, not a rewrite)

| Reasoning node | Lane | Today it lives in | Migration |
|---|---|---|---|
| `source.connect` / `ingest` | D | `h2_source.py`, connectors | wrap as node, declare `inputs=[source_files]` |
| `profile.stats` | D | profiler + `h2_eda.py` | wrap; outputs profile facts |
| `pii.detect` / `pii.deidentify` / `trust.boundary` | L/D, D, D | **new** (§6) | front-of-graph nodes |
| `relationship.infer` / `key.grain.infer` | D(+L) | `h2_enrich.py`, `h2_semantics.py` | wrap; write `REFERENCES`/grain edges |
| `ontology.map` | L→verify D | **new** (§4) | the missing "meaning" node |
| `goal.parse` | L→verify D | implicit in `h2_project_relevance.py` candidate slots | promote to structured intent |
| `question.gen` | L→verify D / D | `h2_project_relevance.py:472` templates | **becomes graph traversal** (P2) |
| `stat.battery` | M | seed in `h2_eda.py` | expand into the families table (§5) |
| `sql.synthesise` / `execute` | D | `h2_answer.py`, `h2_execute.py` | wrap; declare needed-columns inputs |
| `insight.narrate` | L over M | `h2_insight.py` (deterministic seed) | L narrates the M finding, checked back |
| `readiness.contracts` / `judge.certify` | D / L | `h2_readiness.py`, `h2_certify.py` | wrap; **fail-closed** edits (§5.3) |
| `confidence` / `attention` | D | blended in readiness | calibrated function over evidence subgraph (§5.2) |

The recompute spine (`recompute_project`, `h2_pipeline.py:741`) becomes a **graph
description + a `NodeRunner.run()` call**. The staged order it documents
(relevance → readiness → draft → execute) is exactly the topo-sort of the new graph — so
P1 is provably behaviour-preserving on day one.

---

## 3. Dimension B — the Knowledge Projection (de-risked: interface first)

### 3.1 The interface (the single most important de-risking move)

```python
# headwater/knowledge/projection.py
class KnowledgeProjection(Protocol):
    def upsert_node(self, n: GraphNode) -> None: ...
    def upsert_edge(self, e: GraphEdge) -> None: ...
    def neighbors(self, node_id: str, rel: str | None = None) -> list[GraphNode]: ...
    def paths(self, src: str, dst: str, *, max_hops: int = 3,
              via: list[str] | None = None) -> list[Path]: ...     # join-path finder
    def match(self, pattern: GraphPattern) -> list[Match]: ...      # measure×dimension
    def provenance_of(self, fact_id: str) -> list[ProvenanceRef]: ...
    def drop_and_rebuild(self) -> None: ...                          # it is derived
```

Nothing in the engine talks to a graph store directly — it talks to this interface. That
single seam is what lets us start trivially and never bet the product core on a dependency
that can disappear (the Kuzu lesson).

### 3.2 Backend strategy (start smallest, earn the upgrade)

- **Backend 0 — `SQLiteGraphBackend` (default, P2).** Two tables in the existing metadata
  DB: `graph_node(id, type, props_json)` and `graph_edge(src, rel, dst, props_json)`.
  Traversal = indexed recursive SQL / Python BFS-DFS. For our scale (tens of tables,
  hundreds of columns, ≤3-hop join paths) this is *more* than adequate, needs **zero new
  dependencies**, and keeps everything inside the SQLite SoR. This is the review's
  "smallest durable projection."
- **Backend 1 — `DuckPGQBackend` (spike, time-boxed, optional).** DuckDB is already in the
  stack; DuckPGQ adds property-graph + path queries. Attractive for Arrow-native traversal
  at larger scale, but it is a **community extension with incomplete-feature risk** — so it
  is a *spike behind the interface*, adopted only if Backend 0 hits a real wall.
- **Backend 2 — `KuzuBackend` (optional, frozen).** Only ever a pinned, local, capability-
  gated backend if traversal performance demands a real graph engine. Never the strategic
  bet; never the default; never required for a minimal install.

Acceptance to *graduate* a backend: a benchmark on the real catalog where Backend 0's
path-finding or pattern-match latency exceeds the fast-lane budget. Until then, Backend 0
ships.

### 3.3 The ontology — a compact, domain-agnostic upper model

Not OWL. A small upper model every dataset maps onto (no-domain-hardcoding invariant: the
*model* is fixed, the *assignments* are inferred and human-confirmed):

```
Concept types:  Entity · Event · Measure · Dimension · Code/Enum · Actor ·
                Location · TimeAnchor · Identifier · Derivation
Relations:      MEASURES · OCCURS_AT(time) · LOCATED_IN · PERFORMED_BY ·
                BELONGS_TO · REFERENCES(fk) · DERIVED_FROM · MAPS_TO_CODE ·
                SEGMENTS(dimension)
```

`ontology.map` (L proposes → D verifies against dtype/profile/cardinality → human confirms
+ **locks**, advisory boundary I-4/I-6) assigns each column a concept:
`events.total_duration → Measure(unit=duration)`, `events.activity → Dimension(kind=step)`,
`sites.zone_id → Location`. Locks are ground truth on the next run (I-6 semantic locks).

### 3.4 Goal-as-a-graph-query (the literal fix for "delays in hours")

`goal.parse` turns the goal into a structured intent over the ontology:
`{target_measure, by_dimensions[], grain, unit, time_scope, comparison}`. `question.gen`
then **traverses** the projection for valid `(Measure × Dimension × join-path)` patterns
satisfying that intent, ranked by expected information gain — *not* a template.

Worked example, end to end, through the graph:

```
goal "where do delays occur, in hours?"
  goal.parse →  target_measure = duration-like Measure   ("delays")
                by_dimensions  = [Location | step-kind Dimension]  ("where")
                unit           = hours                    ("in hours")
  question.gen → projection.match(Measure{unit:duration} × Dimension{kind∈location,step})
              →  projection.paths(events.total_duration → sites.zone_id, max_hops=2)
              →  unit coercion: minutes → hours  (or a user-confirmable Derivation node)
  result      → "Where do delays (hours) concentrate by step/zone?"  with a valid
                join path and a unit conversion, each as a provenance edge.
```

Two different goals on the same schema **must** yield different traversals ⇒ different
questions. That is the regression we lock in (P1/P2 acceptance).

---

## 4. Dimension C — novel insight battery + fail-closed certification

### 4.1 The deterministic/ML battery (M nodes)

A battery of `M` nodes (Polars/DuckDB + scipy/statsmodels/scikit-learn) produces candidate
findings, each carrying its own evidence. `h2_eda.py` is the seed to expand:

| Family | Method | Yields |
|---|---|---|
| Distribution | quantiles, skew, outlier share | "p95 is 4.2× the median" |
| Association | Pearson/Spearman, Cramér's V, mutual info | "step ↔ duration: V=0.61" |
| Group difference | Cohen's d / η² + test | "Admitted vs Stable: d=1.3" |
| Trend / change | Mann-Kendall, PELT changepoint | "delay rose after week 7" |
| Drivers | gradient-boosted + permutation importance | "modality explains 38% of delay" |
| Segments | clustering / tree splits | "two latent visit profiles" |
| Anomaly | isolation forest, residuals vs expected | "Room 3 is 3σ slow" |

**Novelty** = patterns the four templates never ask. **No fabrication** = the LLM only
*narrates* a computed finding (`insight.narrate` over the M output); the number, n, effect
size, and p-value come from the battery and the narration is checked back against them
(NAACL 2025 claim-level correctness). **Contrastive framing**: prefer "X is high *given* Y"
(residual vs an expected baseline) over bare "X is highest" — that reads as an *insight*,
not a sort. Each finding is written as an `Insight` node with `DERIVED_FROM` edges to the
columns/stats/query → full provenance.

### 4.2 True confidence (replace the heuristic blend)

Confidence becomes a **calibrated function of the evidence subgraph**: statistical power
(n), effect size, data completeness, contract pass, *and* judge verification — each shown.
The components are already surfaced today; this makes them principled rather than weights
someone picked. (Calibration *over time* via the safe-aggregate layer is **parked** with
P5 — §7; for this arc, confidence is a transparent, fixed function with components on
display.)

### 4.3 Fail-closed certification (the review's non-negotiable — pull forward)

Two concrete bugs make the badge a lie today. Both are small, high-value, and should land
**as soon as the contracts move into nodes** (do not wait for P3):

- **`insight_confidence` (`h2_readiness.py:296`)** defaults to `passed=True` when EDA has
  not run. Fix: uncomputed insight evidence ⇒ the contract is **not satisfied** and the
  answer **caps at Draft**. It does not block generation or exploration — it blocks the
  *credential*. This is exactly the vision's gating model: *soft on generation, hard on the
  credential* (HEADWATER2_VISION §"Gating model"). New contract state: `pass | fail |
  unknown`, where `unknown` can never certify.
- **`no_misleading` (`h2_readiness.py:271`)** is hardcoded `passed=True`. Fix: implement it
  — scan the answer's lineage for unresolved misleading patterns (null-spike, grain
  fan-out on the join path, code-like dimension with no mapping, stale source snapshot,
  conflicting definition). Any unresolved hit ⇒ fail ⇒ Draft, and open a Resolve card.

Certification rule (unchanged from vision, now enforced): the badge is **recomputed from
the evidence contract set**, never set by a points threshold. *Missing evidence is not a
pass.* "The badge is sacred."

### 4.4 "Requires attention" as a graph query

The attention queue is literally `high impact ∧ low confidence ∧ (weak support ∨
data-quality flag ∨ model-disagreement)` over the projection — a ranked, queryable view,
not a status field. It is the few things a human should look at first.

---

## 5. Dimension D — PII / de-identification (structural, pulled forward, gated)

Privacy is built as **front-of-graph nodes**, operationalising I-3 structurally:

- `pii.detect` (D regex/format patterns + L NER for names/free-text) → a **PII map**
  classifying columns as direct identifier / quasi-identifier / sensitive (advisory,
  human-confirmable). Presidio-style Analyzer, behind a capability flag so a minimal
  install still runs.
- `pii.deidentify` (D) applies a per-column strategy: tokenise/hash (stable joins
  preserved), generalise, redact; run k-anonymity / l-diversity checks on quasi-identifier
  sets.
- `trust.boundary` (D) — a hard edge in the graph. **Everything downstream** (profiling
  stats, LLM context, exports, the analytical DuckDB tables used for answers) sees only
  de-identified data. "Never raw rows to the LLM" (I-3) becomes "**nothing crosses the
  trust boundary raw**."

**Decision gate (the one fact only you know):** *Will target data contain real PII soon?*
- **Yes** → schedule PII right after P2 (position 3), ahead of the full insight battery —
  it becomes the concrete "ingestion-first" win and a precondition for safe LLM context.
- **No / not yet** → build the nodes' interfaces in P2 (so the trust boundary exists in the
  graph) but defer the heavy detector to its own phase. The boundary is present from day
  one; the detector is lazy-loaded.

Either way the *trust boundary edge* exists from P2 — only the detector's scheduling moves.

---

## 6. Parked — safe-aggregate learning (was P5)

Out of this arc. NIST differential-privacy guidance is explicit: aggregation/de-id alone
does not give a strong guarantee, and privacy budgets **compose** across repeated releases.
Building a learning/telemetry layer before the evidence model is stable risks baking in a
privacy liability and optimising against an unstable target.

**Keep the door open cheaply:** because provenance and findings are already stored at the
**ontology level** (concepts, not columns), a future telemetry layer is a *read-only
projection* over data we keep anyway — not a re-architecture. Revisit only after
certification is proven to hold up on held-out answers.

---

## 7. How the invariants survive (non-negotiable)

| Invariant | How it holds in the new engine |
|---|---|
| I-1 SQLite=meta / DuckDB=analytical | Projection is **derived**; SQLite stays SoR (also holds node cache + provenance); DuckDB unchanged. Default graph backend *is* SQLite tables. |
| I-2 Arrow-native, no Pandas | Battery uses Polars/Arrow; DuckDB/DuckPGQ Arrow-native; no Pandas anywhere. |
| I-3 never raw rows to LLM | **Strengthened**: `trust.boundary` + "L proposes / D·M verify." L nodes see only graph + stats, downstream sees only de-identified data. |
| I-4/I-6 advisory + semantic locks | ontology maps, PII classes, questions, insights = **proposals**; human confirms + **locks**; locks are ground truth next run. |
| No domain hardcoding | Ontology is a fixed domain-agnostic *upper model*; concept *assignments* are inferred, never coded. |
| Central truth / single recompute path | Becomes the per-node dependency DAG with **surgical** re-execution — same axiom, finer grain. Provenance ledger is the one source of truth every view projects from. |
| I-8 quality gates | ruff → pytest → pyright unchanged; each new package gated behind a capability so minimal install stays green. |
| I-9 layered imports | New `reasoning/` and `knowledge/` packages sit at the `analyzer`/`generator` tier; nodes import inward only. |
| Local-first, model-agnostic | Default backend embedded (no network); provider stays swappable (qwen2.5:14b floor, gemma/anthropic optional). |

---

## 8. Revised phased roadmap (each phase shippable behind the same UI)

The big resequencing vs the direction doc: **P1 is no longer a pure invisible refactor** —
it carries one vertical proof; and the **fail-closed certification fix is pulled forward**
(it is small and the badge is currently dishonest).

- **P1 — Reasoning-graph skeleton + one vertical proof + fail-closed cert.**
  - Build `reasoning/` (Node, NodeRunner, cache, provenance ledger). Wrap the current
    recompute stages as typed nodes; replace the single fingerprint with per-node hashing.
  - Thin vertical: a minimal `ontology.map` + `goal.parse` + one **goal-aware** question
    with provenance (Backend 0 graph, ≤2-hop).
  - Land the two fail-closed cert edits (`insight_confidence`, `no_misleading`).
  - **Proof (product, not tests):** (a) same schema + two different goals → two different
    question sets; (b) "where delays occur in hours" maps to a duration Measure × a
    step/Location Dimension × a valid join path × a unit coercion; (c) editing the goal
    re-runs only the question subgraph; (d) an answer with no insight evidence shows
    **Draft**, never Certified.

- **P2 — Knowledge Projection + full ontology + question generation by traversal.**
  - Stand up `KnowledgeProjection` (Backend 0). Full ontology map node. Retire the
    `h2_project_relevance.py:472` templates; questions become graph traversals incl.
    multi-hop joins. Trust-boundary edge present (detector lazy).
  - **Proof:** templates removed; a *multi-hop* question (measure in table A, dimension in
    table B via a join path) the templates could never produce; two goals still diverge.

- **P3 — Insight battery + true confidence + attention.**
  - Expand `h2_eda.py` into the M-node families; `Insight` nodes with `DERIVED_FROM`;
    `insight.narrate` (L over M, checked back); calibrated confidence (fixed function,
    components shown); attention as a graph query.
  - **Proof:** a novel, statistically-backed, contrastively-framed finding the templates
    never produced, with full provenance and an n / effect-size / p-value the narration is
    verified against.

- **P4 — PII detector + de-identification (scheduled per §5 gate).**
  - Presidio-style detector behind the already-present trust boundary; k-anonymity /
    l-diversity on quasi-identifiers.
  - **Proof:** a column with names is tokenised before any stat or LLM sees it; joins still
    work post-tokenisation.

- **P5 — PARKED** (safe-aggregate learning). Revisit only after P3's certification is shown
  to hold up on held-out answers.

Cross-cutting: P1's fail-closed cert and per-node hashing are the keystone; they make every
later phase affordable and make the badge honest immediately.

---

## 9. UI contract (the graph is plumbing)

Never surface "knowledge graph," "reasoning graph," "ontology," or "node" to the user.
Surface only the four questions a data professional actually asks, each backed by a
provenance query:

| User sees | Backed by |
|---|---|
| "What I think this column means" | `ontology.map` concept + lock state |
| "Why this question was proposed" | the traversal path that produced it |
| "What this answer depends on" | the `DERIVED_FROM` evidence subgraph |
| "What would break certification" | the unsatisfied / fragile contracts in the ledger |

This keeps the existing five-stage stepper (Frame → Understand → Resolve → Readiness →
Answer & Share) intact; the engine changes what is *behind* each screen, not the screen.

---

## 10. Risks, caveats, exit criteria

- **Dependency weight.** Battery libs (scipy/statsmodels/sklearn) and any optional graph
  backend add heft. Mitigation: each behind a capability flag; minimal install runs with
  Backend 0 (SQLite) and the D-lane only.
- **Local model quality.** Comprehension/narration scales with the model; qwen2.5:14b is
  the floor. The D/M verification layer guarantees *correctness/safety* regardless — only
  *insightfulness* degrades on a weak model.
- **Scope discipline.** This is a quarter-scale engine, not a patch. P1 first; resist
  building the graph store before the node abstraction and the interface exist.
- **Two-graph confusion.** Reasoning graph (control) ≠ Knowledge projection (data). Keep
  them in separate packages (`reasoning/`, `knowledge/`).
- **Refactor-without-proof.** The mitigation *is* D2: P1 carries a vertical proof, judged by
  a product demo, not a passing suite.

**The one proof that judges the whole arc** (review's closing line, adopted as the exit
criterion): *can Headwater reliably turn a business goal into a semantically valid,
traceable, certifiable answer — without narrating nonsense?* If P1–P3 land that on one real
problem with two real goals diverging and a badge that fails closed, build outward. If not,
no architecture would have saved the old direction.

---

## 11. Open decisions for you

1. **PII scheduling (§5 gate):** does target data contain real PII soon? (Yes → PII to
   position 3; No → boundary now, detector deferred. *Default if unanswered: No.*)
2. **Backend-1 spike:** authorise a time-boxed DuckPGQ spike during P2, or stay on Backend 0
   until a measured wall? (*Default: stay on Backend 0; spike only on evidence.*)
3. **Confidence calibration:** accept a transparent fixed function for this arc (calibration
   parked with P5), or is calibrated-over-time in scope sooner? (*Default: fixed function.*)
4. **First-slice breadth:** P1 vertical proof as scoped above (one goal-aware question), or
   widen it to the full two-goal question *set* divergence in P1? (*Default: as scoped;
   full divergence is P2's proof.*)
