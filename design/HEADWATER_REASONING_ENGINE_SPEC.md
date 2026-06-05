# Headwater — Reasoning Engine: Implementation Spec

Status: BUILD SPEC. Implements `HEADWATER_REASONING_ENGINE_PLAN.md`. P1 is specced to the
file/function level (buildable as-is); P2–P4 land behind interfaces this spec fixes.
Date: 2026-06-05.

Ground rules carried from the plan: in-house typed-node graph (no LangGraph dep);
`KnowledgeProjection` interface with a SQLite-adjacency default (no Kuzu); certification
**fails closed**; everything flag-gated so the legacy path stays intact until parity is
proven. Quality gate per run: `ruff → pytest → pyright` (I-8). All new packages sit at the
analyzer/generator tier and import inward only (I-9).

---

## 0. What P1 delivers (the contract for "done")

1. `headwater/reasoning/` — typed-node graph, runner, per-node hashing, cache, provenance
   ledger. The existing recompute stages run *through* it with byte-identical output (parity).
2. `headwater/knowledge/` — `KnowledgeProjection` interface + `SQLiteGraphBackend`, the
   compact ontology types, and `paths()`/`match()` traversal.
3. Three new reasoning nodes — `ontology.map`, `goal.parse`, `question.gen` (vertical) —
   producing **one goal-aware question with provenance**.
4. Fail-closed edits to `h2_readiness.py` (`insight_confidence`, `no_misleading`).
5. Surgical recompute: a goal edit re-runs only `goal.parse → question.gen`.

Acceptance is a product demo (§12), not just a green suite: two goals on the radiology
schema diverge; "delays in hours" maps to duration×step/location×join-path×unit; an answer
with no insight evidence shows **Draft**, never Certified.

---

## 1. Package layout

```
headwater/headwater/
  reasoning/                     # CONTROL PLANE (new)
    __init__.py
    types.py        # InputRef, GraphFact, NodeResult, ProvenanceRef, NodeCost, ProjectState, NodeCtx, ChangeEvent, RunReport
    node.py         # Node protocol, BaseNode, DeterministicNode/MLNode/LLMNode mixins
    graph.py        # Graph: register, topo_sort, descendants
    runner.py       # NodeRunner: dirty detection, cache, projection.apply, ledger.record
    cache.py        # NodeCache (SQLite-backed)
    ledger.py       # ProvenanceLedger (SQLite-backed) == certification ledger substrate
    nodes/          # node implementations (thin wrappers around existing services)
      ingest.py  profile.py  relationship.py  ontology_map.py  goal_parse.py
      question_gen.py  readiness.py  answer.py  judge.py
  knowledge/                     # DATA PLANE (new)
    __init__.py
    projection.py   # KnowledgeProjection Protocol + GraphNode/GraphEdge/Path/Match types
    sqlite_backend.py  # SQLiteGraphBackend (default)
    ontology.py     # Concept/Relation literals, ConceptAssignment, verification predicates
    backends/
      duckpgq_backend.py   # P2 spike (stub in P1)
      kuzu_backend.py      # optional/frozen (stub in P1)
  core/store.py     # +4 tables (node_cache, node_provenance, graph_node, graph_edge) + methods
  core/config.py    # + engine/backend capability flags
  services/h2_pipeline.py    # recompute_project → build graph + NodeRunner.run
  services/h2_readiness.py   # fail-closed contract edits
```

No file in `services/` is deleted in P1 — nodes **wrap** them. `h2_project_relevance.py`
templates stay as the fallback until P2 retires them.

---

## 2. Core types — `reasoning/types.py`

```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
import hashlib, json

Lane = Literal["D", "M", "L"]
InputRef = str   # canonical key, e.g. "project.goal", "source:radiology.events.columns",
                 # "claim:locked", "node:profile.stats"

@dataclass(frozen=True, slots=True)
class GraphNode:
    id: str                       # "col:radiology.events.total_duration"
    type: str                     # ontology concept or system type ("Measure","Question",...)
    props: dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True, slots=True)
class GraphEdge:
    src: str
    rel: str                      # "MEASURES","SEGMENTS","REFERENCES","DERIVED_FROM",...
    dst: str
    props: dict[str, Any] = field(default_factory=dict)

GraphFact = GraphNode | GraphEdge

@dataclass(frozen=True, slots=True)
class ProvenanceRef:
    produced_by: str              # node id
    input_hash: str
    lane: Lane
    model_id: str | None = None   # set for L nodes
    fact_ids: tuple[str, ...] = ()

@dataclass(frozen=True, slots=True)
class NodeCost:
    lane: Lane
    wall_ms: int = 0
    model_id: str | None = None

@dataclass(slots=True)
class NodeResult:
    output: Any
    facts: list[GraphFact] = field(default_factory=list)
    provenance: ProvenanceRef | None = None
    cost: NodeCost | None = None

def stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()
```

`ProjectState` is a thin, read-mostly view over the store plus a scratch dict of node
outputs already computed this run:

```python
@dataclass(slots=True)
class ProjectState:
    project_id: str
    store: "HeadwaterStore"
    projection: "KnowledgeProjection"
    outputs: dict[str, Any] = field(default_factory=dict)   # node_id -> output
    def adopt(self, node_id: str, result: NodeResult) -> None:
        self.outputs[node_id] = result.output

@dataclass(slots=True)
class NodeCtx:
    settings: "HeadwaterSettings"
    llm: "LLMProvider"            # NoLLMProvider when offline → D fallback
    run_slow: bool = False        # gate for the slow lane (M/L heavy nodes)

@dataclass(slots=True)
class RunReport:
    ran: list[str] = field(default_factory=list)      # node ids actually executed
    skipped: list[str] = field(default_factory=list)  # unchanged / cache hit
    facts_written: int = 0
```

---

## 3. The Node contract — `reasoning/node.py`

```python
class Node(Protocol):
    id: str
    lane: Lane
    def inputs(self, state: ProjectState) -> list[InputRef]: ...
    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult: ...

class BaseNode:
    id: str = ""
    lane: Lane = "D"
    def inputs(self, state: ProjectState) -> list[InputRef]:
        raise NotImplementedError
    def input_hash(self, state: ProjectState) -> str:
        # default: hash the *resolved values* of declared inputs (not just their keys)
        return stable_hash([_resolve(state, ref) for ref in self.inputs(state)])
    def compute(self, state: ProjectState, ctx: NodeCtx) -> NodeResult:
        raise NotImplementedError
```

`_resolve(state, ref)` is a small registry that maps an `InputRef` to its current value
from the store/state (goal dict, a table's column metadata, the set of locked claims, an
upstream node output). It is the single place that knows how to read an input — which is
what makes `input_hash` precise and recompute surgical.

L-node rule (enforced structurally, strengthens I-3): an `LLMNode` never returns facts
directly. It returns a `proposal`, and a paired `verify(proposal, state) -> NodeResult`
grounds every column/number/join against the projection + profile before emitting facts.

```python
class LLMNode(BaseNode):
    lane: Lane = "L"
    def propose(self, state, ctx) -> dict: ...           # the only place the model is called
    def verify(self, proposal: dict, state, ctx) -> NodeResult: ...   # D grounding
    def compute(self, state, ctx) -> NodeResult:
        proposal = self.propose(state, ctx) if not isinstance(ctx.llm, NoLLMProvider) else {}
        return self.verify(proposal, state, ctx)         # empty proposal → deterministic fallback
```

---

## 4. Graph + runner — `reasoning/graph.py`, `reasoning/runner.py`

```python
class Graph:
    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}
    def add(self, node: Node) -> "Graph": ...
    def topo_sort(self, state) -> list[Node]:
        # edges are implicit: A→B if any InputRef of B names "node:A".
        # cycle => raise (the graph is a DAG by construction).
    def descendants(self, node_id: str, state) -> set[str]: ...
```

Runner — the heart of surgical recompute:

```python
class NodeRunner:
    def __init__(self, cache, projection, ledger): ...
    def run(self, graph, state, ctx, *, changed: set[InputRef]) -> RunReport:
        report = RunReport()
        dirty: set[str] = set()
        for node in graph.topo_sort(state):
            ihash = node.input_hash(state)
            upstream_dirty = any(f"node:{u}" in _input_set(node, state) and u in dirty
                                 for u in graph._nodes)
            inputs_changed = bool(set(node.inputs(state)) & changed)
            if not inputs_changed and not upstream_dirty:
                if cached := self.cache.get(node.id, ihash):
                    state.adopt(node.id, cached); report.skipped.append(node.id); continue
                # never computed at this hash → must run
            if cached := self.cache.get(node.id, ihash):
                state.adopt(node.id, cached); report.skipped.append(node.id); continue
            if ctx.run_slow is False and node.lane in ("M", "L") and not _is_vertical(node):
                report.skipped.append(node.id); continue           # two-speed gate
            result = node.compute(state, ctx)
            self.cache.put(node.id, ihash, result.output)
            if result.facts:
                self.projection.apply(result.facts); report.facts_written += len(result.facts)
            if result.provenance:
                self.ledger.record(result.provenance)
            state.adopt(node.id, result)
            dirty.add(node.id)                                     # descendants re-run
            report.ran.append(node.id)
        return report
```

Properties: (a) a node runs iff its declared inputs changed **or** an upstream node it
depends on ran; (b) identical inputs ⇒ cache hit ⇒ skip (memoization keyed on
`(node_id, input_hash)`); (c) the slow lane (M/L) only runs when `ctx.run_slow` or the node
is the P1 vertical; (d) every emitted fact is written to the projection and every output to
the ledger — the ledger is replayable and *is* the certification provenance.

---

## 5. Persistence — additions to `core/store.py`

Append to `_SCHEMA_SQL` (idempotent `CREATE TABLE IF NOT EXISTS`; `store.init()` already
`executescript`s it — additive, no migration needed, matches the existing `*_json` +
`created_at`/`updated_at` convention):

```sql
CREATE TABLE IF NOT EXISTS node_cache (
    node_id      TEXT NOT NULL,
    input_hash   TEXT NOT NULL,
    output_json  TEXT NOT NULL DEFAULT '{}',
    computed_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (node_id, input_hash)
);

CREATE TABLE IF NOT EXISTS node_provenance (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id      TEXT,
    produced_by  TEXT NOT NULL,
    input_hash   TEXT NOT NULL,
    lane         TEXT NOT NULL,
    model_id     TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_node_provenance_fact ON node_provenance(fact_id);

CREATE TABLE IF NOT EXISTS graph_node (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    props_json  TEXT NOT NULL DEFAULT '{}',
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS graph_edge (
    src        TEXT NOT NULL,
    rel        TEXT NOT NULL,
    dst        TEXT NOT NULL,
    props_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (src, rel, dst)
);
CREATE INDEX IF NOT EXISTS idx_graph_edge_src ON graph_edge(src, rel);
CREATE INDEX IF NOT EXISTS idx_graph_edge_dst ON graph_edge(dst, rel);
```

Store methods to add (parameterized SQL only, per python-patterns): `cache_get/cache_put`,
`provenance_record`, `graph_upsert_node/graph_upsert_edge/graph_neighbors/graph_drop`.
These keep the graph inside the SQLite SoR (I-1) and are droppable/rebuildable.

---

## 6. KnowledgeProjection — `knowledge/projection.py` + `sqlite_backend.py`

```python
@dataclass(frozen=True, slots=True)
class Path:
    nodes: tuple[str, ...]; edges: tuple[GraphEdge, ...]
    @property
    def hops(self) -> int: return len(self.edges)

@dataclass(frozen=True, slots=True)
class Match:
    measure: str; dimension: str; join_path: Path | None; score: float

class KnowledgeProjection(Protocol):
    def apply(self, facts: list[GraphFact]) -> None: ...
    def upsert_node(self, n: GraphNode) -> None: ...
    def upsert_edge(self, e: GraphEdge) -> None: ...
    def neighbors(self, node_id: str, rel: str | None = None) -> list[GraphNode]: ...
    def paths(self, src: str, dst: str, *, max_hops: int = 3) -> list[Path]: ...
    def match_measure_dimension(self, *, measure_kinds: set[str],
                                dim_kinds: set[str], max_hops: int = 2) -> list[Match]: ...
    def provenance_of(self, fact_id: str) -> list[ProvenanceRef]: ...
    def drop_and_rebuild(self) -> None: ...
```

`SQLiteGraphBackend` implementation notes:

- `apply` dispatches node/edge upserts in one transaction.
- `neighbors` = indexed `SELECT … FROM graph_edge WHERE src=? [AND rel=?]` join `graph_node`.
- `paths` = bounded BFS in Python over `graph_edge` (only `REFERENCES`/join-eligible rels),
  `max_hops ≤ 3`; returns all simple paths. At our scale (tens of tables) this is
  microseconds; the bound prevents pathological fan-out.
- `match_measure_dimension` = enumerate `Measure` nodes with `props.unit/kind ∈ measure_kinds`
  × `Dimension`/`Location` nodes with `kind ∈ dim_kinds`; for each pair, find a join `Path`
  (same table ⇒ hop 0); score by `(dimension cardinality fitness) × (join confidence) ×
  (1/hops)`. This is the engine behind goal-aware questions.

Backend selection: `knowledge.make_projection(settings)` returns `SQLiteGraphBackend` unless
`HEADWATER_KNOWLEDGE_BACKEND` says otherwise; `duckpgq`/`kuzu` raise `NotImplementedError`
in P1 (stubs present so the seam exists).

---

## 7. The ontology — `knowledge/ontology.py`

```python
Concept = Literal["Entity","Event","Measure","Dimension","Code","Actor",
                  "Location","TimeAnchor","Identifier","Derivation"]
Relation = Literal["MEASURES","OCCURS_AT","LOCATED_IN","PERFORMED_BY","BELONGS_TO",
                   "REFERENCES","DERIVED_FROM","MAPS_TO_CODE","SEGMENTS"]

@dataclass(frozen=True, slots=True)
class ConceptAssignment:
    col_ref: str                 # "events.total_duration"
    concept: Concept
    props: dict[str, Any]        # {"unit":"duration","kind":"step"} etc.
    confidence: float
    locked: bool = False
    source: Literal["llm","heuristic","user"] = "heuristic"
```

Deterministic verification predicates (the D half of `ontology.map`) — these *gate* every
LLM proposal, and also provide the heuristic fallback when no model is present:

| Concept | Verifies when | Props derived |
|---|---|---|
| `Measure` | numeric dtype, high distinct count, non-id name | `unit` from name/derivation (duration/count/amount) |
| `Dimension` | low/medium cardinality, non-numeric or coded | `kind` (step/category/status) from name+distincts |
| `TimeAnchor` | temporal dtype or time-like name + parseable | `grain` |
| `Location` | name/role hints + categorical | — |
| `Identifier` | high distinct, key/grain candidate, fk participation | — |
| `Code` | low cardinality + code-like values | needs `MAPS_TO_CODE` mapping |
| `Derivation` | a confirmed `derivation` claim exists | source cols |

No domain terms anywhere — only dtype, cardinality, name-shape, profile stats, and existing
relationship/claim facts (no-domain-hardcoding invariant).

---

## 8. P1 nodes (concrete)

### 8.1 `ontology.map` (L→verify D) — `nodes/ontology_map.py`
- `inputs`: `["source:<s>.<t>.columns", "node:profile.stats", "node:relationship.infer", "claim:locked"]`
- `propose`: one LLM call over the **I-3-safe schema brief** (reuse the brief defined in
  `HEADWATER2_QUESTION_COMPREHENSION.md` §4 — per-column dtype/role/distinct/null/min/max/
  top-6, relationships, coverage window). Asks for a concept + props per column. Strict JSON.
- `verify`: drop any assignment that fails the §7 predicate; fall back to the heuristic
  predicate for unproposed/!rejected columns. Emit `GraphNode(type=concept)` per column +
  `MEASURES`/`SEGMENTS`/`REFERENCES` edges. Respect locks (I-6): a locked assignment is
  ground truth, never overwritten.
- Advisory: assignments are proposals surfaced as "what I think this column means"; user
  confirm flips `locked=True`.

### 8.2 `goal.parse` (L→verify D) — `nodes/goal_parse.py`
```python
@dataclass(frozen=True, slots=True)
class GoalIntent:
    target_measure_kinds: set[str]      # {"duration"} from "delays"
    by_dimension_kinds: set[str]        # {"location","step"} from "where"
    grain: str | None
    unit: str | None                    # "hours"
    time_scope: str | None
    comparison: Literal["rank","trend","segment","coverage","none"]
```
- `inputs`: `["project.goal", "node:ontology.map"]`
- `propose`: LLM maps free-text goal → `GoalIntent` against the *available* concept kinds
  (passed in, so it cannot invent a kind that isn't in the ontology map). `verify`:
  intersect requested kinds with kinds actually present; deterministic keyword fallback
  (`delay|wait|duration→duration`, `where|location|site→location/step`, `in hours→unit`).

### 8.3 `question.gen` (vertical) — `nodes/question_gen.py`
- `inputs`: `["node:goal.parse", "node:ontology.map", "node:relationship.infer"]`
- `compute`:
  1. `matches = projection.match_measure_dimension(measure_kinds=intent.target_measure_kinds,
     dim_kinds=intent.by_dimension_kinds, max_hops=2)`
  2. take top-1 by score (P1 vertical = one question; P2 widens to a set).
  3. if `intent.unit` ≠ measure's native unit → attach a unit coercion or open a
     user-confirmable `Derivation` (e.g. minutes→hours).
  4. persist via the existing `_persist_question` (reuse `needed_columns`, `col_roles`,
     `question_type`=intent.comparison, `source="reasoning"`); the **downstream pipeline
     (draft SQL incl. join path, execute, finding, readiness, judge) is unchanged.**
  5. emit `Question` node + `DERIVED_FROM` edges to the measure col, dimension col, the
     join path edges, and the goal — this is "why this question was proposed" + "what this
     answer depends on".
- Fallback: empty matches / `NoLLMProvider` → existing heuristic templates
  (`h2_project_relevance.py`) so the screen is never empty.

### 8.4 Wrapped existing stages (parity nodes, no behavior change)
| Node | Wraps | inputs |
|---|---|---|
| `profile.stats` (D) | profiler / `h2_eda.py` profile pass | source columns |
| `relationship.infer` (D) | `h2_enrich.py`/`h2_semantics.py` | node:profile.stats |
| `readiness.contracts` (D) | `h2_readiness.evaluate_question` | node:question.gen, node:profile.stats, claims, resolve |
| `answer.draft+execute` (D) | `h2_answer.py`/`h2_execute.py` | node:question.gen, node:ontology.map |
| `judge.certify` (L) | `h2_certify.py` | node:answer (slow lane only) |

`recompute_project`'s documented order (relevance → readiness → draft → execute → judge) is
exactly the topo-sort of these nodes, so P1 parity is provable.

---

## 9. Fail-closed certification — edits to `h2_readiness.py`

The contract result gains a tri-state so "uncomputed" can never certify:

```python
@dataclass(slots=True)
class ContractResult:
    contract_type: ContractType
    passed: bool                 # kept for back-compat
    note: str
    evidence: dict[str, Any] = field(default_factory=dict)
    status: Literal["pass","fail","unknown"] = "pass"   # NEW; "unknown" never certifies
```

**`insight_confidence` (replaces `h2_readiness.py:296–315`).** Uncomputed EDA ⇒ `unknown`,
not pass:

```python
if eda_contract:
    ok = bool(eda_contract.get("passed"))
    results.append(ContractResult("insight_confidence", ok,
        str(eda_contract.get("note") or "Insight confidence computed."),
        dict(eda_contract.get("evidence") or {}),
        status="pass" if ok else "fail"))
else:
    results.append(ContractResult("insight_confidence", False,
        "Insight evidence not computed — answer can be Draft, not Certified. Run `hw2 eda`.",
        {}, status="unknown"))
```

**`no_misleading` (replaces the hardcoded `passed=True` at `h2_readiness.py:271–279`).**
Real detection over the needed columns' lineage:

```python
flags = _misleading_flags(needed_columns, profile_map, claims, snapshot)
# checks: null-spike on a needed col, grain fan-out on the join path, code-like dimension
# with no MAPS_TO_CODE mapping, stale source snapshot, conflicting definition.
results.append(ContractResult("no_misleading", not flags,
    "No misleading patterns in needed columns." if not flags
    else f"Misleading risk: {', '.join(flags[:3])}",
    {"flags": flags}, status="pass" if not flags else "fail"))
```

**State derivation.** `certified` requires **all** contracts `status == "pass"` (no `fail`,
no `unknown`); any `unknown` ⇒ at best `draft`. Generation/exploration stays open — only the
badge is gated (the vision's "soft on generation, hard on the credential"). Each unresolved
`no_misleading` flag opens a Resolve card so it is actionable, never silent.

---

## 10. Wiring recompute → graph — `h2_pipeline.py`

`project_input_fingerprint` is **kept** but factored into *sub-fingerprints* (one per
InputRef group: `goal`, `scope`, `columns`, `claims`, `resolve`). The change set is the diff
between the stored sub-fingerprints and the current ones:

```python
def recompute_project(store, project_id, *, settings=None, run_judge=False):
    if not settings or not settings.reasoning_engine:        # flag off → legacy path intact
        return _legacy_recompute_project(store, project_id, settings=settings, run_judge=run_judge)
    state = ProjectState(project_id, store, make_projection(settings))
    ctx = NodeCtx(settings, make_provider(settings), run_slow=run_judge)
    changed = _changed_inputs(store, project_id)             # sub-fingerprint diff
    report = NodeRunner(cache, state.projection, ledger).run(build_graph(), state, ctx, changed=changed)
    _store_subfingerprints(store, project_id)                # advance state
    return _summarise(state, report)
```

`run_judge` maps to `run_slow` (the existing fast-path/certify split — memory: answer split).
The legacy `_legacy_recompute_project` is the current function, renamed, untouched.

---

## 11. Settings & capabilities — `core/config.py`

```python
reasoning_engine: bool = False                  # HEADWATER_REASONING_ENGINE — gate the whole engine
knowledge_backend: Literal["sqlite","duckpgq","kuzu"] = "sqlite"  # HEADWATER_KNOWLEDGE_BACKEND
insight_battery: bool = False                   # P3 — heavy stat libs behind a flag
pii_detection: bool = False                     # P4 — Presidio behind a flag
```

Minimal install: `reasoning_engine=False` runs exactly today's code. `True` +
`knowledge_backend="sqlite"` needs **zero new third-party deps** (graph is SQLite tables).

---

## 12. Testing & acceptance (P1 proofs encoded)

| Test | Asserts | Type |
|---|---|---|
| `test_two_goals_diverge` | two goals on radiology → different `question.gen` output | stub LLM provider |
| `test_delays_in_hours_maps` | "where delays in hours" → Measure(duration)×Dimension(step/location)×join path×unit=hours | stub + real ontology |
| `test_surgical_recompute` | editing `project.goal` ⇒ `report.ran ⊆ {goal.parse, question.gen, readiness, answer}`; `profile.stats`/`relationship.infer` in `report.skipped` | runner |
| `test_node_cache_hit` | unchanged inputs ⇒ second run all-skipped, `facts_written==0` | runner |
| `test_provenance_ledger` | every emitted Question has `DERIVED_FROM` rows in `node_provenance` | store |
| `test_cert_fails_closed_no_eda` | no EDA ⇒ `insight_confidence.status=="unknown"` ⇒ state `draft`, never `certified` | readiness |
| `test_cert_fails_closed_misleading` | injected null-spike/fan-out ⇒ `no_misleading` fail ⇒ `draft` + Resolve card opened | readiness |
| `test_paths_bounded` | `paths()` respects `max_hops`, returns simple paths only | projection |
| `test_parity_legacy_vs_engine` | flag on vs off ⇒ identical questions/verdicts on radiology (the wrapped stages) | integration |
| `test_no_model_fallback` | `NoLLMProvider` ⇒ heuristic question still produced, schema-valid | node |

Tests use in-memory SQLite + a stub `LLMProvider` returning goal-conditioned JSON (real
behavior, not mocks — per python-patterns). A separate manually-run check exercises
`qwen2.5:14b` on the radiology goals and records before/after.

---

## 13. File-by-file change list (the build checklist)

| File | Change | Est LOC | PR |
|---|---|---|---|
| `reasoning/types.py` | new core types | ~120 | PR1 |
| `reasoning/node.py` | Node/BaseNode/LLMNode | ~90 | PR1 |
| `reasoning/graph.py` | Graph + topo_sort | ~70 | PR1 |
| `reasoning/runner.py` | NodeRunner (dirty/cache/ledger) | ~110 | PR1 |
| `reasoning/cache.py`,`ledger.py` | SQLite-backed cache + ledger | ~80 | PR1 |
| `core/store.py` | +4 tables + ~8 methods | ~120 | PR1 |
| `core/config.py` | +4 capability flags | ~10 | PR1 |
| `reasoning/nodes/*.py` (wraps) | profile/relationship/readiness/answer/judge wrappers | ~200 | PR2 |
| `services/h2_pipeline.py` | recompute → graph; rename legacy | ~80 | PR2 |
| `knowledge/projection.py` | Protocol + types | ~70 | PR3 |
| `knowledge/sqlite_backend.py` | backend incl. paths/match | ~220 | PR3 |
| `knowledge/ontology.py` | concepts + verify predicates | ~140 | PR3 |
| `reasoning/nodes/ontology_map.py` | L→verify map node | ~150 | PR4 |
| `reasoning/nodes/goal_parse.py` | GoalIntent node | ~110 | PR4 |
| `reasoning/nodes/question_gen.py` | traversal vertical | ~140 | PR4 |
| `services/h2_readiness.py` | fail-closed edits + `_misleading_flags` | ~90 | PR5 |
| `tests/reasoning/…`,`tests/knowledge/…` | the §12 suite | ~400 | every PR |

Total new/changed ≈ 2.4k LOC, additive; legacy path untouched until parity (PR2) passes.

---

## 14. Sequencing within P1 (5 PRs, each shippable)

- **PR1 — Engine core, dark.** types/node/graph/runner/cache/ledger + store tables + flag
  (default off). No node wired. Proof: unit tests for runner dirty/cache logic.
- **PR2 — Wrap & prove parity.** Wrap the existing stages as nodes; `recompute_project`
  routes through the graph when the flag is on. Proof: `test_parity_legacy_vs_engine` green;
  `test_surgical_recompute` shows a goal edit skipping profile/relationship.
- **PR3 — Knowledge projection.** SQLiteGraphBackend + ontology types + paths/match. Proof:
  projection unit tests; graph populated from the wrapped relationship/profile facts.
- **PR4 — The vertical.** ontology.map + goal.parse + question.gen producing one goal-aware
  question with provenance. Proof: `test_two_goals_diverge`, `test_delays_in_hours_maps`.
- **PR5 — Fail-closed cert.** tri-state contracts + `no_misleading`. Proof:
  `test_cert_fails_closed_*`.

PR5's cert fix is independent and can ship first if the dishonest badge is the priority.

---

## 15. Risks & rollback

- **Rollback is the flag.** `reasoning_engine=False` reverts to `_legacy_recompute_project`
  with zero schema cost (new tables are inert). The projection is droppable
  (`drop_and_rebuild`).
- **Parity risk.** Mitigated by PR2 landing before any new behavior; the wrapped nodes must
  reproduce current output byte-for-byte before PR4 changes questions.
- **Weak local model.** Verification predicates (§7) guarantee schema-valid, safe output
  even on `gemma`/`llama`; only sharpness degrades. `qwen2.5:14b` is the floor.
- **Scope creep into P2.** P1 ships **one** goal-aware question; the full two-goal *set*
  divergence and template retirement are P2. Resist widening in P1.
