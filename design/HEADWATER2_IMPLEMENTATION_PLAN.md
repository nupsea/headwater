# Headwater 2 — Implementation Plan

Status: Draft execution plan
Date: 2026-05-28

This plan converts the Headwater 2 vision, UX handoff, CTO/data-science review, and
current H1 code audit into an implementation sequence. The product must remain
dataset-agnostic: radiology, MovieLens, and TLC are validation fixtures only, never
product branches. Any behavior that needs business meaning must come from generated
metadata, user-vouched semantics, or project resources, not hard-coded domain logic.

## Product Target

Headwater 2 is a readiness workspace for data professionals. It connects to a shared
source, profiles it once, builds a reusable semantic layer, proposes business questions
the data can credibly answer, ranks unresolved human decisions, computes a per-question
readiness verdict, and helps produce answers that carry a truthful certification state.

The core loop is:

```text
Connect source
  -> generate source metadata and semantic catalog
  -> create project goal and select source scope
  -> propose answerable / not-answerable questions
  -> resolve high-impact ambiguity
  -> compute per-question readiness
  -> draft answer + report
  -> re-check certification when data or definitions change
```

The first production wedge is not a generic dashboard. It is a goal-anchored readiness
audit and answer workspace whose value is: "what can this data answer, what can it not
answer, what is risky, and what has changed since it was certified?"

## Non-Negotiable Architecture Rules

1. No dataset-specific product code. Domain fixtures can live in tests and golden
   assertions only.
2. Source metadata is shared. Tables, columns, profiles, relationships, source snapshots,
   and generic semantic types are source-owned and reused across projects.
3. Project meaning is scoped. Goal, selected tables, question set, semantic locks,
   resources, unresolved gaps, and readiness verdicts are project/question-owned.
4. Certification is derived, never clicked into existence. A question is certified only
   when its evidence contract set passes and the generated insight clears confidence
   thresholds.
5. Generation is soft-gated; the badge is hard-gated. Users can query and share draft
   outputs, but uncertified output must never look certified.
6. LLM use is optional and never receives raw rows. It may see schema, statistics,
   relationships, redacted resource text, and locked definitions.
7. Data professionals stay in control. Headwater recommends, ranks, and explains; it does
   not silently apply business definitions.

## Target Package Shape

Build Headwater 2 as a strangler inside `headwater/headwater`, preserving the healthy H1
spine and adding project-centric layers around it.

```text
core/store.py          small SQLite authority for source/project/question/readiness state
connectors/            keep; shared source access and capability registry
profiler/              keep; stats, keys, relationships, grain
semantics/             new/mined; semantic typing, definitions, locks, resource fusion
eda/                   new/mined; generic anomaly, period, segment, correlation kernels
project/               new; project spec, source scope, question lifecycle
relevance/             new; goal-to-source matching and question proposal
readiness/             new; evidence contracts, verdicts, certification derivation
answer/                new; grounded SQL drafts, chart specs, insight confidence
export/                new; Markdown audit and provenance report
api/cli/               thin H2 entry points
ui/                    implements the handoff: Catalog, Query, Connect, Projects,
                       Understand, Resolve, Readiness, Answer
```

H1 modules to keep: `connectors`, `profiler`, `quality`, `generator`, `executor`.
H1 modules to mine: `explorer/statistical.py`, `analyzer/semantic_types.py`,
`analyzer/heuristics.py`, LLM redaction/orchestration. H1 modules to avoid carrying
forward: NL-to-SQL, broad context suite, overbuilt drift UI, large metadata god file.

## Data Architecture

### Source-Owned Metadata

These artifacts are computed once per source snapshot and reused by every project on
that source:

- source connection and connector capability record
- discovery run and source snapshot id
- schemas, tables, columns, row counts
- column profiles: nulls, distincts, ranges, histograms/quantiles where supported,
  examples of value shapes but not raw row storage
- key candidates, FK candidates, relationship confidence, grain inference
- generic semantic type evidence: identifier, timestamp, duration, categorical, measure,
  free text, flag, code, quantity, currency, location-like, etc.
- quality facts that are source-global: uniqueness, referential integrity, impossible
  ranges, freshness, vocabulary drift

### Project-Owned Metadata

These artifacts depend on the business goal and must not be treated as global truth:

- project goal: statement, decision, target metric, entities, time horizon
- selected source scope: tables and columns considered for the goal
- resources: glossary, data dictionary, metric notes, stakeholder definitions
- semantic definitions and locks: metrics, dimensions, entities, enum mappings
- proposed and curated questions
- relevance rationale for included and excluded columns
- resolve cards and human decisions
- per-question evidence contract state
- readiness verdict, certification state, answer artifacts, exported report

### Semantic Layer Contract

The semantic layer is not a static dictionary. It is a versioned evidence graph with:

- `subject`: source/table/column/question/metric/entity
- `claim`: semantic type, definition, enum meaning, lineage, grain, quality fact
- `evidence`: profile statistic, relationship check, resource quote reference, prior lock,
  user decision, LLM summary, or source snapshot diff
- `confidence`: bounded score with reason
- `scope`: source-global, project-specific, or question-specific
- `status`: inferred, proposed, locked, rejected, needs_review
- `freshness`: source snapshot and timestamp used to compute it

This lets Headwater reuse metadata without pretending a column has the same business
meaning in every goal.

## Implementation Stages

Each stage must be reviewable on its own. Unless a stage is UI-only, it must include unit
tests and at least one real fixture run. Radiology is the first vertical-slice fixture;
MovieLens and TLC are cross-domain checks.

### S0 — Branch, Guardrails, And H2 Entry Points

Build:

- create `feat/headwater2-slice`
- add H2 package directories without moving H1 code
- add `hw2` CLI command group behind a feature flag or separate Typer group
- add architecture boundary tests that prevent dataset names in production modules
- add golden fixture policy: answer-key docs may be used for tests only, not pipeline input

Verify:

- `uv run hw2 --help`
- `uv run ruff check .`
- `uv run pytest tests/test_architecture_boundaries.py`
- grep check fails if production H2 modules contain fixture-only tokens such as
  `radiology`, `taxi`, `movielens`, `patient_type H`, or TLC-specific column names

Done when:

- H2 can be invoked without disturbing H1
- guardrails exist before semantic or insight work begins

### S1 — Lean Metadata Store

Build:

- add `core/store.py` as the H2 SQLite authority
- create schema for sources, snapshots, tables, columns, profiles, relationships,
  projects, project_sources, resources, semantic_claims, questions, resolve_items,
  readiness_contracts, readiness_verdicts, answer_artifacts, decisions
- keep append-only decisions for user actions and certification revocations
- add migration/version metadata

Verify:

- in-memory SQLite schema creation
- round-trip source, table, profile, project, question, semantic claim, decision
- snapshot immutability test: verdict references a source snapshot id, not "latest"

Done when:

- every future H2 stage can persist state without expanding H1 `metadata.py`

### S2 — Source Connect And Discovery Reuse

Build:

- wrap existing connectors behind an H2 source service
- implement source registration, capability inspection, bounded sampling, and profile
  execution
- persist source snapshot, table inventory, row counts, profiles, and relationships
- support CSV/JSON/DuckDB/SQLite first; keep Postgres through existing connector paths

Verify:

- radiology CSV source: table and row counts match files
- at least one local non-radiology fixture loads through the same path
- no connector mutates source systems
- large-table safety test: no full table pull when connector can aggregate in place

Done when:

- a source can be profiled once and reopened by multiple projects without re-ingest

### S3 — Generic Profiling, Grain, And Relationships

Build:

- formalize profile outputs needed by H2: nulls, uniqueness, value shapes, temporal
  coverage, numeric quantiles, categorical cardinality, freshness
- reuse PK/FK/composite key detection and referential-integrity checks
- persist grain candidates with confidence and evidence
- emit source-global quality facts

Verify:

- known fixture relationships detected without fixture-specific rules
- negative fixture avoids false FK confidence
- grain checks catch duplicate-grain fan-out
- profiles include enough evidence to explain each relationship

Done when:

- Understand can say "what this data is" from generic structure alone

### S4 — Semantic Typing And Source-Level Catalog

Build:

- mine semantic typing into `semantics/`
- infer generic column roles from names, types, statistics, patterns, and sibling
  consistency
- add source catalog APIs for editable descriptions, semantic type overrides, and locks
- preserve locks across regeneration

Verify:

- semantic type tests cover identifiers, timestamps, durations, enums, quantities, flags,
  free text, and ambiguous codes
- locked edits survive reprofile/regenerate
- LLM path, if enabled, receives only schema/stat/resource summaries, never raw rows

Done when:

- a data professional can correct generated metadata once and reuse it across projects

### S5 — Project Spec And Goal Capture

Build:

- add editable YAML/JSON Project Spec
- required: project name, goal statement, source id, selected tables
- optional: decision, target metrics, entities, time horizon, resources
- CLI: create, validate, import, export spec
- API model mirrors the spec

Verify:

- valid project spec persists into SQLite
- invalid spec errors are actionable
- selected scope is bounded to the source catalog
- no questions are required up front

Done when:

- projects are business problems over a profiled source, not separate ingestions

### S6 — Resource Intake And Semantic Claim Fusion

Build:

- accept resources: Markdown, text, CSV dictionaries, URLs later
- classify resources by sensitivity before use
- extract candidate definitions, enum mappings, aliases, business metrics, and caveats
- store them as semantic claims with source, confidence, and scope
- allow user to lock resource-derived definitions

Verify:

- resource parser creates claims without overwriting existing locks
- conflicting claims lower confidence and create Resolve candidates
- redaction tests pass for external LLM summaries
- pipeline still works with zero resources

Done when:

- project context improves semantics without becoming required or hard-coded

### S7 — Goal Relevance And Scope Ranking

Build:

- parse project goal into neutral signals: entities, measures, time concepts, lifecycle
  terms, segmentation hints, decision intent
- match goal signals against source semantic claims and profiles
- rank tables/columns as relevant, supporting, irrelevant, or risky
- store inclusion/exclusion rationale
- expose "something's off" correction path that updates semantic claims or project scope

Verify:

- radiology registration goal selects event/workflow/wait columns generically through
  lifecycle and duration semantics
- radiology device-utilization goal selects modality/room/scan-duration columns on the
  same source without reprofile
- MovieLens goal selects rating/movie/time/entity columns without code changes
- irrelevant columns are explainable, not silently hidden

Done when:

- Headwater can narrow a source to a goal-specific semantic slice

### S8 — Question Proposal

Build:

- generate proposed questions from relevance output and generic insight families
- classify each proposal as answerable, answerable-with-caveat, or cannot-answer
- include needed columns, missing evidence, confidence basis, and suggested next action
- persist curated keep/edit/drop decisions

Verify:

- no blank "ask your question" start state is required
- proposal tests cover temporal, segmentation, relationship, coverage, and impossible
  trend questions
- cannot-answer is treated as a first-class positive outcome with reason
- proposed question templates use semantic roles, not fixture/domain names

Done when:

- Understand can show "questions this data can credibly answer" and "questions it cannot"

### S9 — Generic EDA And Insight Families

Build:

- mine EDA kernels: robust z-score/MAD, seasonality/period detection, change point,
  correlation, quantiles, FDR correction
- replace domain families with generic role-driven insight family specs
- initial families: coverage, volume, duration, segmentation, relationship integrity,
  data quality, vocabulary drift
- push aggregations to DuckDB/source where possible; avoid `SELECT *` on large sources
- rank by effect size, population size, confidence, and actionability

Verify:

- radiology finds arrival/hour and wait/workflow signals
- MovieLens finds rating/time/entity signals
- TLC acceptance examples are produced by generic roles and config, not taxi branches
- performance test confirms large data aggregation path avoids full-table pulls

Done when:

- insights are deep enough to guide a professional while staying domain-agnostic

### S10 — Resolve Card Engine

Build:

- convert unresolved semantic claims, ambiguous definitions, enum mappings, drift events,
  and risky quality facts into ranked Resolve cards
- rank by affected questions, contract impact, confidence gap, and user-only-knowability
- support confirm, edit, map, mark-gap, skip, and batch-accept-low-risk
- every card declares which evidence contracts it can clear

Verify:

- only high-impact unresolved items appear by default
- "I don't know" records an explicit gap and does not guess
- confirming a card updates semantic claims/contracts, not a cosmetic progress number
- batch accept cannot certify a question unless all evidence contracts truly pass

Done when:

- Resolve is anti-overload: a few consequential decisions, each tied to a verdict impact

### S11 — Readiness Contracts And Certification Derivation

Build:

- define per-question contract types:
  - referenced columns profiled, locked/vouched, and lineage traced
  - no blocking gaps
  - structural integrity on query/join path
  - no unresolved misleading finding in lineage
  - definition consistency and traceability
  - insight confidence threshold met
- compute question state: certified, draft, cannot_answer, demoted
- compute project readout as a summary of question contract status, never as a badge
- store verdict buckets: have, trustworthy, risky, missing/gaps, misleading

Verify:

- tests prove certification cannot be set directly
- a clean question certifies only from passing contracts
- a low-confidence insight remains Draft even on clean data
- cannot-answer question has a clear reason and no fake certification pathway

Done when:

- Readiness is the truth engine of the product

### S12 — Markdown Audit Report

Build:

- generate a goal-anchored Markdown report:
  - project goal and source snapshot
  - curated questions and readiness state
  - what the data has, what is trustworthy, what is risky, what is missing, what is
    misleading
  - Resolve decisions and remaining gaps
  - evidence/provenance appendix
  - certified/draft stamps per answer
- add deterministic rendering for golden tests

Verify:

- radiology registration report contains expected generic findings
- device-utilization report differs by goal while reusing same source profile
- report never includes answer-key text unless ingested as a user resource

Done when:

- the first consultant-facing artifact is useful without the UI

### S13 — Grounded Answer Drafting

Build:

- generate SQL drafts from curated questions, semantic roles, selected scope, grain, and
  relationship evidence
- use templates and safe query planning, not free-form NL-to-SQL
- attach chart specs for simple line/bar/table outputs
- calculate insight confidence from sample size, coverage, variance, freshness, and
  contract state
- stamp outputs Draft, Certified, Cannot answer, or Demoted

Verify:

- SQL references only scoped, vouched columns
- unsafe SQL is rejected
- grain/fan-out tests prevent misleading joins
- answer remains Draft when any required contract fails

Done when:

- Answer & Share can produce defensible draft answers without pretending to be BI

### S14 — Continuous Certification

Build:

- implement lean source snapshot diff: schema, profile, relationship, vocabulary,
  freshness, and definition-lock changes
- map diffs to impacted semantic claims, questions, contracts, and answer artifacts
- auto-demote certified questions when a contract fails
- create Resolve cards for the broken contract and record revocation decisions

Verify:

- synthetic vocabulary drift revokes only affected question badges
- relationship drift reopens structural-integrity contracts
- definition edit recomputes impacted questions
- demotion message includes prior certified date, source snapshot, and reason

Done when:

- the trust badge is alive and can revoke itself honestly

### S15 — API And UI Implementation From Handoff

Build:

- implement the prototype screens in the production UI:
  - Source Catalog
  - Query console
  - Connect source
  - Projects home
  - Frame/Generate
  - Understand
  - Resolve
  - Readiness
  - Answer
- wire every durable action through API routes
- keep catalog inspection as on-demand power tooling, not part of the guided overload path
- enforce source/project limits from config, not hard-coded UI constants

Verify:

- Playwright happy paths:
  - connect/profile source
  - create project
  - generate understanding
  - curate proposed questions
  - resolve a high-impact gap
  - view readiness
  - export report
  - answer draft is stamped correctly
- screenshot checks for desktop/mobile workflow pages
- UI cannot mark a question certified by local state mutation

Done when:

- the clickable handoff behavior exists on real Headwater state

### S16 — Cross-Domain Product Validation

Build:

- add fixture suites for:
  - radiology workflow: two projects on one source
  - MovieLens/media: engagement or retention goal
  - TLC-scale or sampled TLC: domain-aware insight acceptance through generic roles
- add evaluation harness that compares expected facts, not exact prose
- add hard-coded-domain scanner to CI

Verify:

- same engine produces useful metadata, relevance, questions, verdicts, and reports across
  all fixtures
- adding a new fixture does not require production code changes
- fixture-specific expectations stay in tests/goldens only

Done when:

- Headwater can credibly claim dataset-agnostic behavior

## Stage Gates

Every stage has the same quality gate unless explicitly waived:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
uv run pyright
```

Pipeline stages also run a real fixture E2E with temporary SQLite and DuckDB stores. UI
stages add Playwright verification. Each stage should land as one focused commit with
the verification output in the commit body.

## Acceptance Criteria For V2

V2 is ready for user evaluation when:

- a new source can be connected and profiled once
- multiple projects can reuse the same source metadata
- generated metadata can be edited, locked, and preserved across regeneration
- a project goal produces relevant scope and proposed questions without requiring the
  user to author questions first
- Headwater can say "cannot answer" with concrete missing evidence
- Resolve shows only high-impact human decisions by default
- readiness is computed per question from evidence contracts
- certified, draft, cannot-answer, and demoted states are visibly distinct
- a certified answer is automatically demoted when source drift breaks a contract
- exported reports explain provenance, gaps, and freshness
- at least three unrelated data domains pass the same generic pipeline

## Deferred Until After V2 Wedge

- conversational NL querying and pluggable Ollama/third-party query harness
- full BI dashboarding, scheduled refreshes, embedding, row-level security
- broad enterprise governance workflows
- dbt export beyond the initial report/provenance artifact
- relationship graph UI beyond the simple Understand flow
- large context suite resurrection from H1

## Main Delivery Risks

- Overfitting to the radiology demo. Mitigation: cross-domain tests begin before Readiness
  and domain-token scans run in CI.
- UI progress becoming cosmetic. Mitigation: only evidence contracts can affect
  certification; UI state derives from backend verdicts.
- Metadata schema creep. Mitigation: H2 store starts lean; new tables require a product
  state-machine reason.
- Insight shallowness. Mitigation: role-driven EDA families must pass TLC and non-TLC
  acceptance examples.
- LLM leakage or hallucination. Mitigation: LLM is optional, redacted, summary-only, and
  never sets certification.

