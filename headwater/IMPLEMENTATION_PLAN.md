# Headwater Domain-Agnostic Context Implementation Plan

**Status:** Active implementation plan  
**Created:** May 19, 2026  
**Amended:** May 20, 2026
**Primary objective:** make Headwater produce useful, evidence-backed analysis
for an arbitrary ingested dataset on day one, while ensuring business meaning
lives only in reviewed project context.

Headwater's generic runtime should infer structure, shape, statistics,
relationships, and generic semantic types. It should not know what a taxi trip,
payment type, borough, claim, patient, factory line, shipment, account, or
revenue metric means.

Business meaning must enter through a project context layer that is generated
during ingestion, enriched from user-provided resources, reviewed by users, and
consumed by analyzer, explorer, insights, and assistants.

## Architecture Rule

Generic code may do these things:

- Discover schemas, tables, columns, dtypes, constraints, comments, profiles,
  nulls, cardinality, uniqueness, distributions, and relationships.
- Detect structural shapes: identifier, timestamp-like field, numeric measure,
  low-cardinality dimension, free text, enum/code shape, lookup table, join path,
  drift, and data quality risk.
- Detect generic semantic types from values and formats: email, phone, URL,
  UUID, IP address, ISO date/time variants, currency code, monetary amount,
  percentage, latitude/longitude, country/state/postal codes, IBAN, and similar
  non-business formats.
- Generate project context proposals with confidence, evidence, provenance, and
  review state.
- Apply approved or locked project context.
- Run generic statistical primitives: trend, anomaly, distribution, ranking,
  correlation, change point, segment comparison, and quality checks.

Generic code may not do these things:

- Hard-code dataset, industry, or business terms.
- Hard-code enum labels such as payment codes.
- Prefer dimensions because they are named `zone`, `borough`, `site`,
  `region`, `status`, `category`, or any other business term.
- Classify an insight as revenue, compliance, service, location, route, or any
  business lens through built-in keyword lists.
- Generate domain-specific derived fields such as route, speed, wait time, claim
  cycle time, patient stay, machine downtime, margin, or shipment delay unless
  those definitions come from project context.
- Scope projects through built-in aliases like taxi, TLC, FHV, yellow, or green.

## Target Workflow

The user-facing workflow should stay small:

```text
1. Ingest Source
2. Discovery Summary
3. Context Review
4. Business Insights
5. Explore / Ask
```

The detailed metadata universe should live behind that workflow:

```text
source ingestion
-> structural discovery and profiling
-> semantic type detection
-> context bootstrap proposals
-> resource/context enrichment
-> prioritized review queue
-> approved/locked project context
-> analyzer/catalog/explorer/insights consume context
-> drift-aware refresh
-> generated context files and review docs
```

## Day-One Value Contract

With zero project context and only an ingested source, Headwater must still
produce useful generic value. For any supported table or multi-table source,
the cold-start output must include:

- row grain and row entity proposals
- primary key and foreign key candidates
- canonical time anchor candidates
- top distributional facts and quality risks
- top-K dimensions and measures by statistical usefulness
- 3 to 5 structurally reasonable exploration questions
- explicit uncertainty, evidence, and review status for every proposal

This is the proof that "domain-agnostic" means useful on arbitrary data, not
only free of hard-coded terms.

## Context Layer Files

The metadata store is authoritative at runtime. Files under
`metadata/<project>/` are generated/importable projections for review, version
control, and portability.

```text
metadata/<project>/
  context.yaml
  semantic_types.yaml
  semantic_schema.yaml
  derived_fields.yaml
  insight_families.yaml
  lookups.yaml
  glossary.yaml
  business_lenses.yaml
  presentation.yaml
  question_templates.yaml
  column_policies.yaml
  relationship_hints.yaml
  resources.yaml
  advisor_packs.yaml
  REVIEW.md
```

Reusable vertical content should live in importable advisor packs:

```text
metadata/packs/<pack_name>/
  pack.yaml
  semantic_schema.yaml
  insight_families.yaml
  business_lenses.yaml
  question_templates.yaml
  column_policies.yaml
  glossary.yaml
```

Project metadata may `extends` one or more packs and override individual items.
Pack content is context, not generic runtime behavior.

## Canonical Context Item Types

The store should support these item types. Existing types should be reused where
possible; new types should be added only when they map to a real consumer.

- `dataset_summary`
- `table_profile`
- `row_grain`
- `row_entity`
- `time_anchor`
- `pk_candidate`
- `fk_candidate`
- `column_semantics`
- `semantic_role`
- `semantic_type_rule`
- `derived_field`
- `relationship`
- `relationship_hint`
- `lookup`
- `enum_mapping`
- `glossary_term`
- `business_lens`
- `insight_family`
- `insight_priority`
- `question_template`
- `visualization_hint`
- `column_policy`
- `project_alias`
- `source_alias`
- `table_alias`
- `advisor_pack`
- `resource`
- `open_question`

Every item must carry:

- stable `id`
- `project_id`
- optional `source_name`, `table_name`, `column_name`
- `item_type`
- structured `value`
- `status`: `proposed`, `approved`, `rejected`, `locked`, `needs_review`
- calibrated `confidence` in `[0.0, 1.0]`
- `source` producer
- evidence records
- drift state when applicable
- version and snapshot identifiers
- decision log references

## Evidence And Confidence Model

Every producer must emit evidence in the same shape:

- `evidence_id`
- `producer`: `profile`, `constraint`, `semantic_type`, `resource`, `llm`,
  `user`, `import`, or `advisor_pack`
- `method`
- `input_snapshot_id`
- `source_ref`
- `observed_value`
- `support_count`
- `sample_size`
- `confidence`
- `created_at`

Confidence uses a bounded `[0.0, 1.0]` scale. Producer anchors:

| Producer | Confidence anchor |
| --- | --- |
| Exact declared source constraint | `0.98` before user review, `1.0` only after user approval or trusted import |
| Exact regex/format semantic type match | `>=0.95` when support is high and conflicts are absent |
| Distributional structural inference | `0.55` to `0.9` based on support and ambiguity |
| Resource exact match | `0.75` to `0.95`; `1.0` only when source authority is confirmed |
| Advisor pack proposal | `0.6` to `0.85` until mapped to project evidence |
| LLM single-shot proposal | `<=0.7` until corroborated by another producer or evidence count threshold |
| User-approved item | `1.0` unless drift invalidates the item |
| User-rejected item | excluded from automatic re-proposal unless new evidence appears |

When multiple producers vote, use a deterministic combiner that preserves
evidence and does not let weak duplicate signals swamp strong evidence:

- identical high-confidence evidence may raise confidence to a capped maximum
- conflicting evidence lowers confidence and moves the item toward review
- user decisions dominate machine producers until drift invalidates them
- the combiner must be deterministic and covered by ordering tests

## Review Prioritization

The UI should not show all generated metadata equally. It should prioritize
items that affect correctness or business value.

Show prominently:

- drift-invalidated approved or locked context
- resource conflicts with approved or locked context
- missing or ambiguous row grain
- missing or ambiguous row entity
- missing or ambiguous primary key
- missing canonical time field
- relationship changes
- high-impact metric, dimension, or semantic role proposals used by insights
- missing resource/domain context that blocks meaningful insight generation

Show in secondary context panels:

- proposed column descriptions
- glossary terms
- enum mappings
- lookup mappings
- display labels
- question templates
- visualization hints

Keep hidden by default but available in files/API:

- raw profile evidence
- all bootstrap evidence
- stable high-confidence structural proposals
- raw resource extraction details
- low-impact proposed context

## Safety And Provenance

These requirements cut across all phases and must be implemented before any LLM
or NL-to-SQL surface is enabled by default.

### PII And Resource Classification

- Classify resources at ingest as `public`, `internal`, `sensitive`, or
  `unknown`; default to `unknown`.
- Require explicit configuration before `internal`, `sensitive`, or `unknown`
  resources are sent to any external LLM provider.
- Run redaction before any LLM call, including calls sourced from resources,
  comments, catalog metadata, or sampled values.
- Keep the existing invariant that raw rows are not sent to external LLMs.

### SQL Safety

- NL-to-SQL must route through a generic SQL safety layer.
- Allow only read-only statement shapes.
- Scope queries to the active project/source/schema allowlist.
- Apply row, time, byte, and execution-time limits.
- Block DDL, DML, multi-statement execution, unsafe functions, and unbounded
  cross joins.
- Persist the planned SQL, safety decision, and result limits as evidence.

### Provenance, Versioning, And Rollback

- Add an append-only `decision_log` per context item: who/what made the change,
  timestamp, prior value, new value, evidence ids, reason, and source snapshot.
- Snapshot discovery/profile/resource inputs on each ingest so run N can be
  reconstructed.
- Support reverting a specific context decision and replaying downstream
  derived context and generated files.
- Preserve approved/locked decisions across re-ingestion unless a defined drift
  rule moves them to `needs_review`.

### Telemetry Feedback Loop

- Log every approve, reject, lock, edit, and defer event.
- Capture item id, item type, producer, prior confidence, user action,
  time-to-decision, and whether the item was later invalidated by drift.
- Use the log for future ranking/calibration improvement; model training is a
  non-goal for this plan.

## Drift Semantics

Drift must have concrete detector behavior and review consequences.

| Drift type | Detector | Severity examples | Context consequence |
| --- | --- | --- | --- |
| Schema drift | table/column/type/constraint diff | removed column, type change, new key candidate | impacted items move to `needs_review` |
| Distributional drift | null/cardinality/top-value/range/freshness diff | major null-rate jump, new dominant enum value | impacted semantic and policy items move to review when thresholds are exceeded |
| Relationship drift | FK support, join cardinality, uniqueness diff | FK support drops, lookup becomes many-to-many | relationships, row grain, and downstream insights move to review |
| Semantic drift | resource, glossary, pack, or context file update | metric definition changes, resource authority changes | derived context from the resource moves to review |

Each detector must emit severity, affected items, evidence, and the rule that
triggered the state change.

## Phase 0: Audit And Guardrails

**Goal:** identify and prevent hard-coded business/domain behavior in generic
runtime code.

### Implementation

- Add a tracked audit document or test fixture containing known forbidden terms
  and allowed locations.
- Add an architecture boundary test that scans generic runtime modules for
  domain/business vocabulary.
- Allow domain vocabulary only in:
  - `metadata/<project>/`
  - `metadata/packs/<pack_name>/`
  - tests explicitly marked as fixture/domain examples
  - generated review docs
  - comments that are not used by runtime behavior, where unavoidable
- Start with the known violations:
  - `headwater/explorer/readability.py`
  - `headwater/api/project_scope.py`
  - `headwater/analyzer/semantic_schema.py`
  - `headwater/explorer/statistical.py`
  - `headwater/api/routes/insights.py`
  - `headwater/explorer/nl_to_sql.py`
  - `headwater/explorer/suggestions.py`
  - `headwater/explorer/visualization.py`
  - `headwater/profiler/key_detection.py`

### Verification

- `uv run python -m pytest tests/test_architecture_boundaries.py -q`
- `uv run ruff check headwater tests`

### Exit Criteria

- A failing test can catch newly added business vocabulary in generic modules.
- Known violations are documented with target metadata destination.
- Future phases can remove violations incrementally without losing visibility.

## Phase 0.5: Semantic Type Library

**Goal:** add a generic format/distributional semantic layer between raw
structure and business context.

### Implementation

- Introduce a registered, extensible detector library for non-business semantic
  types.
- Initial detectors:
  - email
  - phone
  - URL
  - UUID
  - IPv4 and IPv6
  - ISO date/time variants
  - monetary amount
  - currency code
  - percentage
  - latitude and longitude
  - country, subdivision, and postal code shapes
  - IBAN and other checksum-backed account formats where safe
- Each detector returns:
  - semantic type
  - confidence
  - support count and sample size
  - conflicting type evidence
  - whether values are likely sensitive
- Feed semantic type evidence into:
  - column policies
  - PII redaction
  - visualization hints
  - question generation
  - cold-start summaries
- Do not attach business labels or industry meanings in detector code.

### Verification

- Detector fixtures cover positive, negative, ambiguous, null-heavy, and mixed
  format columns.
- PII-sensitive detectors mark policy proposals without exposing raw values.
- No detector contains domain/business vocabulary outside generic formats.

### Exit Criteria

- Generic semantic type evidence is available to context bootstrap and review.
- PII, visualization, and question fallback behavior can use semantic type
  evidence without project metadata.

## Phase 1: Context Schema Expansion

**Goal:** give project context enough structure to replace hard-coded registries
and make row grain/entity first-class.

### Implementation

- Extend canonical context import/export to include:
  - `derived_fields.yaml`
  - `insight_families.yaml`
  - `business_lenses.yaml`
  - `presentation.yaml`
  - `question_templates.yaml`
  - `column_policies.yaml`
  - `relationship_hints.yaml`
  - `advisor_packs.yaml`
- Add import/export support for these item types:
  - `row_grain`
  - `row_entity`
  - `time_anchor`
  - `pk_candidate`
  - `fk_candidate`
  - `enum_mapping`
  - `derived_field`
  - `insight_family`
  - `business_lens`
  - `question_template`
  - `visualization_hint`
  - `column_policy`
  - `project_alias`
  - `source_alias`
  - `table_alias`
  - `relationship_hint`
  - `advisor_pack`
- Add `ProjectContextProvider` accessors instead of ad hoc item traversal:
  - row grain and row entity
  - canonical time anchors
  - key candidates and relationship hints
  - aliases
  - enum mappings
  - value labels
  - low-signal columns
  - preferred dimensions
  - business lenses
  - insight family configs
  - question templates
  - visualization hints
  - derived fields
  - advisor pack inheritance and overrides
- Bootstrap row-grain and row-entity proposals from:
  - uniqueness ratios
  - declared and inferred keys
  - event-vs-state heuristics
  - table/column naming evidence as weak evidence only
  - time anchors and relationship topology

### Verification

- Context export includes all new files.
- Context import round-trips every new file into canonical store records.
- Approved and locked records are preserved on import and re-ingestion.
- Tests cover at least one item per new context type.
- Row grain, row entity, time anchor, PK, and FK proposals appear for a
  zero-context fixture.

### Exit Criteria

- Generic runtime consumers can retrieve all business/domain hints from the
  context provider.
- YAML/Markdown remain projections, not separate runtime authority.
- Row grain and row entity are reviewable, persisted, and versioned.

## Phase 1.5: Confidence Calibration And Evidence Model

**Goal:** make review ordering meaningful across heuristic, LLM, resource,
imported, and user-produced context.

### Implementation

- Implement the canonical evidence schema described above.
- Add producer-specific confidence anchors and enforce bounds in code.
- Add the deterministic confidence combiner.
- Require every context proposal to cite at least one evidence record.
- Normalize evidence from:
  - structural profiling
  - declared constraints
  - semantic type detectors
  - resources
  - LLM outputs
  - advisor packs
  - imports
  - user decisions

### Verification

- Calibration fixtures assert ordering across producers:
  - exact regex/format match outranks unsupported LLM guess
  - user approval outranks machine producers
  - conflicting evidence lowers confidence and moves items toward review
  - declared constraints outrank inferred candidates unless drift conflicts
- Confidence scores are deterministic across repeated runs on identical inputs.

### Exit Criteria

- The review queue can be sorted by impact, confidence, conflict, and drift with
  stable semantics.
- Every proposed item explains why it exists and how confident Headwater is.

## Phase 2: Remove Hard-Coded Label And Enum Behavior

**Goal:** make readable labels and enum translations context-driven.

### Implementation

- Remove `BUILTIN_ENUM_LABEL_REGISTRY` and
  `BUILTIN_ENUM_DIMENSION_LABELS` from `explorer/readability.py`.
- Replace readable-label token lists with structural and semantic-type rules:
  - textual dtype
  - non-id
  - low/null-safe cardinality
  - lookup relationship
  - explicit context label hint
  - generic semantic type evidence
- Load enum mappings from:
  - canonical `enum_mapping` items
  - `lookups.yaml`
  - resource-derived dictionary rows
  - advisor pack mappings applied through project context
- Add context bootstrap for enum candidates:
  - low-cardinality code-like columns
  - top values captured as proposed mappings without labels
  - companion/resource labels when available

### Verification

- Existing taxi/payment behavior works only when metadata supplies the mapping.
- A healthcare/manufacturing/finance fixture can define its own enum mapping and
  see readable labels without code changes.
- No built-in enum value labels remain in generic code.
- The removal commit includes the replacement fixture and taxi accepted-delta
  regression test.

### Exit Criteria

- Generic readability code has no business enum labels.
- All readable business labels are traceable to context evidence.

## Phase 3: Remove Project Alias Bias

**Goal:** project scoping must use explicit metadata or structural source links,
not hard-coded dataset aliases.

### Implementation

- Remove taxi/TLC/FHV/yellow/green alias logic from `api/project_scope.py`.
- Add `project_alias`, `source_alias`, and `table_alias` context items.
- Generate initial aliases from:
  - project slug/display name
  - source name
  - table names
  - user-provided resources
  - imported metadata files
  - advisor packs
- Use aliases from `ProjectContextProvider` in project scoping.
- Add UI/API review support for aliases only when they affect source/table
  inclusion.

### Verification

- Taxi project scoping works only when aliases exist in project context.
- Non-taxi projects can add aliases without code changes.
- Tests prove no domain aliases are hard-coded in `project_scope.py`.
- The removal commit includes the replacement fixture and taxi accepted-delta
  regression test.

### Exit Criteria

- Project scoping is context-driven and domain-neutral.

## Phase 4: Semantic Roles, Derived Fields, And Multi-Table Structure

**Goal:** core semantic schema should infer only generic primitives and apply
project-defined roles/derived fields.

### Implementation

- Keep generic primitives:
  - timestamp-like field
  - numeric measure
  - identifier
  - dimension
  - text
  - lookup key
  - generic semantic type
- Remove built-in role mappings for:
  - `service_type`
  - `origin_id`
  - `destination_id`
  - `location_id`
  - `distance`
  - `duration`
  - `amount`
  - `tip_amount`
- Remove universal derived fields:
  - `wait_min`
  - `route_pair`
  - `speed_per_hour`
  - any domain-specific lifecycle metric
- Add project-defined derived fields:
  - expression template
  - required roles
  - output name
  - output semantic type
  - confidence/status/evidence
- Keep only generic time buckets from approved timestamp roles:
  - date bucket
  - hour bucket
  - weekday bucket
- Add multi-table structural coverage:
  - cross-table FK discovery
  - fact-vs-dimension heuristics
  - lookup-table promotion
  - conformed dimension candidates
  - source/table alias resolution across sources
  - relationship confidence and support evidence

### Verification

- Without project metadata, a source receives only structural semantic roles.
- With `metadata/nytaxi/derived_fields.yaml`, route/speed/wait behavior returns.
- A healthcare fixture can define `length_of_stay` without code changes.
- A manufacturing fixture can define `downtime_minutes` without code changes.
- Multi-table fixtures produce FK, lookup, and conformed-dimension candidates.
- The removal commit includes the replacement fixture and taxi accepted-delta
  regression test.

### Exit Criteria

- `semantic_schema.py` contains no domain-specific role vocabulary beyond
  generic primitives.
- Business-specific derived fields are entirely context-driven.
- Multi-table structure is inferred generically and reviewed through context.

## Phase 5: Insight Families As Metadata

**Goal:** generic insights should provide statistical primitives; project
context should decide business families and language.

### Implementation

- Split insight execution into:
  - generic statistical primitive engine
  - project insight family planner
  - business presentation layer
- Keep generic primitives:
  - coverage
  - volume trend
  - distribution
  - ranking
  - segment comparison
  - anomaly
  - change point
  - correlation
  - quality check
- Move these into `insight_families.yaml`:
  - location distribution
  - path/route distribution
  - distance efficiency
  - wait/service patterns
  - revenue leakage
  - compliance risk
  - patient stay
  - manufacturing downtime
  - finance margin or spend analysis
- Remove hard-coded service labels and industry terms from
  `explorer/statistical.py`.
- Make family text templates context-driven:
  - title template
  - detail template
  - required roles
  - primitive query type
  - ranking priority
  - suppression rules

### Verification

- Existing taxi semantic insights work through `metadata/nytaxi/insight_families.yaml`.
- A non-taxi fixture can define a project insight family without touching Python.
- Generic fallback insights still work with no project family metadata.
- The removal commit includes the replacement fixture and taxi accepted-delta
  regression test.

### Exit Criteria

- No project-specific insight family or label exists in generic statistical code.
- Every business-facing semantic insight can cite a context item or metadata file.

## Phase 6: Business Lenses, Ranking, And Presentation Policies

**Goal:** insight categories, dimension preferences, low-signal suppression, and
visualization hints should be project context.

### Implementation

- Move business category tokens from `api/routes/insights.py` into
  `business_lenses.yaml`.
- Move preferred dimensions and low-signal columns into `column_policies.yaml`
  and `presentation.yaml`.
- Move visualization name hints into `presentation.yaml`.
- Rank insights using:
  - statistical significance
  - support count
  - evidence quality
  - context-approved business lens priority
  - user review status
- Stop hard-coding categories like `Revenue`, `Compliance`, and `Operations`
  unless supplied by context.

### Verification

- With no business lenses, insights are categorized generically.
- With project lenses, insights receive project-specific categories and
  priorities.
- Visualization recommendations remain shape-driven when context is absent.
- The removal commit includes the replacement fixture and taxi accepted-delta
  regression test.

### Exit Criteria

- `insights.py` and `visualization.py` have no built-in business vocabulary.
- Presentation behavior is explainable from result shape plus context.

## Phase 7: NL-To-SQL And Suggestions Vocabulary Refactor

**Goal:** generated questions and NL query behavior should be grounded in
catalog/context, not built-in industry nouns.

### Implementation

- Move hard-coded metric/dimension/question vocabulary from:
  - `explorer/nl_to_sql.py`
  - `explorer/suggestions.py`
  - `explorer/query_planner.py`
  - `explorer/decomposition.py`
- Add context-driven question templates:
  - metric by dimension
  - trend over time
  - segment comparison
  - top contributors
  - quality risk
  - project-defined business lens questions
- Generate row nouns from:
  - approved `row_entity`
  - approved `row_grain`
  - table/entity metadata
  - fallback `records`
- Use catalog metrics/dimensions and approved context roles for scoring.
- Route generated SQL through the safety layer defined above.

### Verification

- Question suggestions for taxi, healthcare, manufacturing, and finance fixtures
  differ only because their project context differs.
- Removing project metadata falls back to structural suggestions.
- No built-in industry nouns are needed for high-value questions.
- Unsafe NL-to-SQL attempts are blocked with explainable safety decisions.
- The removal commit includes the replacement fixture and taxi accepted-delta
  regression test.

### Exit Criteria

- NL-to-SQL and suggestions consume context vocabularies and catalog artifacts.
- Generic code contains only analytical verbs and structural concepts.
- SQL execution is read-only, scoped, and bounded.

## Phase 8A: Structural Bootstrap Proposals

**Goal:** ingestion should generate rich zero-context proposals while staying
domain-neutral.

### Implementation

- Generate context from structural evidence:
  - table profiles
  - column profiles
  - declared constraints
  - inferred relationships
  - lookup shapes
  - enum/code shapes
  - semantic type evidence
  - temporal coverage
  - numeric distributions
  - text columns
  - duplicate/grain risks
- Propose:
  - row grain
  - row entity
  - PK/FK candidates
  - canonical time anchors
  - top-K dimensions and measures
  - quality risks
  - fallback questions
- Never auto-approve business meaning.

### Verification

- New source ingestion creates context proposals automatically.
- Approved/locked context survives re-ingestion.
- Drift marks impacted context as `needs_review`.
- Synthetic random-schema fixture produces the day-one contract outputs without
  project metadata.

### Exit Criteria

- Ingestion is the main producer of structural project context proposals.
- Zero-context sources produce useful reviewable output.

## Phase 8B: Resource Extraction With Safety

**Goal:** turn user-provided resources into evidence without weakening privacy
or provenance boundaries.

### Implementation

- Add resource classification and override flow.
- Extract from:
  - CSV dictionaries
  - Markdown/text notes
  - YAML/dbt-like docs
  - PDFs where dependencies are available
  - URL text extraction where explicitly allowed/configured
  - dbt manifest/catalog imports
  - BI metric export imports
- Redact content before any LLM call.
- Persist resource extraction evidence separately from proposed context items.
- Detect resource conflicts with approved context.

### Verification

- Resource imports produce evidence and proposals with calibrated confidence.
- Sensitive or unknown resources are not sent to external LLM providers by
  default.
- Resource conflicts appear in the review queue.

### Exit Criteria

- Resources can enrich context while preserving classification, redaction, and
  auditability.

## Phase 8C: Context File Generation And Re-Import

**Goal:** make reviewable files a faithful projection of the authoritative
SQLite context store.

### Implementation

- Generate proposed metadata files after ingestion when enabled:
  - write to `metadata/<project>/`
  - preserve approved/locked store records
  - include `REVIEW.md`
  - include evidence and confidence summaries
- Import generated files back into canonical records without semantic drift.
- Support project-level `extends` for advisor packs.

### Verification

- Generated files can be imported back without changing meaning.
- File diffs are stable across repeated runs with identical inputs.
- Approved/locked context survives export/import/re-ingestion.

### Exit Criteria

- Files are portable and reviewable without becoming a second runtime authority.

## Phase 8D: Optional Local ML And Embedding Assist

**Goal:** add local matching assistance only as a gated, deterministic,
never-auto-approve enhancement.

### Implementation

- Add an explicit feature flag; default off.
- Use local ML/embedding assist for:
  - term-to-column matching
  - column grouping
  - resource-to-table matching
  - glossary clustering
- Cache inputs and outputs by content hash.
- Record model name/version and evidence ids.
- Never approve automatically.

### Verification

- Offline mode works with feature flag off.
- Cached replay yields identical proposals for tests.
- No CI test requires live model access.

### Exit Criteria

- Local assist improves ranking when enabled but is not required for core
  domain-agnostic behavior.

## Phase 9: Cold-Start Day-One Deliverable

**Goal:** make arbitrary-dataset usefulness a first-class acceptance target.

### Implementation

- Add a fixture-independent cold-start acceptance test that ingests a synthetic
  random-schema dataset with no project metadata.
- Assert production of:
  - row grain proposal
  - row entity proposal or explicit unknown with evidence
  - time anchor candidate when time-like data exists
  - PK/FK candidates when support exists
  - top-K dimensions and measures
  - top distributional facts and quality risks
  - 3 to 5 fallback questions
  - calibrated confidence and evidence for each proposal
- Ensure all UI states render useful summaries without business metadata.

### Verification

- The cold-start test runs in CI without external services.
- Structural suggestions degrade gracefully when the fixture omits keys, time, or
  measures.

### Exit Criteria

- A user dropping an unknown CSV or connecting an unknown database receives
  actionable structure, risks, and questions before adding context.

## Phase 10: Cleaner User Workflow

**Goal:** the UI should guide users through only the review that matters.

### Implementation

- Replace broad context item lists with a prioritized review queue.
- Add filters:
  - `Needs action`
  - `Drift`
  - `High impact`
  - `Resources`
  - `All context`
- Add compact review cards for:
  - row grain
  - row entity
  - canonical time
  - primary/foreign keys
  - semantic roles used by insights
  - metric/dimension proposals used by catalog
  - resource conflicts
- Move raw metadata detail into:
  - context files
  - item detail drawer
  - generated `REVIEW.md`
- Show business insight readiness:
  - structural discovery complete
  - context coverage
  - reviewed critical items
  - resource coverage
  - unresolved drift

### Verification

- A user can ingest a source and identify the next review action without reading
  raw metadata dumps.
- The full context remains accessible for audit/export/import.
- UI tests or build verification cover context review states.

### Exit Criteria

- Ingestion to insights is clean and review-driven.
- Detailed semantic data remains available without cluttering the primary flow.

## Phase 11: Cross-Domain Evaluation Harness

**Goal:** prove Headwater is domain-agnostic across materially different
datasets using graded, repeatable metrics.

### Fixtures

- Generic operations/events dataset.
- Healthcare-style fixture.
- Manufacturing-style fixture.
- Finance-style fixture.
- Taxi fixture using only `metadata/nytaxi/`.
- Synthetic random-schema cold-start fixture.

### Gold Files

Each fixture must define:

- gold row grain
- gold row entity where knowable
- gold PK/FK candidates
- gold time anchor
- gold top-K dimensions and measures
- gold question list or acceptable question intents
- gold insight families where project metadata exists
- gold "no business term leak" assertion
- accepted deltas for taxi parity during migration

### Scoring

- Add a scoring script that records precision/recall or exact-match metrics per
  gold category.
- Track regression metrics per fixture across commits.
- Fail CI when metrics drop below configured thresholds.
- Keep fixture outputs reproducible through cached inputs and deterministic
  prompts.

### Required Assertions

- No fixture requires Python code changes for domain-specific labels, insights,
  aliases, question templates, or derived fields.
- Removing project metadata degrades to structural insights, not wrong business
  insights.
- Adding metadata restores project-specific business value.
- Review state and drift behavior work consistently across fixtures.

### Verification

- Focused architecture tests.
- Analyzer/catalog tests.
- Explorer/NL-to-SQL tests.
- Insights tests.
- Evaluation scoring script.
- UI build.

### Exit Criteria

- The same generic pipeline works across all fixtures.
- Domain behavior is traceable to project context.
- Regressions are visible as numbers, not subjective fixture inspection.

## Phase 12: LLM Determinism, Cost Controls, And Replay

**Goal:** make LLM-assisted context enrichment reproducible, bounded, and safe.

### Implementation

- Build prompts deterministically from sorted, scoped, redacted inputs.
- Cache LLM responses by content-addressed hash of:
  - prompt template version
  - redacted input payload
  - model/provider
  - configuration
- Add per-source and per-run token budgets.
- Add offline mode that disables live calls and uses cached responses only.
- Add replay mode for tests.
- Store raw provider responses only when classification policy allows it; store
  normalized evidence otherwise.

### Verification

- CI can replay cached LLM outputs without network access.
- Token budget exhaustion produces a clear partial-result state.
- Prompt and response hashes make repeated runs auditable.

### Exit Criteria

- LLM assist is optional, reproducible, and safe to test.

## Phase 13: Advisor Packs

**Goal:** make vertical content reusable across projects without embedding it in
generic code.

### Implementation

- Add importable advisor packs with metadata, version, dependencies, and
  supported item types.
- Allow project context to `extends` packs and override individual pack items.
- Treat pack items as proposals unless a project explicitly locks or approves
  them.
- Track pack provenance in evidence and decision logs.
- Add conflict handling when two packs propose incompatible context.

### Verification

- Healthcare, manufacturing, finance, and taxi packs can be imported into
  different projects.
- Project overrides take precedence over pack defaults.
- Pack removal or version update moves affected derived items to review.

### Exit Criteria

- Vertical insight families, lenses, glossary terms, and templates are reusable
  assets rather than project-local copy-paste.

## Phase 14: Documentation And Operating Model

**Goal:** make the architecture enforceable by humans and tests.

### Implementation

- Update `docs/ARCHITECTURE.md` with the context boundary.
- Add a short guide for adding project metadata.
- Add a guide for adding a new context item type.
- Document confidence calibration and evidence producers.
- Document review statuses and drift semantics.
- Document resource classification, redaction, and SQL safety.
- Document advisor packs.
- Document LLM offline/replay behavior.
- Document how assistants receive scoped context.
- Document what must never be added to generic code.

### Verification

- Documentation references current APIs and file names.
- Architecture boundary tests match the documentation.

### Exit Criteria

- New contributors have a clear rulebook.
- The codebase has automated checks for the most important boundary.

## Implementation Order

Recommended execution sequence:

1. Phase 0: audit and guardrails.
2. Phase 0.5: semantic type library.
3. Phase 1: context schema expansion.
4. Phase 1.5: confidence calibration and evidence model.
5. Phase 2: labels and enum mappings.
6. Phase 3: project aliases.
7. Phase 4: semantic roles, derived fields, and multi-table structure.
8. Phase 5: insight families.
9. Phase 6: business lenses and presentation.
10. Phase 7: NL-to-SQL and suggestions.
11. Phase 8A: structural bootstrap proposals.
12. Phase 8B: resource extraction with safety.
13. Phase 8C: context file generation and re-import.
14. Phase 8D: optional local ML and embedding assist.
15. Phase 9: cold-start day-one deliverable.
16. Phase 10: UI workflow cleanup.
17. Phase 11: cross-domain evaluation harness.
18. Phase 12: LLM determinism, cost controls, and replay.
19. Phase 13: advisor packs.
20. Phase 14: documentation.

Phases 2 through 7 must be implemented as small removals of one hard-coded
registry at a time. Each removal commit must ship with:

- a replacement context fixture
- taxi parity or accepted-delta regression coverage
- a forbidden-term boundary test update when needed
- a note explaining any intentionally accepted degradation

Main should not silently degrade between removal phases.

## Definition Of Done

This plan is complete when these measurable gates pass:

- `0` hits on the forbidden-term scan in generic runtime modules, excluding
  approved allowlist locations.
- `100%` of domain-specific aliases, labels, roles, derived fields, insight
  families, question templates, and presentation preferences come from project
  context or advisor packs.
- At least `4` distinct-vertical fixtures plus the synthetic cold-start fixture
  pass the evaluation harness.
- Cold-start ingestion produces row grain/entity, key candidates, time anchor
  candidates where applicable, top-K dimensions/measures, distributional facts,
  quality risks, and `3` to `5` fallback questions without project metadata.
- Confidence calibration ordering tests pass across structural, semantic type,
  resource, LLM, advisor pack, import, and user producers.
- Generated context files export/import round-trip without semantic changes.
- LLM replay tests pass offline from cached responses.
- SQL safety tests block unsafe NL-to-SQL plans and enforce project scoping and
  limits.
- Drift tests cover schema, distributional, relationship, and semantic drift,
  with affected approved items moved to `needs_review`.
- Telemetry records approve/reject/edit/lock/defer actions with prior
  confidence and time-to-decision.
- UI build and context review tests cover prioritized review states.

## Non-Goals

These are intentionally out of scope for this plan:

- Automatic approval of business logic.
- Autonomous changes to marts, contracts, or source systems without human review.
- Multi-tenant RBAC or managed-cloud account isolation.
- Real-time streaming ingestion.
- Training a ranking model from feedback telemetry.
- Guaranteeing perfect business meaning from raw data without reviewed context.
