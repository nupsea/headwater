# Headwater Domain-Agnostic Context Implementation Plan

**Status:** Active implementation plan  
**Created:** May 19, 2026  
**Primary objective:** remove domain-specific behavior from generic code and make
project context the only place where business meaning lives.

Headwater's generic runtime should infer structure, shape, statistics, and
relationships. It should not know what a taxi trip, payment type, borough,
claim, patient, factory line, shipment, account, or revenue metric means.

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
-> context bootstrap proposals
-> resource/context enrichment
-> prioritized review queue
-> approved/locked project context
-> analyzer/catalog/explorer/insights consume context
-> drift-aware refresh
-> generated context files and review docs
```

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
  REVIEW.md
```

## Canonical Context Item Types

The store should support these item types. Existing types should be reused where
possible; new types should be added only when they map to a real consumer.

- `dataset_summary`
- `table_profile`
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
- `resource`
- `open_question`

Every item must carry:

- stable `id`
- `project_id`
- optional `source_name`, `table_name`, `column_name`
- `item_type`
- structured `value`
- `status`: `proposed`, `approved`, `rejected`, `locked`, `needs_review`
- `confidence`
- `source`
- evidence records
- drift state when applicable

## Review Prioritization

The UI should not show all generated metadata equally. It should prioritize
items that affect correctness or business value.

Show prominently:

- drift-invalidated approved or locked context
- resource conflicts with approved or locked context
- missing or ambiguous row grain
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

## Phase 1: Context Schema Expansion

**Goal:** give project context enough structure to replace hard-coded registries.

### Implementation

- Extend canonical context import/export to include:
  - `derived_fields.yaml`
  - `insight_families.yaml`
  - `business_lenses.yaml`
  - `presentation.yaml`
  - `question_templates.yaml`
  - `column_policies.yaml`
  - `relationship_hints.yaml`
- Add import/export support for these item types:
  - `enum_mapping`
  - `derived_field`
  - `insight_family`
  - `business_lens`
  - `question_template`
  - `visualization_hint`
  - `column_policy`
  - `project_alias`
  - `table_alias`
  - `relationship_hint`
- Add `ProjectContextProvider` accessors instead of ad hoc item traversal:
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

### Verification

- Context export includes all new files.
- Context import round-trips every new file into canonical store records.
- Approved and locked records are preserved on import and re-ingestion.
- Tests cover at least one item per new context type.

### Exit Criteria

- Generic runtime consumers can retrieve all business/domain hints from the
  context provider.
- YAML/Markdown remain projections, not separate runtime authority.

## Phase 2: Remove Hard-Coded Label And Enum Behavior

**Goal:** make readable labels and enum translations context-driven.

### Implementation

- Remove `BUILTIN_ENUM_LABEL_REGISTRY` and
  `BUILTIN_ENUM_DIMENSION_LABELS` from `explorer/readability.py`.
- Replace readable-label token lists with structural rules:
  - textual dtype
  - non-id
  - low/null-safe cardinality
  - lookup relationship
  - explicit context label hint
- Load enum mappings from:
  - canonical `enum_mapping` items
  - `lookups.yaml`
  - resource-derived dictionary rows
- Add context bootstrap for enum candidates:
  - low-cardinality code-like columns
  - top values captured as proposed mappings without labels
  - companion/resource labels when available

### Verification

- Existing taxi/payment behavior works only when metadata supplies the mapping.
- A healthcare/manufacturing/finance fixture can define its own enum mapping and
  see readable labels without code changes.
- No built-in enum value labels remain in generic code.

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
- Use aliases from `ProjectContextProvider` in project scoping.
- Add UI/API review support for aliases only when they affect source/table
  inclusion.

### Verification

- Taxi project scoping works only when aliases exist in project context.
- Non-taxi projects can add aliases without code changes.
- Tests prove no domain aliases are hard-coded in `project_scope.py`.

### Exit Criteria

- Project scoping is context-driven and domain-neutral.

## Phase 4: Semantic Roles And Derived Fields Refactor

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

### Verification

- Without project metadata, a source receives only structural semantic roles.
- With `metadata/nytaxi/derived_fields.yaml`, route/speed/wait behavior returns.
- A healthcare fixture can define `length_of_stay` without code changes.
- A manufacturing fixture can define `downtime_minutes` without code changes.

### Exit Criteria

- `semantic_schema.py` contains no domain-specific role vocabulary beyond
  generic primitives.
- Business-specific derived fields are entirely context-driven.

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
  - dataset context `row_represents`
  - table/entity metadata
  - fallback `records`
- Use catalog metrics/dimensions and approved context roles for scoring.

### Verification

- Question suggestions for taxi, healthcare, manufacturing, and finance fixtures
  differ only because their project context differs.
- Removing project metadata falls back to structural suggestions.
- No built-in industry nouns are needed for high-value questions.

### Exit Criteria

- NL-to-SQL and suggestions consume context vocabularies and catalog artifacts.
- Generic code contains only analytical verbs and structural concepts.

## Phase 8: Ingestion Context Bootstrap Enhancement

**Goal:** ingestion should generate rich context proposals while staying
domain-neutral.

### Implementation

- Generate context from structural evidence:
  - table profiles
  - column profiles
  - declared constraints
  - inferred relationships
  - lookup shapes
  - enum/code shapes
  - temporal coverage
  - numeric distributions
  - text columns
  - duplicate/grain risks
- Generate proposed metadata files after ingestion when enabled:
  - write to `metadata/<project>/`
  - preserve approved/locked store records
  - include `REVIEW.md`
- Add resource extraction improvements:
  - CSV dictionaries
  - Markdown/text notes
  - YAML/dbt-like docs
  - PDFs where dependencies are available
  - URL text extraction where explicitly allowed/configured
  - dbt manifest/catalog imports
  - BI metric export imports
- Add optional local ML/embedding assist:
  - term-to-column matching
  - column grouping
  - resource-to-table matching
  - glossary clustering
  - never approve automatically

### Verification

- New source ingestion creates context proposals automatically.
- Generated files can be imported back without changing meaning.
- Approved/locked context survives re-ingestion.
- Drift marks impacted context as `needs_review`.

### Exit Criteria

- Ingestion is the main producer of project context proposals.
- Business-specific metadata is generated or supplied, not embedded in core code.

## Phase 9: Cleaner User Workflow

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

## Phase 10: Cross-Domain Verification Suite

**Goal:** prove Headwater is domain-agnostic across materially different
datasets.

### Fixtures

- Generic operations/events dataset.
- Healthcare-style fixture.
- Manufacturing-style fixture.
- Finance-style fixture.
- Taxi fixture using only `metadata/nytaxi/`.

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
- UI build.

### Exit Criteria

- The same generic pipeline works across all fixtures.
- Domain behavior is traceable to project context.

## Phase 11: Documentation And Operating Model

**Goal:** make the architecture enforceable by humans and tests.

### Implementation

- Update `docs/ARCHITECTURE.md` with the context boundary.
- Add a short guide for adding project metadata.
- Add a guide for adding a new context item type.
- Document review statuses and drift semantics.
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
2. Phase 1: context schema expansion.
3. Phase 2: labels and enum mappings.
4. Phase 3: project aliases.
5. Phase 4: semantic roles and derived fields.
6. Phase 5: insight families.
7. Phase 6: business lenses and presentation.
8. Phase 7: NL-to-SQL and suggestions.
9. Phase 8: ingestion bootstrap enhancement.
10. Phase 9: UI workflow cleanup.
11. Phase 10: cross-domain verification.
12. Phase 11: documentation.

Phases 2 through 7 should be implemented as small removals of one hard-coded
registry at a time, each with a replacement context fixture and regression
coverage.

## Definition Of Done

This plan is complete when:

- Generic code contains no business/domain extraction vocabulary.
- All domain-specific aliases, labels, roles, derived fields, insight families,
  question templates, and presentation preferences come from project context.
- Ingestion automatically creates reviewable context proposals.
- Users can enrich context with resources and approve or reject proposals.
- Re-ingestion preserves reviewed context and flags drift.
- Insights and Explore produce useful business output for multiple domains
  without code changes.
- Architecture tests prevent business vocabulary from creeping back into generic
  runtime modules.
