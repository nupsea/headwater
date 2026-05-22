# Headwater Architecture

**Last updated:** May 22, 2026

## Runtime Shape

Headwater is a local application with four main layers:

```text
Next.js UI
  -> FastAPI API routes
    -> connector / discovery / profiling / context / generation services
      -> source systems, DuckDB analytical runtime, SQLite metadata store
```

## Core Boundary

Headwater is split between:

- generic runtime behavior
- project context

Generic runtime may infer structure, statistics, keys, relationships, generic
semantic types, drift, and quality risk. It may not hard-code business meaning.

Project context is where reviewed business meaning lives. That includes row
semantics, aliases, enum mappings, derived fields, insight families, business
lenses, question templates, visualization hints, advisor packs, and user-added
resources.

If a behavior needs industry nouns, business labels, metric definitions, or
project-specific phrasing, it belongs in project context rather than generic
code.

## Storage Responsibilities

| Store | Responsibility |
| --- | --- |
| Source systems | Organization data. Headwater must not mutate these. |
| DuckDB | Local analytical execution, bounded previews, generate-mode staging/mart validation. |
| SQLite metadata | Durable Headwater state: sources, discovery history, context items, decision logs, drift, contracts, activity. |
| Browser/UI state | Current view state only. Durable review actions must go through the API. |

The SQLite metadata store is the runtime authority. Files under
`metadata/<project>/` are projections for review, version control, and import,
not a second source of truth.

## Backend Modules

| Area | Main modules | Responsibility |
| --- | --- | --- |
| API app | `headwater/api` | FastAPI app and route handlers. |
| Connectors | `headwater/connectors` | Source-specific access, capability declaration, metadata/profile methods. |
| Profiling | `headwater/profiler` | Schema extraction, statistics, key and relationship candidates. |
| Analyzer | `headwater/analyzer` | Generic semantic enrichment, catalog generation, descriptions, domains. |
| Explorer | `headwater/explorer` | NL-to-SQL planning, SQL safety, question suggestions, decomposition. |
| Generator | `headwater/generator` | Staging, mart, and contract generation. |
| Quality | `headwater/quality` | Contract checks and quality reports. |
| Services | `headwater/services` | Context bootstrap/import/export/drift, source sync, rerun planning, model impacts. |
| Metadata | `headwater/core/metadata.py` | SQLite schema and durable persistence helpers. |

## User Workflow

The intended user-facing flow is:

```text
1. Ingest Source
2. Discovery Summary
3. Context Review
4. Business Insights
5. Explore / Ask
```

The detailed runtime flow behind that is:

```text
source registration
  -> discovery and profiling
  -> semantic type detection
  -> context bootstrap proposals
  -> resource enrichment
  -> prioritized review
  -> approved or locked project context
  -> analyzer / explorer / insights consume context
  -> drift-aware refresh
  -> projected metadata files and REVIEW.md
```

## Connector Modes

Headwater distinguishes two connector patterns:

- **Generate mode:** data can be loaded or sampled into DuckDB for local model
  generation and validation.
- **Observe mode:** data is not copied wholesale; profiling and evidence are
  gathered through source-side read-only queries.

Current capability flags include:

- `list_tables`
- `list_columns`
- `list_constraints`
- `estimate_row_count`
- `profile_table`
- `sample_arrow`
- `execute_readonly`
- `load_to_duckdb`
- `modes`

## Project Context Model

Canonical context items are stored in SQLite and exposed through
`ProjectContextProvider` in `headwater/services/project_context.py`.

Important item types include:

- `dataset_summary`
- `row_grain`
- `row_entity`
- `time_anchor`
- `pk_candidate`
- `fk_candidate`
- `column_semantics`
- `semantic_role`
- `derived_field`
- `relationship`
- `relationship_hint`
- `enum_mapping`
- `business_lens`
- `insight_family`
- `question_template`
- `visualization_hint`
- `column_policy`
- `project_alias`
- `source_alias`
- `table_alias`
- `advisor_pack`
- `resource`
- `open_question`

Each item must carry:

- stable `id`
- `project_id`
- optional source/table/column scope
- `status`
- bounded `confidence`
- `source`
- evidence records

## Context Files

Projected files live under `metadata/<project>/` and currently include:

- `context.yaml`
- `semantic_types.yaml`
- `semantic_schema.yaml`
- `derived_fields.yaml`
- `insight_families.yaml`
- `lookups.yaml`
- `glossary.yaml`
- `business_lenses.yaml`
- `presentation.yaml`
- `question_templates.yaml`
- `column_policies.yaml`
- `relationship_hints.yaml`
- `resources.yaml`
- `advisor_packs.yaml`
- `REVIEW.md`

Import and export are implemented in:

- `headwater/services/context_projection.py`
- `headwater/services/context_import.py`

## Evidence And Confidence

Every context proposal should be explainable through evidence. Producer classes
include structural profiling, declared constraints, semantic type detectors,
resources, imports, advisor packs, LLM outputs, and user decisions.

Confidence is bounded to `[0.0, 1.0]` and normalized in
`headwater/core/context_confidence.py`.

Current rules:

- user-reviewed decisions dominate machine proposals until drift reopens them
- declared constraints outrank heuristic guesses
- conflicting evidence lowers confidence
- repeated identical weak signals should not swamp stronger evidence

## Review Statuses

Project context items use these statuses:

- `proposed`
- `approved`
- `rejected`
- `locked`
- `needs_review`

Use them as follows:

- `proposed`: machine- or import-produced candidate
- `approved`: accepted and active
- `locked`: accepted and intended to remain stable across refreshes
- `rejected`: explicitly denied
- `needs_review`: previously accepted context invalidated by drift or conflict

Review history is append-only. Context decisions and reversions are stored in
the metadata decision log and surfaced through the project context history API.

## Drift Semantics

Drift is not just a boolean. Current drift handling in
`headwater/services/context_drift.py` can move impacted approved or locked items
back to `needs_review`.

Drift categories:

- schema drift
- distributional drift
- relationship drift
- semantic drift

Each drift event should identify:

- triggering rule
- affected items
- severity
- evidence
- review action

## Resource Classification And Redaction

User-provided project resources are classified before enrichment through
`headwater/services/resource_safety.py`.

Classification levels:

- `public`
- `internal`
- `sensitive`
- `unknown`

Rules:

- external LLM use must respect resource classification
- redaction runs before any external LLM call
- raw rows must not be sent to external LLM providers

## SQL Safety

Natural-language exploration must route through the SQL safety layer in
`headwater/explorer/sql_safety.py`.

Current safety expectations:

- read-only statements only
- scope queries to the active project/source/schema allowlist
- enforce row, time, byte, and execution limits
- block DDL, DML, multi-statement execution, unsafe functions, and unbounded
  cross joins
- persist planned SQL and safety decisions as evidence when relevant

## Advisor Packs

Advisor packs are reusable project-context bundles for vertical or repeated
domain content. They are represented as `advisor_pack` context items and
projected through `advisor_packs.yaml`.

Rules:

- packs are context, not generic runtime behavior
- pack provenance must remain visible in evidence
- project-specific review decisions take precedence over imported defaults
- explicit project-level `extends` declarations should round-trip through
  `advisor_packs.yaml`

## Offline, Replay, And Determinism

LLM-assisted enrichment must remain optional and testable.

Operational rules:

- prompts should be built deterministically from sorted, scoped, redacted input
- cached outputs must be replayable in CI without network access
- offline mode should disable live provider calls
- token budgets should be bounded per source and per run

Relevant modules:

- `headwater/analyzer/llm.py`
- `headwater/api/routes/settings.py`
- `headwater/services/context_resource_enrichment.py`

## How Assistants Should Read Context

Assistants and runtime consumers should not walk raw context items ad hoc when a
typed accessor exists. Use `ProjectContextProvider` to read:

- row grain and row entity
- time anchors
- key candidates and relationship hints
- aliases
- enum mappings and value labels
- low-signal columns
- preferred dimensions
- business lenses
- insight families
- question templates
- visualization hints
- derived fields
- advisor packs

## What Must Not Be Added To Generic Code

Do not add any of the following to generic runtime modules:

- hard-coded industry labels
- built-in enum translations tied to one dataset
- project-specific aliases
- business-specific insight categories
- derived metrics that require business definitions
- NL-to-SQL vocabulary that assumes one domain

If a new feature needs business wording to behave correctly, add a context item
type or extend project metadata instead of embedding that wording in Python or
TypeScript runtime logic.
