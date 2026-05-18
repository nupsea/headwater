# Project Context Grounding Plan

This plan turns ingestion into a deliberate project-context generation workflow.
The goal is to keep Headwater's core analyzer domain-agnostic while generating a
rich, reviewable context layer that agents, analysts, and deterministic services
can use before reasoning over data.

The design follows the grounding pattern described in Starburst's "Agent
Grounding: The Missing Discipline in Enterprise AI": metadata must be harvested
from live infrastructure, structured into business semantics, assembled into a
scoped package, and delivered to agents before they answer questions.

## Principles

- Core analyzer logic remains structural: dtypes, profiles, uniqueness,
  cardinality, relationships, locks, and documented resources.
- Domain vocabulary lives in project context, not analyzer code.
- Ingestion creates proposals; humans certify or reject them.
- Runtime services consume one canonical context model, not separate YAML,
  Markdown, UI, and assistant states.
- YAML files are portable import/export views. Markdown is generated review
  documentation. The metadata store is authoritative at runtime.
- Every context item carries stable identity, confidence, status, provenance,
  evidence, and drift state.

## Target Workflow

```text
source ingest
-> physical discovery and profiling
-> metadata bootstrap proposals
-> context store write
-> analyzer/catalog/explorer consume context
-> user review and certification
-> scoped context assembly for assistant/agent use
-> drift-aware refresh on future ingestions
```

## Canonical Context Model

Add a canonical project context model to the metadata store. Each record should
include:

- `id`
- `project_id`
- `source_name`
- `item_type`
- `payload_json`
- `status`: `proposed`, `approved`, `rejected`, `locked`, `stale`, `needs_review`
- `confidence`
- `source`: `profile`, `relationship`, `resource`, `embedding`, `llm`, `human`
- `evidence_json`
- `created_at`, `updated_at`, `reviewed_at`

Initial item types:

- physical assets: tables, columns, profiles, freshness, PII risk
- semantic types: metric, dimension, id, temporal, text, lookup key
- semantic roles: lifecycle start/end, request timestamp, canonical measures
- entity mappings: equivalent identifiers across tables/sources
- ontology domains: bounded business areas and their assets
- metrics: executable definitions, lineage, validation status
- lookups and enums: id-to-label maps and value dictionaries
- glossary terms: definitions, variants, disambiguation rules
- business rules: filters, caveats, severity, protected metrics
- resources: uploaded docs, URLs, dbt manifests, BI exports, notes
- open questions: low-confidence assumptions needing review

## Phase 1: Store And Context API

Create the canonical context storage and access layer.

- Add metadata-store tables for context items, evidence, resources, and review
  events.
- Add CRUD helpers in `core/metadata.py`.
- Add `ProjectContextProvider` to assemble approved/proposed context for a
  project.
- Add `GET /api/projects/{project_id}/context`.
- Add import/export services for:
  - `context.yaml`
  - `semantic_types.yaml`
  - `semantic_schema.yaml`
  - `catalog.yaml`
  - `lookups.yaml`
  - `glossary.yaml`
  - `resources.yaml`
- Keep `metadata/<project>` YAML support as projection/import-export, not the
  long-term runtime authority.

## Phase 2: Metadata Bootstrap During Ingestion

Insert `MetadataBootstrapService` after discovery/profile/relationship detection
and before analyzer/catalog generation.

Bootstrap should generate proposals for:

- structural types from profiles and dtypes
- lookup tables and enum mappings
- candidate primary and foreign keys
- entity equivalence candidates using name, relationship, and value overlap
- semantic type rules derived from observed schema clusters
- semantic role rules derived from lifecycle-like column groups, but stored only
  as project metadata
- catalog hints such as aggregation choices, metric labels, and synonyms
- glossary candidates from companion docs and resources
- open questions for ambiguous/high-impact assumptions

The service must be idempotent:

- preserve approved and locked records
- update proposed records when evidence changes
- mark invalidated records as `stale` or `needs_review`
- avoid overwriting human edits

## Phase 3: Resource Enrichment

Make resources a first-class way to enrich context.

Supported resources:

- CSV data dictionaries
- Markdown/text notes
- PDFs
- URLs
- dbt `manifest.json` and catalog artifacts
- BI metric exports
- governance glossaries

Resource processing should:

- extract glossary terms
- map docs to tables/columns
- extract enum/code descriptions
- propose metric definitions and business rules
- attach evidence to generated context items
- add review questions where confidence is low

Use deterministic extractors first. Add local embeddings or clustering where it
reduces manual curation, especially for grouping columns, matching resource
terms to schema, and identifying entity-equivalence candidates.

## Phase 4: Analyzer And Catalog Refactor

Make all analyzer services consume canonical context through
`ProjectContextProvider`.

- `analyzer/heuristics.py`: structural/profile inference only.
- `analyzer/semantic_schema.py`: consume semantic role context; no domain
  vocabulary.
- `analyzer/catalog.py`: consume catalog hints, synonyms, metric definitions,
  and aggregation choices from context.
- `analyzer/metadata_retrieval.py`: merge companion docs, resources, lookups,
  and approved/proposed context.
- `explorer/*` and insights routes: pass `project_id` through every context
  lookup.

Approved/locked context should override generated inference. Proposed context
can be used with confidence/caveat propagation.

## Phase 5: Review API And UI

Expose review as a workflow rather than raw files.

API endpoints:

- `GET /api/projects/{project_id}/context`
- `GET /api/projects/{project_id}/context/review`
- `PATCH /api/projects/{project_id}/context/items/{item_id}`
- `POST /api/projects/{project_id}/context/items/{item_id}/approve`
- `POST /api/projects/{project_id}/context/items/{item_id}/reject`
- `POST /api/projects/{project_id}/context/items/{item_id}/lock`
- `POST /api/projects/{project_id}/context/resources`
- `POST /api/projects/{project_id}/context/bootstrap`
- `POST /api/projects/{project_id}/context/export`
- `POST /api/projects/{project_id}/context/import`

UI sections:

- project framing
- physical assets
- semantic types
- semantic roles
- entity resolution
- metrics and catalog
- lookups and enums
- glossary
- business rules
- resources
- open questions
- drift and stale context

Review should support bulk approval for high-confidence proposals and detailed
editing for medium/low-confidence proposals.

## Phase 6: Scoped Agent Context

Add a scoped context assembly service for assistant/agent use.

The service should support:

- lightweight index mode: domain/entity summaries kept in the assistant prompt
- scoped retrieval: `get_project_context(project_id, domains, entities, terms)`
- bounded context packages sized for model context windows
- confidence-ranked tables, joins, metrics, rules, and glossary entries
- explicit caveats for proposed or stale context

The assistant should not search raw metadata during reasoning. It should receive
one assembled context package with the relevant physical assets, semantic roles,
verified joins, metric SQL, glossary definitions, and business rules.

## Phase 7: Drift And Continuous Maintenance

On each ingestion:

- compare schema/profile changes against context items
- mark impacted records stale
- propose new replacements
- keep approved and locked records intact
- produce a context drift report
- regenerate YAML and Markdown projections

Important drift cases:

- missing column used by approved metric or semantic role
- dtype changed for key/measure/timestamp fields
- lookup cardinality changed substantially
- relationship confidence dropped
- resource changed or disappeared
- metric SQL no longer validates

## Runtime Projections

The canonical metadata store is runtime authority.

Generated projections:

- YAML: machine-readable import/export and version control
- Markdown: human review summary
- API JSON: assistant/analyzer runtime context
- UI: review and certification workflow

Do not maintain independent state in YAML, Markdown, and UI. They are views over
the same canonical records.

## Success Criteria

- Adding a source produces project context proposals automatically.
- Core analyzer contains no domain/dataset extraction vocabulary.
- Assistant receives scoped context before answering data questions.
- Users can approve, reject, edit, lock, and enrich generated context.
- Re-ingestion preserves approved context and flags drift.
- Existing exploration and insight generation improve through richer context
  without reintroducing hardcoded domain heuristics.
