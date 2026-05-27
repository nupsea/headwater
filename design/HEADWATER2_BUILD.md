# Headwater 2 — Build Layout & First Slice

Status: Draft. Companion to `HEADWATER2_VISION.md`, `_PERSONAS.md`, `_AUDIT.md`.
Deliberately NOT a phase plan. One vertical slice, then we learn.

## Package layout (strangler inside the existing package)

Keep the spine packages where they are; add project-centric modules alongside; delete
the cut modules progressively as the new path subsumes them.

```
headwater/headwater/
  core/
    config.py        KEEP
    models.py        TRIM -> ~18 kept + ~5 mined semantic models
    store.py         NEW  small project-centric SQLite schema (~14 tables); replaces metadata.py (149KB)
  connectors/        KEEP  shared source catalog (CSV/JSON/SQL/warehouses)
  profiler/          KEEP  structure, stats, PK/FK/grain
  eda/               NEW   MINED from explorer/statistical.py -- anomaly/seasonality/change-point/correlation kernel (no domain families)
  semantics/         NEW   MINED from analyzer/semantic_types.py + heuristics.py -- typing + definitions + locks
  llm/               NEW   MINED from analyzer/semantic.py -- orchestration + I-3 redaction
  project/           NEW   Project entity, project_sources (M:N + per-goal scoping), goal
  relevance/         NEW   goal -> relevant tables/columns (the relevance function)
  readiness/         NEW   the trust verdict: quality + definitions + lineage + gaps (graded)
  generator/         KEEP  SQL templates (extend)
  export/            NEW   audit report (md/pdf) FIRST, then dbt emit; reuses generator
  executor/          KEEP  DuckDB run
  answer/            NEW   query draft + chart specs + dashboard (Stage 4, gated)
  quality/           KEEP  contract checks (extend)
  api/ , cli/        NEW   thin entry points; old routes cut progressively

  # left on disk as reference, untouched, deleted once mined:
  explorer/  services/context_*  drift/
```

Import direction (unchanged invariant I-9):
core -> connectors -> profiler -> {eda, semantics, llm} -> {project, relevance,
readiness} -> generator -> executor -> export/answer -> api/cli

## The first vertical slice — prove ONE thing

One real business goal + 3-5 real sources, end to end, to a single artifact.

Flow:
```
goal + sources
  -> connectors (ingest, shared)
  -> profiler (stats, PK/FK, grain)
  -> relevance (scope to the goal)
  -> eda (anomalies/seasonality/segmentation on the relevant slice)
  -> semantics (types + definitions)
  -> readiness (graded trust verdict + gaps)
  -> export (Markdown audit report)
```

The one artifact: a **goal-anchored data-readiness audit report** — what you have, what's
trustworthy, what's broken, what's missing, what's misleading — carrying the graded
trust badge. This is simultaneously the consultant's engagement deliverable and the
Stage-4 trust credential.

Out of the slice (deliberately deferred): query editing, charts, dashboards (Stage 4
interactive), dbt export, the live-monitoring loop, the UI. Slice can be CLI-first.

## Definition of done

- Runs end to end on one real problem from a single command.
- The audit report is something a real analyst/consultant reads and says "that just
  saved me a week."
- If yes: build outward (Stage 4 interactive, dbt export, UI). If no: stop and rethink
  the wedge before building more.

## Test bed (resolved 2026-05-27)

Two real open sources, three candidate projects -- the radiology source feeding two
projects is the many-to-many proof (shared profiled data, two goals, two verdicts).

| Source | Location | License | Project(s) |
|---|---|---|---|
| MovieLens `ml-latest-small` (ratings/movies/tags/links) | `data/media/` (gitignored) | No redistribution | Viewer engagement / retention |
| Radiology workflow (`cases`/`exams`/`events`.csv) | `data/radiology/` (committable) | CC BY 4.0 (see ATTRIBUTION.md) | (1) Patient registration workflow optimization; (2) Device/modality utilization |

Tested NAKED: provided docs (`dataset_details.txt`, `intended_uses.txt`) are withheld to
`data/_answer_key/` as a grading key, never pipeline input. The tool must infer from raw
CSVs. Blind read confirmed: ~80% inferable (grain, relationships, workflow sequence,
intra-day arrival peak); genuine human-confirm points are few and specific (patient_type
codes A/D/H/S, activity semantics, registration boundary, `events.modality=None`
meaning); detectable issues (US vs UltraSound vocab split, `ExaminacionExtra?` artifact,
two duration formats).

Built-in wedge demos in the data:
- MovieLens records *ratings*, not *watches* -> readiness flags the watched-vs-rated
  definition gap.
- Radiology: inconsistent duration formats (`00:00` vs `0 days 00:22:00`), `None`
  modality, mixed-language activities (`Llegada_Pacientes_H`), undefined `patient_type`
  code `H`, redundant derived columns -> rich quality + definition findings.

## First slice target

Build the pipeline on the **radiology source, project (1) patient registration workflow
optimization** to the Markdown audit report. Then add project (2) device utilization on
the SAME already-profiled source -- the cheapest possible demonstration of many-to-many
(no re-ingest, only the goal-scoped relevance/verdict layer differs). MovieLens follows
as the cross-domain check.

## Staged build plan (verifiable + reviewable at every stage)

> GATED: implementation (S0+) does NOT start until the renewed architecture, design, and
> guided-workflow/UX flows are vetted. H1's core failure was workflow + UX overload, not
> engines. The guided workflow and UX flows are first-class deliverables that must be
> approved first. Data is tested NAKED -- provided docs are withheld as a grading key.

Branch: `feat/headwater2-slice`. One commit per stage. A stage is not "done" until its
verification passes.

**Verification protocol (every stage):**
1. Quality gate, in order (invariant I-8): `uv run ruff check . && uv run ruff format
   --check . && uv run pytest && uv run pyright` -- all green.
2. E2E on real data: stages touching the pipeline run against the actual
   `data/radiology/*.csv` and assert known-true facts (no mocks; in-memory SQLite +
   temp DuckDB per python-patterns).
3. Reviewable changeset: one focused commit; the verification command output goes in the
   commit body. The diff is the review unit.

### Inputs model: the Project Spec

Every project is a declarative, editable, reviewable spec (YAML) capturing the two
first-class inputs:

- **goal** (required, structured): statement, decision to make, outcome metrics,
  entities of interest, time horizon. This is what the relevance engine consumes.
  May be LLM-drafted from a one-line statement, then confirmed (non-blocking).
- **resources** (optional, "any other inputs"): glossary / data dictionary / domain
  notes / definitions. These feed semantic typing, relevance, become semantic locks
  (I-6), and serve as ground truth the readiness verdict checks gaps against.

Ordering consequence: cheap semantic typing runs on all columns BEFORE relevance (so
relevance matches meaning, not just names); goal interpretation drives relevance.

### Stages

| Stage | Goal | Key changes | Verify (observable) | Review artifact |
|---|---|---|---|---|
| S0 | Scaffolding + guardrails | branch; `hw2` CLI entrypoint stub; new module dirs (`project/relevance/readiness/eda/semantics/export`); test harness | `uv run hw2 --help` works; smoke pytest green; ruff clean | dir structure diff |
| S1 | Metadata store | `core/store.py` small project-centric schema (~14 tables incl. `projects`, `project_goal`, `project_resources`, `semantic_definitions`); trim `core/models.py` | unit test: create schema in in-memory SQLite, round-trip a Project + goal + source | schema DDL + models diff |
| S2 | Project Spec + goal capture | project spec loader/validator (Pydantic); CLI `hw2 project new <name> --source <s> --goal "..."` scaffolds an editable spec; structured goal persisted | `hw2 project new`; spec file created; goal block parsed + stored; invalid spec rejected with clear error | the spec file + parsed goal |
| S3 | Ingestion | wire connectors spine; ingest radiology CSVs to DuckDB; register source/tables/columns | `hw2 source add data/radiology`; ingested row counts == CSV line counts; 3 tables registered | CLI transcript + counts |
| S4 | Profiling | reuse profiler spine; persist stats, PK/FK, grain | assert PK `case_id`; FK `exams.case_id`->`cases`; `events.modality` `None` flagged; modality cardinality | profile output |
| S5 | Light semantic typing | mine `analyzer/semantic_types.py` (cheap typing over ALL columns); name normalization | types timestamps / durations / ids / codes across all 3 tables | typed columns |
| S6 | Resource intake (optional inputs) | `hw2 project add-resource <file>`; parse glossary/notes/definitions into hints + locks | a definition (e.g. `patient_type H`) and a metric definition are ingested and stored as ground truth | parsed resources/locks |
| S7 | Goal interpretation + relevance | structure goal -> match to typed columns + resources -> scope; non-blocking confirm | for "registration workflow": selects `events.activity/timestamps`, `cases.wait/throughput/patient_type`; deprioritizes `exams.scan_time_duration`; rationale cites goal terms | relevance + rationale |
| S8 | EDA depth (mined) | mine kernel from `explorer/statistical.py` (anomaly/period/change-point), drop domain "families"; run on the relevant slice | detects arrival-by-hour peak and wait-time anomalies | insights list |
| S9 | Deep semantics + definition gaps | enrich relevant slice; reconcile against resource definitions; surface gaps | flags inconsistent duration formats (`00:00` vs `0 days 00:22:00`), undefined codes not covered by resources, mixed-language activities | semantic findings |
| S10 | Readiness verdict | combine quality + definitions + relationships + gaps -> graded verdict, scoped to goal | verdict: have / trustworthy / broken / missing / misleading + gaps vs the goal's stated metrics | verdict object |
| S11 | Export: Markdown audit report | render verdict to report with graded trust badge + provenance | `hw2 report` emits Markdown; golden test asserts key findings + badge + goal restated | the rendered report (the deliverable) |
| S12 | Many-to-many proof | add project (2) device utilization (new spec/goal) on the SAME profiled source | second report; relevance now selects `exams.modality/scan_time`; assert profile + typing reused (no re-ingest/re-profile), output differs by goal | two reports side by side |

After S12: cross-domain check on MovieLens (new spec, no engine changes) confirms the
engine is domain-agnostic. Then decide on Stage-4 interactive (query/charts/dashboard),
dbt export, and UI -- only if the audit report passes the "saved me a week" test.
