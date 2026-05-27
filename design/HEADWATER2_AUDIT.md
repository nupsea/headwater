# Headwater 2 — Code Audit (keep / mine / cut)

Status: Draft. Companion to `HEADWATER2_VISION.md` and `HEADWATER2_PERSONAS.md`.
Audited 2026-05-27 against the H1 tree.

Verdict: strangler, not rewrite. The spine is healthy and contains two buried kernels
worth real money. The salvage is concentrated and clear.

## KEEP wholesale — the spine (~5K LOC, clean)

| Module | What it gives H2 | Maps to |
|---|---|---|
| `connectors/*` | CSV, JSON, SQLite, MySQL, Postgres, Redshift, Snowflake, DuckDB loaders + `base`/`capabilities`/`registry`/`schema_filter` | Shared source catalog. The engine-agnostic discipline is already half-built in `base`/`capabilities`. |
| `profiler/*` | Schema extraction, numeric/string/temporal column stats, pattern detection, PK/FK/composite-key detection, referential-integrity checks, cardinality inference (~1.1K LOC) | Explore + Model structure discovery |
| `quality/*` | Contract checks: not-null, unique, range, cardinality, row-count, count-expression | Data-quality half of Evaluate (extend) |
| `generator/*` | Jinja2 SQL templates, staging render, `MartCandidate`/`PatternMatcher` | Portable export base (extend to dbt) |
| `executor/*` | DuckDB model runner | Answer & Share run capability |

## MINE — extract the generic kernel, drop the domain coupling

| Source | Extract (gold) | Drop (pollution) |
|---|---|---|
| `explorer/statistical.py` | Highest-value asset in the repo. MAD z-score, period/seasonality detection (`_detect_period`/`_deseasonalize`), change-point detection, FDR multiple-testing correction, winsorize, normality, correlations, temporal anomalies | Hardcoded domain "families": `_geo_/_route_/_congestion_/_wait_/_peak_/_duration_family` — the taxi/transport hardcoding that domain-locked H1. Keep the math, strip the families. |
| `analyzer/semantic_types.py` | Evidence-based semantic typing (`SemanticTypeEvidence`, value-shape + range detection, currency/IBAN validation, conflict dedup) — nearly whole | already clean |
| `analyzer/heuristics.py` | `classify_semantic_type_with_confidence`, temporal-grain inference, sibling-consistency checks, name humanization, domain clustering | `_context_*_override` coupling to the H1 context suite |
| `analyzer/semantic.py` | LLM orchestration + redaction pattern (`_redact_prompt_stat_value`) enforcing invariant I-3 (never send raw rows to an LLM) | H1 companion/context wiring around it |

The two kernels — statistical EDA and semantic typing — are what make Explore and
Evaluate *deep*. Without them, months of stats work get rebuilt.

## CUT — do not carry forward (reference on disk only)

| Module | Size | Why |
|---|---|---|
| `explorer/nl_to_sql.py`, `query_planner.py`, `decomposition.py` | ~8.9K LOC | NL-to-SQL — out of scope per vision |
| `explorer/suggestions.py` | 94KB | H1 question/insight surface; superseded by goal-anchored relevance |
| `services/context_*` suite | ~3.8K LOC | v2/v3 context machinery; replaced by shared-source + project-scope model |
| `drift/`, `sync_*`, `schema_snapshots`, `drift_reports` | — | Over-built; "living verdict" monitoring is simpler — mine only the snapshot-diff idea |
| `core/metadata.py` | 149KB | God file with duplicate `CREATE TABLE` statements — rebuild (below) |

## Metadata schema: 47 tables (with duplicates) -> ~14

A tell: the H1 `CREATE TABLE` list redefines several tables (`quality_runs`,
`project_context_*`, `model_reviews`, `evidence_records`, `warehouse_insight_plans`
appear twice). H2's schema, project-centric:

- **Shared / source-owned:** `sources`, `discovery_runs`, `tables`, `columns`,
  `profiles`, `relationships`
- **Project-owned:** `projects`, `project_sources` (M:N link carrying per-goal scoping),
  `semantic_definitions` (metrics/dimensions/entities + locks; folds in `catalog_*`),
  `readiness_verdicts`, `gaps`, `models`, `contracts` + `quality_runs/results`,
  `execution_results`, `decisions`
- **Drop:** `project_context_*` cluster, `dataset_contexts`, `companion_docs`,
  `context_feedback_events`, `warehouse_insight_plans`, `evidence_records`,
  `activity_log`, `model_answers`, `model_reviews`, `model_impacts`, `sync_*`,
  `schema_snapshots`, `drift_reports` (keep a lean LLM-audit row only if I-3 needs it)

`core/models.py` (46 classes): keep ~18 (SourceConfig, ColumnInfo/TableInfo,
ColumnProfile, Relationship, GeneratedModel, ContractRule, QualityReport, Project,
Metric/Dimension/EntityDefinition, SemanticCatalog, StatisticalInsight,
VisualizationSpec, ProjectProgress, ExecutionResult ...), mine ~5 semantic, cut the ~23
context/dictionary/explorer DTOs.

## Net

H2 reuses the entire spine plus two mined kernels, rebuilds only the metadata schema
(small) and the project/relevance/readiness/answer layers around the problem-centric
model. The ~12.7K LOC of explorer + context stays on disk untouched as reference; two
files get mined. The hard parts — connectors, profiling, PK/FK, the stats engine,
semantic typing, LLM redaction — are already done.
