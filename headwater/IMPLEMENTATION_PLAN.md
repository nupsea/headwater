# Headwater Implementation Plan

**Date:** April 30, 2026  
**Status:** Active concise roadmap  
**Archived predecessor:** `archives/IMPLEMENTATION_PLAN_2026-04-30_pre_docs_reset.md`

This file is now the active, short-form implementation plan. Historical detail
has been archived so day-to-day planning stays aligned with the current product
shape.

## Current Objective

Make Headwater credible on real organizational data across maturity levels:

```text
files / OLTP / OLAP -> governed discovery -> reviewable semantics -> safe insights
```

The near-term goal is not connector breadth for its own sake. The goal is to
prove that Headwater can connect to real sources, capture available metadata,
profile large tables safely, ask users to confirm ambiguous semantics, and
produce useful business insights.

## Completed Baseline

- Source catalog and sync event backbone.
- Connector capability model and support status catalog.
- OLTP/OLAP source evaluation layer with readiness, maturity mode, capability
  gaps, and safe profiling policy recommendations exposed through the API and
  Sources page.
- Supported local/file connectors: CSV, JSON, DuckDB, SQLite.
- Supported Postgres connector for table/column discovery, pushdown profiling,
  bounded sampling, and read-only validation.
- Preview MySQL connector.
- Preview Snowflake connector for warehouse metadata, bounded profiles,
  row-limited samples, row estimates, and read-only validation.
- Discover & Access merged with dictionary review.
- Persistent PK decisions across rediscovery.
- Source-scoped state reset on disconnect.
- Data & Query preview fallback for staging/source table names.
- Smart quote normalization for pasted SQL.
- Business-oriented Insights page with diversified visual patterns.
- Explore initial load fixes.

## Open Product Stories

### Story A: Postgres OLTP Constraint Import

**User story:** As a data engineer connecting an existing Postgres OLTP system,
I need Headwater to import declared PKs, FKs, unique constraints, checks, and
comments so it starts from system-of-record metadata rather than heuristics.

**Implementation:**

- Add connector contract methods for `list_constraints`, `list_indexes`, and
  `list_comments`.
- Implement Postgres catalog introspection from `pg_catalog` and
  `information_schema`.
- Mark declared PK/FK evidence as high confidence.
- Preserve user overrides over imported constraints.
- Surface imported-vs-inferred-vs-user-confirmed provenance in Discover & Access.

**Acceptance criteria:**

- Declared Postgres PKs and FKs appear immediately after sync.
- Imported constraints survive refresh and rediscovery.
- User rejections/overrides take precedence until source reset.
- Tests cover composite PKs and multi-column FKs.

### Story B: Manual FK Review and Editing

**User story:** As a reviewer, I need to manually add, edit, or remove FK
relationships when they are missing from source DDL or inferred incorrectly.

**Implementation:**

- Add FK editor controls in Discover & Access.
- Allow selecting source column, target table, and target column.
- Persist manual relationships through the metadata store.
- Refresh Insights and relationship views after save.
- Track relationship provenance and audit decisions.

**Acceptance criteria:**

- Users can add `table.column -> table.column` relationships without suggestions.
- Manual FK decisions are visible in Keys & Relationships immediately.
- Manual decisions persist across rediscovery.

### Story C: AWS Redshift OLAP Connector

**User story:** As an analytics engineer with Redshift, I need Headwater to
observe warehouse schemas, metadata, and aggregate profiles without copying
large fact tables locally.

**Implementation:**

- Add Redshift connector with observe mode.
- Read schemas/tables/views/columns from Redshift catalog views.
- Import table and column comments where available.
- Use bounded aggregate profiling with configurable row/table limits.
- Support read-only validation SQL with statement timeout.
- Treat PK/FK declarations as informational, not necessarily enforced.

**Acceptance criteria:**

- A Redshift schema can be discovered without full table transfer.
- Large tables are profiled through aggregates or samples only.
- Connector reports profiling limits and skipped tables clearly.

### Story D: AWS Athena and Glue Catalog Connector

**User story:** As a team using S3 data lakes through Athena, I need Headwater
to discover Glue catalog metadata and run safe aggregate queries for insights.

**Implementation:**

- Add Athena connector with Glue catalog discovery.
- Read databases, tables, partitions, columns, comments, and table properties.
- Use Athena query execution for aggregate profiling with output-location config.
- Respect partition filters and maximum scanned bytes.
- Capture Iceberg/Hive table metadata where exposed through Glue/Athena.

**Acceptance criteria:**

- Glue catalog tables appear in Discover & Access.
- Partitioned tables show partition columns and freshness hints.
- Profiling can be limited by partition/time filters and scan budget.

### Story E: Smart Large-Table Profiling Policy

**User story:** As an operator connecting large or multiple sources, I need
Headwater to fetch metadata and aggregate summaries intelligently instead of
pulling raw data or issuing unsafe full scans.

**Implementation:**

- Add source-level profiling policy:
  - max tables
  - max columns
  - max sample rows
  - max scanned bytes or estimated rows
  - preferred time/partition filter
  - aggregate-only mode
- Add connector capability fields for cost controls and estimate support.
- Store profiling coverage and skipped reasons.
- Generate insights from aggregate profiles and sketches where raw samples are
  unavailable.
- Make UI show whether insight evidence came from full profile, sample, or
  aggregate sketch.

**Acceptance criteria:**

- Large tables are never copied by default.
- Users can see what was profiled, sampled, skipped, or estimated.
- Insights remain explainable with evidence coverage.

### Story F: Data Ecosystem Maturity Modes

**User story:** As a data lead, I need Headwater to adapt to whether my org has
raw OLTP, a basic warehouse, or a mature semantic layer.

**Status:** Partially implemented. Headwater now classifies connector/source
evaluation as files, OLTP, or OLAP and reports maturity mode from capability and
observed source state. Deeper onboarding questions and semantic-layer detection
remain open.

**Implementation:**

- Add source maturity classification:
  - raw files
  - OLTP with constraints
  - warehouse with marts
  - warehouse plus dbt/semantic layer
  - governed catalog/lineage ecosystem
- Tailor onboarding questions and evidence collection by maturity.
- Prefer declared metadata where mature, heuristics where metadata is absent.

**Acceptance criteria:**

- Headwater explains which evidence sources it used.
- Review queue changes based on maturity gaps.
- Business insights prioritize trusted marts when available.

### Story G: Cost-Aware Warehouse Insight Engine

**User story:** As a data lead in a mature Snowflake/OLAP environment, I need
Headwater to generate useful insights by running safe, evidence-backed warehouse
queries directly against trusted marts and semantic models, without creating
unbounded compute cost or presenting weak samples as truth.

**Product direction:**

Headwater should not treat mature OLAP systems like raw files. For warehouses,
it should become a data product auditor and insight evidence planner:

- Prefer governed marts, dbt models, semantic-layer metrics, and documented
  dimensions over raw fact tables.
- Generate insight candidates from metadata, model lineage, query history,
  freshness, quality checks, and semantic definitions before querying data.
- Run direct Snowflake aggregate queries only when the evidence planner can bound
  cost and explain why the query is needed.
- Label every insight by evidence type: `metadata_only`, `aggregate_pushdown`,
  `stratified_sample`, `full_scan`, or `semantic_metric`.
- Attach confidence, coverage, scan budget, and reproducibility metadata to each
  generated insight.

**Implementation:**

- Add an insight planning stage before execution:
  - classify source maturity and trusted model layer
  - choose candidate tables/models/metrics
  - estimate table size and cost risk
  - choose full aggregate, approximate aggregate, stratified sample, or skip
  - require a configured compute/scan budget
- For Snowflake, use warehouse-native queries for:
  - aggregate metric summaries
  - time-window comparisons
  - freshness and volume drift
  - null/uniqueness/reference checks
  - top contributors and segment outliers
  - approximate distincts/quantiles where available
- Prefer partition/time-window predicates for fact tables.
- Use deterministic sampling only when aggregate evidence is insufficient.
- Store generated SQL, query purpose, estimated cost tier, rows scanned where
  available, and result confidence.
- Surface “why this query was safe to run” beside each insight.
- Refuse or downgrade insights when confidence, coverage, or budget constraints
  are not met.

**Acceptance criteria:**

- Snowflake insight generation can run without copying source tables locally.
- A max warehouse-query budget prevents unbounded scans by default.
- Each insight includes evidence type, confidence, coverage, and generated SQL.
- Mature semantic models are preferred over raw tables when available.
- Expensive candidates are skipped with clear reasons rather than silently run.
- Sample-derived insights are visibly labeled as directional, not authoritative.

## Verification Gate for Next Milestone

- Postgres constraint import tested against a real or containerized Postgres DB.
- Manual FK editing works end-to-end in Discover & Access.
- At least one AWS OLAP connector story is implemented behind preview status.
- Large-table profiling policy is visible in source sync results.
- Snowflake direct insight queries are mediated by a cost-aware evidence planner
  and produce evidence labels/confidence metadata.
- README, architecture, and progress docs remain current.

## Next Wave Implementation and Evaluation Plan

The next wave should be evaluated as a mature-data-system product, not as a
demo pipeline. The core question is: can Headwater inspect an existing governed
warehouse, decide what is safe to query, run only bounded evidence-gathering
queries, and produce trustworthy recommendations with clear cost/confidence
metadata?

### Wave 1: Evidence and Cost Control Foundation

**Status:** Started. Headwater now persists warehouse insight plans and evidence
records, and exposes `/api/sources/{source_name}/insight-plan/dry-run`,
`/api/evidence`, `/api/warehouse-insight-plans`, and
`/api/warehouse-insight-plans/{plan_id}/execute`. The first planner produces
metadata-only and aggregate-pushdown candidates, applies conservative
row-count-based cost gates, persists skipped reasons, and executes only after
explicit approval through connector read-only hooks. Timeout and query-id
capture are now in place; live Snowflake validation and warehouse-side cost
capture remain for the next slice.

**Implementation steps:**

- Add persisted evidence records for discovery/profile/insight outputs:
  - `evidence_type`: `metadata_only`, `aggregate_pushdown`,
    `stratified_sample`, `full_scan`, `semantic_metric`
  - `source_type`, `source_name`, `table_name`, `model_name`, `metric_name`
  - generated SQL or metadata query
  - query purpose
  - row/table coverage
  - sample design and seed when applicable
  - estimated and observed cost tier
  - skipped/refused reason
  - confidence score and confidence rationale
- Add a source-level warehouse budget model:
  - max query count per run
  - max tables/models considered
  - max rows sampled
  - max estimated bytes/credits tier where available
  - max runtime
  - allowed warehouses/roles
  - required time-window predicate for large fact tables
- Extend connector capabilities to report:
  - row-count estimate support
  - bytes/partition estimate support
  - query history support
  - table statistics support
  - semantic-model/dbt artifact support
  - warehouse query tagging support
- Add a planner dry-run mode that produces the proposed evidence plan without
  executing warehouse queries.

**Evaluation steps:**

- Unit tests prove expensive candidates are refused when budgets are exceeded.
- Unit tests prove every planned query has a purpose, evidence type, and budget
  classification.
- API tests prove dry-run plans can be reviewed before execution.
- Golden fixtures cover small, medium, and large table metadata with different
  allowed budgets.
- UI tests confirm skipped/refused work is visible and understandable.

**Done when:**

- No direct insight query can run without a budget context.
- Every evidence-producing query is auditable from persisted metadata.
- The UI can show “what Headwater did not query and why.”

### Wave 2: Snowflake Mature Warehouse Discovery

**Implementation steps:**

- Add live-tested Snowflake metadata import:
  - databases/schemas/tables/views
  - columns and nullable/type metadata
  - table/column comments
  - row-count estimates
  - clustering/partition hints where available
  - freshness signals from table metadata
  - query history where allowed
- Add Snowflake query tagging for Headwater-generated queries.
- Add optional dbt artifact ingestion:
  - `manifest.json`
  - `catalog.json`
  - exposures
  - tests
  - model descriptions
  - metric/semantic definitions where present
- Rank trusted model layers:
  - semantic metrics/dbt semantic layer
  - curated marts
  - intermediate models
  - staging/raw tables

**Evaluation steps:**

- Integration test against a real or ephemeral Snowflake account with:
  - tiny dimension table
  - large fact table
  - documented mart
  - missing comments
  - stale table
  - duplicate/ambiguous metric definition
- Verify no full fact-table copy occurs.
- Verify query tags appear in Snowflake query history.
- Verify row-count/table metadata is imported without scanning table data.
- Verify Headwater prefers marts over raw tables for insight planning.
- Verify missing comments/tests/freshness issues appear as model improvement
  recommendations.

**Done when:**

- Snowflake can be promoted from preview to supported for metadata-first
  discovery after live integration coverage.
- Headwater can produce a useful warehouse fitness report without sampling any
  fact table rows.

### Wave 3: Cost-Aware Insight Planner

**Implementation steps:**

- Build insight candidates before execution:
  - metric over time
  - segment concentration
  - freshness/volume drift
  - null/uniqueness drift
  - referential integrity risk
  - top contributor/outlier segment
  - model usage/cost anomaly from query history
- Score each candidate by:
  - business relevance
  - trusted-model availability
  - estimated query cost
  - expected statistical power
  - freshness and coverage
  - novelty versus previous runs
- Select execution strategy:
  - metadata-only recommendation
  - aggregate pushdown
  - approximate aggregate
  - stratified sample
  - refuse/skip
- For samples, use statistically explicit designs:
  - stratify by time window, high-cardinality business segment, status, and
    known dimensions
  - oversample rare-but-important classes when detectable
  - preserve deterministic seeds
  - calculate confidence intervals and minimum detectable effect where possible
- Run Snowflake insight queries directly only through the planner.

**Evaluation steps:**

- Golden insight plans assert that:
  - raw fact tables are avoided when trusted marts exist
  - aggregate pushdown is preferred over row sampling
  - large unpartitioned tables require a time predicate or are skipped
  - sample-derived insights include directional labels and confidence intervals
  - low-confidence candidates are downgraded or refused
- Statistical tests on synthetic data verify:
  - stratified sampling recovers known segment effects better than naive random
    sampling under the same row budget
  - rare-event scenarios are not overclaimed
  - confidence intervals widen correctly when coverage is weak
- Regression tests ensure generated SQL stays read-only and bounded.
- Snapshot tests verify each insight includes evidence type, confidence,
  coverage, generated SQL, and skip/refusal reasons.

**Done when:**

- Insights from mature warehouses are not presented unless they have adequate
  evidence.
- Sample-based insights are clearly directional and never mixed with
  full-coverage aggregate claims.
- The product can answer “why should I trust this insight?” and “what did it
  cost to produce?”

### Wave 4: Data Product and Platform Improvement Recommendations

**Implementation steps:**

- Add model-quality evaluators:
  - ambiguous grain
  - fanout join risk
  - duplicated metrics across marts
  - marts built directly from raw sources
  - missing tests on primary keys, uniqueness, non-null, freshness
  - stale or unused models
  - expensive models with low usage
- Add platform-quality evaluators:
  - repeatedly slow/expensive queries
  - oversized scans from missing predicates
  - missing clustering/partition opportunities
  - unused tables/models
  - excessive intermediate layers
- Add semantic-quality evaluators:
  - conflicting metric names/definitions
  - undocumented dimensions
  - inconsistent temporal grains
  - BI/query usage that bypasses governed marts
- Generate improvement actions with severity, owner hint, affected assets,
  supporting evidence, and recommended SQL/dbt/test changes.

**Evaluation steps:**

- Fixtures include intentionally flawed dbt/warehouse projects:
  - duplicate revenue metrics
  - customer/order fanout risk
  - stale mart
  - missing uniqueness test
  - high-cost low-use model
- Tests assert recommendations are specific, evidence-backed, and not generic.
- Human-review eval rubric scores recommendations for:
  - correctness
  - actionability
  - severity calibration
  - noise/false positives
  - whether the proposed fix is safe
- Track acceptance/ignore rates as product feedback signals.

**Done when:**

- Headwater can produce a credible “data product review” for a mature warehouse.
- Recommendations point to concrete model/platform/quality improvements, not
  only business observations.

### Missing Pieces to Resolve Before Implementation

- Define the persistent evidence schema and migration plan.
- Decide how Snowflake budgets are configured in UI/API/env.
- Decide whether `snowflake-connector-python` remains optional or becomes an
  install extra.
- Add live Snowflake credentials/secrets strategy for CI or manual integration.
- Decide first dbt artifact import path and expected file locations.
- Define a minimal semantic metric contract independent of any one vendor.
- Add UI surfaces for evidence labels, confidence, skipped work, and query cost.
- Add a “dry run insight plan” screen before warehouse insight execution.
- Define statistical thresholds for confidence labels and when to refuse claims.
