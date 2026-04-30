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
- Supported local/file connectors: CSV, JSON, DuckDB, SQLite.
- Supported Postgres connector for table/column discovery, pushdown profiling,
  bounded sampling, and read-only validation.
- Preview MySQL connector.
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

## Verification Gate for Next Milestone

- Postgres constraint import tested against a real or containerized Postgres DB.
- Manual FK editing works end-to-end in Discover & Access.
- At least one AWS OLAP connector story is implemented behind preview status.
- Large-table profiling policy is visible in source sync results.
- README, architecture, and progress docs remain current.
