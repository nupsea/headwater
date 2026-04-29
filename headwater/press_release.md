# Headwater Refocuses on Decision-Ready Data Operations

**An open-source advisory workspace for data professionals: connect sources, understand the data estate, harden models and pipelines, and move toward decision intelligence without adding another black-box platform.**

---

**April 2026**

Data teams do not need another tool that promises to “ask anything” and returns unverifiable SQL. They need a system that makes the existing data estate understandable, trustworthy, and operationally mature.

Headwater is being refocused around that practical goal: help data professionals connect to their sources, inspect what is actually there, surface risks and opportunities, and turn a loose collection of tables, models, and checks into a decision-ready data system.

The product direction is deliberately advisory. Headwater discovers, profiles, explains, proposes, and monitors. Humans approve business definitions, model logic, governance choices, and pipeline changes. This is not a limitation; it is the trust boundary that makes the product useful in real organizations.

---

## What Exists Today

Headwater already has a strong foundation:

- **Discovery pipeline** for JSON, CSV, DuckDB, SQLite, and Postgres sources.
- **Profiling and relationship detection** across tables and columns.
- **Semantic enrichment** with heuristic and optional LLM-assisted descriptions.
- **Staging and mart model generation** with review-oriented assumptions and questions.
- **Quality contract generation** and validation.
- **SQLite metadata store** with sources, tables, columns, profiles, relationships, models, contracts, decisions, drift reports, and audit logs.
- **FastAPI backend and Next.js UI** covering briefing, sources, health, discovery, dictionary, models, quality, data, insights, explore, and settings.
- **Briefing-led workflow** that starts from “what needs attention?” instead of a generic dashboard.
- **Source registry and event log** for connected sources and sync events.

The direction is right. The gaps are in workflow integration, connector depth, event semantics, and decision-level insight.

---

## What Needs to Change

Headwater should stop positioning itself as a broad “connect to anything and solve everything” platform until the workflow is defensible end-to-end. The next refactor should optimize for depth of value, not breadth of claims.

### Not Worth Pursuing Now

- **Catalog connectors before source connectors**: Glue, Unity Catalog, and Iceberg REST are valuable later, but they add metadata breadth before Headwater has source-sync depth.
- **A full semantic layer runtime**: generating semantic metadata and metric definitions is useful; running a competing semantic query engine is not worth the maintenance burden.
- **Streaming support**: batch and scheduled sync cover the immediate pain. Streaming would distract from source discovery, drift, quality, and model maturity.
- **Managed cloud and enterprise features**: SSO, RBAC, billing, and multi-tenant hosting should wait until local OSS usage proves repeatable value.
- **Marketplace-style plugin architecture too early**: define stable connector interfaces first; external plugins before interface maturity create churn.
- **Autonomous model changes**: no “auto-fix my marts” workflow. Headwater can propose changes and impact analysis, but humans own business logic.
- **Broad connector logos without support**: the UI may show future connectors, but public docs should clearly separate supported, preview, and planned connectors.
- **Generic AI assistant as the centerpiece**: natural-language query is useful only after catalog, lineage, quality, and model semantics are reliable.

---

## The New Product Promise

Headwater helps a data organization answer four operational questions:

1. **What data do we have, and can we trust it?**
2. **What changed since the last time we looked?**
3. **Which models, contracts, and definitions need human review?**
4. **Which decisions can this data support with evidence?**

This reframes Headwater from a one-shot discovery demo into a continuous data-improvement tool.

---

## The Refactor Direction

### 1. Source-Native Sync

Every source should have the same lifecycle:

`registered -> tested -> discovered -> profiled -> drift-compared -> quality-checked -> briefed`

The existing `/sources/{name}/sync` endpoint should stop being a connection test only. It should become the source-scoped entry point for the full continuous workflow.

### 2. Connector Contract

Headwater should standardize a connector protocol that works for databases, warehouses, files, and catalogs:

- list namespaces/schemas
- list tables
- list columns and constraints
- estimate row counts
- run safe aggregate profiles
- fetch bounded Arrow samples where allowed
- expose freshness metadata where available
- identify capabilities such as `can_sample`, `can_profile_pushdown`, `can_materialize`, and `observe_only`

This makes “connect to any data source” technically credible without pretending every connector supports the same operations.

### 3. Event-Driven State

All important changes should emit normalized events:

- source registered
- sync started/completed/failed
- schema drift detected
- statistical drift detected
- quality check failed/recovered
- model proposed/approved/rejected/executed
- column description confirmed
- relationship confirmed/rejected
- metric definition changed

Briefing, Sources, Insights, Health, and Review Queue should read from this event layer instead of each page inventing its own state logic.

### 4. Decision Intelligence Layer

Headwater should introduce a decision graph:

`source -> table -> column -> relationship -> metric -> model -> contract -> insight -> decision`

The product should not merely answer “what does this column mean?” It should answer:

- Which metrics are decision-ready?
- Which decisions are blocked by missing definitions, failing contracts, or stale data?
- Which upstream changes affect a dashboard, metric, model, or business question?
- What evidence supports a generated insight?

This is the novel direction. Most data tools stop at metadata, lineage, observability, or BI. Headwater should connect those layers into a practical maturity path.

---

## Phased Implementation Plan

### Phase 0: Reality Alignment

- Make docs and UI copy match shipped support: JSON, CSV, Postgres.
- Label unsupported connectors as planned, not available.
- Stop claiming catalog support, Parquet support, managed cloud, or dbt export until implemented.
- Add a supported/preview/planned matrix to README and UI.

### Phase 1: Continuous Source Workflow

- Refactor `/sources/{name}/sync` into full source-scoped discovery.
- Persist sync lifecycle events.
- Wire schema snapshots and drift reports into every discovery run.
- Persist quality check results and contract runtime status.
- Make Briefing and Sources reflect real sync, drift, and quality state.

### Phase 2: Connector Platform

- Replace ad hoc connector methods with a capability-aware protocol.
- Implement first-class Postgres, CSV, JSON, DuckDB, SQLite, and MySQL connectors.
- Add strict safety controls: query timeout, row/sample limits, read-only mode, schema allowlists, and credential redaction.
- Add connector contract tests so every connector proves the same behavior.

### Phase 3: Model and Pipeline Maturity

- Stop auto-approving mart models outside demo mode.
- Add model maturity states: drafted, reviewed, approved, materialized, monitored, deprecated.
- Add dependency and impact analysis from sources through models and contracts.
- Add rerun planning: when a source changes, show what must be regenerated, reviewed, or revalidated.
- Export reviewed models and contracts to dbt-compatible files after core maturity is stable.

### Phase 4: Deep Insights

- Build statistical insight generation over profiles, relationships, quality history, and drift.
- Require every insight to include evidence, confidence, affected assets, and recommended next action.
- Add anomaly and segment discovery: outliers, high-null clusters, orphan records, unexpected cardinality changes, and relationship integrity breaks.
- Add “insight review” so humans can mark useful, irrelevant, false positive, or needs investigation.

### Phase 5: Decision Readiness

- Introduce metrics and decision entities.
- Score decision readiness from semantic confidence, model review state, quality pass rate, freshness, drift, and lineage completeness.
- Add decision briefs: “This metric is safe for weekly revenue review, but not board reporting because refund logic is unresolved.”
- Add controlled NL question answering only over reviewed metadata, approved models, and evidence-backed insights.

---

## Why This Matters

Most data teams do not fail because they lack dashboards. They fail because no one can tell which data is trustworthy, which definitions are official, which pipelines are fragile, and which metrics are safe to use for decisions.

Headwater’s opportunity is to become the advisory system that makes that maturity visible and achievable:

- **From unknown data** to discovered data.
- **From discovered data** to documented data.
- **From documented data** to modeled data.
- **From modeled data** to monitored data.
- **From monitored data** to decision-ready data.

That is a stronger position than “AI data assistant.” It is also more useful.

---

## Current Availability

Available now:

- JSON and CSV file discovery
- Postgres discovery
- Profiling and relationship detection
- Semantic enrichment and locking
- Model and contract generation
- Quality validation
- FastAPI backend
- Next.js review UI
- Briefing and Sources pages
- Sync event table and source metadata
- Schema drift storage primitives

Needs implementation or hardening:

- Source-scoped full sync
- Statistical drift detection
- Connector capability protocol
- DuckDB, SQLite, MySQL, warehouse, and catalog connectors
- Persisted quality result history
- Event-driven UI invalidation
- Decision graph and readiness scoring
- dbt export
- Managed cloud

---

*Headwater. From connected data to decision-ready intelligence.*
