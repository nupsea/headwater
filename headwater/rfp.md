# Headwater Refactor RFP

**Goal:** make Headwater a source-connected, evidence-driven advisory workspace that helps data organizations mature their data models, pipelines, contracts, and decision readiness.

| | |
|---|---|
| **Product** | Headwater |
| **Document Type** | Refactor RFP and implementation plan |
| **Date** | April 2026 |
| **Status** | Working plan |

---

## 1. Executive Summary

Headwater already has useful pieces: discovery, profiling, semantic enrichment, model generation, quality contracts, drift storage, FastAPI routes, and a Next.js review UI. The current weakness is that these pieces still behave too much like a one-shot demo pipeline. The next refactor should turn them into a continuous operational workflow:

`connect -> sync -> profile -> compare -> generate -> review -> monitor -> brief -> improve`

The product should not try to become a warehouse, BI tool, semantic layer runtime, enterprise catalog, observability suite, and AI assistant at the same time. The valuable path is narrower and stronger:

> Headwater connects to the data estate, understands what changed, identifies what needs review, and explains whether the data is ready to support decisions.

This RFP replaces broad claims with an implementation plan anchored in current code and realistic sequencing.

---

## 2. Current Product Assessment

### 2.1 Already Valuable

- **Discovery and profiling pipeline** exists for local files and Postgres.
- **Metadata store** persists sources, tables, columns, profiles, relationships, models, contracts, decisions, drift reports, projects, catalog items, and activity.
- **FastAPI backend** exposes routes for discovery, pipeline, data preview/query, models, quality, drift, confidence, graph, project, settings, sources, and briefing.
- **Next.js UI** already has the new design direction: Briefing as the homepage, Sources page, Project Health, Discover, Dictionary, Models, Quality, Data, Insights, Explore, and Settings.
- **Advisory workflow** exists conceptually: staging models are mechanical, marts require review, contracts observe before enforcement, and confidence metrics track trust.
- **Graph/vector primitives** exist for relationships and semantic retrieval.

### 2.2 Core Gaps

- **Source sync is shallow**: source sync tests a connection but does not run full discovery, drift, quality, or invalidation.
- **Drift is not fully wired**: schema snapshot and drift report storage exist, but the discovery persistence flow does not consistently create drift reports.
- **Statistical drift is missing**: schema drift detects structural change, but Headwater also needs value distribution, null-rate, cardinality, row-count, freshness, and referential-integrity drift.
- **Quality state is incomplete**: quality report results are not persisted as a durable history that Briefing and Insights can reliably aggregate.
- **Connector support is narrower than public copy**: JSON, CSV, and Postgres are real; broad catalog, warehouse, Parquet, and enterprise connector claims should be treated as planned.
- **Events are under-modeled**: sync events exist, but the system needs a normalized event contract for all pipeline and human-review actions.
- **Model maturity is coarse**: statuses do not yet express drafted, reviewed, approved, materialized, monitored, deprecated, and impacted-by-change.
- **Decision intelligence is not modeled**: there is no first-class link between sources, metrics, models, contracts, insights, and business decisions.

---

## 3. What Is Not Worth Pursuing Now

These items are attractive but should not be on the next refactor critical path.

| Do Not Prioritize | Reason |
|---|---|
| **Full semantic layer runtime** | High maintenance, unclear standard, and not necessary. Generate semantic metadata and exports instead. |
| **Streaming ingestion** | Batch and scheduled sync solve current user pain. Streaming multiplies complexity. |
| **Managed cloud** | Premature until local OSS workflow repeatedly proves value. |
| **Enterprise RBAC/SSO/audit compliance** | Important later, but not the blocker for usefulness. |
| **Catalog connectors before source sync maturity** | Glue/Unity/Iceberg are useful, but source lifecycle and drift must be solid first. |
| **Large connector marketplace** | Stable connector protocol must come first. External plugin churn will hurt trust. |
| **Autonomous fixes to marts or metrics** | Unsafe. Headwater can propose and explain; humans approve business logic. |
| **Generic chatbot-first experience** | NLQ without trusted metadata, models, and contracts creates hallucinated confidence. |
| **Too many design pages** | Fewer pages with live, correct state are better than many pages with partial state. |
| **Cloud deployers and Terraform modules** | Operational burden outweighs value for the core project. |

---

## 4. Product North Star

Headwater should make data maturity inspectable and actionable.

### 4.1 Maturity Ladder

| Stage | Meaning | Headwater Outcome |
|---|---|---|
| **Connected** | Source is registered and reachable. | Health, credentials status, connector capabilities. |
| **Discovered** | Schemas, tables, columns, and constraints are known. | Searchable source catalog and profile baseline. |
| **Understood** | Descriptions, domains, relationships, and metrics are proposed or confirmed. | Review queue and semantic confidence. |
| **Modeled** | Staging and marts are generated, reviewed, and materialized. | Model maturity and impact graph. |
| **Monitored** | Quality, freshness, drift, and lineage are tracked. | Briefing priorities and operational alerts. |
| **Decision-Ready** | Data can support named decisions with evidence. | Decision readiness score and decision brief. |

### 4.2 Primary User Questions

Headwater should answer these questions without requiring the user to inspect five disconnected tools:

- What data sources do we have, and are they healthy?
- What changed since the last sync?
- Which tables, models, definitions, or contracts need review?
- What quality or drift issues could affect downstream models?
- Which data products are mature enough for stakeholder or executive decisions?
- What evidence supports each generated insight?
- What should I fix next to improve maturity fastest?

---

## 5. Target Architecture

### 5.1 Source Lifecycle

Every source follows the same lifecycle:

```text
registered
  -> connection_tested
  -> discovered
  -> profiled
  -> drift_compared
  -> contracts_checked
  -> models_impacted
  -> briefing_updated
```

The source lifecycle is the spine of the product. UI pages read from lifecycle state and emitted events.

### 5.2 Connector Protocol

Connectors must expose capabilities instead of pretending every source supports the same operations.

```python
class ConnectorCapabilities(BaseModel):
    can_list_schemas: bool = True
    can_list_tables: bool = True
    can_profile_pushdown: bool = False
    can_sample_arrow: bool = False
    can_execute_sql: bool = False
    can_materialize: bool = False
    can_read_freshness: bool = False
    observe_only: bool = False
```

Required connector methods:

- `connect(config)`
- `test()`
- `capabilities()`
- `list_schemas()`
- `list_tables(schema_filter=None)`
- `list_columns(table_ref)`
- `list_constraints(table_ref)`
- `estimate_row_count(table_ref)`
- `profile_table(table_ref, profile_spec)`
- `sample_arrow(table_ref, limit)`
- `execute_readonly(sql, limit)`
- `close()`

The pipeline adapts to capabilities:

- database connectors use pushdown profiling where possible;
- file connectors load bounded local Arrow/DuckDB data;
- warehouse connectors may be observe-only;
- catalog connectors provide metadata first and optionally delegate profiling to a paired compute connector.

### 5.3 Event Contract

All state-changing work emits events.

```json
{
  "id": 123,
  "source_name": "prod",
  "event_type": "schema_drift_detected",
  "severity": "warning",
  "artifact_type": "table",
  "artifact_id": "public.orders",
  "summary": "orders gained 2 columns",
  "payload": {},
  "invalidates": ["briefing", "sources", "insights", "models"],
  "created_at": "2026-04-28T10:00:00Z"
}
```

This event stream powers:

- Briefing priorities
- Sources cards
- Project Health
- Review Queue
- Insights anomalies
- Rerun banners
- Model impact warnings

### 5.4 Decision Graph

Headwater needs a graph of operational and semantic dependencies:

```text
Source
  -> Table
  -> Column
  -> Relationship
  -> Metric
  -> Model
  -> Contract
  -> Insight
  -> Decision
```

The graph should support:

- impact analysis;
- join-path explanation;
- contract coverage;
- metric readiness;
- model maturity;
- evidence-backed insights;
- safe NLQ context scoping.

### 5.5 Decision Readiness Score

Decision readiness is a derived score, not a vibe.

Inputs:

- source health;
- freshness;
- schema drift status;
- statistical drift status;
- quality pass rate;
- contract precision;
- lineage completeness;
- semantic confidence;
- model review status;
- metric definition status;
- recent human review.

Example output:

```text
Weekly Revenue Review: 82/100
- Freshness: pass
- Revenue model: approved and materialized
- Refund treatment: confirmed
- Quality: 96% contracts passing
- Risk: payment.status distribution drifted 18% since last baseline
- Recommendation: review payment status drift before board reporting
```

---

## 6. Phased Implementation Plan

### Phase 0: Baseline and Documentation Reset

**Goal:** make claims match reality and reduce product ambiguity.

Deliverables:

- Update public docs to mark connectors as supported, preview, or planned.
- Add a workflow map showing source lifecycle and review lifecycle.
- Remove claims for catalog connectors, cloud connectors, Parquet, dbt export, managed cloud, and plugin marketplace until implemented.
- Add a “not worth pursuing now” section to planning docs.
- Add architecture notes for event contract, connector capabilities, and decision graph.

Acceptance criteria:

- A new user can tell exactly what works today.
- The README, press release, RFP, UI connector picker, and API behavior do not contradict each other.

### Phase 1: Real Source Sync and Event Backbone

**Goal:** make Sources and Briefing reflect real work, not shallow connection state.

Backend deliverables:

- Refactor `/sources/{name}/sync` to run source-scoped discovery.
- Split connection testing from sync: `/sources/{name}/test` and `/sources/{name}/sync`.
- Persist sync runs with started, finished, status, table count, error, and duration.
- Wire schema snapshots and drift reports into discovery persistence.
- Emit events for sync start, sync completion, sync failure, schema drift, quality failure, model impact, and review changes.
- Persist quality check results as durable history.
- Fix contract runtime state so failed checks are visible to Briefing and Insights.

Frontend deliverables:

- Sources page shows last real sync, run duration, drift count, quality issue count, and affected tables.
- Briefing aggregates from events and durable state.
- Rerun banners appear when source changes invalidate downstream artifacts.

Acceptance criteria:

- Registering a Postgres, JSON, or CSV source and clicking Sync runs a complete source-scoped workflow.
- Drift reports are created on schema changes.
- A failed quality check appears in Briefing without restarting the app.
- Events are sufficient to explain why each Briefing priority exists.

### Phase 2: Capability-Aware Connector Platform

**Goal:** make “connect to any data source” credible through a stable contract.

Deliverables:

- Define `ConnectorCapabilities`, `TableRef`, `ColumnRef`, `TableProfile`, and `SourceProfile`.
- Refactor Postgres, CSV, and JSON connectors to the new protocol.
- Add DuckDB and SQLite connectors as low-friction high-value targets.
- Add MySQL after the protocol stabilizes.
- Implement connector contract tests shared by every connector.
- Add safety controls: read-only enforcement, schema filters, query timeout, sample limit, max tables, max columns, credential redaction.
- Store source config as redacted/encrypted metadata; never expose credentials in API responses or logs.

Connector priority:

1. Postgres hardening
2. CSV and JSON hardening
3. DuckDB
4. SQLite
5. MySQL
6. Snowflake observe mode
7. BigQuery observe mode
8. Catalog connectors only after source sync is mature

Acceptance criteria:

- Each connector passes the same behavior test suite.
- The pipeline degrades gracefully when a connector is observe-only.
- Unsupported connector operations produce actionable errors.

### Phase 3: Model and Pipeline Maturity

**Goal:** help data teams improve models and pipelines over time.

Deliverables:

- Add model states: `drafted`, `review_pending`, `approved`, `materialized`, `monitored`, `deprecated`, `invalidated`.
- Keep mart auto-approval only in demo mode.
- Add model review records with reviewer, decision, reason, and diff summary.
- Add model impact analysis from changed source tables and columns.
- Add contract lifecycle: `proposed`, `observing`, `enforced`, `failing`, `recovered`, `disabled`.
- Add pipeline maturity scoring per source/project.
- Add rerun planner: what to regenerate, revalidate, or ask a human to review after changes.

Acceptance criteria:

- A source schema change shows affected models and contracts.
- A model can be traced from source tables to quality contracts.
- The UI shows what is blocking maturity progression.

### Phase 4: Deep Insight Engine

**Goal:** generate insights that are useful because they are evidence-backed.

Deliverables:

- Build insight generation over profiles, relationships, quality history, and drift.
- Add insight types:
  - null-rate anomalies;
  - cardinality shifts;
  - freshness breaches;
  - referential-integrity decay;
  - unexpected new categories;
  - orphan records;
  - duplicate key candidates;
  - segment-level outliers;
  - likely PII exposure;
  - stale models or untested contracts.
- Each insight includes evidence, confidence, affected assets, and recommended action.
- Add insight review: useful, false positive, irrelevant, needs investigation.
- Feed insight outcomes into confidence metrics.

Acceptance criteria:

- Insights are traceable to data evidence and not just narrative text.
- False positives can be recorded and used to tune thresholds.
- Briefing uses top insights as priorities.

### Phase 5: Semantic Metrics and Decision Readiness

**Goal:** connect technical maturity to business decision readiness.

Deliverables:

- Add metric entities with owner, formula, source models, dimensions, and status.
- Add decision entities with linked metrics, required freshness, quality threshold, owner, cadence, and risk tolerance.
- Compute readiness scores for metrics and decisions.
- Add decision briefs with evidence, risks, blockers, and next actions.
- Allow NLQ only within reviewed and readiness-scored contexts.
- Add “why this answer?” evidence trace for every generated answer.

Acceptance criteria:

- A user can define or confirm a business decision and see whether the data is ready.
- Headwater can explain blockers in operational terms.
- NLQ refuses or qualifies answers when readiness is too low.

### Phase 6: Exports and Ecosystem

**Goal:** integrate with mature stacks without becoming every downstream tool.

Deliverables:

- dbt export for reviewed staging/mart models and contracts.
- Metric export to dbt semantic models or MetricFlow-compatible YAML where feasible.
- Great Expectations or Soda export for quality contracts if demand is clear.
- Slack/email notifications from the event stream.
- OpenAPI and CLI parity for source sync, review, maturity, and decision readiness.

Acceptance criteria:

- Headwater can bootstrap local understanding and then hand off reviewed artifacts to the user’s stack.
- Exports are deterministic and test-covered.

---

## 7. Refactor Workstreams

### 7.1 Backend

- Introduce source sync service layer independent of FastAPI route handlers.
- Move pipeline orchestration out of route files.
- Add explicit run records for sync, profile, generate, execute, and quality.
- Add event writer and typed event constants.
- Persist quality result history.
- Add drift comparison for profiles, not only schemas.
- Add model impact service.

### 7.2 Frontend

- Replace page-local polling with shared query keys and event-driven invalidation.
- Make Briefing the primary operational surface.
- Make Sources the source lifecycle surface.
- Make Project Health the maturity surface.
- Make Models the review and impact surface.
- Make Insights evidence-first, not chart-first.
- Add a Decision Readiness page only after Phase 5 backend entities exist.

### 7.3 Data Model

Add or harden these tables:

- `sync_runs`
- `events`
- `quality_check_runs`
- `quality_check_results`
- `profile_snapshots`
- `profile_drift_reports`
- `model_reviews`
- `model_impacts`
- `metrics`
- `decisions`
- `decision_readiness_scores`
- `insight_reviews`

Existing `sync_events` can either be migrated to `events` or retained as a compatibility view.

### 7.4 Testing

- Connector contract tests.
- Golden source fixtures for CSV, JSON, SQLite, DuckDB, and Postgres.
- Drift regression tests for schema and statistical drift.
- Quality result persistence tests.
- Briefing aggregation tests from events.
- Model impact tests from source changes.
- End-to-end sync tests.

---

## 8. Product Principles

- **Advisory, not autonomous**: humans approve business logic.
- **Evidence before narrative**: every insight must cite its data basis.
- **Capabilities over assumptions**: connectors declare what they can safely do.
- **Continuous over one-shot**: value compounds through repeated syncs and reviews.
- **Maturity over feature count**: users should see what to fix next.
- **Local-first**: OSS remains useful without managed cloud.
- **No raw data to LLM by default**: use schemas, stats, and redacted examples only when explicitly allowed.
- **No hidden trust**: expose confidence, false positives, edit distance, and review outcomes.

---

## 9. Success Metrics

### Product Metrics

- Time from source registration to first briefing.
- Percentage of sources with successful last sync.
- Percentage of tables profiled.
- Percentage of columns with accepted descriptions.
- Percentage of models reviewed.
- Contract precision and false-positive rate.
- Drift detection lead time.
- Number of actionable insights accepted as useful.
- Number of decisions with readiness score above threshold.

### Engineering Metrics

- Connector contract test pass rate.
- End-to-end sync runtime by source size.
- API route latency while sync runs.
- Event aggregation correctness.
- Quality result persistence coverage.
- Drift test coverage.

---

## 10. Initial Milestone Backlog

### Milestone 1: Sync Is Real

- Add `/sources/{name}/test`.
- Refactor `/sources/{name}/sync` to call a sync service.
- Create `sync_runs`.
- Create normalized `events`.
- Emit sync lifecycle events.
- Run discovery from source metadata.
- Update Sources page to show sync run state.

### Milestone 2: Drift and Quality Are Real

- Wire schema snapshots into every discovery run.
- Add profile snapshot persistence.
- Add statistical drift detector.
- Add quality run/result persistence.
- Update Briefing to aggregate failed quality results and drift events.

### Milestone 3: Connectors Are Real

- Define connector capability protocol.
- Refactor Postgres connector.
- Refactor CSV and JSON connectors.
- Add SQLite and DuckDB connectors.
- Add connector contract test suite.

### Milestone 4: Maturity Is Real

- Add model lifecycle states.
- Add model impact analysis.
- Add rerun planner.
- Add Project Health maturity scoring.
- Add review blockers to Briefing.

### Milestone 5: Insights Are Real

- Add insight entity and review state.
- Generate evidence-backed insight candidates.
- Add confidence and false-positive feedback.
- Surface top insights in Briefing and Insights.

### Milestone 6: Decision Readiness Is Real

- Add metrics and decisions.
- Add readiness scoring.
- Add decision brief UI.
- Add NLQ guardrails based on readiness.

---

## 11. Open Technical Decisions

- Whether to migrate from SQLite to Postgres metadata before or after normalized events.
- Whether `sync_events` should be replaced by `events` or retained as a view.
- Whether DuckDB materialization should be per source schema, per project schema, or both.
- How to handle source credentials locally: encrypted SQLite field, external secrets, or environment references.
- How much profile history to retain by default.
- Whether model generation should target native SQL first or dbt-compatible structure first.
- Whether graph storage remains Kuzu long-term or metadata tables are enough until decision graph complexity grows.

---

## 12. Recommended Next Refactor Sequence

The next refactor should start with backend workflow, not UI polish.

1. Build source sync service.
2. Add durable run and event records.
3. Wire drift and quality history.
4. Update Briefing and Sources to use durable state.
5. Stabilize connector protocol.
6. Add DuckDB/SQLite/MySQL connectors.
7. Add model impact and maturity scoring.
8. Add evidence-backed insights.
9. Add decision readiness.
10. Add exports and ecosystem integrations.

This sequence makes Headwater useful earlier and avoids building advanced AI features on unreliable operational state.

---

*Headwater should become the operating layer between connected data and decision-ready intelligence. The next refactor should make that true in code, not just in copy.*
