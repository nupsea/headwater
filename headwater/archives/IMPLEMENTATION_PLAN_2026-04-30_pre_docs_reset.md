# Headwater Implementation Plan

**Date:** April 28, 2026  
**Status:** Active refactor plan  
**Archived predecessor:** `../archives/IMPLEMENTATION_PLAN_legacy_2026-04-28.md`  
**Related strategy docs:** `press_release.md`, `rfp.md`

---

## 1. Objective

Refactor Headwater from a one-shot discovery pipeline into a continuous, source-connected advisory workspace for data professionals.

The implementation target is:

```text
connect -> sync -> profile -> compare -> generate -> review -> monitor -> brief -> improve
```

The product should help teams mature from unknown data to decision-ready data:

```text
Connected -> Discovered -> Understood -> Modeled -> Monitored -> Decision-Ready
```

This plan uses phase-wise product stories with explicit engineering verification gates. A phase is not complete until its verification gate passes.

---

## 2. Scope Boundaries

### In Scope

- Source-scoped sync lifecycle.
- Durable run and event records.
- Schema and statistical drift.
- Quality result history.
- Connector capability protocol.
- Postgres, JSON, CSV hardening.
- DuckDB, SQLite, and MySQL connectors.
- Model and contract maturity lifecycle.
- Evidence-backed insights.
- Metric and decision readiness scoring.
- Deterministic exports after core maturity is stable.

### Not in Scope for This Refactor

- Managed cloud.
- Multi-tenant auth, RBAC, SSO, billing.
- Streaming ingestion.
- Full semantic layer runtime.
- Autonomous business-logic changes.
- Connector marketplace before protocol maturity.
- Generic chatbot-first experience.
- Cloud deployers or Terraform modules.

---

## 3. Definitions

### Story Format

Each story uses:

- **User story:** the user-visible need.
- **Implementation:** backend/frontend/data-model changes.
- **Acceptance criteria:** observable behavior required for completion.
- **Verification:** tests, commands, or manual checks required before closing.

### Severity

- **Blocking:** must ship in the phase.
- **Recommended:** should ship unless it threatens the phase gate.
- **Deferred:** valid but intentionally out of phase.

### Verification Tiers

- **Unit:** isolated function/service tests.
- **Integration:** route/service/store tests.
- **E2E:** source registration through UI/API-visible state.
- **Manual:** targeted UI or CLI smoke checks.

---

## 4. Phase 0: Baseline and Documentation Reset

**Goal:** make the project honest, navigable, and internally consistent before deeper refactors.

**Primary outcome:** a user can tell what works today, what is planned, and what is intentionally not being built yet.

### Story 0.1: Archive Legacy Plan

**User story:** As a maintainer, I need the outdated implementation plan preserved but no longer active, so planning does not mix old waves with the new refactor.

**Implementation:**

- Move `headwater/IMPLEMENTATION_PLAN.md` to `archives/IMPLEMENTATION_PLAN_legacy_2026-04-28.md`.
- Create a new active implementation plan at `headwater/IMPLEMENTATION_PLAN.md`.

**Acceptance criteria:**

- Legacy content is still available in `archives/`.
- Active plan points to the archived predecessor.
- `git status` shows a rename/addition rather than silent deletion of planning history.

**Verification:**

- `test -f archives/IMPLEMENTATION_PLAN_legacy_2026-04-28.md`
- `test -f headwater/IMPLEMENTATION_PLAN.md`

### Story 0.2: Reality-Based Connector Matrix

**User story:** As a new user, I need connector support clearly labeled, so I do not try unsupported sources and lose trust.

**Implementation:**

- Add a connector matrix to `README.md`.
- Align `press_release.md`, `rfp.md`, UI connector picker, and API catalog around three statuses:
  - `supported`
  - `preview`
  - `planned`
- Current supported baseline:
  - JSON
  - CSV
  - Postgres
- Planned baseline:
  - DuckDB
  - SQLite
  - MySQL
  - Snowflake observe mode
  - BigQuery observe mode
  - catalog connectors after source sync maturity

**Acceptance criteria:**

- No public document claims Glue, Unity, Iceberg, Snowflake, BigQuery, MySQL, Parquet, dbt export, or managed cloud as currently shipped unless code supports it.
- UI connector picker communicates planned connectors as planned, not silently selectable production features.
- API connector catalog returns status metadata.

**Verification:**

- `rg -n "Glue|Unity|Iceberg|Snowflake|BigQuery|Parquet|managed cloud|dbt export" README.md headwater/press_release.md headwater/rfp.md headwater/ui/src headwater/headwater`
- Manual review of each hit for current-vs-planned wording.

### Story 0.3: Workflow Map

**User story:** As a contributor, I need one workflow map, so backend routes and UI pages implement the same product lifecycle.

**Implementation:**

- Add source lifecycle to `README.md` or `docs/workflow.md`:

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

- Add review lifecycle:

```text
proposed -> reviewed -> approved/rejected -> monitored -> invalidated/deprecated
```

**Acceptance criteria:**

- The lifecycle references concrete routes/services planned in this document.
- Briefing, Sources, Project Health, Models, Quality, and Insights each map to lifecycle responsibilities.

**Verification:**

- Documentation review only.

### Phase 0 Gate

- [ ] Legacy implementation plan archived.
- [ ] Active implementation plan exists.
- [ ] Current connector support is documented honestly.
- [ ] Unsupported features are labeled planned.
- [ ] Source and review lifecycle are documented.

---

## 5. Phase 1: Real Source Sync and Event Backbone

**Goal:** make Sources and Briefing reflect real operational work, not shallow connection state.

**Primary outcome:** clicking Sync on a source runs a complete source-scoped workflow and emits events that explain resulting UI state.

### Story 1.1: Source Sync Service

**User story:** As a data professional, I need source sync to run real discovery and profiling, so the Sources page represents current source state.

**Implementation:**

- Add `headwater/headwater/services/source_sync.py`.
- Move orchestration out of FastAPI route handlers.
- Create a `SourceSyncService` that accepts:
  - source name
  - optional schema/table filters
  - sync mode: `test`, `profile`, `full`
- Refactor `/api/sources/{name}/sync` to call the service.
- Add `/api/sources/{name}/test` for connection testing only.

**Acceptance criteria:**

- `/sources/{name}/test` only verifies connectivity and capabilities.
- `/sources/{name}/sync` runs discovery and persistence for the named source.
- Sync updates `last_sync_at`, `health`, run status, table counts, and event history.
- Sync failures are persisted with actionable error detail.

**Verification:**

- Unit: source sync service can run against a temporary JSON/CSV source.
- Integration: API test registers source, tests it, syncs it, and sees tables/profiles.
- Manual: create source from UI, click Sync, refresh Sources and Briefing.

### Story 1.2: Sync Runs Table

**User story:** As an operator, I need durable sync run history, so I can understand what happened and when.

**Implementation:**

- Add metadata table `sync_runs`:

```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
source_name TEXT NOT NULL
mode TEXT NOT NULL
status TEXT NOT NULL
started_at TEXT NOT NULL
finished_at TEXT
duration_ms INTEGER
tables_seen INTEGER DEFAULT 0
profiles_written INTEGER DEFAULT 0
contracts_checked INTEGER DEFAULT 0
error TEXT
payload_json TEXT
```

- Add `MetadataStore` helpers:
  - `start_sync_run`
  - `finish_sync_run`
  - `fail_sync_run`
  - `list_sync_runs`
  - `get_latest_sync_run`

**Acceptance criteria:**

- Every source sync has one durable run row.
- Failed syncs are not lost on app restart.
- Source detail API includes recent runs.

**Verification:**

- Unit: metadata CRUD tests.
- Integration: failed connector test creates failed run row.

### Story 1.3: Normalized Events

**User story:** As a user, I need Briefing priorities to explain their cause, so I can trust the recommendations.

**Implementation:**

- Add `events` table or migrate `sync_events` into `events`.
- Required fields:

```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
source_name TEXT
event_type TEXT NOT NULL
severity TEXT NOT NULL
artifact_type TEXT
artifact_id TEXT
summary TEXT NOT NULL
detail TEXT
payload_json TEXT
invalidates_json TEXT DEFAULT '[]'
created_at TEXT NOT NULL DEFAULT (datetime('now'))
acknowledged_at TEXT
```

- Add typed event constants.
- Emit events for:
  - source_registered
  - connection_tested
  - sync_started
  - sync_completed
  - sync_failed
  - schema_drift_detected
  - profile_drift_detected
  - quality_check_failed
  - quality_check_recovered
  - model_proposed
  - model_approved
  - model_rejected
  - model_invalidated
  - column_description_confirmed
  - relationship_confirmed
  - relationship_rejected

**Acceptance criteria:**

- Event rows are sufficient to explain Sources, Briefing, and rerun banners.
- Existing `sync_events` route remains backward-compatible or redirects to normalized events.
- Events include invalidation hints for frontend query keys.

**Verification:**

- Unit: event writer serializes payload and invalidates fields.
- Integration: model approval emits model event.
- Integration: failed sync emits sync_failed event.
- Integration: Briefing priority contains source event evidence.

### Story 1.4: Wire Schema Drift Into Sync

**User story:** As a data engineer, I need schema changes detected on every sync, so downstream breakage is caught early.

**Implementation:**

- Update discovery persistence to:
  - build current schema snapshot;
  - load prior snapshot;
  - compare snapshots;
  - save new snapshot;
  - save drift report;
  - emit drift event when changes exist.
- Ensure first run is treated as baseline, not noisy drift.

**Acceptance criteria:**

- First sync creates baseline snapshot.
- Second sync with no schema change reports no drift.
- Sync after added/removed/type-changed column creates drift report and warning event.

**Verification:**

- Unit: schema compare tests for added, removed, changed type, changed nullability.
- Integration: run source sync twice with modified fixture and assert drift report.

### Story 1.5: Durable Quality History

**User story:** As a data professional, I need failed quality checks to remain visible, so quality state survives app restarts and powers Briefing.

**Implementation:**

- Add `quality_check_runs`:

```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
source_name TEXT
started_at TEXT NOT NULL
finished_at TEXT
status TEXT NOT NULL
total INTEGER DEFAULT 0
passed INTEGER DEFAULT 0
failed INTEGER DEFAULT 0
skipped INTEGER DEFAULT 0
```

- Add `quality_check_results`:

```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
run_id INTEGER NOT NULL
rule_id TEXT NOT NULL
model_name TEXT NOT NULL
column_name TEXT
passed INTEGER NOT NULL
observed_value TEXT
message TEXT
severity TEXT
created_at TEXT NOT NULL DEFAULT (datetime('now'))
```

- Persist quality report results from `run_quality_checks` and full pipeline sync.
- Update Briefing to use durable results rather than contract status assumptions.

**Acceptance criteria:**

- Failed check creates quality_check_failed event.
- Recovered check creates quality_check_recovered event.
- Briefing shows failed quality checks after API restart.

**Verification:**

- Unit: quality history store tests.
- Integration: force failing contract, run quality, assert Briefing priority.

### Story 1.6: Frontend Sync State

**User story:** As a user, I need the Sources page to show what happened during sync, so I know whether Headwater is watching the source.

**Implementation:**

- Update Sources page to show:
  - current sync status;
  - latest run duration;
  - latest run table/profile counts;
  - schema drift count;
  - quality issue count;
  - recent event list.
- Update Briefing to aggregate from normalized events and quality history.

**Acceptance criteria:**

- A source can be idle, syncing, healthy, warning, or error.
- Sync button is disabled while sync is running.
- Error states include last error summary.
- Briefing links to the source or affected artifact.

**Verification:**

- Frontend type check/build.
- Manual UI sync smoke test.

### Phase 1 Gate

- [ ] `/api/sources/{name}/test` tests connection only.
- [ ] `/api/sources/{name}/sync` runs real source-scoped workflow.
- [ ] `sync_runs` records success and failure.
- [ ] Normalized events exist and power Briefing/Sources.
- [ ] Schema drift is created on real schema change.
- [ ] Quality failures persist and appear in Briefing.
- [ ] Existing E2E pipeline still passes.

Suggested commands:

```bash
cd headwater
uv run pytest tests/test_sources.py tests/test_drift.py tests/test_quality.py tests/test_api.py
uv run pytest tests/test_e2e_pipeline.py
```

---

## 6. Phase 2: Capability-Aware Connector Platform

**Goal:** make “connect to any data source” credible by standardizing connector capabilities and behavior.

**Primary outcome:** every connector declares what it can safely do, and the pipeline adapts rather than assuming uniform behavior.

### Story 2.1: Connector Capability Types

**User story:** As a contributor, I need a stable connector contract, so adding new sources does not require changing pipeline internals.

**Implementation:**

- Add connector domain models:
  - `ConnectorCapabilities`
  - `TableRef`
  - `ColumnRef`
  - `ColumnProfileSpec`
  - `TableProfile`
  - `SourceProfile`
- Update base connector protocol to include:
  - `test`
  - `capabilities`
  - `list_schemas`
  - `list_tables`
  - `list_columns`
  - `list_constraints`
  - `estimate_row_count`
  - `profile_table`
  - `sample_arrow`
  - `execute_readonly`
  - `close`

**Acceptance criteria:**

- Protocol supports file, database, warehouse, and catalog-style sources.
- Unsupported capability raises a clear, typed exception.
- Capabilities are exposed through API connector catalog.

**Verification:**

- Unit: capability serialization tests.
- Static: all built-in connectors satisfy protocol.

### Story 2.2: Connector Contract Test Suite

**User story:** As a maintainer, I need one behavior suite for connectors, so new connectors cannot silently break discovery.

**Implementation:**

- Add reusable tests in `tests/connectors/contract.py`.
- Define fixture contract:
  - connector instance;
  - sample config;
  - expected tables;
  - expected columns.
- Verify:
  - connection test behavior;
  - capabilities are accurate;
  - table listing works;
  - column listing works;
  - row count is non-negative or explicitly unsupported;
  - profile shape is stable;
  - sampling respects limits where supported;
  - `close` is idempotent.

**Acceptance criteria:**

- JSON, CSV, and Postgres use the same contract tests.
- Adding DuckDB, SQLite, or MySQL requires only fixture-specific setup.

**Verification:**

- `uv run pytest tests/test_connectors.py tests/connectors`

### Story 2.3: Refactor Existing Connectors

**User story:** As a user, I need existing connectors to keep working after the protocol change.

**Implementation:**

- Refactor:
  - `json_loader.py`
  - `csv_loader.py`
  - `postgres_loader.py`
  - `registry.py`
- Replace pipeline-specific type inference with registry-driven detection.
- Add `supported`, `preview`, and `planned` status to connector catalog.

**Acceptance criteria:**

- Existing JSON, CSV, and Postgres workflows pass.
- Connector catalog distinguishes supported and planned connectors.
- Unsupported connector attempts fail before storing bad source config.

**Verification:**

- Existing connector tests.
- API connector catalog test.
- Manual register/sync for supported source.

### Story 2.4: DuckDB Connector

**User story:** As a data professional, I need to connect an existing DuckDB database, so local analytics datasets are first-class sources.

**Implementation:**

- Add `duckdb_loader.py`.
- Support:
  - file path config;
  - schema/table listing;
  - column listing;
  - aggregate profiling;
  - bounded Arrow sample;
  - read-only query execution.

**Acceptance criteria:**

- DuckDB connector can profile a sample `.duckdb` file.
- Connector does not mutate source database.
- Tables are source-scoped in metadata.

**Verification:**

- Unit/integration connector tests with temporary DuckDB file.

### Story 2.5: SQLite Connector

**User story:** As a small team, I need to connect SQLite databases, so lightweight operational data can be discovered without migration.

**Implementation:**

- Add `sqlite_loader.py`.
- Support:
  - file path config;
  - table listing;
  - PRAGMA column inspection;
  - basic constraints;
  - aggregate profiling;
  - bounded Arrow sample.

**Acceptance criteria:**

- SQLite source sync discovers tables and profiles columns.
- Connector handles empty tables safely.

**Verification:**

- Integration tests using temp SQLite database.

### Story 2.6: MySQL Connector

**User story:** As a data engineer, I need MySQL support, so Headwater can cover common OLTP sources beyond Postgres.

**Implementation:**

- Add `mysql_loader.py`.
- Use optional dependency for MySQL driver.
- Implement safe information_schema discovery.
- Implement pushdown aggregate profiling.
- Implement bounded sample query.

**Acceptance criteria:**

- MySQL connector is preview until integration tests run against real MySQL in CI/local Docker.
- Missing driver error is actionable.
- Query timeout and read-only safety are enforced.

**Verification:**

- Unit tests for DSN parsing and error wrapping.
- Optional Docker integration test.

### Story 2.7: Credential Safety

**User story:** As an operator, I need credentials protected, so connecting production sources does not leak secrets in logs or API responses.

**Implementation:**

- Redact DSNs in logs.
- Redact API responses.
- Add source config storage strategy:
  - environment reference preferred;
  - encrypted local value if key configured;
  - explicit warning for plaintext local dev.
- Add helper `redact_secret`.

**Acceptance criteria:**

- No API response returns raw password.
- No event payload stores raw credentials.
- Tests cover URI redaction.

**Verification:**

- `rg -n "postgresql://.*:.*@"` on generated logs/fixtures where feasible.
- Unit tests for redaction.

### Phase 2 Gate

- [ ] Connector capability protocol implemented.
- [ ] JSON, CSV, Postgres refactored and passing.
- [ ] DuckDB connector passing.
- [ ] SQLite connector passing.
- [ ] MySQL connector preview implemented with clear dependency handling.
- [ ] Connector catalog reports supported/preview/planned statuses.
- [ ] Credential redaction tests pass.

Suggested commands:

```bash
cd headwater
uv run pytest tests/test_connectors.py tests/test_postgres_connector.py
uv run pytest tests/test_sources.py tests/test_api.py
uv run ruff check .
```

---

## 7. Phase 3: Model and Pipeline Maturity

**Goal:** help teams understand and improve model and pipeline maturity over time.

**Primary outcome:** source changes show affected models/contracts and what needs review.

### Story 3.1: Model Lifecycle States

**User story:** As an analytics engineer, I need model state to reflect review and operational status, so I know what can be trusted.

**Implementation:**

- Extend model status lifecycle:
  - `drafted`
  - `review_pending`
  - `approved`
  - `materialized`
  - `monitored`
  - `invalidated`
  - `deprecated`
  - `rejected`
- Keep automatic mart approval only for explicit demo mode.
- Add migration for existing statuses.

**Acceptance criteria:**

- Mart models generated by normal sync are review_pending.
- Demo pipeline can still auto-approve when explicitly requested.
- Approved/materialized status updates on execution.

**Verification:**

- Unit tests for state transitions.
- API tests for approve/reject/execute.

### Story 3.2: Model Review Records

**User story:** As a lead, I need an audit trail of model review decisions, so model trust is inspectable.

**Implementation:**

- Add `model_reviews`:

```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
model_name TEXT NOT NULL
source_name TEXT
reviewer TEXT
decision TEXT NOT NULL
reason TEXT
diff_summary TEXT
payload_json TEXT
created_at TEXT NOT NULL DEFAULT (datetime('now'))
```

- Record approve, reject, edit, and deprecate actions.
- Emit corresponding events.

**Acceptance criteria:**

- Approving a model creates review row and event.
- Rejection reason is persisted.
- UI can show review history for a model.

**Verification:**

- Metadata tests.
- API model approve/reject tests.

### Story 3.3: Model Impact Analysis

**User story:** As a data engineer, I need to know which models are affected by source drift, so I can review only what matters.

**Implementation:**

- Add model dependency index:
  - source table;
  - source column;
  - generated model;
  - contract.
- Add `model_impacts` table.
- On schema/profile drift, compute affected models/contracts.
- Mark affected models `invalidated` or `review_pending` based on severity.

**Acceptance criteria:**

- Added source column does not invalidate unrelated models.
- Removed/renamed/type-changed referenced column invalidates affected model.
- Impact event links drift report and affected model.

**Verification:**

- Unit tests with synthetic dependency graph.
- Integration test: modify source fixture, sync, assert model impact.

### Story 3.4: Contract Lifecycle

**User story:** As an operator, I need contract state to reflect real monitoring, so quality maturity is meaningful.

**Implementation:**

- Extend contract states:
  - `proposed`
  - `observing`
  - `enforced`
  - `failing`
  - `recovered`
  - `disabled`
- Update quality checker to persist runtime status separately from authoring status if needed.
- Add false-positive feedback loop.

**Acceptance criteria:**

- Proposed contracts enter observing before enforcement.
- Failing contract is visible in Quality and Briefing.
- Marking false positive updates precision metrics.

**Verification:**

- Quality lifecycle tests.
- Confidence metric tests.

### Story 3.5: Rerun Planner

**User story:** As a user, I need Headwater to tell me what to rerun after a change, so I do not waste time rerunning everything blindly.

**Implementation:**

- Add service `RerunPlanner`.
- Inputs:
  - drift report;
  - model impacts;
  - contract impacts;
  - source capabilities.
- Output:
  - regenerate descriptions;
  - regenerate models;
  - rerun contracts;
  - human review required;
  - no action needed.

**Acceptance criteria:**

- Drift report produces actionable rerun plan.
- Rerun banner links to plan.
- Plan avoids unaffected assets.

**Verification:**

- Unit tests for common drift scenarios.
- UI manual check for rerun banner.

### Story 3.6: Project Maturity Scoring

**User story:** As a data lead, I need to see maturity blockers, so the team can prioritize work.

**Implementation:**

- Compute maturity from:
  - source health;
  - profile coverage;
  - semantic review;
  - model review;
  - contract coverage;
  - quality pass rate;
  - drift status;
  - recent review.
- Update Project Health page to show:
  - maturity stage;
  - score;
  - top blockers;
  - next best actions.

**Acceptance criteria:**

- Score increases as tables are profiled, descriptions confirmed, models approved, and contracts pass.
- Score decreases or blocks on severe drift/failing quality.
- Briefing links to maturity blockers.

**Verification:**

- Unit tests for scoring.
- API tests for project progress.
- Manual UI check.

### Phase 3 Gate

- [ ] Normal mart models require review.
- [ ] Model review history is durable.
- [ ] Source drift computes model/contract impact.
- [ ] Contract lifecycle reflects observed quality results.
- [ ] Rerun planner produces targeted next actions.
- [ ] Project Health shows maturity and blockers.

Suggested commands:

```bash
cd headwater
uv run pytest tests/test_models.py tests/test_generator.py tests/test_quality.py tests/test_drift.py tests/test_confidence.py
uv run pytest tests/test_e2e_pipeline.py
```

---

## 8. Phase 4: Evidence-Backed Insight Engine

**Goal:** generate insights that are useful because they are traceable to data evidence.

**Primary outcome:** every insight has evidence, confidence, affected assets, and a recommended action.

### Story 4.1: Insight Entity

**User story:** As an analyst, I need insights to be persisted and reviewable, so they can improve over time.

**Implementation:**

- Add `insights` table:

```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
source_name TEXT
insight_type TEXT NOT NULL
severity TEXT NOT NULL
confidence REAL NOT NULL
artifact_type TEXT
artifact_id TEXT
headline TEXT NOT NULL
detail TEXT NOT NULL
evidence_json TEXT NOT NULL
recommended_action TEXT
status TEXT NOT NULL DEFAULT 'open'
created_at TEXT NOT NULL DEFAULT (datetime('now'))
```

- Add `insight_reviews`:

```sql
id INTEGER PRIMARY KEY AUTOINCREMENT
insight_id INTEGER NOT NULL
decision TEXT NOT NULL
reason TEXT
created_at TEXT NOT NULL DEFAULT (datetime('now'))
```

**Acceptance criteria:**

- Insights are durable.
- Each insight has structured evidence.
- Human feedback is stored.

**Verification:**

- Metadata CRUD tests.
- API list/review insight tests.

### Story 4.2: Profile-Based Insight Rules

**User story:** As a data professional, I need Headwater to surface obvious data problems automatically, so I can focus review time.

**Implementation:**

- Implement deterministic rules for:
  - high-null columns;
  - sudden null-rate increase;
  - cardinality shift;
  - duplicate key candidates;
  - constant columns;
  - unexpected new categories;
  - outlier numeric distributions;
  - freshness breach;
  - potential PII exposure.

**Acceptance criteria:**

- Each rule returns evidence fields needed to reproduce the claim.
- Each rule has severity and confidence calibration.
- Rules avoid emitting duplicate noisy insights.

**Verification:**

- Unit tests per rule.
- Golden fixture tests for expected insights.

### Story 4.3: Relationship and Lineage Insights

**User story:** As a modeler, I need Headwater to detect relationship risks, so joins and downstream models are safer.

**Implementation:**

- Implement insights for:
  - referential integrity decay;
  - orphan records;
  - missing expected FK;
  - many-to-many ambiguity;
  - nullable FK warnings;
  - model depends on unstable table.

**Acceptance criteria:**

- Relationship insights cite tables, columns, counts, and integrity percentages.
- Cross-table insights only use valid join paths.

**Verification:**

- Unit tests with synthetic relationships.
- Integration tests against sample dataset.

### Story 4.4: Insight Review Feedback

**User story:** As a user, I need to mark insights as useful or false positive, so Headwater gets less noisy.

**Implementation:**

- Add UI actions:
  - useful;
  - false positive;
  - irrelevant;
  - needs investigation;
  - resolved.
- Feed review outcomes into confidence metrics.
- Suppress repeated false-positive pattern when evidence has not materially changed.

**Acceptance criteria:**

- Review action persists.
- False positive affects precision metrics.
- Briefing deprioritizes dismissed insights.

**Verification:**

- API tests for insight review.
- UI manual check.

### Story 4.5: Briefing Integration

**User story:** As a user, I need the daily briefing to surface the most important insights, not a raw list of all anomalies.

**Implementation:**

- Add insight prioritization:
  - severity;
  - affected downstream assets;
  - novelty;
  - confidence;
  - unresolved status.
- Briefing includes top insights with evidence links.

**Acceptance criteria:**

- Briefing shows high-impact insights first.
- A dismissed insight does not keep reappearing unless evidence changes.
- Each Briefing insight links to Insights page or affected source/model.

**Verification:**

- Briefing aggregation tests.
- Manual UI review.

### Phase 4 Gate

- [ ] Insight entities persisted.
- [ ] Profile-based insight rules have tests.
- [ ] Relationship insight rules have tests.
- [ ] Insight reviews affect confidence/precision.
- [ ] Briefing uses top unresolved insights.
- [ ] Insights page is evidence-first.

Suggested commands:

```bash
cd headwater
uv run pytest tests/test_insights.py tests/test_explorer.py tests/test_graph_store.py
uv run pytest tests/test_confidence.py tests/test_api.py
```

---

## 9. Phase 5: Semantic Metrics and Decision Readiness

**Goal:** connect technical data maturity to business decision readiness.

**Primary outcome:** Headwater can explain whether named metrics and decisions are safe to use, with evidence and blockers.

### Story 5.1: Metric Entity

**User story:** As a data team, I need first-class metrics, so business definitions are explicit and reviewable.

**Implementation:**

- Add or harden metric metadata:
  - name;
  - display name;
  - formula;
  - owner;
  - source models;
  - dimensions;
  - filters;
  - status;
  - review history.
- Link metrics to models/contracts/columns.

**Acceptance criteria:**

- A metric can be proposed, reviewed, approved, deprecated.
- Metric status changes emit events.
- Metric is traceable to models and source columns.

**Verification:**

- Metadata tests.
- API tests for metric lifecycle.

### Story 5.2: Decision Entity

**User story:** As a data lead, I need to define business decisions that depend on metrics, so maturity has a business target.

**Implementation:**

- Add `decisions` table:

```sql
id TEXT PRIMARY KEY
name TEXT NOT NULL
description TEXT
owner TEXT
cadence TEXT
risk_tolerance TEXT
required_freshness_hours INTEGER
required_quality_pct REAL
metric_ids_json TEXT DEFAULT '[]'
created_at TEXT NOT NULL DEFAULT (datetime('now'))
updated_at TEXT NOT NULL DEFAULT (datetime('now'))
```

**Acceptance criteria:**

- Decision can link to one or more metrics.
- Decision has readiness requirements.
- Decisions can be listed and inspected in API.

**Verification:**

- Metadata CRUD tests.
- API tests.

### Story 5.3: Readiness Scoring

**User story:** As a stakeholder, I need a clear readiness score with blockers, so I know whether to trust data for a decision.

**Implementation:**

- Add scoring service using:
  - source health;
  - freshness;
  - schema drift;
  - statistical drift;
  - quality pass rate;
  - contract precision;
  - lineage completeness;
  - semantic confidence;
  - model review status;
  - metric definition status;
  - recent human review.
- Persist `decision_readiness_scores`.

**Acceptance criteria:**

- Score includes component breakdown.
- Blockers are concrete and actionable.
- Score updates after sync, quality check, model review, and metric review.

**Verification:**

- Unit tests for scoring scenarios.
- Integration test: failing quality lowers readiness.
- Integration test: model approval increases readiness.

### Story 5.4: Decision Brief

**User story:** As an executive or data lead, I need a short brief explaining readiness, risks, and next actions.

**Implementation:**

- Add endpoint:
  - `GET /api/decisions/{id}/brief`
- Brief includes:
  - score;
  - readiness status;
  - metric status;
  - blockers;
  - recent drift;
  - quality summary;
  - recommended actions;
  - evidence links.

**Acceptance criteria:**

- Brief is generated from persisted evidence, not free-form unsupported claims.
- Brief distinguishes safe for exploration vs safe for operational reporting vs safe for board/executive reporting.

**Verification:**

- API tests for generated brief.
- Manual review against seeded fixture.

### Story 5.5: NLQ Guardrails

**User story:** As a user asking questions, I need Headwater to qualify or refuse unsafe answers, so it does not create confident misinformation.

**Implementation:**

- Scope NLQ to:
  - reviewed metadata;
  - approved models;
  - evidence-backed insights;
  - readiness-scored metrics.
- Return answer states:
  - answered;
  - answered_with_caveat;
  - needs_review;
  - refused_low_readiness.
- Add evidence trace to every answer.

**Acceptance criteria:**

- NLQ does not answer decision-critical questions from unreviewed metrics without caveat.
- Low-readiness decisions produce blockers and suggested next steps.
- Every answer includes source/model/metric evidence references.

**Verification:**

- Golden question tests.
- Low-readiness refusal tests.
- Fuzz tests for unsafe/malformed questions.

### Phase 5 Gate

- [ ] Metrics are first-class and reviewable.
- [ ] Decisions are first-class and linked to metrics.
- [ ] Readiness scoring has component breakdown and tests.
- [ ] Decision brief is evidence-backed.
- [ ] NLQ is readiness-aware and guarded.

Suggested commands:

```bash
cd headwater
uv run pytest tests/test_catalog.py tests/test_confidence.py tests/test_explorer.py
uv run pytest tests/golden
```

---

## 10. Phase 6: Exports and Ecosystem

**Goal:** integrate with mature stacks after Headwater has a reliable internal workflow.

**Primary outcome:** reviewed artifacts can be exported deterministically to downstream tools.

### Story 6.1: dbt Export

**User story:** As an analytics engineer, I need reviewed Headwater models exported to dbt, so I can adopt Headwater output in my existing stack.

**Implementation:**

- Add dbt export module.
- Export:
  - reviewed staging models;
  - reviewed mart models;
  - schema YAML descriptions;
  - generic tests for mappable contracts;
  - singular tests for custom contracts.

**Acceptance criteria:**

- Only reviewed/approved artifacts export by default.
- Generated dbt project has deterministic structure.
- SQL references are converted to `ref()` where possible.

**Verification:**

- Export tests against sample data.
- Optional dbt parse if dbt-core installed.

### Story 6.2: Quality Export

**User story:** As a data engineer, I need quality contracts exportable, so existing quality tooling can consume them.

**Implementation:**

- Export to dbt tests first.
- Consider Great Expectations or Soda only if demand is clear.

**Acceptance criteria:**

- Contract export preserves status and review metadata.
- Unsupported contract types are clearly reported, not silently dropped.

**Verification:**

- Contract mapping tests.

### Story 6.3: Notification Hooks

**User story:** As an operator, I need important events delivered outside the UI, so I do not have to watch Headwater continuously.

**Implementation:**

- Add notifier protocol.
- Start with email/webhook or Slack webhook.
- Trigger from normalized events.
- Add severity filters.

**Acceptance criteria:**

- High-severity sync, drift, and quality events can notify.
- Notification failures do not fail the sync run.
- Notification events are logged.

**Verification:**

- Unit tests with fake notifier.
- Manual webhook smoke test if configured.

### Story 6.4: CLI and OpenAPI Parity

**User story:** As a data professional, I need CLI and API parity, so Headwater works in automation and UI workflows.

**Implementation:**

- Add CLI commands:
  - `sources list`
  - `sources test`
  - `sources sync`
  - `events list`
  - `quality history`
  - `models impacts`
  - `decisions brief`
  - `export dbt`
- Ensure FastAPI OpenAPI docs expose equivalent routes.

**Acceptance criteria:**

- CLI can run source sync and print latest briefing summary.
- API and CLI return equivalent core fields.

**Verification:**

- CLI tests.
- Manual CLI smoke test.

### Phase 6 Gate

- [ ] dbt export works for reviewed artifacts.
- [ ] Contract export is deterministic.
- [ ] Notification protocol sends selected events.
- [ ] CLI covers source sync, events, quality history, and export.
- [ ] OpenAPI docs expose all relevant routes.

Suggested commands:

```bash
cd headwater
uv run pytest tests/test_cli.py tests/test_generator.py tests/test_api.py
uv run ruff check .
```

---

## 11. Cross-Phase Verification Matrix

| Area | Phase 0 | Phase 1 | Phase 2 | Phase 3 | Phase 4 | Phase 5 | Phase 6 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Docs match reality | Required | Required | Required | Required | Required | Required | Required |
| Source sync E2E | - | Required | Required | Required | Required | Required | Required |
| Event aggregation | - | Required | Required | Required | Required | Required | Required |
| Drift tests | - | Schema | Schema/profile | Impact | Insight input | Readiness input | Export metadata |
| Quality history | - | Required | Required | Lifecycle | Insight input | Readiness input | Export input |
| Connector contract | - | - | Required | Required | Required | Required | Required |
| Model maturity | - | - | - | Required | Required | Required | Export input |
| Insight evidence | - | - | - | - | Required | Required | Optional export |
| Decision readiness | - | - | - | - | - | Required | CLI/API parity |

---

## 12. Recommended Execution Order

Do not start with connector breadth or UI polish. The critical path is operational state.

1. Archive old plan and align docs.
2. Build source sync service.
3. Add `sync_runs` and normalized `events`.
4. Wire schema drift and quality history.
5. Update Sources and Briefing to use durable state.
6. Stabilize connector protocol.
7. Refactor existing connectors and add DuckDB/SQLite/MySQL.
8. Add model impact and maturity scoring.
9. Add evidence-backed insights.
10. Add metrics, decisions, and readiness scoring.
11. Add exports and notifications.

---

## 13. Rollback and Compatibility Notes

- Keep existing `/api/status`, `/api/sources`, `/api/sync-events`, and `/api/briefing/today` response shape stable where possible.
- If `events` replaces `sync_events`, keep `sync_events` as a compatibility view or route adapter for one release.
- Preserve existing CLI commands while adding source-specific commands.
- Existing metadata databases should migrate in place; destructive metadata resets require explicit user action.
- Demo mode may preserve auto-approval behavior only behind an explicit demo flag.

---

## 14. Final Completion Criteria

The refactor is complete when:

- A user can register a supported source and run a real sync.
- Briefing explains current priorities from durable evidence.
- Sources shows latest sync, drift, quality, and run state.
- Connectors declare capabilities and pass shared behavior tests.
- Model and contract maturity is inspectable.
- Insights are evidence-backed and reviewable.
- Metrics and decisions have readiness scores.
- NLQ is constrained by readiness and evidence.
- Reviewed artifacts can be exported.
- Public docs no longer overclaim current capabilities.

---

*This plan intentionally favors operational correctness and trust over breadth. Headwater becomes more useful by making connected data understandable, monitored, and decision-ready before it tries to be everywhere.*
