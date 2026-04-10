# Headwater Phase 1 MVP — User Stories

## Conventions

- **P0** — Blocker: gates any external exposure
- **P1** — Core MVP: must ship before "Phase 1 complete"
- **P2** — Enhances MVP: ships if capacity allows
- Effort: **S** < 1 day | **M** 1–3 days | **L** 3–7 days | **XL** > 1 week

---

## EP-01: Source Connectors

> **Architectural decision:** Two source archetypes exist — `generate` (raw/OLTP sources: profile
> in-place, generate deployable SQL models) and `observe` (existing golden layer: document and
> govern what's already there, zero data movement). The connector abstraction is designed for both
> in Phase 1. Only `generate` mode is implemented in Phase 1. `observe` mode connectors
> (Snowflake, BigQuery, Redshift) are Phase 2. This avoids a breaking interface change later
> without overloading the Phase 1 scope.

### US-100: Connector Abstraction — Two-Mode BaseConnector | P0 | S
- **As a** platform developer
- **I want** the `BaseConnector` interface and `SourceConfig` model to support both `generate` and `observe` modes from the start
- **So that** adding `observe` mode connectors in Phase 2 requires no breaking changes to the interface or call sites
- **Acceptance criteria:**
  - [ ] `SourceConfig` gains `mode: Literal["generate", "observe"] = "generate"`
  - [ ] `BaseConnector` defines two abstract methods: `profile(table) -> ColumnStats` (runs aggregate queries in-place, returns stats only — no data rows) and `sample(table, n) -> pa.Table` (returns Arrow batch of N rows for local DuckDB validation)
  - [ ] `generate` mode connectors implement both methods; `observe` mode connectors implement only `profile()` (sample raises `NotImplementedError` with a clear message)
  - [ ] Existing JSON and CSV connectors updated to implement the new interface (backwards compatible — their current load behavior maps to `sample()`)
  - [ ] `SourceConfig.mode` stored in `metadata.db` sources table
- **Technical notes:** No new dependencies. Pure interface change to `connectors/base.py` and `core/models.py`. All existing connector tests must pass after the refactor.

### US-101: Postgres Connector — Pushdown Profiling, No Bulk Copy | P0 | L
- **As a** data engineer
- **I want** to run `headwater discover --source postgres://user:pass@host:5432/dbname` against a large OLTP database without Headwater copying all the data locally
- **So that** I can profile and document a production database with millions of rows without performance or storage concerns
- **Acceptance criteria:**
  - [ ] `PostgresConnector` implements `BaseConnector` in `generate` mode
  - [ ] `profile(table)`: runs aggregate SQL directly in Postgres (`SELECT COUNT(*), MIN(x), MAX(x), COUNT(DISTINCT x) ...`) and returns only stats — no rows transferred
  - [ ] `sample(table, n)`: fetches at most N rows (default 10k) via Arrow using `adbc-driver-postgresql`; used only for local DuckDB validation of generated SQL
  - [ ] DuckDB's `postgres_scan` extension used for schema introspection (`information_schema.columns`) — not for data bulk load
  - [ ] Generated staging and mart SQL targets Postgres table names (not DuckDB-local names) so the output is deployable back to the source or to a warehouse
  - [ ] `SourceConfig.type == "postgres"` accepted by connector registry
  - [ ] Integration test: spins up local Postgres via `pytest-docker`, verifies profile stats match actual data without bulk copy
  - [ ] Works in air-gapped (no LLM) mode
- **Technical notes:** `uv add adbc-driver-postgresql`. Profiling queries run via `psycopg2` (stdlib-friendly). Sample uses ADBC Arrow transfer. Schema names preserved: `public.orders` → `public_orders` in generated SQL. Never register a full Postgres table into DuckDB — only the sample Arrow batch.

### US-102: Postgres Connection UX and Error Feedback | P1 | S
- **As a** data engineer
- **I want** clear, actionable error messages when my Postgres connection fails
- **So that** I can diagnose and fix connection issues without reading stack traces
- **Acceptance criteria:**
  - [ ] Human-readable message for: host unreachable, auth failure, permission denied
  - [ ] Permission denied on a table logs a warning and skips the table (does not abort)
  - [ ] SSL mode configurable via DSN query param (`?sslmode=require`)
  - [ ] `headwater status` shows connected sources with connector type

### US-103: Unified CLI `discover` Command | P1 | S
- **As a** data engineer
- **I want** a single `headwater discover` that auto-detects connector type from the source argument
- **So that** the CLI experience is consistent regardless of source type
- **Acceptance criteria:**
  - [ ] `headwater discover --source postgres://...` works
  - [ ] `headwater discover --source /path/to/csv/dir` works (backwards compatible)
  - [ ] Type auto-detected from URI scheme; `--type` flag overrides
  - [ ] `--name` flag sets source name (defaults to hostname for DB, dir name for files)
  - [ ] `--mode generate|observe` flag accepted (defaults to `generate`); `observe` raises a clear "not yet implemented" message in Phase 1

### US-104: Observe Mode Connectors — Snowflake, BigQuery, Redshift | PHASE 2 | XL
> Deferred. Target user is a data team that already has a mature warehouse with existing marts.
> Zero data movement — profiling and quality contracts run entirely in the warehouse.
> Requires warehouse-specific connectors, OAuth/service-account auth, and SQL dialect handling
> per warehouse. Interface is designed in US-100; implementation is Phase 2.

---

## EP-02: Trust & Locks

### US-201: Semantic Locks — Preserve Approved Descriptions | P0 | M
- **As a** data engineer
- **I want** descriptions I edit or approve to be locked so re-runs don't overwrite them
- **So that** my domain knowledge accumulates instead of being erased
- **Acceptance criteria:**
  - [ ] `columns` table gains `locked BOOLEAN DEFAULT FALSE` and `locked_at TEXT`
  - [ ] `tables` table gains equivalent columns
  - [ ] `PATCH /api/columns/{source}/{table}/{column}` sets `locked = true`
  - [ ] Analyzer skips locked columns; passes locked description as ground truth in LLM prompt
  - [ ] Re-run log prints `Skipped enrichment for N locked column(s)`
  - [ ] `decisions` table receives row on lock set
  - [ ] Integration test: run → edit → lock → re-run → verify locked description unchanged
- **Technical notes:** Guard in `_enrich_table_with_llm` and `_analyze_heuristic`. Metadata schema migration required.

### US-202: Explicit Unlock Action | P1 | S
- **As a** data engineer
- **I want** to unlock a locked description to allow re-enrichment on the next run
- **Acceptance criteria:**
  - [ ] `PATCH /api/columns/.../` accepts `{"locked": false}`
  - [ ] Unlock writes a `decisions` row with `action='unlocked'`
  - [ ] CLI: `headwater unlock --source <name> --table <t> --column <c>` (or `--all`)

### US-203: Re-run Additive Updates, Not Full Overwrites | P1 | M
- **As a** data engineer
- **I want** re-running discovery to add new tables/columns and update changed ones without dropping existing data
- **Acceptance criteria:**
  - [ ] Existing `description` and `locked` fields not reset on re-insert
  - [ ] Removed tables marked with `removed_in_run_id` (not deleted)
  - [ ] Re-run summary: N unchanged, M updated, K added, J removed
- **Technical notes:** Separate schema update writes from description/enrichment writes.

### US-204: Lock Status Visible in Discovery Browser | P2 | M
- Lock icon next to locked descriptions; inline edit auto-locks on save; API includes `locked` and `locked_at` per column

---

## EP-03: Confidence & Learning

### US-301: Record Every Review Decision | P0 | S
- **As a** data engineer
- **I want** every approve/reject/edit action recorded in the `decisions` table
- **So that** Headwater has raw data to compute confidence metrics
- **Acceptance criteria:**
  - [ ] `decisions` table gains `payload_json TEXT` column (before/after values)
  - [ ] Approve/reject model writes decisions row
  - [ ] Description edit writes decisions row with original and new text
  - [ ] Contract status change writes decisions row
- **Technical notes:** Add `record_decision(store, artifact_type, artifact_id, action, payload)` helper to `core/metadata.py`. Hard prerequisite for US-302, US-303.

### US-302: Description Acceptance Rate | P1 | S
- **As a** data engineer
- **I want** to see what percentage of auto-generated descriptions I accepted without editing
- **Acceptance criteria:**
  - [ ] `GET /api/confidence` returns `description_acceptance_rate: float`
  - [ ] Minimum 5 decisions before surfacing; returns `null` with reason below threshold
  - [ ] Scoped per source: `?source=<name>`
  - [ ] Dashboard shows metric as stat card

### US-303: Model Edit Distance + Contract Precision | P1 | M
- **As a** data engineer
- **I want** to see how much I modify mart models and what percentage of contract alerts are real
- **Acceptance criteria:**
  - [ ] `GET /api/confidence` includes `model_edit_distance_avg: float | null`
  - [ ] `GET /api/confidence` includes `contract_precision: float | null`
  - [ ] Each metric shows sample size N
- **Technical notes:** Edit distance via `difflib.SequenceMatcher`. Contract precision requires US-304.

### US-304: Mark Quality Contract Alerts as False Positives | P1 | S
- **As a** data engineer
- **I want** to mark a contract alert as a false positive
- **Acceptance criteria:**
  - [ ] "False Positive" button in quality viewer per failing contract
  - [ ] `POST /api/contracts/{rule_id}/mark-false-positive` writes decisions row
  - [ ] Quality viewer shows false-positive rate per contract as tooltip
  - [ ] Marking does not change contract status

### US-305: Confidence Dashboard Panel | P2 | M
- All three metrics in one dashboard card; "Building baseline" placeholder below minimum sample; trend indicator vs previous run

---

## EP-04: Drift Detection

### US-401: Schema Snapshot Storage | P1 | S
- JSON snapshot of all table/column metadata per run stored in new `schema_snapshots` table; prerequisite for US-402

### US-402: Schema Drift Detection | P1 | M
- **As a** data engineer
- **I want** Headwater to compare the current run against the previous snapshot and tell me what changed
- **Acceptance criteria:**
  - [ ] `compare_schemas(snapshot_a, snapshot_b)` in new `headwater/drift/schema.py` returns `SchemaDiff`
  - [ ] SchemaDiff: added/removed tables, added/removed columns, type-changed columns, nullability-changed columns
  - [ ] Stored in `drift_reports` table; CLI prints summary; `GET /api/drift`
  - [ ] First run (no previous snapshot) handled gracefully

### US-403: Drift Alerts in UI | P1 | M
- Dashboard banner when drift detected; detail view with color-coded changes (added=green, removed=red, type-changed=amber); dismissable

### US-404: Data Freshness Monitoring | P2 | M
- Track max timestamp for temporal columns across runs; warn when data stops advancing; configurable staleness threshold (default: 2x typical inter-run interval)

---

## EP-05: Mart Intelligence

### US-501: Domain-Agnostic Mart Pattern Library | P0 | L
- **As a** data engineer
- **I want** Headwater to propose mart models based on what it actually discovers, not hard-coded templates
- **So that** I get useful mart proposals regardless of my domain
- **Acceptance criteria:**
  - [ ] Hard-coded `_MART_DEFINITIONS` replaced by archetype-based pattern matching
  - [ ] Archetypes: `aggregation`, `funnel`, `period_comparison`, `entity_summary`, `cohort`, `bridge`
  - [ ] Matching driven by detected semantic types + relationship graph
  - [ ] Any source with temporal columns gets a `period_comparison` proposal
  - [ ] Any source with metric columns + FK to dimension gets an `entity_summary` proposal
  - [ ] All proposals remain `status='proposed'`
  - [ ] Test: source with no relationships produces zero mart proposals
- **Technical notes:** `PatternMatcher` class in `generator/marts.py`; takes `DiscoveryResult`, yields `MartCandidate` objects, rendered to `GeneratedModel` via Jinja2.

### US-502: LLM-Enhanced Mart Proposals | P2 | M
- LLM generates mart names, descriptions, clarifying questions; SQL stays heuristic-template-based; heuristic fallback always present; logs to `llm_audit_log`

### US-503: Mart Proposal Quality Gate | P1 | S
- Minimum evidence thresholds: >=2 relationships, >=1 metric column, >=100 rows; configurable via `HeadwaterSettings`; test verifies no-relationship source = zero proposals

---

## EP-06: Security & Stability (All P0)

### US-601: Fix `eval()` in quality/checker.py | P0 | S
- Remove `eval()` at line 150; replace with `re`-based parser for supported operators: `>=`, `<=`, `>`, `<`, `==`, `BETWEEN ... AND ...`; raise `ContractExpressionError` for invalid formats; all 209 existing tests must pass

### US-602: Fix SQL Viewer Bug in Review Queue | P0 | S
- Fetch `ModelDetail` (including `sql`) when Review Queue tab activates; `SqlViewer` in `models/page.tsx` receives actual SQL not placeholder comment; error state shown if fetch fails

### US-603: Populate LLM Audit Log | P0 | M
- `AnthropicProvider.analyze()` writes to `llm_audit_log` after every call; inject `MetadataStore` via provider factory `get_provider(settings, store=store)`; `GET /api/audit` returns last 100 entries; integration test verifies at least one row per analyzed table

### US-604: Fix Silent LLM SQL Generation Failure | P0 | S
- Recovery attempt for non-JSON-wrapped responses; WARNING log with first 200 chars of raw response; strengthen system prompt to require JSON; unit test for plain SQL response path

---

## EP-07: Onboarding

### US-701: `headwater demo` Polish | P1 | S
- Explanatory callouts at each pipeline step; "What happened" summary using Rich panels; ends with next-step instructions; completes in under 60 seconds

### US-702: Empty State Guidance in UI | P1 | S
- Contextual empty states on all pages; explains what the tool does and what CLI command to run next; consistent design system styling

### US-703: Source Connection Setup in UI | P2 | L
- Sources page with list, re-run button, Connect New Source form, test-connect validation

### US-704: `headwater init` Setup Wizard | P2 | M
- Interactive prompts for source type, DSN, LLM mode; validates connection before proceeding; writes `~/.headwater/config.yaml`

---

## Story Summary

| Priority | Story | Effort |
|---|---|---|
| P0 | US-100 Two-Mode BaseConnector Abstraction | S |
| P0 | US-101 Postgres Connector (pushdown, no bulk copy) | L |
| P0 | US-201 Semantic Locks | M |
| P0 | US-301 Record Decisions | S |
| P0 | US-501 Domain-Agnostic Mart Patterns | L |
| P0 | US-601 Fix eval() | S |
| P0 | US-602 Fix SQL Viewer | S |
| P0 | US-603 LLM Audit Log | M |
| P0 | US-604 Fix Silent LLM Failure | S |
| P1 | US-102 Postgres Connection UX | S |
| P1 | US-103 Unified CLI discover | S |
| P1 | US-202 Semantic Lock Unlock | S |
| P1 | US-203 Re-run Additive Updates | M |
| P1 | US-302 Description Acceptance Rate | S |
| P1 | US-303 Model Edit Distance + Contract Precision | M |
| P1 | US-304 Mark False Positives | S |
| P1 | US-401 Schema Snapshot Storage | S |
| P1 | US-402 Schema Drift Detection | M |
| P1 | US-403 Drift Alerts in UI | M |
| P1 | US-503 Mart Proposal Quality Gate | S |
| P1 | US-701 headwater demo Polish | S |
| P1 | US-702 Empty State Guidance | S |
| P2 | US-204 Lock Status in UI | M |
| P2 | US-305 Confidence Dashboard Panel | M |
| P2 | US-404 Data Freshness Monitoring | M |
| P2 | US-502 LLM-Enhanced Mart Proposals | M |
| P2 | US-703 Source Connection UI | L |
| P2 | US-704 headwater init Wizard | M |

---

## Recommended 4-Week Build Sequence

**Week 1 — Unblock everything (P0):** Fix eval(), fix SQL viewer bug, fix silent LLM failure, start populating audit log, start Postgres connector, start semantic locks, start decision recording.

**Week 2 — Complete P0, begin core P1:** Finish Postgres + error UX, finish semantic locks + unlock, finish audit log, begin domain-agnostic mart patterns, unified CLI discover, schema snapshots.

**Week 3 — Confidence + Drift:** Decision recording complete, acceptance rate, schema diff, drift alerts, mart quality gate, false positive marking.

**Week 4 — P1 polish + P2 starts:** Model edit distance, re-run additive updates, demo polish, empty states, confidence dashboard.

---

## Critical Files for Implementation

- `headwater/quality/checker.py:150` — eval() fix (US-601)
- `ui/src/app/models/page.tsx:549` — SQL viewer bug (US-602)
- `headwater/generator/marts.py` — domain-agnostic mart patterns (US-501)
- `headwater/core/metadata.py` — semantic locks, decisions, snapshots, confidence metrics (US-201, US-301, US-401)
- `headwater/analyzer/llm.py` — audit log population, silent failure fix (US-603, US-604)
