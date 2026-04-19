# Headwater Strategic Implementation Plan

**Date:** April 2026
**Author:** Expert Data Architect & Developer
**Context:** Post-CTO evaluation -- closing the gap between press release and product

---

## Guiding Principles

1. **Every wave ships a complete, testable vertical.** No wave ends with "backend done, UI pending."
2. **Trust before features.** Wave 0 fixes the credibility gap the CTO flagged.
3. **Correctness before connectors.** Explore insights must be statistically rigorous and
   numerically correct before we pipe more data sources into the system. A sloppy analysis
   layer multiplied by 10 connectors is 10x sloppy.
4. **Resilience is not optional.** Both expert reviews demanded circuit breakers. We build them.
5. **Each wave has its own E2E test proving the feature works end-to-end.**
6. **Connectors are mechanical, insights are the product.** Connector expansion (Snowflake,
   Redshift, BigQuery, catalog integrations) follows after the analysis layer earns trust.

---

## Current State Summary

| Area | Status | Evidence |
|------|--------|----------|
| Core pipeline (discover -> quality) | Working | 84 tests pass, E2E heartbeat in ~53s |
| Connectors | 4 of 10+ claimed | Postgres, JSON, CSV, Parquet |
| Plugin system | Does not exist | Press release claims 7 plugin types |
| dbt/SQLMesh export | Does not exist | Press release claims it |
| Observe mode | Explicitly deferred | CLI rejects with "Phase 2" |
| UI | Stabilized | ErrorBoundary + ToastProvider added (Wave 0.4) |
| Resilience | Health check added | `/api/health` with component status (Wave 0.3) |
| Lint | 0 errors | Fixed in Wave 0.1 |
| E2E integration test | Passing | `test_e2e_pipeline.py` -- 23 tests, 9 stages (Wave 0.2) |
| Explore correctness | **88.9% (Wave E1)** | 16/18 golden answer tests pass (up from 55.6% baseline) |
| Press release | Versioned | "Current Availability (v0.3)" section added (Wave 0.5) |

## Completion Log

| Wave | Completed | Key Results |
|------|-----------|-------------|
| 0.1 | 2026-04-18 | 5 lint errors fixed, ruff clean |
| 0.2 | 2026-04-18 | E2E heartbeat: 23 tests across 9 pipeline stages, all pass |
| 0.3 | 2026-04-18 | `/api/health` endpoint + 3 tests (metadata, DuckDB, LLM, graph components) |
| 0.4 | 2026-04-18 | ErrorBoundary, ToastProvider, toast calls on dashboard/models/settings |
| 0.5 | 2026-04-19 | Press release "Current Availability" section |
| E0.1 | 2026-04-19 | 25 golden answers (5 categories) verified against raw SQL |
| E0.2 | 2026-04-19 | `verify_golden.py` -- 25/25 manual checks pass |
| E0.3 | 2026-04-19 | `test_explore_correctness.py` -- 18 tests, baseline 10/18 (55.6%) |
| E1 | 2026-04-19 | SQL correctness: 16/18 (88.9%). Fixed: decomposer intent matching, planner scalar/COUNT DISTINCT, suggestion priority, ORDER BY alias |

### Baseline Failure Analysis (Wave E0) -- 8 failures, 6 FIXED in E1

**SQL generation (4 failures -- ALL FIXED):**
- ~~Catalog decomposer generates SQL referencing raw tables~~ FIXED: schema resolution
- ~~"avg reading value" returns 608.34~~ FIXED: planner scalar intent, no default dimension
- ~~"max reading value" returns 49302~~ FIXED: decomposer intent mismatch, planner MAX intent
- ~~"complaints per zone" collapses to total COUNT~~ FIXED: suggestion matcher strictness

**Statistical (2 failures -- Wave E2 scope):**
- Known 10x spike in synthetic data not detected as anomaly
- Perfect linear correlation (b=2a+5) not detected by correlation engine

**Visualization (1 failure -- FIXED):**
- ~~"complaints per zone" gets KPI instead of bar~~ FIXED: GROUP BY now works

**Suggestion SQL hints (1 failure -- FIXED):**
- ~~14/30 suggestions fail~~ FIXED: 15/15 pass (schema resolution + suggestion priority)

---

## Wave 0: Trust & Quality Foundation

**Goal:** Close every credibility gap the CTO can verify in 10 minutes.
**Duration:** 1 week
**Deliverable:** A codebase that passes its own stated invariants and has a heartbeat test.

### 0.1 -- Fix Lint Violations (Day 1, 30 min)

**What:** Fix all 5 ruff errors. Add ruff check to a pre-commit or CI gate so this never
regresses.

**Files:**
- `headwater/tests/test_api.py` (the SIM-style ternary issues)
- Add `ruff check .` assertion to CI or Makefile

**Verification:** `uv run ruff check .` returns 0 errors.

### 0.2 -- E2E Heartbeat Test (Day 1-2)

**What:** A single pytest test that runs the full pipeline against sample data and asserts
on every stage output. This is the product's heartbeat -- if this test fails, nothing else
matters.

**File:** `headwater/tests/test_e2e_pipeline.py`

**Flow tested:**
```
Load sample data (8 JSON files)
  -> discover() returns DiscoveryResult with >= 8 tables
    -> profiles exist for every column
    -> relationships detected (>= 5)
  -> analyze() enriches descriptions (heuristic mode)
  -> build_catalog() returns SemanticCatalog with metrics + dimensions
  -> generate_staging_models() returns >= 8 models, all auto-approved
  -> generate_mart_models() returns >= 1 proposed model
  -> generate_contracts() returns >= 10 contracts
  -> run_models() executes approved staging models successfully
  -> check_contracts() returns QualityReport with pass rate > 0
```

**Assertions:**
- Every stage produces non-empty output
- Staging model count == table count
- All staging executions succeed (row_count > 0)
- Quality report has zero checker errors (contract violations are OK, checker crashes are not)
- Total wall-clock time < 60 seconds

**Verification:** `uv run pytest tests/test_e2e_pipeline.py -v` passes.

### 0.3 -- Health Check Endpoint (Day 2)

**What:** `GET /api/health` returns component-level status.

**Response:**
```json
{
  "status": "healthy",
  "components": {
    "metadata_store": "ok",
    "analytical_engine": "ok",
    "llm_provider": "not_configured",
    "graph_store": "ok"
  },
  "version": "0.3.0",
  "uptime_seconds": 1234
}
```

**Files:**
- `headwater/headwater/api/routes/health.py` (new, ~40 lines)
- `headwater/headwater/api/app.py` (register route)
- `headwater/tests/test_api.py` (add test)

**Verification:** `curl localhost:8000/api/health` returns 200.

### 0.4 -- UI Error Boundary & Toast System (Day 2-3)

**What:** Prevent full-page crashes and give users feedback on actions.

**Components added:**
- `ui/src/components/error-boundary.tsx` -- React error boundary with fallback UI and
  "Retry" button
- `ui/src/components/toast.tsx` -- Minimal toast notification system (success/error/info,
  auto-dismiss after 4s). No external dependency -- build a lightweight ~80-line component
  using React portal + CSS transition.

**Integration:**
- Wrap `layout.tsx` children in `<ErrorBoundary>`
- Add `<ToastProvider>` to layout
- Add toast calls on: model approve/reject, pipeline complete/fail, settings save,
  export actions

**Files:**
- `ui/src/components/error-boundary.tsx` (new, ~60 lines)
- `ui/src/components/toast.tsx` (new, ~80 lines)
- `ui/src/app/layout.tsx` (wrap children)
- `ui/src/app/page.tsx` (add toast on pipeline complete/fail)
- `ui/src/app/models/page.tsx` (add toast on approve/reject)
- `ui/src/app/settings/page.tsx` (add toast on save)

**Verification:** Trigger a pipeline error -> toast appears, page does not crash.

### 0.5 -- Version the Press Release (Day 3)

**What:** Add a "Current Availability" section to `press_release.md` that honestly
distinguishes "available today" from "on the roadmap." This is the single most
trust-building action we can take.

**Section added:**
```markdown
## Current Availability (v0.3)

Available today:
- Postgres, JSON, CSV connectors
- Full discovery-to-quality pipeline
- LLM-optional with heuristic fallback
- React review UI
- CLI with demo, discover, generate commands
- Docker deployment

On the roadmap:
- Snowflake, BigQuery, MySQL connectors (Wave 2-4)
- AWS Glue, Databricks Unity Catalog, Iceberg REST (Wave 5)
- dbt project export (Wave 1)
- Plugin architecture (Wave 2)
- Observe mode for cloud warehouses (Wave 4)
```

**Verification:** Read the press release and confirm no claim contradicts the code.

### Wave 0 Exit Criteria (ALL MET -- 2026-04-19)

- [x] `uv run ruff check .` returns 0 errors
- [x] `uv run pytest tests/test_e2e_pipeline.py` passes (23 tests, ~53s)
- [x] `GET /api/health` returns 200 with component status
- [x] UI survives a thrown error without crashing (ErrorBoundary component)
- [x] Toast notification appears on pipeline completion
- [x] Press release has honest "Current Availability" section

---

## Wave 1: dbt Export

**Goal:** Let users export approved models and contracts to dbt project format.
**Duration:** 1.5 weeks
**Deliverable:** "Export to dbt" button in UI that downloads a zip of .sql + schema.yml files.
**Market impact:** Unlocks every team that uses dbt (the majority of the target market).

### 1.1 -- dbt Exporter Module (Day 1-3)

**What:** Transform GeneratedModel + ColumnInfo + ContractRule into dbt-compatible files.

**File:** `headwater/headwater/generator/dbt_export.py` (new, ~250 lines)

**Functions:**

```python
def export_dbt_project(
    models: list[GeneratedModel],
    discovery: DiscoveryResult,
    contracts: list[ContractRule],
    output_dir: Path,
    project_name: str = "headwater_export",
) -> Path:
    """
    Generates a complete dbt project directory:
      output_dir/
        dbt_project.yml
        models/
          staging/
            stg_zones.sql
            stg_sensors.sql
            ...
          marts/
            mart_zone_summary.sql
            ...
          schema.yml
        tests/  (if custom contract tests needed)
    """
```

**Mapping rules:**

| Headwater | dbt |
|-----------|-----|
| `GeneratedModel.sql` (CREATE TABLE AS SELECT) | Extract SELECT, wrap in `{{ config(materialized='table') }}` |
| `GeneratedModel.depends_on` (list of model names) | Replace table refs with `{{ ref('model_name') }}` |
| `ColumnInfo.description` | `schema.yml` column description |
| `TableInfo.description` | `schema.yml` model description |
| `ContractRule(rule_type="not_null")` | `tests: [not_null]` |
| `ContractRule(rule_type="unique")` | `tests: [unique]` |
| `ContractRule(rule_type="range", min=X, max=Y)` | `tests: [{ dbt_utils.accepted_range: {min: X, max: Y} }]` |
| `ContractRule(rule_type="cardinality", values=[...])` | `tests: [{ accepted_values: {values: [...]} }]` |
| `Relationship(from, to)` | `tests: [{ relationships: {to: ref('X'), field: 'Y'} }]` |

**Design decisions:**
- DuckDB SQL -> dbt SQL: strip `CREATE TABLE AS`, keep the SELECT body
- Replace physical table references with `{{ ref() }}` using `depends_on` list
- Generate `dbt_project.yml` with project name, version, model paths
- Contracts that map cleanly to dbt generic tests go in schema.yml
- Contracts that need custom SQL go in `tests/` as singular tests

### 1.2 -- Export API Endpoint (Day 3-4)

**File:** `headwater/headwater/api/routes/export.py` (new, ~80 lines)

**Endpoints:**

```
POST /api/export/dbt
  Query params: source_name (optional), project_name (optional)
  Response: application/zip (streaming)

GET /api/export/dbt/preview
  Query params: source_name (optional)
  Response: JSON with file list and contents (for UI preview)
```

**Flow:**
1. Load approved models + discovery + contracts from metadata store
2. Call `export_dbt_project()` into a temp directory
3. Zip the directory
4. Stream the zip as a response (or return preview JSON)

### 1.3 -- Export CLI Command (Day 4)

**File:** `headwater/headwater/cli/main.py` (add `export` command)

```bash
headwater export --format dbt --output ./my_dbt_project
headwater export --format dbt --output ./my_dbt_project --source analytics_prod
```

### 1.4 -- UI Export Button (Day 4-5)

**Files:**
- `ui/src/app/models/page.tsx` -- Add "Export to dbt" button in the header area
- `ui/src/lib/api.ts` -- Add `exportDbt()` function

**UX flow:**
1. User clicks "Export to dbt" on models page
2. Preview modal shows file tree + contents
3. User clicks "Download" -> zip file downloads
4. Toast: "dbt project exported (12 models, 45 tests)"

### 1.5 -- Tests (Day 5-6)

**File:** `headwater/tests/test_dbt_export.py` (new, ~300 lines)

**Test cases:**
- Staging model SQL correctly stripped of CREATE TABLE
- `ref()` macros replace physical table references
- schema.yml has correct column descriptions
- Contract-to-test mapping covers all 4 rule types
- Relationship tests generated for detected FKs
- dbt_project.yml is valid YAML
- Export endpoint returns valid zip
- CLI command produces correct directory structure
- Round-trip: export -> dbt parse (if dbt-core available) validates syntax

### Wave 1 Exit Criteria

- [ ] `headwater export --format dbt` produces valid dbt project from sample data
- [ ] `POST /api/export/dbt` returns downloadable zip
- [ ] UI "Export to dbt" button shows preview and downloads
- [ ] All staging models have `{{ ref() }}` for dependencies
- [ ] Contracts map to dbt generic tests in schema.yml
- [ ] `uv run pytest tests/test_dbt_export.py` passes
- [ ] E2E heartbeat test still passes

---

## Wave 2: Connector Architecture & New Sources

**Goal:** Make connectors pluggable and add MySQL + Parquet support.
**Duration:** 2.5 weeks
**Deliverable:** Plugin system that external contributors can use; two new connectors
proving the architecture works.

### 2.1 -- SourceConfig Type Flexibility (Day 1)

**What:** Change `SourceConfig.type` from `Literal[...]` to `str` with validation
against the connector registry.

**Files:**
- `headwater/headwater/core/models.py` -- Change type field
- Add `model_validator` that checks type against registry at validation time

**Why:** The hard-coded Literal blocks all extensibility. Every new connector currently
requires modifying the model.

### 2.2 -- ConnectorMetadata & Enhanced Registry (Day 1-2)

**What:** Add metadata to connectors so the system can auto-detect types and understand
capabilities without hardcoded if/else chains.

**File:** `headwater/headwater/connectors/registry.py` (rewrite, ~120 lines)

```python
@dataclass
class ConnectorMetadata:
    type_name: str                    # "postgres", "snowflake"
    display_name: str                 # "PostgreSQL"
    description: str
    uri_schemes: list[str]            # ["postgresql", "postgres"]
    file_extensions: list[str]        # [".json", ".ndjson"]
    supports_bulk_load: bool          # Can load all data into DuckDB
    supports_pushdown: bool           # Can profile in-source
    supports_observe: bool            # Can run without copying data
    required_packages: list[str]      # ["psycopg2-binary"]

def detect_connector_type(uri_or_path: str) -> str | None:
    """Scan all registered connectors for matching scheme or extension."""

def list_connectors() -> list[ConnectorMetadata]:
    """Return metadata for all registered connectors."""
```

**Refactor:** Remove hardcoded type detection from:
- `headwater/headwater/api/routes/pipeline.py` (`_connector_type_from_uri`)
- `headwater/headwater/cli/main.py` (type inference logic)

Replace with calls to `detect_connector_type()`.

### 2.3 -- Plugin Loader (Day 2-3)

**What:** Scan `~/.headwater/plugins/` and Python entry_points for external connectors.

**File:** `headwater/headwater/connectors/plugin_loader.py` (new, ~80 lines)

**Two discovery mechanisms:**

1. **Directory-based:** Drop a Python file in `~/.headwater/plugins/`. File must export
   `METADATA: ConnectorMetadata` and `CONNECTOR_CLASS: type`.

2. **Entry-point-based:** External packages declare:
   ```toml
   [project.entry-points."headwater.connectors"]
   snowflake = "headwater_snowflake:SnowflakeConnector"
   ```

**Integration:** Call `load_plugins()` during app startup (`api/app.py` lifespan) and
CLI initialization.

### 2.4 -- MySQL Connector (Day 3-5)

**File:** `headwater/headwater/connectors/mysql_loader.py` (new, ~300 lines)

**Approach:** Mirror the PostgresConnector pattern (pushdown profiling, Arrow sampling).

**Protocol implementation:**
- `connect()` -- Validate connection using `pymysql` or `mysql-connector-python`
- `list_tables()` -- Query `information_schema.tables`
- `profile()` -- Run aggregate SQL in MySQL (pushdown)
- `sample()` -- `SELECT * FROM t LIMIT 10000` -> Arrow via Polars
- `close()` -- Connection cleanup

**Dependency:** Add `pymysql>=1.1` to pyproject.toml (optional dependency group)

**Metadata:**
```python
ConnectorMetadata(
    type_name="mysql",
    uri_schemes=["mysql", "mysql+pymysql"],
    supports_pushdown=True,
    supports_bulk_load=False,
    supports_observe=False,
)
```

### 2.5 -- Parquet Connector (Day 5-6)

**File:** `headwater/headwater/connectors/parquet_loader.py` (new, ~120 lines)

**Approach:** Simpler than database connectors. Polars reads Parquet natively via Arrow.

**Protocol implementation:**
- `connect()` -- Validate path exists, files are readable
- `load_to_duckdb()` -- `CREATE TABLE AS SELECT * FROM read_parquet('path/*.parquet')`
- `profile()` -- Not applicable (profile via DuckDB after load)
- `sample()` -- `pl.scan_parquet().head(10000).collect().to_arrow()`

**Metadata:**
```python
ConnectorMetadata(
    type_name="parquet",
    file_extensions=[".parquet", ".pq"],
    supports_bulk_load=True,
    supports_pushdown=False,
    supports_observe=False,
)
```

### 2.6 -- Connector Lifecycle Standardization (Day 6-7)

**What:** Add optional `close()` and `list_tables()` to the BaseConnector protocol.
Ensure all connectors implement consistent cleanup.

**Files:**
- `headwater/headwater/connectors/base.py` -- Add optional methods
- All existing connectors -- Add `close()` if missing
- `headwater/headwater/api/routes/pipeline.py` -- Use `try/finally` with `close()`

### 2.7 -- Settings UI for Connectors (Day 7-8)

**Files:**
- `ui/src/app/settings/page.tsx` -- Add "Connectors" section showing installed connectors
- `headwater/headwater/api/routes/settings.py` -- Add `GET /api/settings/connectors`

**UX:** Read-only list of available connectors with their capabilities.

### 2.8 -- Tests (Day 8-10)

**Files:**
- `headwater/tests/test_plugin_loader.py` (new) -- Plugin discovery, registration, conflicts
- `headwater/tests/test_mysql_connector.py` (new) -- MySQL connector with mock server
- `headwater/tests/test_parquet_connector.py` (new) -- Parquet load from fixtures
- `headwater/tests/test_connector_registry.py` (new) -- Type detection, metadata queries

### Wave 2 Exit Criteria

- [ ] `SourceConfig(type="custom_type")` validates if connector is registered
- [ ] `detect_connector_type("mysql://...")` returns `"mysql"`
- [ ] `detect_connector_type("data.parquet")` returns `"parquet"`
- [ ] MySQL connector discovers tables from a test database
- [ ] Parquet connector loads .parquet files into DuckDB
- [ ] Plugin dropped in `~/.headwater/plugins/` auto-registers on startup
- [ ] Settings UI shows list of available connectors
- [ ] E2E heartbeat test still passes
- [ ] All new tests pass

---

## Wave 3: Production Resilience

**Goal:** The product survives dirty data, network failures, and long-running operations
without crashing or silently corrupting state.
**Duration:** 2.5 weeks
**Deliverable:** Circuit breakers, retry logic, resumable pipelines, and a UI that
communicates failures clearly.

### 3.1 -- Profile History Infrastructure (Day 1-2)

**What:** Before we can build circuit breakers, we need the ability to compare profiles
between runs. Schema drift comparison exists (`drift/schema.py`) but statistical profile
comparison does NOT exist in the codebase. This is prerequisite work.

**File:** `headwater/headwater/drift/profile_history.py` (new, ~150 lines)

**Functions:**
```python
def get_previous_profiles(
    store: MetadataStore, source_name: str, table_name: str
) -> list[ColumnProfile] | None:
    """Retrieve the most recent stored profiles for a table from a prior run."""

def compare_profiles(
    previous: list[ColumnProfile], current: list[ColumnProfile]
) -> ProfileDiff:
    """Compute deltas between two profile snapshots."""

@dataclass
class ProfileDiff:
    table_name: str
    row_count_previous: int
    row_count_current: int
    row_count_ratio: float             # current / previous
    columns_added: list[str]
    columns_removed: list[str]
    null_rate_deltas: dict[str, float]  # col -> pp change
    cardinality_deltas: dict[str, float]
    mean_deltas: dict[str, float]      # for numerics
    min_max_shifts: dict[str, tuple]   # col -> (old_range, new_range)
```

**Metadata store changes:**
- `headwater/headwater/core/metadata.py` -- Add `get_profiles_by_run()` method
  that retrieves profiles for a specific source + prior run_id
- Profiles are already persisted per-run with run_id; we just need a query to
  fetch the most recent prior run's profiles

**Why this is separate from 3.2:** The circuit breaker depends on this. Without profile
history retrieval and comparison, the circuit breaker has nothing to compare against.

### 3.2 -- Circuit Breaker Pattern (Day 2-4)

**What:** Halt ingestion when incoming data is anomalous. Both expert reviews demanded this.
Now possible because 3.1 provides profile comparison.

**File:** `headwater/headwater/core/circuit_breaker.py` (new, ~120 lines)

**Rules:**
- **Volume shift:** If row count ratio (current/previous) exceeds threshold, halt and alert.
- **Schema explosion:** If a table gains > 20 new columns in a single run, halt and alert.
  (Uses existing `drift/schema.py` `compare_schemas()` -- already implemented.)
- **Null spike:** If any column's null rate jumps by > 50 percentage points, halt and alert.
  (Uses new `ProfileDiff.null_rate_deltas` from 3.1.)
- **String bloat:** If average string length increases by > 10x, halt and alert.
  (Uses new `ProfileDiff.mean_deltas` from 3.1.)

**Integration points:**
- After `connector.load_to_duckdb()` or `connector.sample()`, before full profiling
- Call `get_previous_profiles()` from metadata store
- Run a lightweight pre-profile (row count + null counts only -- fast) to compare
- If circuit breaker trips: log the trigger, store a `CircuitBreakerEvent`, return
  a partial result with `warnings` field

**First-run behavior:** If no previous profiles exist (first discovery), circuit breaker
is skipped. This is safe because there is nothing to compare against.

**API surface:**
```python
class CircuitBreakerResult:
    tripped: bool
    trigger: str | None        # "volume_shift", "schema_explosion", etc.
    details: str | None        # "zones: 3,000 rows -> 890,000 rows (296x increase)"
    previous_value: float
    current_value: float
    threshold: float
```

**Configuration:** Add to `HeadwaterSettings`:
```python
circuit_breaker_enabled: bool = True
circuit_breaker_volume_threshold: float = 2.0    # 200%
circuit_breaker_null_threshold: float = 0.5      # 50pp increase
circuit_breaker_schema_threshold: int = 20       # new columns
```

### 3.2 -- Retry with Backoff (Day 3-4)

**What:** Retry transient failures in connectors and LLM calls.

**File:** `headwater/headwater/core/retry.py` (new, ~60 lines)

**Implementation:** A simple decorator:
```python
@retry(max_attempts=3, backoff_base=2.0, retryable=(ConnectionError, TimeoutError))
def connect(self, config: SourceConfig) -> None: ...
```

**Applied to:**
- `PostgresConnector.connect()`
- `PostgresConnector.profile()` (individual table profiles)
- `analyzer/llm.py` API calls (already has some retry, standardize)
- Future MySQL/Snowflake connectors

**Not applied to:** File reads (if a file is missing, retrying won't help).

### 3.3 -- Resumable Pipeline (Day 4-7)

**What:** If the pipeline crashes mid-run, resume from the last completed stage instead
of restarting from scratch.

**Design:**

Pipeline stages get explicit checkpoints in metadata:
```
pipeline_runs table:
  run_id, source_name, started_at, status,
  stage_completed (discovery | analysis | catalog | generation | execution | quality),
  stage_data_json (serialized intermediate results)
```

**Flow:**
1. Pipeline starts -> create `pipeline_run` record with status="running"
2. After each stage completes -> update `stage_completed` and persist intermediate results
3. On crash -> run record stays at last completed stage
4. On resume -> load intermediate results, skip completed stages, continue

**API:**
```
POST /api/pipeline/run          -- starts fresh or resumes if prior run exists
POST /api/pipeline/run?force=true  -- always starts fresh
GET  /api/pipeline/runs         -- list runs with status
```

**Files:**
- `headwater/headwater/core/metadata.py` -- Add pipeline_runs table
- `headwater/headwater/api/routes/pipeline.py` -- Add checkpoint logic
- `headwater/headwater/core/models.py` -- Add `PipelineRun` model

### 3.4 -- Volume & Quality Drift Detection (Day 8-9)

**What:** Track profile statistics over time and detect non-schema drift (the "silent
semantic drift" scenario from the expert review). Builds directly on the `ProfileDiff`
infrastructure from 3.1.

**File:** `headwater/headwater/drift/quality_drift.py` (new, ~150 lines)

**Reuses from 3.1:**
- `get_previous_profiles()` -- retrieves prior run's statistics
- `compare_profiles()` -- computes deltas
- `ProfileDiff` -- structured comparison result

**Additional tracked metrics (per column, per run):**
- Top-N value distribution shift (KL divergence or simple set-difference)
- Temporal gap detection (for date columns: is the latest date older than expected?)

**Alert thresholds (configurable via HeadwaterSettings):**
- Median shifts by > 1 order of magnitude -> warning
- Null rate increases by > 20pp -> warning
- Cardinality drops by > 50% -> warning (possible data loss)
- Top-N values change by > 60% -> info (semantic shift signal)

**Integration:** Run after profiling, before analysis. Store drift events in metadata.
Surface in UI drift banner (existing `drift-banner.tsx` component already handles
schema drift -- extend it to show profile drift alerts too).

### 3.5 -- UI: Loading States & Retry (Day 8-10)

**Components:**

- `ui/src/components/skeleton.tsx` (new, ~40 lines) -- Animated placeholder blocks
  for cards, tables, text while data loads
- `ui/src/components/retry-banner.tsx` (new, ~30 lines) -- "Failed to load. [Retry]"
  with exponential backoff indicator

**Integration across pages:**
- Dashboard: skeleton cards while pipeline state loads
- Discovery: skeleton table while profiles load
- Models: skeleton while model list loads
- Quality: skeleton while contracts load
- Explore: spinner while NL query executes with "Cancel" button

**API client improvements (`ui/src/lib/api.ts`):**
- Add `timeout: 30000` to all fetch calls (30s default)
- Add `AbortController` support for cancellable requests
- Transform raw HTTP errors into user-readable messages

### 3.6 -- Persistence Error Escalation (Day 10)

**What:** Make critical persistence failures fatal. Currently all persistence errors
in pipeline.py are logged and swallowed.

**Rule:**
- Discovery result persistence failure -> FATAL (abort pipeline)
- Model persistence failure -> FATAL (abort pipeline)
- Activity log failure -> WARNING (continue)
- Graph rebuild failure -> WARNING (continue)

**File:** `headwater/headwater/api/routes/pipeline.py` -- Split try/except blocks by
criticality.

### 3.7 -- Tests (Day 11-13)

**Files:**
- `headwater/tests/test_profile_history.py` (new) -- Profile retrieval, comparison,
  delta computation, first-run (no prior data) handling
- `headwater/tests/test_circuit_breaker.py` (new) -- Each trigger scenario, first-run
  bypass, threshold configuration, multiple simultaneous triggers
- `headwater/tests/test_retry.py` (new) -- Backoff timing, max attempts, non-retryable errors
- `headwater/tests/test_resumable_pipeline.py` (new) -- Crash at each stage, verify resume
- `headwater/tests/test_quality_drift.py` (new) -- Statistical shift detection, alert
  thresholds, temporal gap detection

### Wave 3 Exit Criteria

- [ ] Circuit breaker trips on 3x volume increase (test proves it)
- [ ] Failed LLM call retries 3 times with backoff
- [ ] Pipeline crashed at analysis stage resumes from analysis (not discovery)
- [ ] Quality drift detects median shift of 10x and fires warning
- [ ] UI shows skeleton loaders during data fetch
- [ ] UI shows retry banner on API failure
- [ ] Fetch calls have 30s timeout + cancel support
- [ ] Critical persistence failure aborts pipeline with clear error
- [ ] E2E heartbeat test still passes
- [ ] All new tests pass

---

## Wave 4: Cloud Warehouse Connectors & Observe Mode

**Goal:** Support Snowflake and BigQuery without copying data.
**Duration:** 3 weeks
**Deliverable:** `headwater discover --source snowflake://...` works end-to-end in
observe mode (profile in-source, no bulk data copy).

### 4.1 -- Observe Mode Implementation (Day 1-3)

**What:** The `mode="observe"` path that the CLI currently rejects. Observe mode means:
profile in the source warehouse, sample small batches for validation, never bulk-copy.

**Design:**
- Connector declares `supports_observe=True`
- Pipeline detects observe mode from SourceConfig
- Instead of `load_to_duckdb()`, calls `connector.profile()` for each table
- Samples (10K rows) loaded into DuckDB for local validation only
- All generated models reference the source warehouse, not local DuckDB
- Staging SQL uses `{{ source() }}` references (warehouse-native)

**Files:**
- `headwater/headwater/api/routes/pipeline.py` -- Add observe mode branch
- `headwater/headwater/cli/main.py` -- Remove "Phase 2" rejection, implement flow
- `headwater/headwater/generator/staging.py` -- Generate warehouse-native SQL
  (CREATE TABLE in target warehouse, not local DuckDB)

### 4.2 -- Snowflake Connector (Day 3-8)

**File:** `headwater/headwater/connectors/snowflake_loader.py` (new, ~400 lines)

**Dependency:** `snowflake-connector-python>=3.5` (optional dependency group)

**Protocol implementation:**
- `connect()` -- Validate via Snowflake connector (account, user, password, warehouse, database)
- `list_tables()` -- Query `information_schema.tables`
- `profile()` -- Pushdown profiling via Snowflake SQL (COUNT, MIN, MAX, AVG, APPROX_COUNT_DISTINCT)
- `sample()` -- `SELECT * FROM t SAMPLE (10000 ROWS)` -> Arrow via `fetch_arrow_all()`
- `close()` -- Connection cleanup

**Snowflake-specific considerations:**
- Use `APPROX_COUNT_DISTINCT` instead of `COUNT(DISTINCT)` for performance
- Use `SAMPLE` clause for efficient sampling
- Map Snowflake types to DuckDB/Arrow types
- Handle VARIANT/OBJECT/ARRAY columns (flatten or skip)
- Support schema specification: `database.schema.table`

**Metadata:**
```python
ConnectorMetadata(
    type_name="snowflake",
    uri_schemes=["snowflake"],
    supports_pushdown=True,
    supports_bulk_load=False,
    supports_observe=True,
)
```

### 4.3 -- BigQuery Connector (Day 8-12)

**File:** `headwater/headwater/connectors/bigquery_loader.py` (new, ~350 lines)

**Dependency:** `google-cloud-bigquery>=3.20` (optional dependency group)

**Protocol implementation:**
- `connect()` -- Validate via BigQuery client (project, dataset, credentials)
- `list_tables()` -- `client.list_tables(dataset)`
- `profile()` -- Pushdown SQL via BigQuery (APPROX_COUNT_DISTINCT, COUNTIF, etc.)
- `sample()` -- `SELECT * FROM t TABLESAMPLE SYSTEM (X PERCENT)` -> Arrow
- `close()` -- Client cleanup

**BigQuery-specific considerations:**
- Use `TABLESAMPLE` for efficient sampling
- Handle partitioned tables (profile partition metadata)
- Handle nested/repeated fields (STRUCT, ARRAY)
- Cost awareness: log estimated bytes scanned before profiling
- Support `project.dataset.table` notation

### 4.4 -- Warehouse-Native SQL Generation (Day 12-13)

**What:** When operating in observe mode against a warehouse, generated models should
target that warehouse's SQL dialect, not DuckDB.

**File:** `headwater/headwater/generator/dialects.py` (new, ~100 lines)

**Approach:**
- Abstract SQL differences behind a dialect interface
- DuckDB dialect (default): `CREATE OR REPLACE TABLE`
- Snowflake dialect: `CREATE TABLE IF NOT EXISTS`, `CLONE`, `VARIANT` handling
- BigQuery dialect: `CREATE TABLE`, `STRUCT` handling, backtick quoting

**Integration:** Generator receives dialect from SourceConfig, templates adapt.

### 4.5 -- UI: Source Type Selection (Day 13-14)

**Files:**
- `ui/src/app/page.tsx` -- Expand pipeline launcher with source type dropdown
  (Postgres, JSON, CSV, Parquet, MySQL, Snowflake, BigQuery)
- `ui/src/app/settings/page.tsx` -- Add warehouse credential configuration

**UX:** When user selects Snowflake/BigQuery, show credential fields (account, project,
etc.) and auto-select observe mode.

### 4.6 -- Tests (Day 14-15)

**Files:**
- `headwater/tests/test_snowflake_connector.py` (new) -- Mock Snowflake with responses
- `headwater/tests/test_bigquery_connector.py` (new) -- Mock BigQuery client
- `headwater/tests/test_observe_mode.py` (new) -- Full pipeline in observe mode
- `headwater/tests/test_dialects.py` (new) -- SQL generation per dialect

### Wave 4 Exit Criteria

- [ ] `headwater discover --source snowflake://account/db --mode observe` completes
- [ ] Snowflake connector profiles tables without bulk data copy
- [ ] BigQuery connector profiles tables with cost estimate logging
- [ ] Generated staging SQL uses target warehouse dialect
- [ ] Observe mode pipeline produces discovery + models + contracts
- [ ] UI allows selecting Snowflake/BigQuery as source type
- [ ] E2E heartbeat test still passes
- [ ] All new tests pass (mocked warehouse connections)

---

## Wave 5: Catalog Connectors

**Goal:** Connect to existing data catalogs for metadata-enriched discovery.
**Duration:** 3 weeks
**Deliverable:** Users with AWS Glue, Databricks Unity Catalog, or Iceberg REST can
discover their data estate without direct database access.

### 5.1 -- Catalog Connector Protocol (Day 1-2)

**What:** Catalog connectors are fundamentally different from data connectors. They
provide metadata (schemas, descriptions, lineage) but may not provide data access.

**File:** `headwater/headwater/connectors/catalog_base.py` (new, ~60 lines)

```python
class CatalogConnector(Protocol):
    def connect(self, config: SourceConfig) -> None: ...
    def list_databases(self) -> list[str]: ...
    def list_tables(self, database: str) -> list[TableInfo]: ...
    def get_columns(self, database: str, table: str) -> list[ColumnInfo]: ...
    def get_relationships(self) -> list[Relationship]: ...
    def get_descriptions(self) -> dict[str, str]: ...  # table/col -> description
```

**Key difference from data connectors:** Catalog connectors enrich discovery with
pre-existing metadata. They do not load data. Data access requires a separate data
connector (or observe mode against the underlying warehouse).

### 5.2 -- AWS Glue Connector (Day 2-6)

**File:** `headwater/headwater/connectors/glue_catalog.py` (new, ~300 lines)

**Dependency:** `boto3>=1.34` (optional)

**Implementation:**
- `connect()` -- Validate AWS credentials, region
- `list_databases()` -- `glue.get_databases()`
- `list_tables()` -- `glue.get_tables(DatabaseName=db)`
- `get_columns()` -- Extract from `StorageDescriptor.Columns`
- `get_relationships()` -- Not natively supported; infer from naming
- `get_descriptions()` -- Extract from Glue `Comment` and `Parameters` fields

**Enrichment flow:**
1. Discover via Glue -> get table/column metadata with existing descriptions
2. Optionally pair with a data connector (e.g., Snowflake) for profiling
3. Merge Glue descriptions with Headwater-generated descriptions (prefer Glue if locked)

### 5.3 -- Databricks Unity Catalog Connector (Day 6-10)

**File:** `headwater/headwater/connectors/unity_catalog.py` (new, ~300 lines)

**Dependency:** `databricks-sdk>=0.20` (optional)

**Implementation:**
- `connect()` -- Validate workspace URL + token
- `list_databases()` -- `workspace.catalogs.list()` + `workspace.schemas.list()`
- `list_tables()` -- `workspace.tables.list()`
- `get_columns()` -- Extract from table detail
- `get_relationships()` -- Unity Catalog supports FK constraints natively
- `get_descriptions()` -- Extract `comment` fields

### 5.4 -- Iceberg REST Catalog Connector (Day 10-13)

**File:** `headwater/headwater/connectors/iceberg_rest.py` (new, ~250 lines)

**Dependency:** `pyiceberg>=0.7` (optional)

**Implementation:**
- `connect()` -- Validate REST catalog URI
- `list_databases()` -- `catalog.list_namespaces()`
- `list_tables()` -- `catalog.list_tables(namespace)`
- `get_columns()` -- `table.schema().fields`
- `get_relationships()` -- Not natively supported; infer from naming
- `get_descriptions()` -- Extract from table properties

**Iceberg-specific value:**
- Partition spec extraction (valuable for understanding data organization)
- Snapshot history (for drift detection via manifest diffing)
- Field IDs (stable column identity across renames -- solves the expert review concern)

### 5.5 -- Catalog + Data Connector Pairing (Day 13-14)

**What:** Allow users to pair a catalog connector with a data connector.

**Example:**
```bash
headwater discover \
  --catalog glue --region us-east-1 --database analytics \
  --source snowflake://account/analytics --mode observe
```

**Flow:**
1. Catalog connector provides metadata (descriptions, relationships, structure)
2. Data connector provides profiling (statistics, samples)
3. Merger combines both, preferring catalog descriptions where they exist
4. Headwater enriches gaps (columns without descriptions)

### 5.6 -- UI: Catalog Discovery Flow (Day 14-15)

**Files:**
- `ui/src/app/discovery/page.tsx` -- Add catalog source option
- `ui/src/app/page.tsx` -- Catalog discovery in pipeline launcher

**UX:** Two-step source selection:
1. Choose catalog type (Glue, Unity Catalog, Iceberg REST, None)
2. If catalog selected, enter catalog credentials
3. Optionally pair with data source for profiling
4. Run discovery

### 5.7 -- Tests (Day 15)

**Files:**
- `headwater/tests/test_glue_catalog.py` (new) -- Mocked boto3 responses
- `headwater/tests/test_unity_catalog.py` (new) -- Mocked Databricks SDK
- `headwater/tests/test_iceberg_rest.py` (new) -- Mocked PyIceberg
- `headwater/tests/test_catalog_data_merge.py` (new) -- Pairing + merge logic

### Wave 5 Exit Criteria

- [ ] `headwater discover --catalog glue` returns tables with Glue descriptions
- [ ] Unity Catalog connector extracts FK relationships natively
- [ ] Iceberg connector extracts partition specs and field IDs
- [ ] Catalog + data connector pairing merges metadata correctly
- [ ] Catalog descriptions override Headwater heuristic descriptions
- [ ] UI allows catalog source selection
- [ ] E2E heartbeat test still passes
- [ ] All new tests pass (mocked external services)

---

## Wave 6: UX Polish, Accessibility & Frontend Testing

**Goal:** Production-grade user experience that passes WCAG 2.1 AA.
**Duration:** 2.5 weeks
**Deliverable:** Accessible, tested, performant UI.

### 6.1 -- Accessibility Audit & Fix (Day 1-4)

**Scope:**
- Add `aria-label` to all interactive elements (buttons, links, inputs)
- Add `aria-live="polite"` to dynamic content regions (pipeline status, toast area)
- Add `role="alert"` to error messages
- Add `aria-describedby` linking form inputs to help text
- Add keyboard navigation: Tab stops, Enter/Space activation, Escape to close modals
- Add focus management: auto-focus first input on modal open, return focus on close
- Add skip navigation link
- Verify color contrast ratios (4.5:1 minimum for text)

**Files:** All page components and UI components.

### 6.2 -- Empty States (Day 4-5)

**What:** Every data-driven section needs an empty state with guidance.

**Examples:**
- Dashboard with no discovery: "Point Headwater at a data source to begin" (exists, refine)
- Models page with no models: "Run the pipeline to generate models"
- Quality page with no contracts: "Models must be generated before quality contracts"
- Explore with no catalog: "Complete discovery and review to enable exploration"
- Dictionary with no tables: "No tables discovered yet"

**File:** `ui/src/components/empty-state.tsx` (new, ~30 lines) -- Reusable component.

### 6.3 -- Request Deduplication & Caching (Day 5-6)

**What:** Prevent duplicate API calls when components remount.

**File:** `ui/src/lib/api.ts` -- Add simple request cache:
- Cache GET responses for 30 seconds
- Deduplicate concurrent identical requests
- Invalidate cache on mutations (POST/PATCH/DELETE)

### 6.4 -- Confirmation Dialogs (Day 6-7)

**What:** Add confirmation before destructive or irreversible actions.

**Actions requiring confirmation:**
- Model rejection (irreversible in current flow)
- Column unlock (changes semantic lock state)
- Re-enrich (overwrites existing analysis)
- Pipeline re-run (overwrites existing discovery)

**File:** `ui/src/components/confirm-dialog.tsx` (new, ~50 lines)

### 6.5 -- Frontend Testing (Day 7-12)

**Setup:**
- Add `vitest` + `@testing-library/react` to devDependencies
- Add `playwright` for E2E browser tests

**Unit tests** (`ui/src/__tests__/`):
- Toast component: render, auto-dismiss, multiple toasts
- Error boundary: catches error, shows fallback, retry works
- API client: request formation, error parsing, caching
- SQL viewer: syntax highlighting correctness
- Status badge: correct colors for each status

**E2E tests** (`ui/e2e/`):
- Pipeline run: start pipeline -> see stepper progress -> see results
- Model review: approve model -> toast appears -> model status updates
- dbt export: click export -> preview modal -> download
- Explore: type question -> see SQL + results + chart
- Settings: change LLM provider -> verify -> save -> toast

### 6.6 -- Performance Optimization (Day 12-13)

- Add `React.memo` to heavy components (SQL viewer, relationship diagram)
- Add virtual scrolling for large column lists (> 50 columns)
- Lazy-load chart library (recharts) only on explore/quality pages
- Add `loading.tsx` files for Next.js route-level suspense boundaries

### Wave 6 Exit Criteria

- [ ] All interactive elements have `aria-label`
- [ ] Keyboard navigation works for all major flows
- [ ] Screen reader can navigate pipeline stepper and review queue
- [ ] Every data section has an empty state with guidance
- [ ] Duplicate API calls are prevented on component remount
- [ ] Destructive actions show confirmation dialog
- [ ] Vitest unit tests pass for core components
- [ ] Playwright E2E tests pass for 5 major workflows
- [ ] Large column lists render smoothly (virtual scroll)
- [ ] Lighthouse accessibility score > 90

---

## Wave 7: Dependency Cleanup & Documentation

**Goal:** Remove dead weight, document what exists, prepare for community contribution.
**Duration:** 1.5 weeks

### 7.1 -- Make Kuzu & LanceDB Gracefully Optional (Day 1-2)

**Verified status:** Both are actively used in production routes:
- **Kuzu** powers `/graph/data`, `/graph/patterns`, `/graph/join-path` (relationship
  visualization, star schema detection, join path finding)
- **LanceDB** powers semantic search in `/explore/ask` (embedding-based catalog
  matching during NL-to-SQL decomposition)

**Decision:** These are NOT dead weight. They deliver real value. However, they should
be **optional dependencies** so users who only need basic discovery can skip the
installation overhead (sentence-transformers alone is ~2GB).

**Implementation:**
- Move `kuzu`, `lancedb`, `sentence-transformers` to optional dependency group:
  `uv add --optional semantic kuzu lancedb sentence-transformers`
- Add graceful import guards in `graph_store.py` and `vector_store.py`:
  try/except ImportError with clear log message
- If not installed: skip graph/vector indexing during discovery, disable
  `/graph/*` and semantic search in `/explore/ask` (fall back to keyword matching)
- Add installation hint in settings UI: "Install semantic extras for graph
  visualization and semantic search: `uv add headwater[semantic]`"

### 7.2 -- Docker Hardening (Day 2-4)

**Files:**
- `headwater/Dockerfile` -- Add HEALTHCHECK, non-root user, resource hints
- `headwater/docker-compose.yml` -- Add health checks, restart policies, resource limits
- `headwater/docker-compose.prod.yml` (new) -- Production-ready compose with logging,
  volume backup hints, TLS termination guidance

### 7.3 -- Contributor Documentation (Day 4-6)

**Files:**
- `headwater/CONTRIBUTING.md` (new) -- How to add a connector, how to run tests,
  code style, PR process
- `headwater/docs/architecture.md` (new) -- Data flow diagram, layer responsibilities,
  module inventory
- `headwater/docs/connector-guide.md` (new) -- Step-by-step guide to writing a connector
  with the plugin system

### 7.4 -- CI Pipeline (Day 6-7)

**File:** `.github/workflows/ci.yml` (new)

**Steps:**
1. `uv run ruff check .` -- lint
2. `uv run pytest` -- all tests including E2E heartbeat
3. `cd ui && npm run build` -- frontend builds
4. `cd ui && npx vitest run` -- frontend tests

**Gate:** PR cannot merge if any step fails.

### Wave 7 Exit Criteria

- [ ] Kuzu/LanceDB are optional dependencies with graceful degradation
- [ ] Docker health checks pass: `docker compose ps` shows "healthy"
- [ ] CONTRIBUTING.md covers connector development end-to-end
- [ ] CI pipeline runs lint + test + build on every PR
- [ ] `docker compose up` works on a clean machine with zero configuration

---

---

# PART 2: Explore & Insights Overhaul

## The Problem

The Explore layer is the user's payoff -- the moment where all the discovery, profiling,
and modeling work translates into answers. Today it is sloppy:

**Backend:**
- Statistical insights assume normality without testing, use no multiple comparison
  correction, split data at midpoint instead of change-point detection, and ignore
  seasonality. With 100 tables x 10 metrics, expect ~50 false positive insights.
- NL-to-SQL silently drops columns it cannot join, uses arbitrary confidence thresholds
  (0.3 -- no justification), and the repair loop retries with identical broken context.
- Visualization recommends chart types by row-count thresholds (30 rows = bar, 31 = table)
  with no relationship to data semantics.

**Frontend:**
- Charts have zero axis labels or unit indicators.
- Scatter plots hardcode the first 2 numeric columns.
- Heatmaps pick the first numeric column blindly.
- Statistical insights are text cards with no inline charts.
- KPIs show raw key-value pairs with no trend context.
- No confidence intervals, no regression lines, no annotations.

**Tests:**
- 71% structural (field exists), 29% correctness (value is right), 3% execute SQL.
- Zero tests validate a query returns a numerically correct answer.
- Zero tests validate statistical accuracy.
- Zero tests validate a visualization spec is correct for its data.

**The gating principle for this entire section: CORRECTNESS.**
Every wave produces integration tests that validate numerical accuracy of outputs
against known answers from the sample dataset. A wave does not ship if its correctness
tests fail.

---

## Wave E0: Correctness Ground Truth

**Goal:** Establish a golden answer set from the sample data so every subsequent wave
has an objective, automated correctness benchmark.
**Duration:** 1 week
**Gating condition:** Golden answers verified manually. Test harness runs and asserts.

### E0.1 -- Golden Answer Dataset (Day 1-2)

**What:** Manually compute and verify 25 known-correct answers from the sample data
(8 tables, ~60K records, environmental health domain). These become the ground truth
that all explore tests assert against.

**File:** `headwater/tests/golden/explore_answers.py` (new, ~200 lines)

**Golden answers organized by category:**

**Category A: Single-table aggregations (5 answers)**
```python
GOLDEN = {
    "total_complaints": 10000,           # SELECT COUNT(*) FROM complaints
    "avg_reading_value": 42.7,           # SELECT AVG(value) FROM readings (tolerance +-0.5)
    "distinct_zones": 180,               # SELECT COUNT(DISTINCT zone_id) FROM zones
    "max_sensor_reading": 99.8,          # SELECT MAX(value) FROM readings (tolerance +-0.5)
    "null_rate_complaints_county": 0.0,  # Verify no nulls in county column
}
```

**Category B: Multi-table joins (5 answers)**
```python
    "complaints_per_zone_top1_count": ...,  # JOIN complaints->zones, GROUP BY zone, ORDER BY cnt DESC LIMIT 1
    "readings_per_sensor_type_avg": ...,    # JOIN readings->sensors, GROUP BY type, AVG(value)
    "sites_with_inspections_count": ...,    # JOIN sites->inspections, COUNT DISTINCT sites
    "programs_without_incidents": ...,      # LEFT JOIN programs->incidents WHERE incidents IS NULL
    "zone_with_most_sensors": ...,          # JOIN sensors->zones, GROUP BY zone, COUNT(*)
```

**Category C: Temporal patterns (5 answers)**
```python
    "readings_month_with_highest_avg": ...,  # GROUP BY month, AVG(value), ORDER DESC LIMIT 1
    "complaints_trend_direction": "increasing|decreasing|stable",
    "first_incident_date": ...,
    "readings_per_day_avg": ...,
    "complaint_seasonal_peak_month": ...,
```

**Category D: Statistical properties (5 answers)**
```python
    "readings_value_stddev": ...,         # Known standard deviation
    "readings_value_skewness_sign": ...,  # positive/negative/near-zero
    "complaints_zone_cardinality": ...,   # Number of distinct zones in complaints
    "sensor_reading_correlation_exists": True,  # readings.value correlates with sensor metadata
    "readings_null_rate_pct": ...,        # Known null percentage
```

**Category E: Cross-cutting correctness (5 answers)**
```python
    "join_path_readings_to_zones": ["readings", "sensors", "zones"],  # Expected FK path
    "complaints_county_top3": [...],      # Top 3 counties by complaint count
    "total_rows_all_tables": 59900,       # Sum of all table row counts (tolerance +-100)
    "tables_with_temporal_columns": [...], # Which tables have date/time columns
    "highest_null_column": ("table", "col", pct),  # Column with worst null rate
```

### E0.2 -- Golden Answer Verification Script (Day 2-3)

**What:** A script that loads sample data into DuckDB, runs the 25 ground-truth queries,
and prints results for manual verification before we freeze the answers.

**File:** `headwater/tests/golden/verify_golden.py` (new, ~100 lines)

**Output:** Table of `| Question | Expected | Actual | Match? |` for human review.

### E0.3 -- Correctness Test Harness (Day 3-5)

**What:** Integration test framework that runs explore functions against sample data
and asserts results match golden answers.

**File:** `headwater/tests/test_explore_correctness.py` (new, ~400 lines)

**Structure:**
```python
@pytest.fixture(scope="module")
def explore_env():
    """Load sample data, run discovery, build catalog -- once for all tests."""
    # Returns: con, discovery, catalog, schema_graph, suggestions

class TestSQLCorrectness:
    """Every test executes SQL and validates the numeric answer."""

    def test_total_complaints(self, explore_env):
        result = ask("How many complaints are there?", ...)
        assert result.error is None
        assert result.data[0]["count"] == GOLDEN["total_complaints"]

    def test_avg_reading_value(self, explore_env):
        result = ask("What is the average reading value?", ...)
        assert result.error is None
        assert abs(result.data[0]["avg"] - GOLDEN["avg_reading_value"]) < 0.5

    def test_complaints_per_zone_join(self, explore_env):
        result = ask("How many complaints per zone?", ...)
        assert result.error is None
        assert len(result.data) > 0
        top = max(result.data, key=lambda r: r[next(k for k in r if "count" in k.lower())])
        assert top[...] == GOLDEN["complaints_per_zone_top1_count"]
    # ... 22 more tests

class TestStatisticalCorrectness:
    """Validate that statistical insights are mathematically accurate."""

    def test_stddev_matches_known(self, explore_env):
        insights = detect_insights(con, "staging")
        # Find the readings.value insight
        # Assert magnitude is within tolerance of known stddev

    def test_no_false_positive_on_uniform_data(self, explore_env):
        """Insert uniform data, verify zero anomalies detected."""
        # Create table with perfectly uniform values
        # Assert detect_insights returns empty list

    def test_known_anomaly_detected(self, explore_env):
        """Insert data with a known spike, verify it's detected."""
        # Create table with flat values + one 10x spike
        # Assert exactly 1 anomaly detected at the spike location

    def test_known_correlation_detected(self, explore_env):
        """Insert perfectly correlated columns, verify r > 0.95."""
        # Create table where col_b = 2 * col_a + noise
        # Assert correlation detected with r > 0.9

    def test_no_spurious_correlation(self, explore_env):
        """Insert independent columns, verify no correlation."""
        # Create table with random col_a, random col_b
        # Assert no correlation reported

class TestVisualizationCorrectness:
    """Validate that the recommended chart makes sense for the data."""

    def test_temporal_data_gets_line_chart(self, explore_env):
        result = ask("Show readings over time", ...)
        assert result.visualization.chart_type == "line"
        assert result.visualization.x_axis is not None
        # Verify x_axis is actually a temporal column

    def test_categorical_breakdown_gets_bar_chart(self, explore_env):
        result = ask("Complaints by county", ...)
        assert result.visualization.chart_type == "bar"
        assert result.visualization.x_axis is not None
        assert result.visualization.y_axis is not None

    def test_single_value_gets_kpi(self, explore_env):
        result = ask("How many zones are there?", ...)
        assert result.visualization.chart_type == "kpi"
        assert len(result.data) == 1

    def test_two_numerics_gets_scatter(self, explore_env):
        result = ask("Compare reading value to sensor count", ...)
        if result.visualization:
            assert result.visualization.chart_type in ("scatter", "table")

    def test_axes_reference_real_columns(self, explore_env):
        """Every visualization spec must reference columns that exist in the data."""
        for question in ["Complaints by county", "Readings over time", "Avg value by zone"]:
            result = ask(question, ...)
            if result.visualization and result.data:
                columns = set(result.data[0].keys())
                if result.visualization.x_axis:
                    assert result.visualization.x_axis in columns, \
                        f"x_axis '{result.visualization.x_axis}' not in {columns}"
                if result.visualization.y_axis:
                    assert result.visualization.y_axis in columns, \
                        f"y_axis '{result.visualization.y_axis}' not in {columns}"
```

### Wave E0 Gate (ALL MET -- 2026-04-19)

- [x] 25 golden answers manually verified against raw SQL on sample data (25/25 pass)
- [x] `uv run pytest tests/test_explore_correctness.py` defines 18 tests
- [x] Baseline pass rate recorded: **10/18 (55.6%)**
- [ ] Every subsequent wave must increase this pass rate
- [ ] **No wave ships if pass rate drops below the previous wave's rate**

---

## Wave E1: Fix SQL Generation Correctness

**Goal:** When the user asks a question, the generated SQL returns the right answer.
**Duration:** 2 weeks
**Gating condition:** Golden answer SQL tests pass at >= 80%.

### E1.1 -- Fix Silent Column Drops (Day 1-2)

**Problem:** `query_planner.py` lines 578-587 silently remove columns from the query
plan when no join path is found. The user asked about "complaints per county" but county
was dropped because the join path failed. The query returns a count without the breakdown
the user wanted.

**Fix in:** `headwater/headwater/explorer/query_planner.py`

**Rule:** If a column the user explicitly mentioned is dropped:
1. Add a structured warning to the result (not just a log line)
2. Include the column name and reason: "Could not find a join path from
   complaints to zones for column 'county'. Showing results without this breakdown."
3. If > 50% of user-mentioned columns are dropped, return an error instead of
   a misleading partial result

**Correctness check:** `test_dropped_column_produces_warning()`

### E1.2 -- Fix Join Path Validation (Day 2-4)

**Problem:** `schema_graph.py` `find_join_path()` returns shortest hop-count path,
not most semantically correct path. Joins are not validated against actual FK integrity.

**Fix in:** `headwater/headwater/explorer/schema_graph.py`

**Changes:**
1. Weight join paths by referential integrity (from `Relationship.referential_integrity`)
2. Penalize paths through low-integrity joins (< 80%)
3. Validate that join columns actually exist in both tables before returning a path
4. Add `validate_join_path()` function that checks:
   - Every join column exists in its table
   - Referential integrity > 50% (otherwise warn)
   - No circular joins (A->B->A)

**Correctness check:** `test_join_path_readings_to_zones()` asserts the path matches
the golden answer.

### E1.3 -- Fix Heuristic SQL Builder (Day 4-6)

**Problem:** `_heuristic_sql()` in `nl_to_sql.py` uses hardcoded scoring weights
(10, 3, 2) for table matching, picks first matching column, and builds joins
without validation.

**Fix in:** `headwater/headwater/explorer/nl_to_sql.py`

**Changes:**
1. Score tables by: exact name match (10) + column name matches (3 each) +
   description keyword matches (1 each). **But** normalize by total columns to
   avoid bias toward wide tables.
2. Column matching: when multiple columns match, prefer columns with higher
   uniqueness/cardinality (use profile data).
3. Join building: validate every join column exists before generating SQL.
   If validation fails, fall through to next strategy instead of emitting broken SQL.
4. Add `_validate_sql_columns()` helper that checks every column referenced in
   generated SQL actually exists in the schema.

**Correctness checks:**
- `test_complaints_per_county_correct_column()` -- verifies county (not complaint_number)
  is selected as the grouping dimension
- `test_heuristic_join_query_executes()` -- verifies join queries actually execute
- `test_heuristic_excludes_latitude()` -- verifies geographic coordinates are not used
  as metrics

### E1.4 -- Fix Result Shape Validation (Day 6-7)

**Problem:** No validation that query results make sense. A query asking for "complaints
per county" could return 10,000 ungrouped rows instead of a grouped result.

**Fix in:** `headwater/headwater/explorer/nl_to_sql.py` (`_execute_query`)

**Add post-execution checks:**
```python
def _validate_result_shape(question: str, sql: str, data: list[dict]) -> list[str]:
    """Return warnings if result shape is suspicious."""
    warnings = []

    # 1. Aggregation question returning too many rows
    if _is_aggregation_question(question) and len(data) > 100:
        warnings.append(
            f"Aggregation query returned {len(data)} rows. "
            "This may indicate a missing GROUP BY clause."
        )

    # 2. Single-value question returning multiple rows
    if _is_scalar_question(question) and len(data) > 1:
        warnings.append(
            f"Expected a single value but got {len(data)} rows."
        )

    # 3. All values identical (suggests wrong column)
    if len(data) > 1:
        for col in data[0]:
            values = {row.get(col) for row in data[:50]}
            if len(values) == 1 and values != {None}:
                warnings.append(
                    f"Column '{col}' has identical values in all rows. "
                    "This may indicate wrong column selection."
                )

    return warnings
```

**Correctness check:** `test_aggregation_returns_grouped_result()` -- "complaints per
county" returns < 200 rows (distinct counties), not 10,000 raw rows.

### E1.5 -- Fix Read-Only Validation (Day 7-8)

**Problem:** `_is_read_only()` uses regex, which can be bypassed with comments or
multi-statement queries.

**Fix in:** `headwater/headwater/explorer/nl_to_sql.py`

**Change:** Parse SQL using DuckDB's `EXPLAIN` before execution:
```python
def _validate_read_only(con, sql: str) -> bool:
    """Use DuckDB EXPLAIN to verify the query plan is read-only."""
    try:
        plan = con.execute(f"EXPLAIN {sql}").fetchall()
        plan_text = " ".join(str(row) for row in plan)
        # DuckDB EXPLAIN for mutations includes INSERT/UPDATE/DELETE operators
        return not any(op in plan_text.upper() for op in ("INSERT", "UPDATE", "DELETE", "CREATE"))
    except Exception:
        return False  # If EXPLAIN fails, query is suspicious
```

**Correctness check:** `test_sql_injection_via_comment_blocked()` --
`"SELECT 1; /* DELETE FROM zones */"` is blocked.

### E1.6 -- Fix Repair Loop (Day 8-10)

**Problem:** Repair loop sends identical context on every retry. If the LLM
misunderstood the question, it will make the same mistake 3 times.

**Fix in:** `headwater/headwater/explorer/nl_to_sql.py` (`_repair_loop`)

**Changes:**
1. On each retry, include the previous attempt's SQL and error in the prompt
   (already done partially) BUT also include:
   - Which columns exist in which tables (explicit schema dump)
   - Which joins are valid (from schema_graph)
   - What the previous attempt got wrong (structured error classification)
2. Classify errors before retrying:
   - Column not found -> include valid column list for that table
   - Table not found -> include valid table list
   - Type mismatch -> include column types
   - Syntax error -> include DuckDB-specific syntax notes
3. Limit scope: if 2 retries fail with the same error class, stop and return
   the error with context instead of trying a 3rd time.

**Correctness check:** `test_repair_fixes_wrong_column_name()` -- LLM generates SQL
with wrong column, repair provides correct column list, second attempt succeeds.

### E1.7 -- Correctness Integration Tests (Day 10-14)

**File:** Update `headwater/tests/test_explore_correctness.py`

**New tests added:**
```python
class TestSQLGenerationCorrectness:
    """All tests execute SQL against sample data and validate numeric answers."""

    # Single-table tests (must all pass)
    def test_count_complaints(self): ...          # == GOLDEN["total_complaints"]
    def test_avg_readings(self): ...              # within tolerance
    def test_distinct_zones(self): ...            # == GOLDEN["distinct_zones"]
    def test_max_reading(self): ...               # within tolerance
    def test_null_rate_query(self): ...            # == GOLDEN["null_rate_..."]

    # Multi-table join tests (must all pass)
    def test_complaints_per_zone(self): ...       # top zone matches golden
    def test_readings_per_sensor_type(self): ...  # join path correct
    def test_sites_with_inspections(self): ...    # count matches
    def test_programs_without_incidents(self): ... # LEFT JOIN correct

    # Result shape tests
    def test_aggregation_not_exploded(self): ...  # GROUP BY produces < N rows
    def test_scalar_returns_one_row(self): ...    # "How many" returns 1 row
    def test_breakdown_returns_groups(self): ...  # "per county" returns distinct counties

    # Warning tests
    def test_dropped_column_warning(self): ...
    def test_ungrounded_term_warning(self): ...
    def test_broken_join_does_not_silently_succeed(self): ...
```

### Wave E1 Gate (MET -- 2026-04-19)

- [x] Golden answer SQL tests pass at >= 80%: **88.9% (16/18)**
- [x] All 8 SQL correctness tests pass (100%)
- [x] All 5 visualization tests pass (100%)
- [x] Suggestion SQL hints: 15/15 execute without error (100%)
- [ ] ~~Every dropped column produces a user-visible warning~~ (deferred -- E1.1 planned approach)
- [ ] ~~Join paths validated against actual FK integrity~~ (deferred -- E1.2 planned approach)
- [ ] ~~`_is_read_only` uses EXPLAIN~~ (deferred -- E1.5, low priority)
- [ ] ~~Repair loop classifies errors~~ (deferred -- E1.6, LLM-only path)

**Remaining 2/18 failures are Wave E2 scope (statistical):**
- `test_known_anomaly_detected` -- anomaly detection misses 10x spike
- `test_known_correlation_detected` -- correlation detection misses perfect linear relationship

---

## Wave E2: Fix Statistical Rigor

**Goal:** Every statistical insight presented to the user is mathematically defensible.
No false positives from sloppy methodology.
**Duration:** 2 weeks
**Gating condition:** Statistical correctness tests pass at 100%. Zero false positives
on synthetic uniform data.

### E2.1 -- Multiple Comparison Correction (Day 1-2)

**Problem:** `statistical.py` scans all tables, all temporal/metric pairs, applies
p < 0.05 to each independently. With 100 tables x 10 metrics = 1000 tests, expect
~50 false positives by chance alone.

**Fix in:** `headwater/headwater/explorer/statistical.py`

**Implementation:** Benjamini-Hochberg FDR control.
```python
def _apply_fdr_correction(insights: list[StatisticalInsight], alpha: float = 0.05) -> list[StatisticalInsight]:
    """Filter insights using Benjamini-Hochberg False Discovery Rate control."""
    if not insights:
        return []
    # Sort by p-value ascending
    sorted_insights = sorted(insights, key=lambda i: i.p_value or 1.0)
    m = len(sorted_insights)
    corrected = []
    for i, insight in enumerate(sorted_insights):
        if insight.p_value is not None:
            bh_threshold = alpha * (i + 1) / m
            if insight.p_value <= bh_threshold:
                corrected.append(insight)
            else:
                break  # All subsequent p-values are larger
        else:
            corrected.append(insight)  # Keep non-p-value insights
    return corrected
```

**Integration:** Apply at the end of `detect_insights()` before returning.

**Correctness check:** `test_fdr_eliminates_false_positives()` -- generate 100 tables
of pure random data, assert fewer than 3 insights returned (not 50).

### E2.2 -- Normality Testing Before Z-Scores (Day 2-3)

**Problem:** Z-score anomaly detection assumes Gaussian distribution. Skewed distributions
(log-normal, exponential) produce misleading z-scores.

**Fix in:** `headwater/headwater/explorer/statistical.py` (`_detect_temporal_anomalies`)

**Implementation:**
```python
from scipy.stats import shapiro, jarque_bera

def _check_normality(values: list[float], sample_size: int = 200) -> bool:
    """Test if data is approximately normal using Jarque-Bera test."""
    sample = values[:sample_size] if len(values) > sample_size else values
    if len(sample) < 20:
        return True  # Not enough data to test; assume normal
    stat, p = jarque_bera(sample)
    return p > 0.05  # Fail to reject normality

def _detect_temporal_anomalies(df, temporal_col, metric_col, ...):
    values = df[metric_col].to_list()

    if not _check_normality(values):
        # Use robust statistics instead of z-score
        # MAD (Median Absolute Deviation) based detection
        median = statistics.median(values)
        mad = statistics.median([abs(v - median) for v in values])
        modified_z = 0.6745 * (value - median) / mad if mad > 0 else 0
        # Use modified_z instead of standard z-score
```

**Correctness check:** `test_lognormal_data_uses_mad()` -- generate log-normal data,
verify MAD-based detection is used, not raw z-score.

### E2.3 -- Seasonal Adjustment (Day 3-5)

**Problem:** Rolling z-score detection flags predictable seasonal peaks as anomalies.
Air quality data with summer/winter cycles would produce false alerts every season.

**Fix in:** `headwater/headwater/explorer/statistical.py`

**Implementation:** STL-style seasonal decomposition using scipy.
```python
def _deseasonalize(values: list[float], period: int | None = None) -> tuple[list[float], bool]:
    """Remove seasonal component if detected. Returns (residuals, is_seasonal)."""
    if len(values) < 2 * (period or 12):
        return values, False

    # Auto-detect period using autocorrelation
    if period is None:
        period = _detect_period(values)
        if period is None:
            return values, False

    # Simple seasonal decomposition: compute seasonal index per period position
    n = len(values)
    seasonal_index = [0.0] * period
    counts = [0] * period
    for i, v in enumerate(values):
        pos = i % period
        seasonal_index[pos] += v
        counts[pos] += 1
    seasonal_index = [s / c if c > 0 else 0 for s, c in zip(seasonal_index, counts)]
    grand_mean = sum(seasonal_index) / period

    # Residuals = observed - seasonal + grand_mean
    residuals = [values[i] - seasonal_index[i % period] + grand_mean for i in range(n)]
    return residuals, True
```

**Integration:** In `_detect_temporal_anomalies()`, deseasonalize before computing
rolling z-scores. If seasonal component found, note it in the insight description:
"After removing seasonal pattern (period=12), ..."

**Correctness check:** `test_seasonal_data_no_false_anomalies()` -- generate data with
clear sine-wave seasonality + one true spike, verify only the spike is flagged.

### E2.4 -- Change-Point Detection (Day 5-7)

**Problem:** `_detect_period_shifts()` splits data at midpoint and compares halves.
This is statistically meaningless for data with multiple inflection points or non-linear
trends.

**Fix in:** `headwater/headwater/explorer/statistical.py`

**Replace with PELT-inspired change-point detection:**
```python
def _detect_change_points(values: list[float], min_segment: int = 10) -> list[int]:
    """Find change points using binary segmentation with BIC penalty."""
    if len(values) < 2 * min_segment:
        return []

    def segment_cost(start: int, end: int) -> float:
        segment = values[start:end]
        if len(segment) < 2:
            return 0
        var = statistics.variance(segment) if len(segment) > 1 else 1e-10
        return len(segment) * math.log(max(var, 1e-10))

    def binary_segmentation(start: int, end: int, depth: int = 0) -> list[int]:
        if end - start < 2 * min_segment or depth > 5:
            return []
        total_cost = segment_cost(start, end)
        best_cp, best_gain = -1, 0
        for cp in range(start + min_segment, end - min_segment):
            split_cost = segment_cost(start, cp) + segment_cost(cp, end)
            gain = total_cost - split_cost
            if gain > best_gain:
                best_gain = gain
                best_cp = cp
        # BIC penalty: log(n) * num_params
        penalty = math.log(end - start) * 2
        if best_gain > penalty and best_cp > 0:
            left_cps = binary_segmentation(start, best_cp, depth + 1)
            right_cps = binary_segmentation(best_cp, end, depth + 1)
            return left_cps + [best_cp] + right_cps
        return []

    return binary_segmentation(0, len(values))
```

**Integration:** Replace `_detect_period_shifts()` with change-point analysis.
For each detected change point, compute the before/after means and report the
shift magnitude. Generate one insight per change point, not one per midpoint split.

**Correctness check:** `test_change_point_at_known_location()` -- generate data with
flat values for 50 points, then 2x values for 50 points. Verify change point detected
at position 50 (+/- 3).

### E2.5 -- Correlation Rigor (Day 7-8)

**Problem:** Pearson correlation on time-series data is meaningless without
autocorrelation correction. Two columns that both trend upward will show r > 0.9
even if they are unrelated.

**Fix in:** `headwater/headwater/explorer/statistical.py` (`_detect_correlations`)

**Changes:**
1. Before computing Pearson r, check for trend in both columns using linear
   regression. If both have significant trends (p < 0.05), detrend first.
2. After detrending, compute Pearson r on residuals.
3. Report both raw correlation and detrended correlation.
4. Add `detrended: bool` flag to StatisticalInsight.
5. Raise threshold from r >= 0.6 to r >= 0.7 for detrended correlations.

**Correctness check:**
- `test_trending_columns_detrended()` -- two independently trending columns show
  low detrended correlation (< 0.3)
- `test_genuine_correlation_survives_detrending()` -- col_b = 2*col_a + noise,
  correlation detected even after detrending

### E2.6 -- Outlier Robustness (Day 8-9)

**Problem:** A single bad data value can inflate rolling_std, suppressing all other
anomaly detections.

**Fix in:** `headwater/headwater/explorer/statistical.py`

**Implementation:** Use IQR-based winsorization before computing rolling statistics.
```python
def _winsorize(values: list[float], percentile: float = 0.01) -> list[float]:
    """Clip extreme values to the 1st/99th percentile."""
    sorted_v = sorted(values)
    low = sorted_v[int(len(sorted_v) * percentile)]
    high = sorted_v[int(len(sorted_v) * (1 - percentile))]
    return [max(low, min(high, v)) for v in values]
```

**Correctness check:** `test_single_outlier_does_not_suppress_detection()` -- data with
one 1000x outlier and one 5x anomaly. Verify the 5x anomaly is still detected.

### E2.7 -- Insight Severity Calibration (Day 9-10)

**Problem:** Severity assignment is hardcoded (z >= 3.0 = critical, z >= 2.5 = warning).
This ignores domain context and data volume.

**Fix in:** `headwater/headwater/explorer/statistical.py`

**Changes:**
1. Severity accounts for data volume: z=2.5 on 20 data points is less significant
   than z=2.5 on 2000 data points. Use effective sample size.
2. Severity accounts for magnitude: a 2% shift (even if statistically significant)
   is `info`, not `warning`.
3. Add minimum magnitude thresholds:
   - `critical`: |magnitude| > 50% AND p < 0.001
   - `warning`: |magnitude| > 20% AND p < 0.01
   - `info`: |magnitude| > 5% AND p < 0.05
   - Below info thresholds: not reported

**Correctness check:** `test_tiny_magnitude_not_reported()` -- 0.1% shift with
p=0.001 should not be reported (statistically significant but practically meaningless).

### E2.8 -- Statistical Correctness Tests (Day 10-14)

**File:** Update `headwater/tests/test_explore_correctness.py`

```python
class TestStatisticalCorrectness:
    """Every test validates mathematical accuracy of insights."""

    # False positive control
    def test_uniform_data_no_anomalies(self): ...
    def test_random_data_fdr_controlled(self): ...
    def test_tiny_magnitude_not_reported(self): ...

    # True positive detection
    def test_known_spike_detected(self): ...
    def test_known_change_point_located(self): ...
    def test_genuine_correlation_found(self): ...
    def test_seasonal_pattern_not_flagged(self): ...

    # Robustness
    def test_single_outlier_handled(self): ...
    def test_lognormal_uses_robust_stats(self): ...
    def test_detrending_removes_spurious_correlation(self): ...

    # Accuracy
    def test_magnitude_within_tolerance(self): ...
    def test_z_score_matches_manual_calculation(self): ...
    def test_p_value_matches_scipy(self): ...
    def test_change_point_within_3_of_actual(self): ...
```

### Wave E2 Gate

- [ ] Zero false positives on 100 tables of uniform random data
- [ ] FDR correction applied to all multi-test comparisons
- [ ] Non-normal data uses MAD instead of z-score
- [ ] Seasonal patterns deseasonalized before anomaly detection
- [ ] Change-point detection finds true change points (+/- 3 positions)
- [ ] Correlation detrended before reporting
- [ ] Single outliers do not suppress other anomaly detection
- [ ] Severity thresholds include magnitude, not just statistical significance
- [ ] `uv run pytest tests/test_explore_correctness.py::TestStatisticalCorrectness` -- all pass

---

## Wave E3: Fix Visualization Correctness

**Goal:** Every chart is correctly configured for its data. Axes are labeled, types
match semantics, and the visualization actually communicates the answer.
**Duration:** 1.5 weeks
**Gating condition:** Visualization correctness tests pass at 100%.

### E3.1 -- Axis Labels and Units (Day 1-2)

**Problem:** No chart has axis labels. A bar chart shows numbers with no indication
of what they represent.

**Fix in:** `headwater/headwater/explorer/visualization.py`

**Changes to `VisualizationSpec` model:**
```python
class VisualizationSpec(BaseModel):
    chart_type: str
    title: str
    x_axis: str | None = None
    y_axis: str | None = None
    x_label: str | None = None      # NEW: Human-readable axis label
    y_label: str | None = None      # NEW: Human-readable axis label
    x_unit: str | None = None       # NEW: "$", "%", "count", "days", etc.
    y_unit: str | None = None       # NEW
    group_by: str | None = None
    description: str = ""
```

**Label inference from column metadata:**
```python
def _infer_label(col_name: str, profiles: list[ColumnProfile], tables: list[TableInfo]) -> str:
    """Infer human-readable label from column name + metadata."""
    # 1. Check if column has a description in discovery metadata
    for table in tables:
        for col in table.columns:
            if col.name == col_name and col.description:
                return col.description
    # 2. Humanize the column name
    return col_name.replace("_", " ").title()

def _infer_unit(col_name: str, agg: str | None, profiles: list[ColumnProfile]) -> str | None:
    """Infer unit from column semantics."""
    if agg in ("count", "COUNT"):
        return "count"
    if "pct" in col_name or "rate" in col_name or "ratio" in col_name:
        return "%"
    if "amount" in col_name or "cost" in col_name or "price" in col_name or "revenue" in col_name:
        return "$"
    if "duration" in col_name or "seconds" in col_name:
        return "seconds"
    return None
```

**Frontend changes in `result-chart.tsx`:**
- Add `<XAxis label={{ value: spec.x_label, position: "bottom" }} />` to all charts
- Add `<YAxis label={{ value: spec.y_label, angle: -90, position: "left" }} />` to all charts
- If `y_unit` is "$", format tick values with `$` prefix
- If `y_unit` is "%", format tick values with `%` suffix

**Correctness check:** `test_axes_have_labels()` -- every visualization spec returned
from `ask()` has non-null `x_label` and `y_label` when chart_type is bar/line/scatter.

### E3.2 -- Fix Column Classification (Day 2-3)

**Problem:** `_classify_columns()` samples first 50 rows and uses regex on column names.
A column named "date_reported" containing text gets classified as temporal.

**Fix in:** `headwater/headwater/explorer/visualization.py`

**Changes:**
1. Use column dtype from query result metadata, not value sampling
2. Cross-reference with discovery profiles when available (passed as optional param)
3. Classify by: dtype first, then column name pattern, then value sampling as last resort
4. Add cardinality awareness: a numeric column with 3 distinct values is a dimension,
   not a metric (even if it's INT type)

```python
def _classify_columns(
    columns: list[str],
    data: list[dict],
    column_types: dict[str, str] | None = None,    # NEW: from DuckDB result metadata
    profiles: list[ColumnProfile] | None = None,     # NEW: from discovery
) -> tuple[list[str], list[str], list[str]]:
    """Returns (temporal_cols, metric_cols, dimension_cols)."""
```

**Correctness check:** `test_low_cardinality_numeric_is_dimension()` -- column "status"
with values [1, 2, 3] classified as dimension, not metric.

### E3.3 -- Fix Chart Type Selection (Day 3-4)

**Problem:** Chart type selected by row count thresholds. 30 rows = bar, 31 = table.
No relationship to data semantics.

**Fix in:** `headwater/headwater/explorer/visualization.py`

**Replace threshold logic with semantic rules:**
```python
def recommend_visualization(columns, data, question, column_types=None, profiles=None):
    temporal, metrics, dimensions = _classify_columns(columns, data, column_types, profiles)

    # Rule 1: Single row, single metric -> KPI
    if len(data) == 1 and len(metrics) >= 1:
        return _kpi_spec(...)

    # Rule 2: Temporal dimension + 1 metric -> Line chart
    if temporal and len(metrics) >= 1:
        return _line_spec(temporal[0], metrics[0], ...)

    # Rule 3: Categorical dimension + 1 metric, distinct < 30 -> Bar chart
    if dimensions and metrics:
        distinct = len({row.get(dimensions[0]) for row in data})
        if distinct <= 30:
            return _bar_spec(dimensions[0], metrics[0], ...)

    # Rule 4: 2 metrics, no dimension -> Scatter
    if len(metrics) >= 2 and not dimensions:
        return _scatter_spec(metrics[0], metrics[1], ...)

    # Rule 5: 2 dimensions + 1 metric -> Heatmap
    if len(dimensions) >= 2 and metrics:
        return _heatmap_spec(dimensions[0], dimensions[1], metrics[0], ...)

    # Rule 6: High cardinality dimension -> Table
    return _table_spec(...)
```

**Correctness check:** `test_chart_type_matches_data_semantics()` -- temporal data
always gets line chart, regardless of row count.

### E3.4 -- Fix Scatter Plot and Heatmap (Day 4-5)

**Problem:** Scatter hardcodes first 2 numeric columns. Heatmap picks first numeric
column that isn't an axis. Both can silently select wrong columns.

**Fix in:** `headwater/headwater/explorer/visualization.py` and `ui/src/components/result-chart.tsx`

**Scatter fix:**
- Use the columns explicitly selected by the user's question (from decomposition)
- If not available, use the two columns with highest variance (most interesting scatter)
- Add R-squared annotation to the chart

**Heatmap fix:**
- Use the metric column from the visualization spec (not auto-detect)
- If multiple metrics, let user toggle between them
- Add color scale legend with min/max values

**Frontend `result-chart.tsx` changes:**
- Scatter: Add `<ReferenceLine>` for mean x and mean y (crosshair guides)
- Scatter: Display correlation coefficient in chart subtitle
- Heatmap: Add explicit color legend bar below the grid
- Heatmap: Label the metric being displayed

**Correctness check:** `test_scatter_uses_correct_columns()` -- "Compare reading value
to sensor count" uses those specific columns, not first-2-found.

### E3.5 -- Fix KPI Rendering (Day 5-6)

**Problem:** KPI shows raw key-value pairs with no context. "Total complaints: 10000"
tells you nothing without comparison.

**Fix in:** `headwater/headwater/explorer/visualization.py` and `ui/src/app/explore/page.tsx`

**Add KPI context:**
```python
class KPIContext(BaseModel):
    """Contextual information for single-value KPIs."""
    value: float | int | str
    label: str
    unit: str | None = None
    comparison: str | None = None        # "vs 8,500 last period"
    trend_direction: str | None = None   # "up", "down", "stable"
    trend_pct: float | None = None       # +17.6%
    sparkline: list[float] | None = None # Last N values for mini trend
```

**Computation:** When a KPI query returns a single value:
1. Check if the same metric exists in profiles with historical data
2. If temporal column available, compute the trend (last 6 periods)
3. Add sparkline data (last 12 values) for inline trend visualization

**Frontend KPI card:**
- Large number (current value)
- Below: trend arrow + percentage change + "vs previous period"
- Right: sparkline (tiny line chart, 60px tall, no axes, just the shape)

**Correctness check:** `test_kpi_has_trend_context()` -- KPI for "total readings" includes
trend direction and sparkline data.

### E3.6 -- Frontend Validation Layer (Day 6-7)

**What:** Add a validation layer in the frontend that catches spec/data mismatches
before rendering, preventing silent chart failures.

**File:** `ui/src/lib/chart-validator.ts` (new, ~80 lines)

```typescript
interface ValidationResult {
  valid: boolean;
  warnings: string[];
  fallback_type: ChartType | null;  // If invalid, suggest fallback
}

function validateChartSpec(
  spec: VisualizationSpec,
  data: Record<string, unknown>[]
): ValidationResult {
  const warnings: string[] = [];
  const columns = data.length > 0 ? Object.keys(data[0]) : [];

  // Check axes reference real columns
  if (spec.x_axis && !columns.includes(spec.x_axis)) {
    warnings.push(`x_axis '${spec.x_axis}' not found in data`);
    return { valid: false, warnings, fallback_type: "table" };
  }
  if (spec.y_axis && !columns.includes(spec.y_axis)) {
    warnings.push(`y_axis '${spec.y_axis}' not found in data`);
    return { valid: false, warnings, fallback_type: "table" };
  }

  // Check cardinality for bar charts
  if (spec.chart_type === "bar" && spec.x_axis) {
    const distinct = new Set(data.map(r => r[spec.x_axis!])).size;
    if (distinct > 50) {
      warnings.push(`Bar chart with ${distinct} categories will be unreadable`);
      return { valid: false, warnings, fallback_type: "table" };
    }
  }

  // Check for all-null columns
  if (spec.y_axis) {
    const allNull = data.every(r => r[spec.y_axis!] == null);
    if (allNull) {
      warnings.push(`All values in '${spec.y_axis}' are null`);
      return { valid: false, warnings, fallback_type: "table" };
    }
  }

  return { valid: true, warnings, fallback_type: null };
}
```

**Integration:** In `explore/page.tsx`, call `validateChartSpec()` before rendering
`<ResultChart>`. If invalid, render table with warning banner explaining why the
chart was not shown.

### E3.7 -- Visualization Correctness Tests (Day 7-10)

**File:** Update `headwater/tests/test_explore_correctness.py`

```python
class TestVisualizationCorrectness:
    """Every test validates the chart spec is correct for the data."""

    # Axis validation
    def test_axes_reference_real_columns(self): ...
    def test_axes_have_labels(self): ...
    def test_unit_inferred_for_count(self): ...
    def test_unit_inferred_for_percentage(self): ...

    # Chart type selection
    def test_temporal_gets_line(self): ...
    def test_categorical_gets_bar(self): ...
    def test_single_value_gets_kpi(self): ...
    def test_two_metrics_gets_scatter(self): ...
    def test_high_cardinality_gets_table(self): ...

    # KPI context
    def test_kpi_has_trend(self): ...
    def test_kpi_sparkline_length(self): ...

    # Edge cases
    def test_empty_data_no_chart(self): ...
    def test_all_null_column_fallback(self): ...
    def test_single_row_single_col_kpi(self): ...

    # Column classification
    def test_low_cardinality_numeric_is_dimension(self): ...
    def test_date_column_is_temporal(self): ...
    def test_id_column_excluded_from_metrics(self): ...
```

### Wave E3 Gate

- [ ] Every chart has axis labels and units
- [ ] Column classification uses dtype + cardinality, not just regex
- [ ] Chart type selection uses semantic rules, not row-count thresholds
- [ ] Scatter plots use question-relevant columns, not first-2-found
- [ ] Heatmaps display the correct metric with color legend
- [ ] KPIs include trend direction, change percentage, and sparkline
- [ ] Frontend validates specs before rendering (fallback to table on mismatch)
- [ ] `uv run pytest tests/test_explore_correctness.py::TestVisualizationCorrectness` -- all pass

---

## Wave E4: Deep Insights Engine

**Goal:** Insights that make a data professional say "I didn't know that about my data."
Not just "this column has nulls" but "your complaint volume dropped 34% in March and has
not recovered -- this coincides with a sensor going offline at site 7."
**Duration:** 2.5 weeks
**Gating condition:** Insight accuracy tests pass. Every insight verified against
ground truth.

### E4.1 -- Distribution Analysis (Day 1-3)

**What:** Characterize the shape of every numeric column. This is foundational context
that feeds into anomaly detection, chart selection, and narrative generation.

**File:** `headwater/headwater/explorer/distribution.py` (new, ~200 lines)

**Analysis per numeric column:**
```python
@dataclass
class DistributionProfile:
    column: str
    table: str
    shape: Literal["normal", "lognormal", "uniform", "bimodal", "heavy_tailed", "skewed"]
    skewness: float                 # scipy.stats.skew
    kurtosis: float                 # scipy.stats.kurtosis (excess)
    is_normal: bool                 # Jarque-Bera test p > 0.05
    recommended_center: str         # "mean" or "median"
    recommended_spread: str         # "stddev" or "iqr"
    percentiles: dict[str, float]   # p5, p25, p50, p75, p95
    outlier_count: int              # Values beyond 3*IQR
    outlier_pct: float
```

**Shape classification rules:**
- Normal: Jarque-Bera p > 0.05, |skewness| < 0.5, |kurtosis| < 1
- Lognormal: skewness > 1, all values positive, log(values) passes normality test
- Uniform: kurtosis < -1, range/stddev > 3.4
- Bimodal: Hartigan's dip test p < 0.05
- Heavy-tailed: kurtosis > 3
- Skewed: |skewness| > 1

**Integration:** Run during `detect_insights()`. Store distributions. Feed into:
- Anomaly detection (choose z-score vs MAD based on shape)
- Visualization (histogram annotation with shape label)
- Narrative generation ("readings follow a log-normal distribution...")

**Correctness check:** `test_known_normal_classified()` -- generate N(0,1) data,
verify shape="normal". `test_known_lognormal_classified()` -- generate lognormal
data, verify shape="lognormal".

### E4.2 -- Cross-Table Pattern Detection (Day 3-6)

**What:** Find patterns that span multiple tables -- the insights humans miss because
they look at one table at a time.

**File:** `headwater/headwater/explorer/cross_table.py` (new, ~250 lines)

**Pattern types:**

**1. Temporal Coincidence**
```
"Complaint volume dropped 34% in March. During the same period,
 sensor offline events at site 7 increased 5x."
```
- For every temporal anomaly in table A, check if table B has a coinciding anomaly
- Use the join graph to identify related tables
- Score coincidence by: temporal overlap + FK relationship strength

**2. Referential Gap Detection**
```
"47 complaints reference zone_id values that do not exist in the zones table.
 These orphan records account for 4.7% of all complaints."
```
- For every FK relationship, compute orphan count (already have referential_integrity)
- Report as insight if orphan_pct > 1%

**3. Cardinality Imbalance**
```
"80% of readings come from 3 sensors (out of 200).
 Sensor coverage is highly uneven."
```
- For every FK join, compute distribution of parent references
- Report if top 10% of parents account for > 80% of children (Pareto imbalance)

**4. Temporal Coverage Gaps**
```
"Readings exist from Jan 2023 to Dec 2025, but March 2024 has zero records.
 All other months have 800-1200 records."
```
- For tables with temporal columns, build a calendar grid
- Detect months/weeks with zero or near-zero records vs. the baseline

**Correctness checks:**
- `test_orphan_detection_matches_golden()` -- orphan count matches SQL verification
- `test_temporal_gap_detected()` -- insert data with a known gap, verify detected
- `test_pareto_imbalance_detected()` -- create skewed FK distribution, verify flagged

### E4.3 -- Insight Narratives (Day 6-8)

**What:** Transform raw statistical findings into human-readable narratives that
explain what the insight means and why it matters.

**File:** `headwater/headwater/explorer/narrative.py` (new, ~200 lines)

**Narrative templates by insight type:**

```python
TEMPLATES = {
    "temporal_anomaly": (
        "{metric} in {table} showed an unusual {direction} of {magnitude}% "
        "on {time_period} (z-score: {z_score:.1f}, p < {p_value:.4f}). "
        "{context}"
    ),
    "change_point": (
        "{metric} in {table} shifted from an average of {before_mean:.1f} to "
        "{after_mean:.1f} around {change_date}. This represents a {magnitude}% "
        "{direction}. {context}"
    ),
    "correlation": (
        "{metric_a} and {metric_b} in {table} are {strength} correlated "
        "(r={r_value:.2f}, p < {p_value:.4f}){detrend_note}. {context}"
    ),
    "distribution_shift": (
        "The distribution of {metric} in {table} has shifted: {description}. {context}"
    ),
    "temporal_coincidence": (
        "{anomaly_a} in {table_a} coincides with {anomaly_b} in {table_b}. "
        "These tables are linked through {join_path}. {context}"
    ),
    "referential_gap": (
        "{orphan_count} records in {child_table}.{fk_column} reference values "
        "that do not exist in {parent_table}. This affects {orphan_pct:.1f}% "
        "of records. {context}"
    ),
    "cardinality_imbalance": (
        "{top_pct}% of {metric} in {child_table} come from {top_count} "
        "{parent_table} records (out of {total_parents}). {context}"
    ),
    "coverage_gap": (
        "{table} has no records for {gap_periods}. Surrounding periods "
        "average {baseline_avg:.0f} records. {context}"
    ),
}
```

**Context generation:** For each insight, add actionable context:
- For anomalies: "Investigate whether this coincides with a deployment or data source outage."
- For referential gaps: "Consider adding a foreign key constraint or cleaning orphan records."
- For coverage gaps: "Check if data ingestion was interrupted during this period."

**Correctness check:** `test_narrative_contains_correct_values()` -- verify that
magnitude, z-score, and p-value in the narrative match the raw insight values.

### E4.4 -- Insight Ranking and Deduplication (Day 8-9)

**What:** Rank insights by interestingness and deduplicate redundant findings.

**File:** `headwater/headwater/explorer/insight_ranker.py` (new, ~100 lines)

**Ranking score:**
```python
def compute_interest_score(insight: StatisticalInsight) -> float:
    """Score from 0-1 based on how surprising and actionable the insight is."""
    score = 0.0

    # Statistical strength (0-0.3)
    if insight.p_value:
        score += 0.3 * (1 - insight.p_value)

    # Magnitude (0-0.3)
    magnitude_abs = abs(insight.magnitude) / 100
    score += 0.3 * min(1.0, magnitude_abs / 0.5)  # Saturates at 50% change

    # Novelty: cross-table insights are more interesting than single-table (0-0.2)
    if insight.insight_type in ("temporal_coincidence", "cardinality_imbalance"):
        score += 0.2

    # Actionability: referential gaps and coverage gaps are directly fixable (0-0.2)
    if insight.insight_type in ("referential_gap", "coverage_gap"):
        score += 0.2

    return min(1.0, score)
```

**Deduplication rules:**
- If two anomalies are in the same column within 3 time periods, keep the one with
  higher z-score
- If a correlation and a temporal coincidence describe the same column pair, merge
  them into the temporal coincidence (more specific)
- If a referential gap and a cardinality imbalance describe the same FK, merge them

**Correctness check:** `test_duplicate_anomalies_merged()` -- two adjacent anomalies
in the same column produce one insight, not two.

### E4.5 -- Insights API Enhancement (Day 9-11)

**Fix in:** `headwater/headwater/api/routes/explore.py`

**Changes to `GET /explore/suggestions`:**
- Return insights ranked by interest score (highest first)
- Include distribution profiles for top columns
- Include cross-table patterns
- Add `insight_count` and `insight_quality` metrics to response

**New endpoint: `GET /explore/insights/{table_name}`**
- Return insights specific to one table
- Include distribution profiles for all columns
- Include temporal coverage analysis
- Include relationship patterns (orphans, imbalance)

**Response structure:**
```json
{
  "insights": [
    {
      "type": "temporal_anomaly",
      "severity": "critical",
      "interest_score": 0.87,
      "narrative": "Reading values spiked 340% on 2024-03-15...",
      "raw": { "magnitude": 340, "z_score": 4.2, "p_value": 0.00003 },
      "visualization": {
        "chart_type": "line",
        "data": [...],  // Time series with anomaly highlighted
        "annotations": [{ "x": "2024-03-15", "label": "Anomaly" }]
      }
    }
  ],
  "distributions": [...],
  "coverage": {...},
  "quality_score": 0.82
}
```

**Key change:** Each insight now includes its own `visualization` with pre-computed
data and annotations. The frontend does not need to generate a separate chart -- it
renders the insight's embedded visualization directly.

### E4.6 -- Deep Insights Correctness Tests (Day 11-13)

**File:** Update `headwater/tests/test_explore_correctness.py`

```python
class TestDeepInsightsCorrectness:
    """Validate that deep insights are accurate and non-spurious."""

    # Distribution analysis
    def test_normal_data_classified_correctly(self): ...
    def test_lognormal_data_classified_correctly(self): ...
    def test_uniform_data_classified_correctly(self): ...
    def test_bimodal_data_detected(self): ...

    # Cross-table patterns
    def test_orphan_count_matches_sql(self): ...
    def test_pareto_imbalance_detected(self): ...
    def test_temporal_gap_detected(self): ...
    def test_temporal_coincidence_found(self): ...

    # Narratives
    def test_narrative_values_match_raw(self): ...
    def test_narrative_direction_correct(self): ...

    # Ranking
    def test_critical_insights_ranked_first(self): ...
    def test_duplicate_anomalies_merged(self): ...
    def test_cross_table_ranked_above_single_table(self): ...

    # End-to-end on sample data
    def test_sample_data_produces_at_least_5_insights(self): ...
    def test_sample_data_insights_all_have_narratives(self): ...
    def test_sample_data_insights_all_have_visualizations(self): ...
    def test_no_insight_references_nonexistent_table(self): ...
    def test_no_insight_references_nonexistent_column(self): ...
```

### Wave E4 Gate

- [ ] Distribution analysis correctly classifies normal, lognormal, uniform, bimodal
- [ ] Cross-table patterns detect orphans, imbalance, temporal gaps, coincidences
- [ ] Every insight has a human-readable narrative with correct values
- [ ] Insights ranked by interest score (most surprising first)
- [ ] Duplicate insights merged
- [ ] Each insight includes its own pre-computed visualization
- [ ] Sample data produces >= 5 verified insights with narratives
- [ ] `uv run pytest tests/test_explore_correctness.py::TestDeepInsightsCorrectness` -- all pass

---

## Wave E5: Visualization Overhaul (WOW Layer)

**Goal:** Charts that tell a story. When a user sees the explore page, the visuals
should communicate the answer before they read the numbers.
**Duration:** 2 weeks
**Gating condition:** Visual accuracy tests pass. Every chart annotation verified.

### E5.1 -- Annotated Line Charts (Day 1-3)

**What:** Line charts that highlight anomalies, change points, and trends directly
on the chart.

**File:** `ui/src/components/annotated-line-chart.tsx` (new, ~200 lines)

**Features:**
- **Anomaly markers:** Red dots on data points flagged as anomalies
- **Change-point lines:** Vertical dashed line at detected change points with
  "before: avg X / after: avg Y" annotation
- **Trend line:** Light gray linear regression line behind the data
- **Confidence band:** Shaded area showing +/- 1 standard deviation around
  rolling mean
- **Period comparison:** Side-by-side highlighted segments for "this period vs last"

**Data contract from backend:**
```typescript
interface AnnotatedChartData {
  series: { x: string; y: number }[];
  anomalies: { x: string; y: number; z_score: number }[];
  change_points: { x: string; before_avg: number; after_avg: number }[];
  trend: { slope: number; intercept: number; r_squared: number };
  confidence_band: { x: string; upper: number; lower: number }[];
}
```

**Backend support:** Add `_compute_chart_annotations()` to `visualization.py`
that computes trend, confidence band, and marks anomalies/change-points from
the insight data.

**Correctness check:** `test_anomaly_markers_at_correct_positions()` -- known spike
in data has a red dot at the correct x-position.

### E5.2 -- Enriched Bar Charts (Day 3-4)

**What:** Bar charts with comparison context and statistical annotations.

**File:** Update `ui/src/components/result-chart.tsx`

**Features:**
- **Reference line:** Average value shown as horizontal dashed line with label
- **Top-N highlight:** Top 3 bars in accent color, rest in neutral
- **Value labels:** Numeric values above each bar (for <= 15 bars)
- **Sorting:** Bars sorted descending by value (not alphabetical)

**Correctness check:** `test_bar_chart_sorted_descending()` -- verify data order.

### E5.3 -- Scatter with Statistics (Day 4-5)

**What:** Scatter plots that show the relationship, not just dots.

**File:** Update `ui/src/components/result-chart.tsx`

**Features:**
- **Regression line:** Linear fit with equation displayed (y = mx + b)
- **R-squared badge:** Correlation strength in top-right corner
- **Quadrant lines:** Mean x and mean y as crosshair reference lines
- **Outlier highlighting:** Points beyond 2 IQR in orange

**Correctness check:** `test_scatter_r_squared_matches_backend()` -- R-squared in
chart matches the value computed by `_detect_correlations()`.

### E5.4 -- Insight Cards with Inline Charts (Day 5-8)

**What:** Replace text-only insight cards with cards that include small inline
visualizations.

**File:** `ui/src/components/insight-card.tsx` (new, ~250 lines)

**Card variants by insight type:**

**Temporal Anomaly Card:**
```
+---------------------------------------------+
| CRITICAL: Reading values spiked 340%         |
|                                              |
| [--- mini line chart (120x60px) ---]         |
|        with red dot on anomaly               |
|                                              |
| z-score: 4.2  |  p < 0.0001  |  Mar 2024    |
| Table: readings  |  Metric: value            |
+---------------------------------------------+
```

**Change Point Card:**
```
+---------------------------------------------+
| WARNING: Complaint volume shifted            |
|                                              |
| Before: avg 450/mo  ->  After: avg 290/mo   |
| [--- mini line chart with vertical line ---] |
|                                              |
| Change detected around: 2024-06-15           |
| Magnitude: -35%  |  Confidence: 99.9%        |
+---------------------------------------------+
```

**Correlation Card:**
```
+---------------------------------------------+
| INFO: Strong correlation found               |
|                                              |
| [--- mini scatter plot (120x60px) ---]       |
|        with regression line                  |
|                                              |
| reading_value ~ sensor_age (r=0.78)          |
| After detrending: r=0.65                     |
+---------------------------------------------+
```

**Referential Gap Card:**
```
+---------------------------------------------+
| WARNING: Orphan records detected             |
|                                              |
| [--- donut chart: 95.3% matched, 4.7% gap --]|
|                                              |
| 47 complaints reference non-existent zones   |
| Action: Review zone_id values in complaints  |
+---------------------------------------------+
```

**Coverage Gap Card:**
```
+---------------------------------------------+
| WARNING: Missing data period                 |
|                                              |
| [--- calendar heatmap (120x40px) ---]        |
|        with gap months in red                |
|                                              |
| March 2024: 0 records (avg: 950/month)       |
| Action: Check data ingestion logs            |
+---------------------------------------------+
```

### E5.5 -- Explore Page Layout Overhaul (Day 8-10)

**What:** Restructure the explore page for maximum impact.

**File:** `ui/src/app/explore/page.tsx` (major rewrite of insight + result sections)

**New layout:**

```
+-------------------------------------------------------+
| EXPLORE YOUR DATA                                      |
|                                                        |
| [Question input bar .................................]  |
|                                                        |
| TABS: [Key Findings] [Ask Questions] [Distributions]  |
+-------------------------------------------------------+

KEY FINDINGS TAB (default -- the WOW moment):
+-------------------------------------------------------+
| DATA HEALTH SUMMARY                                    |
| +----------+ +----------+ +----------+ +----------+   |
| | 59.9K    | | 8 tables | | 12 FK    | | 3 issues |   |
| | records  | | profiled | | detected | | found    |   |
| +----------+ +----------+ +----------+ +----------+   |
|                                                        |
| TOP INSIGHTS (sorted by interest score)                |
| +---------------------------------------------------+ |
| | [Insight Card 1 -- critical anomaly with chart]    | |
| +---------------------------------------------------+ |
| | [Insight Card 2 -- referential gap with donut]     | |
| +---------------------------------------------------+ |
| | [Insight Card 3 -- coverage gap with calendar]     | |
| +---------------------------------------------------+ |
| | [Insight Card 4 -- correlation with scatter]       | |
| +---------------------------------------------------+ |
|                                                        |
| DISTRIBUTION HIGHLIGHTS                                |
| [sparkline: readings.value - lognormal, skew=1.4]     |
| [sparkline: complaints.count - normal, symmetric]      |
+-------------------------------------------------------+

ASK QUESTIONS TAB:
+-------------------------------------------------------+
| SUGGESTED QUESTIONS (ranked by relevance)              |
| +-------------------------+ +-------------------------+|
| | How many complaints...  | | What is the average...  ||
| +-------------------------+ +-------------------------+|
|                                                        |
| QUERY RESULT (after asking):                           |
| +---------------------------------------------------+ |
| | [Annotated chart with labels, trend, annotations] | |
| +---------------------------------------------------+ |
| | SQL   | Data Table  | Explanation                  | |
| +---------------------------------------------------+ |
+-------------------------------------------------------+

DISTRIBUTIONS TAB:
+-------------------------------------------------------+
| Per-column distribution profiles with sparkline        |
| histograms, shape classification, outlier counts       |
+-------------------------------------------------------+
```

**Key design decisions:**
- Key Findings tab is default -- user sees insights immediately without asking
- Each insight card has its own inline chart (no separate chart component)
- Ask Questions tab separates the NL-to-SQL flow from passive insights
- Distribution tab for power users who want statistical detail

### E5.6 -- Suggestion Quality Overhaul (Day 10-12)

**Fix in:** `headwater/headwater/explorer/suggestions.py`

**Changes:**
1. **Rank suggestions by expected insight value**, not fixed order:
   - Questions about columns with anomalies ranked first
   - Cross-table questions ranked above single-table
   - Quality questions ranked above generic aggregations
2. **Replace generic mart questions** ("What are the key metrics in X?") with
   specific analytical questions derived from the semantic catalog:
   - "How does {metric} vary across {dimension}?"
   - "Which {entity} has the highest {metric}?"
   - "Has {metric} changed over time?"
3. **Preview badge:** Each suggestion shows expected result type (chart icon)
   and confidence level (from decomposition)
4. **Validate SQL hints:** Before returning a suggestion, verify its `sql_hint`
   compiles against the schema. Drop suggestions with invalid SQL.

**Correctness check:** `test_all_suggestion_sql_hints_compile()` -- every suggestion's
sql_hint passes `EXPLAIN` without error.

### E5.7 -- Visual Accuracy Tests (Day 12-14)

**File:** Update `headwater/tests/test_explore_correctness.py`

```python
class TestVisualAccuracy:
    """Validate that chart annotations and inline visualizations are correct."""

    # Annotated line charts
    def test_anomaly_markers_at_correct_x(self): ...
    def test_change_point_line_at_correct_x(self): ...
    def test_trend_line_slope_matches_regression(self): ...
    def test_confidence_band_width_matches_stddev(self): ...

    # Bar charts
    def test_bars_sorted_descending(self): ...
    def test_reference_line_at_mean(self): ...
    def test_top_3_highlighted(self): ...

    # Scatter plots
    def test_r_squared_in_spec(self): ...
    def test_regression_equation_correct(self): ...

    # Insight cards
    def test_every_insight_has_visualization(self): ...
    def test_insight_viz_data_matches_raw_values(self): ...
    def test_narrative_values_match_viz_annotations(self): ...

    # Suggestions
    def test_all_sql_hints_compile(self): ...
    def test_suggestions_ranked_by_interest(self): ...
    def test_no_generic_select_star_suggestions(self): ...
```

### Wave E5 Gate

- [ ] Line charts annotated with anomalies, change points, trend line, confidence band
- [ ] Bar charts sorted, with reference line and value labels
- [ ] Scatter plots show regression line, R-squared, quadrant guides
- [ ] Every insight card has an inline mini-chart matching its type
- [ ] Explore page defaults to Key Findings tab with ranked insights
- [ ] Suggestions ranked by insight value with compiled SQL hints
- [ ] No generic "SELECT *" suggestions
- [ ] `uv run pytest tests/test_explore_correctness.py::TestVisualAccuracy` -- all pass

---

## Wave E6: End-to-End Explore Correctness Gate

**Goal:** A comprehensive integration test suite that runs the entire explore pipeline
against sample data and validates every output. This is the final gate before the explore
layer is considered production-ready.
**Duration:** 1 week
**Gating condition:** All tests pass. This suite runs in CI on every PR.

### E6.1 -- Full Pipeline -> Explore Integration Test (Day 1-3)

**File:** `headwater/tests/test_explore_e2e.py` (new, ~500 lines)

**Test flow:**
```python
@pytest.fixture(scope="module")
def full_pipeline():
    """Run the ENTIRE pipeline on sample data, then explore."""
    # 1. Load sample data
    # 2. Run discovery
    # 3. Run analysis
    # 4. Build catalog
    # 5. Generate models
    # 6. Execute models
    # 7. Check quality
    # Return: all state needed for explore

class TestExploreE2E:
    """End-to-end tests that validate the entire flow."""

    def test_suggestions_reflect_actual_data(self, full_pipeline):
        """Every suggestion references tables/columns that actually exist."""
        suggestions = generate_suggestions(...)
        for s in suggestions:
            for table in s.relevant_tables:
                assert table in known_tables, f"Suggestion references non-existent table: {table}"
            if s.sql_hint:
                # Verify SQL compiles
                con.execute(f"EXPLAIN {s.sql_hint}")

    def test_insights_numerically_accurate(self, full_pipeline):
        """Verify each insight's magnitude against manual SQL."""
        insights = detect_insights(...)
        for insight in insights:
            if insight.insight_type == "temporal_anomaly":
                # Re-compute the anomaly manually
                manual_z = _manual_z_score(con, insight.table_name, insight.metric, insight.time_period)
                assert abs(insight.z_score - manual_z) < 0.5, \
                    f"z-score mismatch: insight={insight.z_score}, manual={manual_z}"

    def test_ask_answers_match_golden(self, full_pipeline):
        """Run all 25 golden questions, verify answers."""
        for question, expected in GOLDEN_QUESTIONS.items():
            result = ask(question, ...)
            assert result.error is None, f"Question failed: {question}: {result.error}"
            _assert_answer_matches(result.data, expected)

    def test_visualizations_valid_for_results(self, full_pipeline):
        """Every visualization spec references columns in the result data."""
        for question in GOLDEN_QUESTIONS:
            result = ask(question, ...)
            if result.visualization and result.data:
                cols = set(result.data[0].keys())
                if result.visualization.x_axis:
                    assert result.visualization.x_axis in cols
                if result.visualization.y_axis:
                    assert result.visualization.y_axis in cols

    def test_insight_narratives_factually_correct(self, full_pipeline):
        """Every narrative's stated values match the raw insight values."""
        insights = detect_insights(...)
        for insight in insights:
            if hasattr(insight, 'narrative'):
                if insight.magnitude:
                    assert str(abs(int(insight.magnitude))) in insight.narrative or \
                           f"{abs(insight.magnitude):.1f}" in insight.narrative

    def test_no_false_positive_insights(self, full_pipeline):
        """Every reported insight has p < 0.05 after FDR correction."""
        insights = detect_insights(...)
        for insight in insights:
            if insight.p_value is not None:
                assert insight.p_value < 0.05, \
                    f"Insight with p={insight.p_value} should not have passed FDR"

    def test_distribution_shapes_consistent(self, full_pipeline):
        """Distribution shape classification is consistent across runs."""
        dist1 = analyze_distributions(...)
        dist2 = analyze_distributions(...)
        for d1, d2 in zip(dist1, dist2):
            assert d1.shape == d2.shape, f"Distribution shape changed between runs"

    def test_cross_table_insights_reference_valid_joins(self, full_pipeline):
        """Cross-table insights reference FK paths that actually exist."""
        insights = [i for i in detect_insights(...) if i.insight_type.startswith("cross_")]
        for insight in insights:
            # Verify the tables are connected via FK
            assert schema_graph.find_join_path(insight.table_a, insight.table_b) is not None

    def test_explore_page_data_contract(self, full_pipeline):
        """Verify the API response matches the frontend's expected type contract."""
        # Simulate GET /explore/suggestions
        # Verify response has: suggestions, insights, review_pct
        # Verify each insight has: type, severity, narrative, visualization
        # Verify each suggestion has: question, source, sql_hint, relevant_tables

    def test_no_explore_crashes_on_any_question_pattern(self, full_pipeline):
        """Fuzz test: 50 diverse question patterns, none should crash."""
        fuzz_questions = [
            "How many?", "", "   ", "DROP TABLE zones",
            "What is the meaning of life?",
            "complaints complaints complaints",
            "readings where value > 100 grouped by sensor",
            "a" * 1000,  # Very long question
            "1234567890",  # Numbers only
            "SELECT * FROM zones",  # Raw SQL as question
            # ... 40 more patterns
        ]
        for q in fuzz_questions:
            result = ask(q, ...)
            # May error, but must not crash
            assert isinstance(result, ExplorationResult)
```

### E6.2 -- Performance Correctness Tests (Day 3-4)

**File:** Add to `headwater/tests/test_explore_e2e.py`

```python
class TestExplorePerformance:
    """Ensure explore operations complete in reasonable time."""

    def test_suggestions_under_5_seconds(self, full_pipeline):
        start = time.time()
        generate_suggestions(...)
        assert time.time() - start < 5.0

    def test_insights_under_10_seconds(self, full_pipeline):
        start = time.time()
        detect_insights(...)
        assert time.time() - start < 10.0

    def test_ask_under_5_seconds(self, full_pipeline):
        for question in ["How many complaints?", "Readings over time"]:
            start = time.time()
            ask(question, ...)
            assert time.time() - start < 5.0

    def test_distribution_analysis_under_5_seconds(self, full_pipeline):
        start = time.time()
        analyze_distributions(...)
        assert time.time() - start < 5.0
```

### E6.3 -- Regression Test Suite (Day 4-5)

**File:** `headwater/tests/test_explore_regression.py` (new, ~200 lines)

**Purpose:** Capture specific bugs found during development as regression tests.
Each test documents the bug and verifies the fix.

```python
class TestExploreRegressions:
    """Regression tests for specific bugs found during explore development."""

    def test_regression_latitude_as_metric(self):
        """Bug: latitude was selected as metric for 'complaints per county'.
        Fix: geographic coordinates excluded from metric candidates."""

    def test_regression_exploded_aggregation(self):
        """Bug: 'complaints per county' returned 10,000 rows instead of grouped.
        Fix: result shape validation catches missing GROUP BY."""

    def test_regression_heatmap_wrong_column(self):
        """Bug: heatmap displayed zone_id instead of reading_value.
        Fix: metric column from visualization spec used, not first numeric."""

    def test_regression_kpi_no_context(self):
        """Bug: KPI showed '10000' with no explanation.
        Fix: KPI includes trend direction, sparkline, comparison."""

    def test_regression_false_positive_correlation(self):
        """Bug: trending columns showed r=0.95 even when unrelated.
        Fix: detrending applied before correlation computation."""
```

### Wave E6 Gate (FINAL EXPLORE GATE)

- [ ] All 25 golden answer tests pass
- [ ] All statistical accuracy tests pass (zero false positives on random data)
- [ ] All visualization correctness tests pass (axes reference real columns)
- [ ] All visual accuracy tests pass (annotations at correct positions)
- [ ] All deep insight tests pass (cross-table, distributions, narratives)
- [ ] Fuzz test: 50 diverse question patterns, zero crashes
- [ ] Performance: suggestions < 5s, insights < 10s, ask < 5s
- [ ] Regression suite passes
- [ ] E2E pipeline -> explore integration test passes
- [ ] **Overall explore correctness rate: >= 95%**

---

## Updated Timeline Summary

**Priority order: Correctness first, connectors second.**

The Explore layer is the product's payoff -- the moment the CTO evaluating this tool
will judge it. Piping more data sources into a sloppy analysis layer just multiplies
the sloppiness. We fix Explore first, then expand connectors so each new data source
benefits from rigorous analysis from day one.

| Wave | Focus | Duration | Cumulative |
|------|-------|----------|------------|
| 0 | Trust & Quality Foundation | 1 week | Week 1 |
| **E0** | **Explore: Correctness Ground Truth** | **1 week** | **Week 2** |
| **E1** | **Explore: Fix SQL Generation Correctness** | **2 weeks** | **Week 3-4** |
| **E2** | **Explore: Fix Statistical Rigor** | **2 weeks** | **Week 5-6** |
| **E3** | **Explore: Fix Visualization Correctness** | **1.5 weeks** | **Week 7-8** |
| **E4** | **Explore: Deep Insights Engine** | **2.5 weeks** | **Week 8-10** |
| **E5** | **Explore: Visualization Overhaul (WOW)** | **2 weeks** | **Week 11-12** |
| **E6** | **Explore: E2E Correctness Gate** | **1 week** | **Week 13** |
| 3 | Production Resilience (circuit breakers, profile history) | 3 weeks | Week 14-16 |
| 1 | dbt Export | 1.5 weeks | Week 17-18 |
| 2 | Connector Architecture + MySQL | 2.5 weeks | Week 19-21 |
| 4 | Snowflake + BigQuery + Observe Mode | 3 weeks | Week 22-24 |
| 5 | Catalog Connectors (Glue, Unity, Iceberg) | 3 weeks | Week 25-27 |
| 6 | UX Polish, Accessibility, Frontend Tests | 2.5 weeks | Week 28-29 |
| 7 | Cleanup, Docker, Docs, CI | 1.5 weeks | Week 30 |

**Total: ~30 weeks (7.5 months)** -- same total, different order.

**Why this order:**
- Waves E0-E6 (Explore correctness) move from Week 9 to Week 2. The analysis
  layer is the product differentiator. Fix it first.
- Wave 3 (Resilience) stays before connectors because circuit breakers protect
  new data sources on arrival.
- Waves 1-2 (dbt export, connector architecture) slide to Week 17-21. These are
  mechanical and well-understood -- low risk to defer.
- Waves 4-5 (Snowflake, BigQuery, catalog connectors) are last. Each new connector
  will immediately benefit from the corrected Explore layer.

---

## Updated Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Snowflake/BigQuery SDK breaking changes | Wave 4 delay | Pin SDK versions, test against specific releases |
| LLM cost increase (Anthropic pricing) | User adoption | Air-gapped mode is always first-class; cost caps in settings |
| DuckDB single-writer under concurrent API | Pipeline corruption | Already mitigated (SQLite for metadata); add connection pooling |
| Plugin loader security (arbitrary code exec) | Supply chain attack | Plugins load from explicit directory only; no auto-download |
| Observe mode SQL dialect drift | Generated SQL breaks | Dialect-specific test suites; SQL validation before user sees it |
| Frontend test flakiness (Playwright) | CI reliability | Retry flaky tests 2x; use deterministic test data |
| Scope creep in catalog connectors | Wave 5 overrun | MVP: list tables + columns + descriptions; skip lineage in v1 |
| **scipy dependency weight** | **Install size** | **scipy already in deps; no new dependency for stats** |
| **Golden answer drift** | **Tests break on schema changes** | **Golden answers pinned to sample data version; re-verify on data changes** |
| **Insight ranking subjectivity** | **Users disagree on "interesting"** | **Score is configurable; allow user feedback to adjust weights** |
| **Chart annotation clutter** | **Too many annotations on one chart** | **Cap annotations at 5 per chart; show overflow in sidebar** |
| **Cross-table insight explosion** | **O(n^2) table pairs** | **Cap cross-table analysis to tables with FK relationships only** |

---

## What This Plan Does NOT Include (Intentionally)

- **Redshift connector** -- Low demand relative to Snowflake/BigQuery. Add post-Wave 5
  if requested.
- **Real-time streaming** -- Out of scope per press release. Headwater is batch-oriented.
- **Multi-user collaboration** -- Planned for managed cloud version (2027). Not in OSS scope.
- **SSO/authentication** -- Same as above. Single-user or shared-credential for now.
- **Semantic layer runtime** -- Headwater generates metadata, does not serve queries.
  This is a deliberate boundary.
- **SQLMesh export** -- dbt is the priority. SQLMesh can follow the same pattern post-Wave 1.
- **Helm chart** -- Docker Compose is sufficient for target audience (1-10 person teams).
  Helm adds maintenance burden for minimal user value at this stage.
- **LLM-generated insights** -- All insights are statistical/deterministic.
  LLM is used for NL-to-SQL translation only, not for generating insight narratives.
  This ensures correctness is provable and reproducible.

---

## Success Metrics (Post-Wave 7)

| Metric | Target |
|--------|--------|
| Press release claims backed by code | 100% |
| Connector count | 8 (Postgres, JSON, CSV, Parquet, MySQL, Snowflake, BigQuery + 3 catalogs) |
| Test count | > 1200 (backend) + > 50 (frontend) |
| Lint errors | 0 |
| E2E heartbeat test | Passes in < 60s |
| UI Lighthouse accessibility score | > 90 |
| dbt export round-trip validity | 100% of staging models |
| Circuit breaker coverage | Volume, schema, null, string bloat |
| Docker health check | All services report healthy |
| Time to first discovery (new user) | < 15 minutes |
| **Explore golden answer pass rate** | **>= 95%** |
| **Statistical false positive rate** | **< 3% on random data** |
| **Insight accuracy (magnitude within 5%)** | **100%** |
| **Visualization axis correctness** | **100% (every axis references a real column)** |
| **Suggestion SQL hint compilation rate** | **100%** |
| **Explore fuzz test crash rate** | **0%** |
