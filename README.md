# Headwater

**Advisory data platform for data professionals.** Auto-discovers, profiles, and documents your data. Generates quality contracts and SQL models for human review -- so you can focus on the work that actually requires your expertise.

Headwater is opinionated about what should be automated (schema extraction, profiling, staging models) and what requires a human (business logic, mart definitions, quality thresholds).

---

## Quickstart

### Option A — Sample data (no database required)

```bash
# Prerequisites: Python 3.11+, uv
cd headwater
uv sync

# Run the full demo against the bundled environmental health dataset
uv run headwater demo --dataset ../data/sample
```

Loads 8 tables (59.9K records), profiles ~90 columns, detects relationships, generates staging + mart models, and runs 193 quality contract checks in under 10 seconds.

### Option B — Real-world data via Postgres

Headwater ships with real-world environmental datasets (~64K rows). Use the ingestion utility to load them into a local Postgres instance, then point Headwater at it.

**Step 1 — Start Postgres**

```bash
# Docker (quickest)
bash tools/docker-postgres.sh

# Or via docker-compose (runs alongside the API and UI)
docker-compose up postgres
```

**Step 2 — Load real-world data**

```bash
# Run via the headwater project venv (polars + psycopg2 live there)
cd headwater && uv run python ../tools/pg_ingest.py

# Custom DSN or schema
uv run python ../tools/pg_ingest.py --dsn postgresql://user:pass@host:5432/mydb --schema raw
```

Re-running is safe. By default the script drops and recreates each table on every run, so it is idempotent. To append to existing tables instead (which will duplicate rows), pass `--no-drop`.

This loads 7 tables into Postgres:

| Table | Rows | Source |
|---|---|---|
| `aqi_by_county` | 992 | EPA AQI by county, 2023 |
| `aqs_monitors` | 10,000 | EPA air quality monitor sites (sampled) |
| `aqs_sites` | 20,077 | EPA AQS site registry |
| `daily_pm25` | 10,000 | Daily PM2.5 readings, 2023 (sampled) |
| `env_complaints` | 15,000 | NYC environmental complaints (sampled) |
| `ej_grants` | 5,246 | EPA environmental justice grants |
| `water_stations` | 3,043 | Maryland water quality stations |

**Step 3 — Start the app and discover from the UI**

```bash
cd headwater && make app
```

Open http://localhost:3000. The source field defaults to the local Postgres DSN — hit **Discover Your Data** to run the full pipeline. Headwater samples each table (10k rows via Arrow, no bulk copy), profiles in DuckDB, detects relationships, and generates staged models and quality contracts ready for review.

To use the bundled sample data instead, change the source field to `/data/sample`.

Alternatively, trigger discovery from the CLI:

```bash
cd headwater
uv run headwater discover --source postgresql://headwater:headwater@localhost:5434/headwater_dev
```

---

## What It Does

1. **Discover** -- Connect to a source (Postgres, JSON, CSV), extract schema via `information_schema`, profile every column (null rates, cardinality, min/max, top-N distinct values, pattern detection), detect foreign key relationships
2. **Analyze** -- Classify tables by domain, assign semantic types to columns, generate descriptions (heuristic or LLM-assisted). Approved descriptions are locked and preserved across re-runs.
3. **Generate** -- Create staging SQL models (auto-approved), propose mart SQL models (individual human review required), generate quality contracts from observed statistics
4. **Execute** -- Materialize approved models in dependency order
5. **Validate** -- Evaluate quality contracts, report pass/fail, track false-positive rate over time
6. **Monitor** -- Detect schema drift between runs, alert on data freshness issues, track confidence metrics (description acceptance rate, model edit distance)

---

## Architecture

### Data Flow

```
Source (Postgres / JSON / CSV)
  -> Connector: profile() -- aggregate SQL runs in-place, stats only
  -> Connector: sample()  -- small Arrow batch (10k rows) for local validation
    -> Profiler (Polars expressions for stats, FK detection)
      -> Analyzer (heuristic or LLM semantic enrichment)
        -> Generator (staging + mart SQL via Jinja2, quality contracts)
          -> Executor (DuckDB materializes approved models)
            -> Quality (contract validation)
              -> Drift (schema snapshot compare, change alerts)
```

### Key Design Decisions

**SQLite for metadata, DuckDB for analytical data**
DuckDB's single-writer lock causes contention between the background pipeline and the API. Metadata (sources, discovery runs, table/column info, profiles, models, decisions, audit log) lives in SQLite. DuckDB holds only the actual data tables and materialized models.

**Pushdown profiling — no bulk data copy**
For database sources (Postgres), profiling queries run directly in the source via aggregate SQL (`COUNT`, `MIN`, `MAX`, `COUNT(DISTINCT)`). Only a small sample (default 10k rows) is fetched locally for validating generated SQL. Large OLTP tables are never bulk-loaded into Headwater.

**Arrow-native flow**
Data moves between Polars and DuckDB as Apache Arrow. No Pandas, no CSV intermediaries, zero-copy.

**Two-mode connector architecture**
- `generate` mode (Postgres, CSV, JSON): profile in-place, generate deployable staging + mart SQL
- `observe` mode (Snowflake, BigQuery, Redshift): profile in-place, govern what's already there — zero data movement. *(Phase 2)*

**LLM is optional**
Three tiers: `none` (heuristics only), `anthropic` (Claude via API). Everything works without a network call. The LLM receives only column names, data types, and statistical summaries -- never raw data rows.

**Advisory by design**
Staging models are auto-approved (mechanical transforms, no business logic). Mart models require individual human review -- no batch approve. Quality contracts start in observation mode. Semantic locks preserve human-approved descriptions across re-runs.

**Confidence tracking**
Every approve, reject, and edit is recorded. Headwater computes description acceptance rate, model edit distance, and contract false-positive rate from these decisions and surfaces them on the dashboard.

---

## Source Connectors

| Type | Mode | Command |
|---|---|---|
| Postgres | generate | `--source postgresql://user:pass@host:5432/db` |
| JSON (NDJSON) | generate | `--source /path/to/dir --type json` |
| CSV | generate | `--source /path/to/dir --type csv` |
| Snowflake / BigQuery / Redshift | observe | Phase 2 |

---

## CLI

```bash
cd headwater

# Full end-to-end demo (bundled sample data, no setup required)
uv run headwater demo --dataset ../data/sample

# Discover a Postgres database
uv run headwater discover --source postgresql://user:pass@host:5432/db --name my_source

# Discover local JSON/CSV files
uv run headwater discover --source /path/to/data --type json --name my_source

# Generate models and contracts from last discovery run
uv run headwater generate

# Show pipeline status and source list
uv run headwater status

# Unlock a semantic-locked description to allow re-enrichment
uv run headwater unlock --source my_source --table orders --column customer_id
# Unlock all columns for a source
uv run headwater unlock --source my_source --all
```

---

## API

Start everything:

```bash
cd headwater && make app   # API (port 8000) + UI (port 3000) together
make api                   # API only
make ui                    # UI only
```

### Pipeline (full run)

| Method | Path | Description |
|---|---|---|
| POST | `/api/pipeline/run` | Discover → generate → execute → quality check in one call. `source_path` accepts a file path or database DSN (default: local Postgres). Auto-detects connector type. |

### Discovery

| Method | Path | Description |
|---|---|---|
| GET | `/api/status` | Pipeline status and source list |
| POST | `/api/discover` | Run discovery only |
| GET | `/api/tables` | List discovered tables |
| GET | `/api/tables/{name}` | Table detail with columns and profile |
| GET | `/api/tables/{name}/profile` | Column-level statistical profiles |
| GET | `/api/relationships` | Detected foreign key relationships |

### Models

| Method | Path | Description |
|---|---|---|
| POST | `/api/generate` | Generate models and contracts |
| GET | `/api/models` | List all models (staging + mart) |
| GET | `/api/models/{name}` | Model detail with SQL, assumptions, questions |
| POST | `/api/models/{name}/approve` | Approve a proposed mart model |
| POST | `/api/models/{name}/reject` | Reject a proposed mart model |
| POST | `/api/execute` | Execute approved models in dependency order |

### Quality

| Method | Path | Description |
|---|---|---|
| GET | `/api/contracts` | List quality contracts |
| POST | `/api/quality/check` | Run quality checks |
| GET | `/api/quality` | Quality report with pass/fail |
| POST | `/api/contracts/{rule_id}/mark-false-positive` | Mark an alert as a false positive |

### Semantic Locks

| Method | Path | Description |
|---|---|---|
| PATCH | `/api/columns/{source}/{table}/{column}` | Edit description or lock/unlock a column |

### Schema Drift

| Method | Path | Description |
|---|---|---|
| GET | `/api/drift` | Latest drift report (optional `?source=name`) |
| PATCH | `/api/drift/{id}/acknowledge` | Dismiss a drift alert |

### Confidence Metrics

| Method | Path | Description |
|---|---|---|
| GET | `/api/confidence` | Acceptance rate, edit distance, contract precision (optional `?source=name`) |

### Audit

| Method | Path | Description |
|---|---|---|
| GET | `/api/audit` | Last 100 LLM audit log entries |

---

## UI

```bash
cd headwater/ui
npm install
npm run dev
```

Open http://localhost:3000 (requires API server on port 8000).

Pages:
- **Dashboard** -- Pipeline controls, status cards, confidence metrics, drift alerts
- **Discovery** -- Table browser with column details, profiles, lock status, relationships
- **Models** -- Review queue (approve/reject per mart), SQL viewer, assumptions, clarifying questions
- **Quality** -- Contract table, run checks, pass/fail results, false-positive marking
- **Explore** -- Natural language queries, suggested questions, statistical insights, chart recommendations

---

## Docker

```bash
# Start everything: API + UI + Postgres
docker-compose up

# Postgres only (for local development)
docker-compose up postgres
```

- API: http://localhost:8000
- UI: http://localhost:3000
- Postgres: `postgresql://headwater:headwater@localhost:5434/headwater_dev`

After starting Postgres, load the real-world data:

```bash
cd headwater && uv run python ../tools/pg_ingest.py
```

---

## Project Structure

```
adm/
  headwater/                  # Python project root (pyproject.toml)
    headwater/
      core/                   # Pydantic models, config, SQLite metadata, exceptions
      connectors/             # Source connectors: JSON, CSV, Postgres (two-mode: generate/observe)
      profiler/               # Schema extraction, statistical profiling, FK detection
      analyzer/               # Heuristic + LLM semantic enrichment, semantic locks
      generator/              # Staging/mart SQL (domain-agnostic pattern matching), contracts
      executor/               # DuckDB execution backend, dependency-ordered runner
      quality/                # Contract validation, false-positive tracking
      drift/                  # Schema drift detection, snapshot compare, change alerts
      api/                    # FastAPI routes
      cli/                    # Typer CLI commands
    tests/                    # 284 tests (integration, no mocks)
    ui/                       # Next.js + TailwindCSS frontend
  data/
    sample/                   # 8 NDJSON files, environmental health domain, 59.9K records
    real_world/               # Real EPA/NYC datasets, 7 CSV files, ~64K records
  tools/
    pg_ingest.py              # Standalone Postgres ingestion utility for real-world CSVs
    docker-postgres.sh        # One-command local Postgres container
```

---

## Sample Data

### Bundled (NDJSON, used by `headwater demo`)

| Table | Records | Description |
|---|---|---|
| zones | 25 | Geographic zones with demographics |
| sites | 500 | Monitored facilities and locations |
| sensors | 832 | Environmental sensors at sites |
| readings | 49,302 | Sensor readings (PM2.5, ozone, etc.) |
| inspections | 1,243 | Site inspections with scores |
| incidents | 5,000 | Public health incidents |
| complaints | 3,000 | Citizen complaints |
| programs | 10 | Intervention programs |

### Real-world (CSV, loaded via `tools/pg_ingest.py`)

| Table | Rows | Dataset |
|---|---|---|
| aqi_by_county | 992 | EPA AQI by county, 2023 |
| aqs_monitors | 10,000 | EPA air quality monitor sites |
| aqs_sites | 20,077 | EPA AQS site registry |
| daily_pm25 | 10,000 | Daily PM2.5 readings, 2023 |
| env_complaints | 15,000 | NYC environmental complaints |
| ej_grants | 5,246 | EPA environmental justice grants |
| water_stations | 3,043 | Maryland water quality stations |

---

## LLM Configuration

Headwater works fully without any LLM. To enable Claude enrichment:

```bash
export HEADWATER_LLM_PROVIDER=anthropic
export HEADWATER_LLM_API_KEY=sk-ant-...
export HEADWATER_LLM_MODEL=claude-sonnet-4-6   # default
```

LLM enrichment adds: semantic column descriptions, domain classification, mart naming and clarifying questions. All LLM calls are logged to the audit log (`GET /api/audit`) for inspection.

The LLM receives only column names, data types, and statistical summaries -- never raw data rows.

---

## Development

```bash
cd headwater
uv sync                    # Install dependencies
uv run pytest              # Run tests (284 passing, 4 skipped)
uv run ruff check .        # Lint
uv run ruff format .       # Format
```

### Ingestion utility dependencies (standalone, not part of headwater package)

```bash
# From repo root
pip install polars psycopg2-binary   # or use your own venv
python tools/pg_ingest.py --help
```

---

## Tech Stack

Python 3.11+ | Polars | DuckDB | SQLite | psycopg2 | FastAPI | Typer | Pydantic v2 | Jinja2 | Rich | Apache Arrow | scipy | Next.js 16 | TailwindCSS | Docker
