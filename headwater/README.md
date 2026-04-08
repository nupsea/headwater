# Headwater

**Advisory data platform for data professionals.** Auto-discovers, profiles, and documents your data. Generates quality contracts and SQL models for human review -- so you can focus on the work that actually requires your expertise.

Headwater is opinionated about what should be automated (schema extraction, profiling, staging models) and what requires a human (business logic, mart definitions, quality thresholds).

## Quick Start

```bash
# Prerequisites: Python 3.11+, uv
cd headwater
uv sync

# Run the full demo against the sample environmental health dataset
uv run headwater demo --dataset ../data/sample
```

This loads 8 tables (59.9K records), profiles ~90 columns, detects foreign key relationships, generates staging + mart models, executes the staging layer, and runs 193 quality contract checks -- all in under 10 seconds.

## What It Does

1. **Discover** -- Load JSON/CSV data into DuckDB, extract schema, profile every column (null rates, cardinality, ranges, patterns), detect foreign key relationships
2. **Analyze** -- Classify tables by domain, assign semantic types to columns, generate descriptions (heuristic or LLM-assisted)
3. **Generate** -- Create staging SQL models (auto-approved, mechanical transforms only), mart SQL models (proposed with assumptions and clarifying questions), and quality contracts from observed statistics
4. **Execute** -- Materialize approved models in DuckDB in dependency order
5. **Validate** -- Evaluate quality contracts against materialized data, report pass/fail

## Architecture

```
Source files (JSON/CSV)
  -> Polars (read, Arrow)
    -> DuckDB (analytical engine)
      -> Profiler (stats, relationships)
        -> Analyzer (heuristic/LLM enrichment)
          -> Generator (SQL via Jinja2)
            -> Executor (DuckDB materialization)
              -> Quality (contract validation)
```

**Key design decisions:**
- **SQLite for metadata, DuckDB for analytical data** -- avoids DuckDB's single-writer lock for concurrent API + pipeline
- **Arrow-native flow** -- Polars to DuckDB via Apache Arrow, zero-copy
- **LLM is optional** -- full functionality without any LLM; Claude enriches when available
- **Advisory by design** -- staging auto-approved (no business logic), marts require individual human review, contracts start in observation mode

## CLI

```bash
uv run headwater demo --dataset ../data/sample   # Full end-to-end demo
uv run headwater discover-cmd ../data/sample      # Discover and profile a data source
uv run headwater generate ../data/sample          # Generate models and contracts
uv run headwater status                           # Show configuration
uv run headwater version                          # Print version
```

## API

Start the API server:

```bash
uv run uvicorn headwater.api.app:app --port 8000
```

Endpoints:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/status` | Pipeline status |
| POST | `/api/discover?source_path=...` | Run discovery pipeline |
| GET | `/api/tables` | List discovered tables |
| GET | `/api/tables/{name}` | Table detail with columns |
| GET | `/api/tables/{name}/profile` | Column profiles |
| GET | `/api/relationships` | Detected relationships |
| POST | `/api/generate` | Generate models + contracts |
| GET | `/api/models` | List generated models |
| GET | `/api/models/{name}` | Model detail with SQL |
| POST | `/api/models/{name}/approve` | Approve a proposed model |
| POST | `/api/models/{name}/reject` | Reject a proposed model |
| POST | `/api/execute` | Execute approved models |
| GET | `/api/contracts` | List quality contracts |
| POST | `/api/quality/check` | Run quality checks |
| GET | `/api/quality` | Quality report |

## UI

```bash
cd ui && npm install && npm run dev
```

Open http://localhost:3000 (requires the API server running on port 8000).

Pages:
- **Dashboard** -- Pipeline controls, status overview
- **Discovery** -- Table browser with column details, statistical profiles, relationships
- **Models** -- Review mart models (approve/reject), view SQL, assumptions, clarifying questions
- **Quality** -- Contract table with filters, run checks, pass/fail results

## Docker

```bash
docker-compose up
```

- API: http://localhost:8000
- UI: http://localhost:3000

## Project Structure

```
headwater/
  headwater/
    core/           # Pydantic models, config, SQLite metadata, exceptions
    connectors/     # JSON/CSV loaders (Polars -> Arrow -> DuckDB)
    profiler/       # Schema extraction, statistical profiling, FK detection
    analyzer/       # Heuristic + LLM semantic enrichment
    generator/      # Staging/mart SQL generation, quality contracts
    executor/       # DuckDB execution backend, dependency-ordered runner
    quality/        # Contract validation, reporting
    api/            # FastAPI routes
    cli/            # Typer CLI commands
  tests/            # 144 tests
  ui/               # Next.js + TailwindCSS frontend
data/
  sample/           # 8 NDJSON files, environmental health domain, 59.9K records
```

## Sample Dataset

The sample dataset models a public health department's environmental monitoring system:

| Table | Records | Description |
|-------|---------|-------------|
| zones | 25 | Geographic zones with demographics |
| sites | 500 | Monitored facilities and locations |
| sensors | 832 | Environmental sensors at sites |
| readings | 49,302 | Sensor readings (PM2.5, ozone, etc.) |
| inspections | 1,243 | Site inspections with scores |
| incidents | 5,000 | Public health incidents |
| complaints | 3,000 | Citizen complaints |
| programs | 10 | Intervention programs |

## Generated Mart Models

These are proposed (not auto-approved) and include clarifying questions for human review:

- **mart_air_quality_daily** -- Daily air quality averages by site and zone with EPA AQI classification
- **mart_incident_summary** -- Incidents by type, severity, zone, and month with demographic overlay
- **mart_inspection_scores** -- Inspection pass rates and violation breakdown by site type and zone
- **mart_complaint_response** -- Complaint resolution times by category, priority, and zone
- **mart_program_effectiveness** -- Program enrollment vs incident rates in target zones

## Development

```bash
cd headwater
uv sync                    # Install dependencies
uv run pytest              # Run tests (144 passing)
uv run ruff check .        # Lint
uv run ruff format .       # Format
```

## Tech Stack

Python 3.11+ | Polars | DuckDB | SQLite | FastAPI | Typer | Pydantic v2 | Jinja2 | Rich | Next.js 16 | TailwindCSS | Docker

## LLM Configuration

Headwater works without any LLM. To enable Claude enrichment:

```bash
export HEADWATER_LLM_PROVIDER=anthropic
export HEADWATER_LLM_API_KEY=sk-ant-...
export HEADWATER_LLM_MODEL=claude-sonnet-4-20250514  # default
```
