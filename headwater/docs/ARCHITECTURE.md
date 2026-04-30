# Headwater Current Architecture

**Last updated:** April 30, 2026

## Runtime Shape

Headwater is a local application with four main layers:

```text
Next.js UI
  -> FastAPI API routes
    -> connector / discovery / profiling / generation services
      -> source systems, DuckDB analytical runtime, SQLite metadata store
```

## Storage Responsibilities

| Store | Responsibility |
| --- | --- |
| Source systems | Organization data. Headwater should not mutate these. |
| DuckDB | Local analytical execution, staging/mart validation, table preview/query for loaded or sampled data. |
| SQLite metadata | Headwater state: sources, events, review decisions, model reviews, contracts, drift/history. |
| Browser/UI state | Active screen selections only. Durable review decisions must be persisted through the API. |

## Backend Modules

| Area | Main modules | Responsibility |
| --- | --- | --- |
| API app | `headwater/api` | FastAPI app and route handlers. |
| Connectors | `headwater/connectors` | Source-specific access, capability declaration, table/column/profile methods. |
| Profiling | `headwater/profiler` | Schema extraction, statistics, key/relationship candidates. |
| Analyzer | `headwater/analyzer` | Semantic enrichment, catalog generation, descriptions, domains. |
| Generator | `headwater/generator` | Staging, mart, and contract generation. |
| Explorer | `headwater/explorer` | NL-to-SQL planning, query suggestions, decomposition. |
| Quality | `headwater/quality` | Contract checks and quality reports. |
| Services | `headwater/services` | Source sync, rerun planning, model impact logic. |
| Metadata | `headwater/core/metadata.py` | SQLite schema and durable persistence helpers. |

## UI Areas

| Page | Responsibility |
| --- | --- |
| Briefing | Daily operational summary and priorities. |
| Sources | Connector catalog, source registration, sync/test events. |
| Discover & Access | Table/schema/profile review, descriptions, roles, PK choices, PK/FK suggestions. |
| Models | Generated staging/mart models, review status, impact. |
| Data & Query | Table preview and read-only SQL. |
| Quality | Contracts and checks. |
| Insights | Business-oriented statistical insights and visuals. |
| Ask a Question | Natural language exploration over discovered data. |

## Connector Modes

Headwater distinguishes two connector patterns:

- **Generate mode:** source data can be loaded or sampled into DuckDB for local
  model generation and validation. File, DuckDB, and SQLite paths fit here.
- **Observe mode:** source data is not copied wholesale. Headwater profiles and
  samples through source-side aggregate/read-only queries. Postgres already uses
  this pattern for profiling; Redshift and Athena should follow it.

Current connector capability flags include:

- `list_tables`
- `list_columns`
- `list_constraints`
- `estimate_row_count`
- `profile_table`
- `sample_arrow`
- `execute_readonly`
- `load_to_duckdb`
- `modes`

`list_constraints` exists as a capability but is not yet implemented for
Postgres/MySQL. That is the next important OLTP metadata improvement.

## Discovery and Review Flow

```text
source registration
  -> connection test
  -> table/column discovery
  -> profiling
  -> relationship/key candidate detection
  -> semantic enrichment
  -> Discover & Access review
  -> persisted decisions
  -> model/contract/insight generation
```

Review decisions are intentionally durable. Confirmed/rejected PK choices are
reapplied during rediscovery. Disconnecting a source clears source-scoped
metadata and review decisions but does not mutate the source database.

## Current Metadata Gaps

For mature OLTP systems, declared DDL metadata usually lives in system catalogs.
For Postgres, this includes `pg_catalog.pg_constraint`, `pg_attribute`,
`pg_class`, `pg_namespace`, and related `information_schema` views.

Current Headwater behavior:

- imports table/column metadata
- computes profiles
- infers PK/FK candidates
- persists user decisions

Planned behavior:

- import declared PK/FK/unique/check constraints
- import comments and descriptions
- preserve source metadata provenance
- let user decisions override imported metadata

For OLAP systems, DDL alone is often insufficient. Headwater should combine:

- warehouse catalogs
- comments/tags
- dbt artifacts
- semantic layer metadata
- lineage/catalog tools
- aggregate profiles
- query history where allowed

## Large Data Principle

Headwater should not assume raw table copy is acceptable. For large OLTP/OLAP
systems, connectors should prefer:

- metadata-only discovery
- source-side aggregate profiling
- bounded samples
- partition/time filters
- scan limits
- explicit evidence coverage in UI
