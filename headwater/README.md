# Headwater

Headwater is a local advisory workspace for data professionals. It connects to
source data, profiles it, builds a semantic understanding of tables and columns,
surfaces review decisions, generates models/contracts, and produces business
insights that can be inspected before they are trusted.

The current product direction is:

```text
connect -> discover -> profile -> review -> model -> validate -> analyze -> improve
```

Headwater is not a managed cloud product in this repository. It runs locally with
a FastAPI backend, a Next.js UI, DuckDB for analytical execution, and SQLite for
Headwater metadata.

## Current Status

The current implementation supports a practical local workflow:

- Source registration, connection testing, sync events, and source-scoped reset.
- Discovery and profiling for file and database sources.
- Discover & Access review for descriptions, roles, semantic types, PK choices,
  and PK/FK suggestions.
- Persistent user key decisions across rediscovery until a new source setup/reset.
- Data & Query table preview and read-only SQL querying.
- Model generation, review, and quality contract checks.
- Business-oriented Insights with diversified chart patterns.
- Ask-a-question exploration over the discovered schema.

Known gaps:

- Manual FK editing is incomplete. FK suggestions can be confirmed, but users
  cannot yet freely choose `from_column -> target_table.target_column`.
- Database-declared constraints are not yet imported from Postgres/MySQL system
  catalogs. PK/FK state currently comes from profiling, heuristics, and user
  review decisions.
- Redshift and Athena are planned, not implemented.
- Snowflake is available as a preview connector with bounded metadata,
  profiling, and sampling. Install `snowflake-connector-python` before use.
- Large-table profiling needs a first-class aggregate/fetch policy per connector.

## Connector Matrix

| Source | Category | Status | Current behavior |
| --- | --- | --- | --- |
| CSV files | Files | Supported | Loads to DuckDB, profiles, samples. |
| JSON/NDJSON files | Files | Supported | Loads to DuckDB, profiles, samples. |
| DuckDB | Embedded OLAP | Supported | Reads existing DuckDB databases. |
| SQLite | Embedded OLTP | Supported | Reads existing SQLite databases. |
| PostgreSQL | OLTP | Supported | Lists tables/columns, pushdown profiles, samples. Constraint import is planned. |
| MySQL | OLTP | Preview | Connector exists with bounded introspection, not default production path. |
| Redshift | AWS OLAP | Planned | Warehouse observe mode and aggregate profiling planned. |
| Athena | AWS OLAP/Lake | Planned | Glue catalog plus query-result profiling planned. |
| Snowflake | Warehouse | Preview | Lists schemas/tables/columns, row estimates, bounded profiles and samples. Requires optional `snowflake-connector-python`. |
| BigQuery | Warehouse | Planned | Observe mode planned. |
| Databricks | Lakehouse | Planned | Observe mode planned. |
| SQL Server | OLTP | Planned | Catalog/constraint import planned. |
| Oracle | OLTP | Planned | Catalog/constraint import planned. |
| ClickHouse | OLAP | Planned | Observe mode planned. |
| Trino | Federated | Planned | Federated observe mode planned. |

Status meanings:

- `Supported`: connector is registered and intended to work in the current build.
- `Preview`: implementation exists, but validation and UX are still limited.
- `Planned`: shown as roadmap, not selectable as a production path.

## Local Development

Backend:

```bash
uv run uvicorn headwater.api.app:create_app --factory --reload
```

UI:

```bash
cd ui
npm run dev
```

Useful checks:

```bash
uv run pytest tests/test_data_api.py -q
uv run ruff check headwater tests
cd ui && npx tsc --noEmit
```

## Documentation

- [Current Architecture](docs/ARCHITECTURE.md)
- [Active Implementation Plan](IMPLEMENTATION_PLAN.md)
- Archived plans live in [archives](archives/).
