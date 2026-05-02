# Headwater Progress

**Last updated:** April 30, 2026

## Recently Completed

- Merged Data Dictionary review into Discover & Access.
- PK edits in Discover & Access now refresh table health immediately.
- PK decisions persist across rediscovery.
- Rejected PK candidates are removed from future suggestions.
- Source disconnect clears source-scoped Headwater metadata and review decisions.
- Normalized operational events are exposed through `/api/events`.
- Sources page uses normalized source events.
- Data & Query can preview generated staging names by falling back to source
  tables when staging materialization is absent.
- Data Query normalizes pasted smart quotes before executing SQL.
- Explore initial load performance was fixed.
- Insights now produce more business-oriented statistical observations with
  diversified visual patterns.
- Source evaluation now classifies registered and catalogued sources as files,
  OLTP, or OLAP, reports readiness, maturity mode, capability gaps, and safe
  profiling policy recommendations.
- Snowflake is now a preview connector with safe metadata introspection,
  bounded profiling, row-limited sampling, and read-only validation.
- Warehouse insight planning now has persisted evidence records and a dry-run
  API that proposes cost-gated pushdown queries without executing them.
- Approved warehouse insight plans can now execute read-only aggregate queries,
  apply query tags when the connector supports them, persist execution
  evidence/results, and capture query IDs plus statement timeouts.
- Project start now uses a guided connection workflow that collects source
  inputs, tests the connection, and can create and ingest the source in one
  path.

## Current Supported Workflow

1. Register or use a supported source.
2. Run discovery/profile.
3. Review tables in Discover & Access.
4. Confirm PKs and suggested relationships where available.
5. Preview/query data.
6. Generate/review models and contracts.
7. Inspect business insights.
8. Ask natural language questions.

## Known Gaps

- Manual FK editor is not complete.
- Postgres declared PK/FK/unique/check constraints are not imported yet.
- Constraint/comment import is missing for MySQL and SQLite as well.
- Redshift and Athena are not implemented.
- Snowflake preview requires the optional `snowflake-connector-python`
  dependency and still needs live integration coverage before production status.
- Large table profiling policy is partially first-class through sync limits,
  dry-run insight budgets, and approved read-only execution limits, but observed
  bytes/credits are not captured yet.
- Insight evidence coverage is now persisted for planned warehouse work, but
  existing generated insights still need to consume those evidence records.
- Snowflake insights have a cost-aware dry-run evidence planner and an approved
  read-only execution path, but live Snowflake validation and warehouse-side
  cost capture are still pending.
- Source evaluation is capability-based today; connector-specific import of
  constraints, warehouse comments, lineage, and table statistics still needs
  deeper implementations.

## Next Recommended Work

1. Add live Snowflake execution validation with statement timeouts and observed
   cost/query-history capture.
2. Connect generated insights to persisted evidence records and confidence
   coverage labels.
3. Add Snowflake live integration coverage and warehouse comments/import hints.
4. Add cost-aware Snowflake aggregate execution with direct aggregate pushdown,
   confidence/coverage labels, and scan-budget enforcement.
5. Add dbt artifact import and trusted-model ranking for mature warehouses.
6. Add data product recommendations for grain, fanout, duplicate metrics,
   missing tests, stale assets, and expensive low-use models.
7. Add manual FK editor in Discover & Access.
8. Add Postgres constraint/comment import.
9. Add Redshift preview connector in observe mode.
10. Add Athena/Glue preview connector in observe mode.

## Useful Recent Commits

- `e431765 Refresh discovery health after metadata edits`
- `9bc40b8 Normalize smart quotes in data queries`
- `143a490 Merge dictionary review into discovery`
- `5ec3581 Resolve staging previews to source tables`
- `254d76d Expose normalized source events`
- `0e9189c Reset source-scoped state on disconnect`
- `ce229c9 Persist key decisions across rediscovery`
- `0dd5a3d Fix Explore initial load performance`
- `e0c3c6f Improve discovery review and business insights`
