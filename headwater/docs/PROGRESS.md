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
- Large table profiling policy is not first-class yet.
- Insight evidence coverage does not yet distinguish full profile vs sample vs
  aggregate sketch clearly enough.

## Next Recommended Work

1. Add manual FK editor in Discover & Access.
2. Add Postgres constraint/comment import.
3. Add connector-level large-table profiling policy.
4. Add Redshift preview connector in observe mode.
5. Add Athena/Glue preview connector in observe mode.
6. Add maturity-aware onboarding and evidence reporting.

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
