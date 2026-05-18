# CTO Directive: Domain-Aware Insight Layer

Audience: Headwater engineering team
Trigger: Data Scientist review of NYC TLC insight output (see `insights_ds_review_tlc.md`)
Goal: Move from generic statistical scans to a domain-aware insight pipeline that solicits user context and retrieves external metadata, without hardcoding any single domain.

## Strategic Principle

> Insights = `f(data signals, domain context, retrieved metadata)`

Today the system uses only the first term. We need a workflow that **solicits** domain context from the user and **seeks** external metadata, then fuses both into the generation pass. The result must remain generic: taxi is the validation case, not the code path.

## The Generic Workflow (5 Stages)

### Stage 1 - Dataset Framing (user-supplied context)

Before profiling runs, the workflow asks the user 4-6 structured questions:

- What does one row represent? (a trip, a transaction, an event, a snapshot)
- What is the time grain and the period covered?
- What are the key entities and their lifecycle? (pickup -> dropoff; order -> ship -> deliver)
- What decisions will this data inform? (operations, pricing, compliance)
- Known data quality caveats? (e.g., "FHV pickup_location is sparse")
- Any external references? (data dictionary URL, schema doc, glossary)

Persist as a `DatasetContext` object on the source. **This is the new first-class artifact.** Framing is optional but encouraged; baseline insights must still work without it.

### Stage 2 - Metadata Retrieval (system-driven)

Given the framing, the system pulls structured metadata from:

1. **User-provided docs** - uploaded PDFs / URLs (e.g. TLC data dictionary), parsed into a glossary.
2. **Column-name dictionaries** - match column names against a registry of known semantic patterns (`pickup_datetime`, `PULocationID`, `total_amount`).
3. **Reference data** - zone lookup tables, ISO codes, business calendars (holidays, DST).
4. **Prior runs** - locked semantic classifications from earlier discoveries on similar data (per invariant I-6).
5. **LLM-aided semantic typing** - column name + sample stats -> candidate canonical role. **No raw rows** (per invariant I-3).

Output: a `SemanticSchema` mapping each column to a canonical role, plus a list of derived fields the system should compute (`duration_min`, `pickup_hour`, `route_pair`, etc.).

### Stage 3 - Canonical Derivation

Materialize derived fields in DuckDB once, not per-insight. This is the equivalent of a staging model - mechanical, auto-approved (per invariant I-4). Pushdown aggregations to DuckDB; never `SELECT *` 50M rows into Polars.

### Stage 4 - Insight Family Generation

Replace today's flat "scan all temporal x metric" loop with **insight families** keyed off semantic roles:

| Family | Triggers when schema has... |
|---|---|
| Coverage & period | any datetime column |
| Volume distribution | event-grain row + datetime |
| Peak / off-peak | volume + hour/day-of-week |
| Travel / duration time | start_ts + end_ts |
| Geographic hotspot | location_id + lookup |
| Route / pair analysis | origin + destination |
| Congestion proxy | distance + duration |
| Data quality | nulls, impossible values, sparsity |

Each family runs aggregations at the right grain (hour, zone, route) and reports quantiles (p50/p90/p95), not just means. Multiple-testing correction is applied **within** each family, not across unrelated ones.

### Stage 5 - Ranking & Surfacing

Rank by `effect_size x population_size x confidence x actionability`, not p-value alone. Wire `/explore/suggestions` to actually return the insight list (current bug at `headwater/headwater/api/routes/explore.py:123` returns `[]`).

## Team Directives - Step by Step

### Backend Lead (owns Stages 2-4)

1. Add `DatasetContext` model in `core/models.py`; persist in SQLite alongside source.
2. Build `analyzer/semantic_schema.py` - column-role inference combining name registry, stats, and LLM (stats only, no rows).
3. Build `analyzer/metadata_retrieval.py` - pluggable retrievers: user-doc parser, lookup-table loader, prior-run lock loader.
4. Refactor `explorer/statistical.py:274` - replace `SELECT *` with DuckDB-side aggregation. Hard rule: no full-table pulls > 1M rows.
5. Replace the flat scan in `explorer/statistical.py:40` with an **insight-family dispatcher** keyed off `SemanticSchema`.

### Frontend Lead (owns Stage 1)

1. New "Dataset Framing" step in the discovery workflow - 4-6 questions, skippable but encouraged.
2. Upload slot for data dictionary / schema docs.
3. Surface the inferred `SemanticSchema` for confirmation; lock approved roles (per invariant I-6).
4. Apply the confirmation-overload rule: auto-accept high-confidence role inferences; only ask on ambiguous columns.

### Data Scientist (owns Stage 4 quality)

1. Define the insight-family catalog as a versioned spec (YAML), not hardcoded Python.
2. For each family: required roles, aggregation grain, quantiles, ranking weights, narrative templates.
3. Seed with 6 families (coverage, volume, peak, duration, geo, quality). Taxi is the validation case.

### Platform / Infra

1. Bench the canonical-derivation step on the 51M-row TLC dataset; target < 30s on a laptop.
2. Add `/explore/insights` endpoint; fix `routes/explore.py:123` so the suggestions endpoint stops shadowing real insights.

## Sequencing

- **Week 1:** `DatasetContext` model + framing UI + `SemanticSchema` inference (column-name registry only, no LLM yet).
- **Week 2:** Insight-family dispatcher + 3 families (coverage, volume, duration). Validate against TLC.
- **Week 3:** Metadata retrieval (doc upload, lookup tables) + remaining 3 families.
- **Week 4:** Ranking, multiple-testing-per-family, narrative templates, end-to-end on TLC plus one non-taxi dataset to prove genericity.

## Guardrails (non-negotiable)

- The framing step is **optional**; baseline insights must still produce without it. Domain context lifts quality, it doesn't gate it.
- Keep the catalog generic. If we add a "taxi" family, we have lost. Roles are generic (`origin`, `destination`, `lifecycle_start_ts`); taxi is a configuration, not a code path.
- LLM only sees schema + stats + glossary, never rows (invariant I-3).
- Mart-equivalent insights (those encoding business definitions) require human review (invariant I-4).

## Acceptance Test

The data scientist's example insights are the acceptance test. If the new pipeline does not produce all five of the following for the TLC dataset, the work is not done:

1. "6 PM is the busiest pickup hour with 3.08M valid trips; PM peak trips are about 2.18 minutes longer than other hours."
2. "Weekday trips are 2.25 minutes longer on average than weekend trips, with p90 38.4 vs 32.9 minutes."
3. "JFK Airport pickups have the longest high-volume travel times: avg 42.1 min, p90 68.2 min."
4. "HVFHV wait time is highest around 4 AM and 7 AM, with p90 wait near 12 minutes."
5. "FHV location analysis is unreliable unless null pickup locations are handled, because 88.4% of FHV pickup zones are missing."

A second dataset (non-taxi) must also produce its own equivalents from the same generic pipeline.
