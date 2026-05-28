# Headwater 2 — Persona Problems mapped to Lifecycle & Modules

Status: Draft. Companion to `HEADWATER2_VISION.md`. Externally validated 2026-05-27.

## The personas, in one frame

| Persona | Lives mostly in | Core anxiety |
|---|---|---|
| Mid-market data analyst | Explore -> Answer&Share | "Am I missing something, and will the number hold up in front of my boss?" |
| Analytics engineer | Model -> Evaluate | "Is the source trustworthy and well-defined enough to build on?" |
| Data consultant | All four, repeatedly, across clients | "Can I quickly assess a brand-new client's data and show value fast?" |

## Evidence the wedge is real

- 67% of organizations don't fully trust the data they use for decisions; 64% name data
  quality as their top integrity challenge. That gap is the wedge.
- ~80% of analyst/scientist time goes to cleaning/prep; 57% call it the worst part of
  the job. H2 attacks the most-hated, highest-cost part of the workflow.
- Trust erodes from inconsistent definitions, not just dirty data ("user,"
  "conversion," "lead" meaning different things; metrics defined by implementation
  convenience rather than business reality; no traceability to source).

## Stage-by-stage: problem -> module

### Explore — "what's here, and what am I missing?"

| Real problem | Felt most by | H2 module |
|---|---|---|
| Outliers/errors invisible in large unknown datasets (age 250, bad zips) | Consultant, analyst | Profiler / anomaly surfacing |
| Trends, seasonality, cyclicality missed without deliberate time-series EDA | Analyst | EDA depth (temporal) |
| Simpson's paradox — aggregate trends reverse when disaggregated; segmentation missed | Analyst | Segmentation / disaggregation pass |
| Heterogeneous formats/vendors (CSV vs JSON, different field labels) on intake | Consultant | Shared source catalog + connectors (spine) |

### Model — "shape it for the question"

| Real problem | Felt most by | H2 module |
|---|---|---|
| Ambiguous data ownership / unclear source quality = top data-prep pain (41%) | Analytics engineer | Relationship + grain discovery; provenance |
| Deciphering structure with no docs; reverse-engineering relationships | Consultant, AE | Structure / PK-FK discovery |
| Knowing what's missing to answer the goal | Analyst, consultant | Gap / guidance advisor |

### Evaluate — "can I trust this for this decision?" (the truth-teller)

| Real problem | Felt most by | H2 module |
|---|---|---|
| Dashboards quietly wrong from duplicates, stale loads, broken grain | All | Readiness / trust verdict |
| Inconsistent metric definitions across reports/systems | AE, analyst | Semantic / definition layer + locks |
| Metrics untraceable to source -> stakeholders stop trusting | Analyst | Lineage / traceability in the verdict |
| Need a fast maturity assessment at engagement start | Consultant | Readiness verdict as deliverable |
| A number right last month is wrong now (source drifted) and nothing flagged it | All, consultant | Continuous re-certification — affected answers re-check; the badge auto-revokes with a reason |

### Answer & Share (gated) — "produce and defend the answer"

| Real problem | Felt most by | H2 module |
|---|---|---|
| Numbers debated in meetings; "whose report is right" | Analyst | Query grounded in certified data + definitions |
| Dashboards lose leadership trust when out of sync/undefined | Analyst | Shareable dashboard carrying trust/definition provenance |

## Explicitly OUT of scope (someone else's job)

- dbt operational pain — slow CI/CD, model-tree navigation, merge conflicts.
- Org governance/ownership process, real-time streaming, security/access control.
- Full self-serve BI (the long tail already ruled out in the vision).

## Three findings folded into the vision

1. Definition consistency is co-equal with data quality in Evaluate — not a sub-bullet.
   The trust crisis is as much "what does this column mean and can I trace it" as "is it
   clean." Semantic/definition layer + traceability is core to the verdict.
2. The many-to-many shared-data model is validated by the consultant persona, whose
   multi-client, multi-format, repeated-assessment workflow IS the architecture.
3. Trust is not a one-shot report. The consultant's "repeated assessment" is really a
   demand for a *living* credential: as shared sources grow and drift, the verdict
   re-checks and a certified answer revokes its own badge with a reason — continuous
   certification, not a point-in-time score.

## Sources

- https://www.precisely.com/data-integrity/2025-planning-insights-data-quality-remains-the-top-data-integrity-challenges/
- https://www.getdbt.com/resources/state-of-analytics-engineering-2026
- https://acuvate.com/blog/challenges-faced-by-data-scientists/
- https://www.cloverdx.com/blog/what-is-customer-data-onboarding
- https://businessanalyticsinstitute.com/exploratory-data-analysis-eda-uncovering-hidden-patterns/
- https://www.perceptive-analytics.com/how-data-quality-issues-quietly-erode-trust-in-dashboards/
- https://leandataengineer.com/blog/why-data-teams-don-t-trust-their-data-and-how-to-fix-it/
