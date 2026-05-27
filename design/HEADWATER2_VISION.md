# Headwater 2 — Vision

Status: Draft for alignment. Supersedes the source-centric framing of Headwater 1.
Last updated: 2026-05-27

## One line

Headwater 2 helps a data professional answer one business question by telling them
the truth about whether their data can answer it — what's there, what's trustworthy,
what's missing, and what's quietly lying — and then, once the data is proven
trustworthy, helps them produce and share a trustworthy answer.

## Who it's for

The beachhead user is the **mid-market analytics professional and the data consultant**:
data analysts, analytics engineers, and independent/agency data consultants who are
handed a business problem and a pile of data and are expected to produce a trustworthy
answer.

We deliberately do **not** target the tiny company with no data (their problem is
"we have no data," not "we can't trust our data" — the wedge doesn't bite) nor the
large enterprise (heavy compliance/procurement/BI-integration motion, deferred — see
GTM below). The data consultant is the sharpest entry point: they run multiple
engagements (multiple projects), reuse and re-scope the same data across them, feel the
trust pain acutely, and act as a distribution channel. They are not merely a channel —
their multi-client, multi-format, repeated-assessment workflow IS the many-to-many
architecture, so building for them builds the core model.

## The pain

Their pain is not writing queries — they can do that. It is everything that silently
sabotages the answer before the query:

- **Missed signal** — the metric, anomaly, seasonality, or segment that mattered was
  buried in volume and never surfaced.
- **Quiet untrustworthiness** — nulls, broken joins, duplicated grain, stale loads,
  misleading names. The query runs, returns a number, and the number is wrong.
- **Structural gaps** — the data needed to answer the question was never captured, and
  nobody noticed until weeks in.
- **No map** — bad schemas, bad relationships, no documentation; days lost
  reverse-engineering what the data even is.

Job to be done: *"Tell me, fast and honestly, whether I can trust this data to answer
my question — and guide me to a trustworthy answer if I can."*

## Core reframe: business problems are the unit of work; data is shared underneath

This is the heart of Headwater 2 and where it breaks from Headwater 1.

A **Project is a business problem with a goal** — reduce churn, grow revenue, optimize
a campaign, drive personalization. It is the primary entity; everything organizes
around it.

**Data sources are a shared, reusable pool, not owned by any one project.** The same
connection feeds a churn project and a revenue project and a personalization project —
each with a different goal, different relevant columns, a different definition of
"good," and a different outcome. The relationship is many-to-many. Therefore:

- **Raw data, profiles, structure, and discovered relationships are computed once and
  shared** — never re-ingested or re-profiled per project.
- **Relevance, semantic meaning, and the trust verdict are project-scoped** — the same
  column is central to churn and irrelevant to revenue; "trustworthy enough" depends on
  the question being asked.
- Sources grow **incrementally** — new uploads, formats, and vendors land into the
  shared pool over time, and every project re-evaluates against its own goal.

The unlock: scope is minimized per problem (analysis stays deep and relevant), while
data work is amortized across problems (not wasteful boil-the-ocean ingestion).

## The product: four stages, one goal, gated

The three roles Headwater always wanted, anchored to a goal, plus a gated payoff stage:

1. **Explore** — *"What in our data bears on this goal?"* Holistic depth, bounded
   breadth. Within the project's scope it digs hard: metrics, distributions, anomalies,
   seasonality, relationships — including what a human would miss in the volume. It does
   not profile the whole warehouse.

2. **Model** — *"Help me shape this into something I can answer the question with."*
   Drafts the dataset/model for the goal, shows discovered relationships, and — most
   importantly — shows what is missing to complete the answer and guides the next step.

3. **Evaluate (the truth-teller)** — *"Can I trust this enough to bet a decision on
   it?"* The distinctive role. States data readiness for this goal: what you have,
   what's trustworthy, what's broken, what's absent, what's misleading. Monitors it over
   time as sources grow. Readiness is a living verdict, not a one-shot report.
   **Definition consistency is co-equal with data quality here**, not a sub-bullet:
   trust erodes as much from "what does this column mean and can I trace it to source"
   (inconsistent metric definitions, untraceable numbers) as from dirty data. The
   semantic/definition layer plus lineage is core to the verdict.

4. **Answer & Share (GATED)** — unlocks only once Evaluate certifies the data is
   ready. Drafts a query grounded in the relationships, grain, semantics, and the
   specific columns H2 has vouched for; lets the analyst edit and run it; renders
   standard charts; and saves a shareable dashboard for stakeholders.

The gate is load-bearing: visualization and dashboards are the **earned payoff of the
trust verdict**, never a free-floating BI feature.

**Gating model — soft on generation, hard on the credential (graduated earned-trust).**
A hard "100% certified or blocked" gate is rejected for three reasons: analysts must
query and chart to *discover* the quality/definition issues they'd certify against
(gating blocks the wedge); a binary gate is incompatible with the *graded* verdict the
Evaluate stage produces; and it violates the standing non-blocking-confirmation
principle. So:

- Exploration and querying are always open. Every output carries its trust state
  visibly as a graded readiness badge.
- Users may build and share an uncertified dashboard — it is stamped "Draft /
  Uncertified," with no trust badge and no provenance trail.
- The one hard rule: uncertified output can never be made to *look* certified. The trust
  badge and provenance trail appear only when the underlying data clears its contracts.
  The badge is sacred.

This turns friction into a credential stakeholders learn to demand — a stronger moat
than a wall, with zero friction in the exploratory phase.

## The wedge

Analysts already have SQL, dbt, notebooks, and BI tools. None of them answer *"can I
trust this data for this decision, and what am I missing?"* before the work starts.
That honest, goal-anchored, holistic-within-scope, living readiness verdict is the moat.
It highlights the truth of the project. Everything else is in service of that.

The Answer & Share differentiator is not "we have charts" — everyone has charts. It is
that **the chart is built on data H2 has already certified, with a query grounded in the
relationships and semantics it discovered.** The trust behind the chart is the value.

## What Headwater 2 is explicitly NOT

- **Not a BI/dashboarding platform.** It draws the line at "enough to prove the answer
  is real and show a stakeholder." No pivot builders, dozens of chart types, scheduled
  refreshes, row-level security, embedding, or alerting. That is a different company,
  and chasing it is what bloated Headwater 1.
- **Not a natural-language-to-SQL product.** H2's job is upstream of the query. (The
  ~8,900-line H1 explorer NL-to-SQL engine is reference material, not foundation.)
- **Not a boil-the-ocean profiler** that connects everything and describes it.
- **Not an auto-applier of business logic.** It stays advisory; the analyst decides.
  Empower the professional, never replace them.
- **Not a 10-phase plan.** One real problem, end to end, first.

## Go-to-market and packaging

Two tiers, same product, different framing. Focus now is entirely the first tier; the
second is a deferred consequence, not a parallel build.

- **Small/mid companies — H2 is the product.** No incumbent BI tool; the full path
  (ingest -> trust -> answer -> dashboard) is the whole value and they live in it.
- **Large orgs (DEFERRED) — H2 is the readiness MVP for a go/no-go.** The position
  nobody occupies: before funding a data project, show whether the data can even answer
  the question, with a working proof and a dashboard for the CXO. Its artifacts then
  export into their stack (SQL/dbt models, contracts, semantic docs, readiness report),
  making H2 the proving ground whose output is the spec their internal team builds from.

We keep the enterprise door open with two cheap disciplines (below) but do not build the
enterprise motion now.

## Architecture stance (to validate in the audit)

The many-to-many model demands a different center of gravity than H1's source-centric
design:

- **Sources / Connections** — shared catalog. Raw data, profiles, structure, discovered
  relationships: computed once, owned by no project.
- **Projects** — goal + problem statement + a selection over the shared sources. Owns
  the goal-scoped layer: relevance, semantic interpretation, the readiness verdict, the
  draft model, the answer/dashboard.
- **Project <-> Source** — a many-to-many link; the link carries project-specific
  scoping (which tables/columns are in play for this goal).

Keep from Headwater 1 (the spine, ~5K LOC, clean): connectors, profiler, executor,
generator, quality. Replace: the 47-table god-object metadata store, the ~3,800-line
context-services suite, and the ~8,900-line NL-to-SQL explorer (mine for heuristics,
do not carry forward).

Two cheap disciplines that preserve scale and the enterprise path without building for
them now:

1. **Engine-agnostic compute** — do not hard-code DuckDB everywhere; keep the spine able
   to push computation down to a warehouse (DuckDB / Snowflake / BigQuery) later.
2. **Portable artifact export** — reuse the generator spine to emit SQL/dbt, contracts,
   and docs. Useful to mid-market today; the large-org handoff tomorrow.

   **Export priority for the consultant beachhead: audit report first, dbt second.** The
   Markdown/PDF Quality & Semantic Audit Report is the engagement-winning deliverable —
   it is what consultants present to stakeholders to prove value and fund the next
   phase, it is used in *every* engagement (not just those that reach a production
   build), and it is nearly free because it is the readiness verdict rendered. Crucially,
   it is the same artifact as the Stage-4 trust credential: the badge + provenance trail
   in portable, shareable form. dbt models + `schema.yml` come second, as the
   graduate-to-production handoff for engagements that proceed (and the future large-org
   export). Keep the report a templated render of the verdict, not a bespoke design tool.

Build for gigabytes; design so terabytes stay reachable.

## The one thing we prove first

Pick one real business problem and a few real sources. Deliver a single artifact: a
**goal-anchored data-readiness verdict** — what you have, what you can trust, what's
missing, what's misleading — and prove a real analyst (ideally a consultant) says "that
just saved me a week." If yes, build outward from the wedge, including the gated
Answer & Share stage. If no, no architecture would have saved the old direction.
