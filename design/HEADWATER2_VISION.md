# Headwater 2 — Vision

Status: Draft for alignment. Supersedes the source-centric framing of Headwater 1.
Last updated: 2026-05-28

> Change log (2026-05-28): readiness reframed as a **continuous, living credential**
> (re-certification as data drifts/grows); **certification redefined as evidence-derived,
> per-answer, and insight-confidence-aware** (never click-derived); **Frame inverted** so
> Headwater proposes the questions the data can credibly answer rather than demanding them.
> A pluggable LLM query harness (Ollama / third-party vendor) is noted as a **v3**
> direction. Previous version snapshotted at
> `archives/h2_design_2026-05-28/HEADWATER2_VISION_2026-05-27.md`.

## One line

Headwater 2 helps a data professional answer a business question by telling them the
truth about whether their data can answer it — what's there, what's trustworthy, what's
missing, and what's quietly lying — then helps them produce a trustworthy answer, and
**keeps that verdict honest over time** as the data underneath changes.

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
sabotages the answer before the query, and everything that silently rots it after:

- **Missed signal** — the metric, anomaly, seasonality, or segment that mattered was
  buried in volume and never surfaced.
- **Quiet untrustworthiness** — nulls, broken joins, duplicated grain, stale loads,
  misleading names. The query runs, returns a number, and the number is wrong.
- **Structural gaps** — the data needed to answer the question was never captured, and
  nobody noticed until weeks in.
- **No map** — bad schemas, bad relationships, no documentation; days lost
  reverse-engineering what the data even is.
- **Silent staleness** — a number that was right last month is wrong today because the
  source drifted, and the dashboard never said so.

Job to be done: *"Tell me, fast and honestly, whether I can trust this data to answer
my question — guide me to a trustworthy answer if I can — and tell me the moment that
answer stops being true."*

## Core reframe: business problems are the unit of work; data is shared underneath

This is the heart of Headwater 2 and where it breaks from Headwater 1.

A **Project is a business problem with a goal** — reduce churn, grow revenue, optimize
a campaign, cut patient wait time. It is the primary entity; everything organizes
around it. A project holds a goal, a set of **questions** under that goal, a scope over
the shared sources, and the living readiness verdict for each question.

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

## The workflow: a loop, not a line

Headwater always wanted three engine roles — **Explore, Model, Evaluate** — plus a
gated payoff, **Answer & Share**. The user meets them as a guided, non-blocking
workflow that *loops*: a project is never "done," because the data underneath keeps
moving.

The guided stages (the user-facing stepper):

1. **Frame — propose, don't demand.** The user states a goal and an initial scope. They
   are **not** required to know their questions up front. Headwater runs the engine over
   the scope, then **proposes the questions this data can credibly answer — and flags the
   ones it can't, with the reason why.** The user curates: keep, edit, add, drop. This
   inverts Headwater 1's blank form: the engine's value shows on the first run, and the
   honest "we can't answer that, because X" appears early, not as a late surprise.
   (Engine roles in play: a first pass of Explore + Model.)

2. **Understand — "here's what this data is."** The reconstructed shape: structure and
   relationships, grain, the inferred workflow/process, and which columns are relevant to
   the goal versus set aside. One high-level confirmation, with a granular "this specific
   thing is wrong" escape hatch that drops the user into *that* correction — never resets
   the flow. (Explore + Model, made legible.)

3. **Resolve — the things only you can know.** The anti-overload step. Ranked by impact
   on the verdict, one decision at a time: boundary definitions, code meanings, null
   semantics, vocabulary merges. Each card states what it unblocks and what is lost if
   skipped. "I don't know — mark gap" is a first-class, non-blocking choice; skipped
   items become explicit gaps in the verdict, not silent guesses. Resolving a card
   advances progress because it removes a *specific blocking evidence item* — progress is
   real, never cosmetic. (The human half of Evaluate.)

4. **Readiness — the living verdict.** Goal-anchored, per question: what you *have*,
   what's *trustworthy*, what's *risky*, what's *missing*, what's *misleading*. The most
   trust-building screen is the honest negative: *"No — this data cannot answer that, and
   here is why."* Treat it as a hero state, not an edge case. (Evaluate's output.)

5. **Answer & Share (GATED).** Per question, a draft query grounded in the relationships,
   grain, semantics, and the specific columns Headwater has vouched for; the analyst edits
   and runs it; standard charts; a shareable, stamped artifact. The chart's value is the
   certified data and traceable query behind it, not the chart itself.

Wrapping all five: **continuous certification** (next section). New data, drift, a
definition edit, or a new question re-runs Explore/Evaluate and updates every affected
verdict — so the loop closes back onto Readiness on its own, without the user restarting.

## Certification: evidence-derived, per-answer, confidence-aware

The whole brand is honesty, so the credential must be earned by **facts, not clicks**.
Three rules:

**1. Per-answer, not a single project score.** Readiness is computed per question. The
same project can hold a certified answer next to an uncertified one — the prototype's own
example: "when is wait worst across the day?" can certify while "which patient_type waits
longest?" stays Draft because a code meaning is unmapped. A single project-level number
hides exactly the distinction that makes us trustworthy. Any progress ring is a *readout*
of resolved evidence ("% of blocking evidence cleared for this answer"), never the cause
of the badge.

**2. The badge flips only when an evidence contract set is satisfied.** For a given
answer, all of the following must hold, and certification is **recomputed** from them —
it is never set by reaching a points threshold:

- Every column on the query's path is profiled and its meaning is **locked/vouched**,
  with lineage traceable to source.
- **No blocking gap remains** for this question (the user-only-knows facts it depends on
  are resolved, not skipped).
- **Structural integrity holds** on the join path: clean grain, verified referential
  integrity, no duplicate-grain fan-out.
- **No unresolved "misleading" finding** sits in this answer's lineage (each is either
  fixed or explicitly dispositioned).
- **Definition consistency** holds: the metric's definition is consistent and traceable
  (this is co-equal with data quality, not a sub-bullet — trust erodes as much from
  "what does this column mean and can I trace it" as from dirty data).

**3. The generated insight must itself be confident.** Certification requires the data
contracts *and* a high-confidence result. The answer/insight produced in Answer carries a
confidence derived from the data — sample size, variance, coverage, freshness. A
low-confidence insight stays **Draft even on clean data** (e.g., "week-over-week change"
on seven days of history is not certifiable no matter how clean the rows are). High
confidence on certified data earns the badge; everything else is Draft.

**Gating model — soft on generation, hard on the credential.** A hard "100% certified or
blocked" gate is rejected: analysts must query and chart to *discover* the issues they'd
certify against; a binary gate is incompatible with the graded, per-answer verdict; and
it violates the non-blocking-confirmation principle. So:

- Exploration and querying are always open. Every output carries its trust state visibly.
- Users may build and share an uncertified answer — stamped **"Draft / Uncertified,"**
  with no trust badge and no provenance trail.
- The one hard rule: uncertified output can never be made to *look* certified. The badge
  and provenance trail appear only when the evidence contract set above is satisfied.
  **The badge is sacred.**

This turns friction into a credential stakeholders learn to demand — a stronger moat than
a wall, with zero friction in the exploratory phase.

## Continuous certification: the credential stays alive

A project does not end at the first answer. The Evaluate role keeps watching the shared
source pool, and **certification has freshness**: every badge carries "certified as of
`<date>` against `<source snapshot>`."

**Progress advances in two directions.**

- *Forward, by the analyst* — as gaps get resolved and questions get certified over many
  sessions, the project matures. Resolved evidence accrues; locked semantics persist and
  are reused across projects. Trust is cumulative, not re-earned each visit.
- *Continuously, by the engine* — readiness is kept honest as the data changes. The
  badge is a promise that must keep being true.

**Re-evaluation triggers** (any of these re-runs the affected checks): new data lands,
schema changes, profile drift (a distribution or null rate moves past threshold), a
definition or lock is edited, or a new question is added.

**On re-evaluation, each certified answer's evidence contracts are re-checked.** If any
contract now fails — drift broke an assumption, a join began to fan out, a source went
stale — the badge is **automatically revoked**, the answer is demoted to *"Draft — was
Certified on `<date>`; re-verify: `<reason>`,"* and the specific broken contract reopens
as a Resolve item. Nothing silently stays green.

This is the durable moat: competitors hand you a number once. Headwater 2 hands you a
**certified answer that stays honest** — and tells you, unprompted, the moment it stops
being true. The investigation flow is the on-ramp; the living credential is the business.

## The wedge

Analysts already have SQL, dbt, notebooks, and BI tools. None of them answer *"can I
trust this data for this decision, what am I missing, and is it still true?"* That
honest, goal-anchored, holistic-within-scope, **living** readiness verdict is the moat.
Everything else is in service of it. The Answer & Share differentiator is not "we have
charts" — it is that the chart sits on data Headwater has certified, with a query
grounded in the relationships and semantics it discovered, and a badge that revokes
itself when that stops holding.

## What Headwater 2 is explicitly NOT

- **Not a BI/dashboarding platform.** It draws the line at "enough to prove the answer is
  real and show a stakeholder." No pivot builders, dozens of chart types, scheduled
  refreshes, row-level security, embedding, or alerting. That is a different company, and
  chasing it is what bloated Headwater 1.
- **Not a natural-language-to-SQL product.** H2's job is upstream of the query. Question
  proposal and query drafting in v2 use H2's own heuristics, templates, and discovered
  semantics (with the optional LLM plug-in only for phrasing). See the roadmap note below
  on the v3 LLM harness.
- **Not a boil-the-ocean profiler** that connects everything and describes it.
- **Not an auto-applier of business logic.** It stays advisory; the analyst decides.
  Empower the professional, never replace them.
- **Not a 10-phase plan.** One real problem, end to end, first.

## Roadmap note — v3: pluggable LLM query harness (deferred)

A pluggable LLM harness — **local (Ollama)** or a **third-party vendor model** — for
richer natural-language querying and insight narration is a **v3 direction, deferred**.
It is attractive (the consultant wants to "just ask"), but it is explicitly *next*, not
now, and it must land behind the existing advisory boundary:

- An **optional plug-in**, swappable between local and hosted models; H2 works fully
  without it.
- Bound by the standing data rule: it **never sees raw rows** beyond the existing
  summary contract (names, types, stats, relationships), preventing PII leakage and
  hallucination on dirty data.
- It **never auto-applies business logic** and never sets certification — it drafts; the
  evidence engine and the analyst decide.

v2 ships the wedge without it. v3 makes querying conversational on top of an already
trustworthy foundation.

## Go-to-market and packaging

Two tiers, same product, different framing. Focus now is entirely the first tier; the
second is a deferred consequence, not a parallel build.

- **Small/mid companies — H2 is the product.** No incumbent BI tool; the full path
  (ingest -> trust -> answer -> living dashboard) is the whole value and they live in it.
- **Large orgs (DEFERRED) — H2 is the readiness MVP for a go/no-go.** The position nobody
  occupies: before funding a data project, show whether the data can even answer the
  question, with a working proof and a dashboard for the CXO. Its artifacts then export
  into their stack (SQL/dbt models, contracts, semantic docs, readiness report), making H2
  the proving ground whose output is the spec their internal team builds from.

We keep the enterprise door open with two cheap disciplines (below) but do not build the
enterprise motion now.

## Architecture stance (to validate in the audit)

The many-to-many, continuously-certified model demands a different center of gravity than
H1's source-centric design:

- **Sources / Connections** — shared catalog. Raw data, profiles, structure, discovered
  relationships: computed once, owned by no project. Carries snapshot/version so drift is
  detectable.
- **Projects** — goal + problem statement + questions + a selection over the shared
  sources. Owns the goal-scoped layer: relevance, semantic interpretation, the readiness
  verdict, the draft model, the answer/dashboard.
- **Project <-> Source** — a many-to-many link; the link carries project-specific scoping
  (which tables/columns are in play for this goal).
- **Certification ledger** — per-answer evidence contracts, their pass/fail state, the
  source snapshot they were checked against, and the decision provenance (who/what/when,
  on what evidence). This ledger *is* the readiness verdict, *is* the badge, and *is* the
  exportable audit report. It must be recomputable from facts, and re-checked on every
  re-evaluation trigger.

Keep from Headwater 1 (the spine, ~5K LOC, clean): connectors, profiler, executor,
generator, quality. Replace: the 47-table god-object metadata store, the ~3,800-line
context-services suite, and the ~8,900-line NL-to-SQL explorer (mine for heuristics, do
not carry forward).

Two cheap disciplines that preserve scale and the enterprise path without building for
them now:

1. **Engine-agnostic compute** — do not hard-code DuckDB everywhere; keep the spine able
   to push computation down to a warehouse (DuckDB / Snowflake / BigQuery) later. This
   also makes incremental re-profiling on drift affordable.
2. **Portable artifact export** — reuse the generator spine to emit SQL/dbt, contracts,
   and docs. Useful to mid-market today; the large-org handoff tomorrow.

   **Export priority for the consultant beachhead: audit report first, dbt second.** The
   Markdown/PDF Quality & Semantic Audit Report is the engagement-winning deliverable — it
   is what consultants present to fund the next phase, used in *every* engagement, and
   nearly free because it is the readiness verdict (the certification ledger) rendered.
   Crucially it is the same artifact as the Stage-4 trust credential: the badge +
   provenance trail in portable, shareable form, with freshness. dbt models +
   `schema.yml` come second, as the graduate-to-production handoff.

Build for gigabytes; design so terabytes stay reachable.

## The one thing we prove first

Pick one real business problem and a few real sources. Deliver a single artifact: a
**goal-anchored, per-question data-readiness verdict** — what you have, what you can
trust, what's missing, what's misleading — derived from evidence, and prove a real
analyst (ideally a consultant) says "that just saved me a week." Then prove the second
half of the wedge: land new/changed data and show a previously-certified answer
**revoke its own badge with a reason**. If both land, build outward — including the gated
Answer & Share stage. If not, no architecture would have saved the old direction.
