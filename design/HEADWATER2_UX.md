# Headwater 2 — Guided Workflow & UX Flows

Status: Draft for vetting. Companion to VISION / PERSONAS / AUDIT / BUILD.
Grounded in the radiology "registration workflow" project, naked-data blind read.
This must be vetted before implementation (see [[feedback_design_first_workflow]]).
Aligned 2026-05-28 to the updated vision: continuous certification (the loop),
evidence-derived per-answer certification, and proposed questions.

## Design principles (the anti-H1 rules)

1. One screen, one job. Never show everything at once.
2. Progressive disclosure: headline first, evidence one layer down (always behind
   "show evidence", never the default view).
3. Generate everything; gate only on genuine unknowns. High-confidence inferences are
   auto-accepted with easy undo -- they are NOT confirmations.
4. Confirmations are few, batched, and ranked by impact on the goal. Never drip 400
   questions; surface the 4 that matter.
5. "I don't know" is a first-class answer -> becomes a recorded gap, never a guess.
6. Readiness is **per question, evidence-derived, never a single project score**. A
   graded indicator is visible on every screen, but the certified badge is recomputed
   from evidence contracts (locked cols + lineage · no blocking gap · structural
   integrity · no unresolved "misleading" · consistent definition) AND a confident
   insight — never from a click count. The same project can hold a certified answer next
   to a Draft one.
7. Non-blocking: you can always proceed; unresolved items become gaps in the verdict
   (soft gate). Only the trust credential is hard-gated.
8. Certification is **alive**: a verdict carries freshness ("as of <date>"); when the
   data underneath changes, affected answers re-check and a broken contract auto-revokes
   the badge with a reason. A project never "finishes" — the flow loops back to Readiness.

## Navigation model

A persistent frame on every screen: project + goal (top), a 5-step stage spine with the
current step and pending-confirmation count, and the live trust badge. The flow is
linear but revisitable -- you can go back to add a source, a glossary, or resolve more.

```
┌───────────────────────────────────────────────────────────────────────────┐
│ Headwater · Registration Workflow                       Trust: ◐ Forming 62%│
│ Goal: Reduce patient registration wait time before imaging                  │
├───────────────────────────────────────────────────────────────────────────┤
│  ① Connect → ② Understand → ③ Resolve (4) → ④ Readiness → ⑤ Answer          │
├───────────────────────────────────────────────────────────────────────────┤
│  [ screen body ]                                                            │
└───────────────────────────────────────────────────────────────────────────┘
```

Trust badge scale: ○ Not started · ◔ Low · ◐ Forming · ◑ Ready-ish · ● Certified.

## The journey (steps + decision points)

0. Projects home -- projects are business problems; shared sources sit underneath.
   Decision: open existing or create new.
1. Frame the project -- plain-language goal (required) + attach sources (reuse from the
   shared pool, no re-scan) + optional context resources. Questions are **not** required
   up front. Produces the Project Spec. Decision: Generate.
2. Generate -- ingest, profile, type, relate, scope-to-goal. Fully automatic, no
   questions. Transparency only.
3. Understand -- the tool narrates what the data IS (grain, reconstructed workflow,
   relationships, the relevant slice) AND **proposes the questions this data can credibly
   answer, flagging the ones it can't (and why)**. The user curates: keep / edit / add /
   drop. Decision: "looks right?" high-level confirm. (Propose, don't demand.)
4. Resolve -- only the genuine ambiguities, ranked by goal impact, one at a time, with
   "why it matters" and a best guess. Decision per item: confirm / provide / don't-know
   (gap) / skip. Non-blocking.
5. Readiness -- the truth, scoped to the goal: have / trustworthy / risky / missing /
   misleading, graded. Decision: improve, or go to Answer.
6. Answer & Share (gated) -- draft query grounded in vouched columns, run, chart, save
   dashboard, export audit report. Output carries the trust badge / Draft stamp.

The flow is a **loop, not a line**: after Answer the project stays live. New/changed data
re-runs Generate -> Readiness for the affected questions; a previously certified answer
can be demoted to "Draft — re-verify" with the broken reason, reopening the specific
Resolve item. See "Continuous certification" below.

## Wireframes

### A. Projects home  (many-to-many made visible)
```
┌ Headwater ───────────────────────────────────────────── [ + New Project ] ┐
│  Your projects (business problems)                                          │
│                                                                             │
│  ┌─────────────────────────────┐   ┌─────────────────────────────┐         │
│  │ Registration Workflow       │   │ Device Utilization          │         │
│  │ Reduce registration wait    │   │ Maximize modality throughput│         │
│  │ Source: radiology           │   │ Source: radiology (shared)  │         │
│  │ Trust: ◐ Forming 62%        │   │ Trust: ○ Not started        │         │
│  └─────────────────────────────┘   └─────────────────────────────┘         │
│                                                                             │
│  Shared sources:  radiology · 3 tables · profiled      [ + Connect source ] │
└─────────────────────────────────────────────────────────────────────────────┘
```

### B. Frame the project  (goal is the hero; sources reuse the pool)
```
┌ New Project ─────────────────────────────────────────────────────────────────┐
│  What problem are you solving?                                                 │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │ Reduce patient registration wait time before imaging                   │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
│  ▸ Add detail (optional): decision to make · target metric · time horizon      │
│                                                                                │
│  Data for this project                                                         │
│  ┌ from shared pool ─────────────────┐   ┌ or add new ─────────────────┐       │
│  │ ☑ radiology (3 tables, profiled   │   │  ⬆ Drop files / connect DB  │       │
│  │   — reused, no re-scan)           │   │                             │       │
│  └───────────────────────────────────┘   └─────────────────────────────┘       │
│                                                                                │
│  ▸ Add context (optional): glossary · data dictionary · definitions · notes    │
│                                                                                │
│                                              [ Cancel ]      [ Generate → ]     │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### C. Generating  (no input needed)
```
┌ Generating understanding ──────────────────────────────────────────────────┐
│  radiology · 34,039 rows across 3 files                                     │
│                                                                             │
│   ✓ Ingested         cases · exams · events                                 │
│   ✓ Profiled         columns · stats · keys                                 │
│   ✓ Found structure  case → exams → events  (event log)                     │
│   ⟳ Typing columns   timestamps · durations · codes …                       │
│   · Scoping to goal                                                         │
│                                                                             │
│   This runs on its own — no input needed.                                   │
└───────────────────────────────────────────────────────────────────────────────┘
```

### D. Understand  (narrative + diagram; irrelevant collapsed; evidence hidden)
```
┌ ② Understand ───────────────────────────────────────── Trust: ◐ Forming 62% ┐
│  Here's what this data is                                                    │
│                                                                              │
│   A patient imaging journey, logged as events:                               │
│                                                                              │
│      [cases] ──< [exams] ──< [events]                                        │
│       1 visit    1+ exams    27,343 workflow steps                           │
│                                                                              │
│   Reconstructed workflow (relevant to registration):                         │
│      Arrival → Recepcion → Front/BackOffice → Room assign → Wait → Exam      │
│               └──────────── registration zone? ──────────┘                   │
│                                                                              │
│   Relevant to your goal:                                                     │
│      cases.arrival_time · cases.total_wait_time · events.activity ·          │
│      events.timestamp_start/end                                              │
│      ▸ 9 columns judged not relevant to this goal (view)                     │
│                                                                              │
│   Questions this data can answer (curate — keep / edit / add / drop):        │
│      ☑ When is wait time worst across the day?                               │
│      ☑ Which workflow step contributes most to wait?                         │
│      ⚠ Which patient_type waits longest? — needs code meanings (Resolve)     │
│      ✗ Has wait changed week-over-week? — only 7 days of history             │
│                                                                              │
│   Looks right?   [ Yes, continue ]   [ Something's off ]      ▸ show evidence │
└────────────────────────────────────────────────────────────────────────────────┘
```

### E. Resolve  (THE anti-overload screen: 4 only, ranked, one at a time)
```
┌ ③ Resolve — 4 things only you can know ─────────────── Trust: ◐ Forming 62% ┐
│  Resolving these raises trust and sharpens your verdict.            1 of 4   │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │ HIGH IMPACT · defines your core metric                               │    │
│  │ Where does "registration" end?                                       │    │
│  │ Your goal measures registration wait. The steps I see:               │    │
│  │   Arrival → Recepcion → FrontOffice → AsignacionSala                 │    │
│  │ Registration is complete after:                                      │    │
│  │   ( ) Recepcion     (•) FrontOffice     ( ) AsignacionSala           │    │
│  │   [ Confirm ]    [ I don't know — mark as gap ]                      │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Up next:                                                                    │
│   2 · What do patient_type codes mean?  A · H · S · D        impact: med     │
│   3 · events.modality empty 88% — missing or N/A?            impact: med     │
│   4 · "UltraSound" vs "US" — same thing?                     impact: low     │
│                                                                              │
│            [ Skip all, treat as gaps ]              [ Resolve next → ]        │
└────────────────────────────────────────────────────────────────────────────────┘
```

### F. Readiness  (the truth: one-line verdict, 5 graded buckets, evidence on demand)
```
┌ ④ Readiness for "Reduce registration wait" ────────── Trust: ◑ Ready-ish 78% ┐
│  You can analyze arrival and wait patterns. The registration-complete         │
│  boundary is user-defined (FrontOffice) and 88% of modality is unlabeled —    │
│  fine for this goal, blocking for device questions.                           │
│                                                                               │
│  ✓ You have       7-day event log · 3,193 visits · arrival + wait times       │
│  ✓ Trustworthy    clean case grain · case→exam→event joins verified           │
│  ⚠ Risky          two duration formats (00:10 vs 0 days 00:22:00)             │
│                   weekday pattern unusual (Sun/Thu high) — verify             │
│  ✗ Missing/gaps   no explicit "registration complete" event                   │
│                   patient_type meanings unconfirmed (skipped)                 │
│  ⚡ Misleading     activity 'ExaminacionExtra?' contains a literal '?'        │
│                                                                               │
│   ▸ each item links to its evidence · verdict is per question · as of May 28  │
│   [ Improve: resolve gaps / add a glossary ]          [ Go to Answer → ]       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### G. Answer & Share  (gated; query grounded in vouched columns; Draft stamp)
```
┌ ⑤ Answer & Share ───────────────────────────────────── Trust: ◑ 78% (Draft) ┐
│  Draft query — grounded in the columns I've vouched for                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ SELECT patient_type, hour_day_of_arrival, avg(total_wait_time)        │    │
│  │ FROM cases GROUP BY 1, 2 ORDER BY 2;                                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│        [ Edit ]   [ Run ]                                                     │
│  ┌ chart ──────────────────────────────────┐                                 │
│  │  wait │      ▁▂▃▅▆▇▆▅▃▂                  │                                 │
│  │       └────────────────────── hour        │                                │
│  └──────────────────────────────────────────┘                                │
│                                                                               │
│  ⚠ Not yet certified: a gap (patient_type meaning) blocks this answer, and    │
│    certification also needs a confident result. "Draft — Uncertified" until    │
│    the evidence contracts pass AND the insight clears its confidence bar.       │
│                                                                               │
│   [ Save as Draft ]   [ Export audit report (.md) ]   [ Resolve to certify ]   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Continuous certification (the loop)

A project does not end at the first answer; readiness is a living credential.

- **Freshness.** Every verdict and badge carries "as of <date>, against <source
  snapshot>" — stakeholders see how fresh the trust is.
- **Re-evaluation triggers.** New data lands · schema changes · a profile drifts past
  threshold · a definition/lock is edited · a question is added -> the affected questions
  re-run Generate -> Readiness automatically. The user does not restart the flow.
- **Auto-revoke, never silent.** If a re-check fails a previously-passing evidence
  contract (drift broke an assumption, a join began to fan out, a source went stale) the
  badge is revoked: the answer flips to "Draft — was Certified on <date>; re-verify:
  <reason>," and the broken contract reopens as a Resolve card. Nothing stays green by
  inertia.
- **Surfacing.** The project shows a "needs re-verify (N)" state; the affected answer
  carries the demotion reason inline. This reuses the Resolve screen — no new surface.

UX-wise this is the snapshot-diff trigger feeding the existing Readiness/Resolve screens,
not a separate monitoring product (the H1 drift suite is cut; only the diff idea is mined
— see AUDIT).

## CLI parity

Same five steps, terser. `hw2 new` (spec) → `hw2 generate` → `hw2 understand` →
`hw2 resolve` (interactive, the 4) → `hw2 readiness` → `hw2 answer` / `hw2 report`.
The first slice is CLI-first; the web UI mirrors this exact flow.

Conversational NL querying via a pluggable LLM harness (Ollama / 3P vendor) is a **v3**
add-on; v2 `hw2 answer` drafts grounded SQL from discovered semantics, not free-form NL.

## Information architecture (revised 2026-05-27)

Entry point is the **Data Source**, not the project. The hierarchy:

```
Workspace
  └─ Data Source        (connect first; reuses H1 connectors: CSV/JSON/Postgres/
       └─ Project         MySQL/Snowflake/Redshift/SQLite/DuckDB)
            └─ guided workflow (Set up -> Understand -> Resolve -> Readiness -> Answer)
```

- Connect a data source first. Its tables are profiled once on connect.
- A **project belongs to one data source** and selects a subset of that source's tables
  (table selection happens in "Set up"). Profiles are reused across that source's
  projects (the shared-pool efficiency, scoped to one source).
- **Limits for now: max 3 data sources; max 5 projects per data source.**
- This scopes the vision's full many-to-many to "one source -> many projects" for now;
  cross-source projects are deferred. Same-source / multi-project reuse is preserved
  (e.g. radiology -> registration workflow + device utilization on shared profiles).

## Workspace shell (refined 2026-05-27, SageMaker/Databricks-inspired)

> Superseded by "Shell — current (refined prototype)" below. Kept for history.

- **Source management is workspace-level**, not under Catalog: a source switcher with a
  `+` to connect (reuses H1 connectors). Connecting is not a catalog/table action.
- **Left rail = persistent nav**: source switcher (+ connect) -> Workspace section
  (Catalog, Query) -> Projects section (+ New project). SageMaker-style clear entry
  points; Databricks-style object browser.
- **Query is a first-class, multi-table surface** (not per-table): a SQL editor with a
  **schema browser** (tables -> columns, click to insert) supporting joins across tables.
  Per-table Catalog has a "Query this table ->" shortcut that opens the editor prefilled.
- **Catalog** = tables landing (grid) + per-table Overview/Metadata (editable + locks).
- **Project** = wizard that graduates into a hub of revisitable read-only tabs
  (Overview/Understand/Resolve/Readiness/Answer); trust ring + badge persist.

## Shell v3 — Apple-inspired (2026-05-27)

> Superseded by "Shell — current (refined prototype)" below — the refined design moved to
> a warm DM Sans aesthetic and a left-rail (not top phase-nav) layout. Kept for history.

Polished redesign per feedback ("unpolished/lame"). Grounded in Apple HIG (clarity,
deference, depth, consistency) + SageMaker Unified Studio structure.

- **Project at the top**: a project selector beside the logo (projects are the unit of
  work); switch / + New project from its menu.
- **Top phase-nav: Overview · Explore · Workflow · Deliver** (centered pill nav).
- **Overview**: readiness home — large animated trust ring, one-line verdict, "resolve N"
  next action, tiles to the other areas.
- **Explore = Catalog + Query combined**: a Data-explorer tree (source → tables →
  columns; `+` connects a source) beside a **tabbed** work area — query tabs (editor +
  Run + results with **table/chart toggle**) and table tabs (editable metadata + locks +
  "Query this →"). Mirrors the SageMaker/Databricks layout.
- **Workflow**: step-by-step Understand → Resolve → Readiness (clean stepper); trust
  ring builds as you resolve.
- **Deliver**: Answer & Share (draft query → chart → export audit report; Draft stamp).
- **Apple skin**: system SF Pro, near-white canvas, hairline dividers, single calm blue
  accent, soft depth, translucent blurred top bar, smooth transitions.

## Shell — current (refined prototype, 2026-05-28)

The refined prototype (`prototype/Headwater2_refined.html`) supersedes the two shell
explorations above. The current shell:

- **Warm, calm aesthetic**: DM Sans / DM Mono, near-white warm paper (`#faf9f6`), a
  single blue accent, hairline warm dividers, translucent blurred top bar. (Not the
  earlier Apple/SF Pro skin.)
- **Persistent left rail**: Source switcher (+ Connect source) -> Workspace (Catalog,
  Query) -> Projects (+ New). Catalog & Query are workspace-level power tools; Projects
  are the guided workflow.
- **Top bar**: logo; when inside a project, a live trust ring + project-title pill.
- **Project view**: a sticky banner (trust ring + goal + question count + table count)
  above a revisitable 5-step stepper — Frame · Understand · Resolve · Readiness · Answer —
  over the contextual screen body.

## To vet

- Resolved by the refined prototype: 5-step spine kept (Frame · Understand · Resolve ·
  Readiness · Answer); Resolve is one-at-a-time, ranked; the 5-bucket verdict reads as
  "the truth"; warm DM Sans shell over the left-rail layout.
- Still open: does a single project-level trust ring over-claim now that readiness is
  **per question** — should the rail/banner show a per-question breakdown instead of one
  number? (Vision rejects a single score as the credential; the ring is only a readout.)
- How prominent should the "needs re-verify (N)" continuous-certification state be without
  nagging?
- How does Resolve scale past ~4 cards (batch high-confidence; cap blocking items)?
