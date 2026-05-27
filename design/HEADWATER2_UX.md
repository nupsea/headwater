# Headwater 2 — Guided Workflow & UX Flows

Status: Draft for vetting. Companion to VISION / PERSONAS / AUDIT / BUILD.
Grounded in the radiology "registration workflow" project, naked-data blind read.
This must be vetted before implementation (see [[feedback_design_first_workflow]]).

## Design principles (the anti-H1 rules)

1. One screen, one job. Never show everything at once.
2. Progressive disclosure: headline first, evidence one layer down (always behind
   "show evidence", never the default view).
3. Generate everything; gate only on genuine unknowns. High-confidence inferences are
   auto-accepted with easy undo -- they are NOT confirmations.
4. Confirmations are few, batched, and ranked by impact on the goal. Never drip 400
   questions; surface the 4 that matter.
5. "I don't know" is a first-class answer -> becomes a recorded gap, never a guess.
6. Trust is the through-line: a single graded badge is visible on every screen and
   updates as you resolve things.
7. Non-blocking: you can always proceed; unresolved items become gaps in the verdict
   (soft gate). Only the trust credential is hard-gated.

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
   shared pool, no re-scan) + optional context resources. Produces the Project Spec.
   Decision: Generate.
2. Generate -- ingest, profile, type, relate, scope-to-goal. Fully automatic, no
   questions. Transparency only.
3. Understand -- the tool narrates what the data IS (grain, reconstructed workflow,
   relationships, the relevant slice). Decision: "looks right?" high-level confirm.
4. Resolve -- only the genuine ambiguities, ranked by goal impact, one at a time, with
   "why it matters" and a best guess. Decision per item: confirm / provide / don't-know
   (gap) / skip. Non-blocking.
5. Readiness -- the truth, scoped to the goal: have / trustworthy / risky / missing /
   misleading, graded. Decision: improve, or go to Answer.
6. Answer & Share (gated) -- draft query grounded in vouched columns, run, chart, save
   dashboard, export audit report. Output carries the trust badge / Draft stamp.

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
│   ▸ each item links to its evidence                                           │
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
│  ⚠ Not yet certified: patient_type meaning is a gap. This dashboard carries   │
│    a "Draft — Uncertified" stamp until resolved.                              │
│                                                                               │
│   [ Save as Draft ]   [ Export audit report (.md) ]   [ Resolve to certify ]   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## CLI parity

Same five steps, terser. `hw2 new` (spec) → `hw2 generate` → `hw2 understand` →
`hw2 resolve` (interactive, the 4) → `hw2 readiness` → `hw2 answer` / `hw2 report`.
The first slice is CLI-first; the web UI mirrors this exact flow.

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

## To vet

- Is 6 steps the right spine, or should Understand + Resolve merge?
- Is the trust badge the right always-on signal, or too prominent?
- Resolve screen: one-at-a-time vs a single ranked list — which fights overload better?
- Does Readiness's 5-bucket verdict read as "the truth" or still too much at once?
