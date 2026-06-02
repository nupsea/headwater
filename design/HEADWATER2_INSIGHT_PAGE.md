# Headwater 2 — Insight Page Proposal

**Status:** proposal for review. No code written. Follows the design-first rule.
**Date:** 2026-06-01
**Author:** working session on `feat/cdx_revision_hw`
**Related:** `HEADWATER2_VISION.md`, `HEADWATER2_REMEDIATION_PLAN.md`, memory
`feedback_central_truth`, `feedback_judge_forward`, `feedback_no_domain_hardcoding`,
invariant I-3 (never send raw rows to an LLM).

---

## 1. Problem

The final page (Step 5 — Answer & Share) today is a *per-question SQL-and-verdict
inspector*. For each question it shows a state pill, a two-factor certification
panel, a chart, a result table, the draft SQL, and caveats. After the central-truth
fixes (judge persistence, no view contradictions, forward-looking Resolve copy) it
is now *correct and consistent* — but it still reads like a query tool, not an
answer. A stakeholder can't look at it and learn anything. There is:

- **No stated finding.** The user must read a chart and infer the point themselves.
  The product's whole promise is a trustworthy *answer*, and the answer is never
  said in words.
- **No project-level view.** Insight is scattered one-question-at-a-time behind a
  sidebar; there is nothing to screen-share that summarizes "what did we learn
  about the goal, and how much of it can we trust."
- **Verdict noise over signal.** The certification panel (a compliance artifact) is
  visually heavier than the result it certifies. Trust should *back* the insight,
  not crowd it out.

## 1b. Prerequisite: answers must be CORRECT before they're presented

Observed 2026-06-01 on the "Radiology Bootstrap Demo" project (goal "Analyse
efficiency"), question **"How does hour day of arrival change over time?"**:

- The chart was blank because it plotted `avg_total_duration`, which was NULL in
  every row. `total_duration` (events.csv) is stored as text like `"00:01"` /
  `"0 days 00:22:00"`; the generated SQL did `AVG(TRY_CAST(total_duration AS
  DOUBLE))`, every cast failed, the average was NULL. (UI now refuses to draw the
  empty chart and states this — but that's a band-aid on a wrong answer.)
- Worse, `total_duration` is the **wrong measure** for the question. The question is
  about `hour_day_of_arrival` (an int 0–23 in cases.csv); the answer averaged an
  unrelated duration column.

**Conclusion: presentation polish is worthless on top of a wrong answer.** A
stated-insight layer or dashboard built over the current generator would just narrate
nonsense confidently — the exact opposite of the trust wedge. Two answer-generation
defects must be fixed first:

1. **Measure/column selection.** The temporal/segment/ranking templates pick
   `measure_cols[0]` / `cat_cols[0]` by list order, with no check that the chosen
   measure is the one the *question* is about. Need: select the measure from the
   question's intent (the concept it names), validate it exists and is meaningful,
   and if the needed concept isn't a usable column, go to the judge-forward path
   (state the gap, ask for the column/derivation) rather than silently substituting
   another column.
2. **Text-duration measures.** `total_duration`, `total_wait_time`, `total_scan_time`,
   `throughput_time` are durations stored as `HH:MM` / `MM:SS` / `"0 days HH:MM:SS"`
   strings. `AVG(TRY_CAST(... AS DOUBLE))` yields NULL. These need a parse-to-minutes
   derivation — but the format is ambiguous (`"00:01"` = 1 min or 1 sec?), so per
   advisory-only + no-hardcoding this is a **user-confirmable derivation** (the
   course-correct path in `feedback_judge_forward`), not an auto-applied cast.

This is the known "answer-quality measure-selection" gap recorded in
`project_h2_remediation`. It is the real blocker the user keeps hitting; it should be
sequenced BEFORE §3/§4 below.

## 2. Principles (inherited, non-negotiable)

1. **One central truth; every view is a projection of it** (`feedback_central_truth`).
   A stated insight is a *derived projection* of the same `FinalizedAnswer` the chart
   and verdict already come from. It must never assert something the data/verdict
   doesn't support, and it must change when they change.
2. **Insight is gated by trust.** A finding shown as fact must be certified. Doubtful
   answers may show a *provisional* finding, clearly stamped, never as settled truth.
   Can't-answer questions state the gap, not a number.
3. **No domain hardcoding** (`feedback_no_domain_hardcoding`). The narration engine
   knows shapes (ranking, trend, segment, coverage), not domains. Human meaning for
   coded values comes only from the truth (`value_labels`, column descriptions,
   resolved claims) — never from a baked-in dictionary.
4. **I-3 holds.** Any LLM narration sees only aggregated `result_stats` +
   column metadata, never raw rows — exactly like the judge.
5. **Persist + go stale like the judge.** A generated insight is stamped with the
   project input fingerprint and rehydrated on the fast path; an input change makes
   it stale and it is re-derived on recompute. Reuse the machinery just built for the
   judge verdict (`judged_fingerprint` → a parallel `insight_fingerprint`).

## 3. The stated-insight layer (smallest valuable step)

Turn each `FinalizedAnswer` into one plain-English **finding** + a **support
metric**, derived from data we already compute (`chart_spec`, `result_stats`,
`columns`, `rows` preview, `value_labels`).

### 3.1 Deterministic core (no LLM, always available)

Keyed on `chart_spec.type` / detected question shape:

| Shape | Finding template (filled from result_stats / top rows) |
|---|---|
| ranking / segment (bar) | "**{top_label}** has the {highest\|lowest} {measure}: {value}, {ratio}x the {opposite_label}." |
| temporal (line) | "{measure} {rose\|fell\|was flat} over {span}; peak {peak_value} at {peak_x}." |
| coverage (table) | "{row_count} records span {earliest}–{latest} across {distinct} {entity}." |

- `{*_label}` uses `value_labels` when the grouping column is coded (so it reads
  "Home patients", not "H"); otherwise the raw category.
- `{measure}` uses the column description from the catalog when present, else the
  column name humanized.
- Ratios/peaks come from `result_stats` (already I-3-safe aggregates), not raw rows.
- This layer is pure, fast, deterministic, and unit-testable. It is the floor: every
  certified answer gets a sentence even with no model running.

### 3.2 Optional LLM enrichment (I-3-safe, advisory)

When a provider is available, a single narration call may *rewrite* the deterministic
finding into one tighter sentence and optionally add a "so what" caveat — given only
the question, the deterministic finding, and `result_stats`. Rules:

- Never invents numbers; it may only restate the stats it is given (guard: numbers in
  the output must appear in the deterministic finding/stats, else fall back).
- Persists and goes stale exactly like the judge verdict.
- Never gates trust. It is prose over an already-certified result.

### 3.3 UI change (per question)

Reorder the active-question panel to lead with meaning:

```
Which patient_type has the highest total_wait_time?   [✓ Certified]

  FINDING
  Home patients wait 2.3x longer than adults — 47m vs 20m average.

  [ chart ]

  ▸ How we know        (collapsed: two-factor panel, contracts, SQL, caveats)
```

The certification panel, result table, and SQL move *inside* a "How we know"
disclosure. Trust is one click away, always present, no longer dominant.

## 4. Project insight dashboard (next layer, builds on §3)

A new project-level summary rendered above (or replacing the default of) the
per-question list — the one scrollable thing you screen-share:

```
GOAL  Understand patient registration bottlenecks and waiting-time distribution
TRUST 3 certified · 1 doubtful · 1 can't answer        [readiness ring]

HEADLINE FINDINGS  (certified only)
  • Home patients wait 2.3x longer than adults (47m vs 20m).
  • Waiting peaks 08:00–10:00 (1.8x the daily mean).
  • Weekly volume is flat — no week-over-week trend.

NEEDS INPUT          (doubtful / can't, each linking to its Resolve card)
  • "total_wait_time over time" — no wait-time column; define a derivation.

DATA MODEL           [the existing readiness data-model diagram]
EXPORT               [audit report .md]  [dashboard link]
```

- "Headline findings" = the certified answers' §3 findings, ranked by impact
  (effect size from `result_stats`, e.g. largest ratio / steepest trend).
- "Needs input" reuses the forward-looking Resolve framing — the dashboard honestly
  shows what it *can't* yet say and links to the exact unblock action.
- Recompute updates this view through the existing `HW2_RECOMPUTED` event; nothing
  here holds private state.

## 5. Recompute consistency (how it stays true)

- Add `stated_insight` (+ optional `insight_fingerprint`, `insight_source:
  derived|llm`) to `FinalizedAnswer` and the answer artifact, persisted in
  `_persist` under the same rules as the judge contract.
- Deterministic findings are recomputed every finalize (cheap, pure) and so are
  always current by construction.
- LLM-enriched findings rehydrate on the fast path and go `stale` on input change,
  reusing `_rehydrate_*` + the fingerprint — same pattern, no new staleness model.
- The dashboard derives entirely from the finalized answers; recompute → re-derive →
  re-render. No metric can exist in one view that another view contradicts.

## 6. Scope / sequence

0. **§1b answer correctness** — measure/column selection + user-confirmable
   duration derivation. *Prerequisite; nothing below is worth shipping first.*
1. **§3.1 deterministic finding** + per-question UI reorder ("How we know"
   disclosure). Self-contained, testable, no model dependency. *Recommended first.*
2. **§4 project dashboard** over those findings + the existing readiness diagram and
   Resolve links.
3. **§3.2 LLM enrichment**, last, behind the persistence/staleness machinery.

## 7. Open questions for review

1. **Impact ranking** for headline findings — effect size only, or also weight by how
   central the question is to the goal? (Latter needs a goal-relevance score we may
   not have.)
2. **Doubtful findings on the dashboard** — show a provisional, clearly-stamped
   number, or suppress entirely and only list the gap? (Principle 2 leans suppress;
   confirm.)
3. **"Dashboard link" / share** — is a read-only in-app view enough for now, or is the
   Markdown/PDF audit report the only share artifact for this milestone? (Vision
   says report-first.)
4. **LLM narration worth it for the MVP?** The deterministic layer may be enough to
   prove the wedge; §3.2 adds a model dependency and a staleness surface for prose.
5. Does the dashboard **replace** the per-question default view, or sit above it as a
   collapsible summary?
