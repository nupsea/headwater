# Headwater 2 — Design Index

Single entry point for the design corpus. Read top-to-bottom: north star → current
direction → build spec. Everything below the canon is reference or history.

## North star (unchanged)

Headwater tells a data professional **whether their data can answer a business question**,
helps them produce a **trustworthy answer**, and **keeps that verdict honest over time**.
Project = a business goal; data sources are a shared pool; certification is evidence-derived,
per-answer, and **fails closed** ("the badge is sacred"). Full statement:
[HEADWATER2_VISION.md](HEADWATER2_VISION.md).

## Current direction — the Reasoning Engine (canonical, build these)

The engine reframe: a stateful **reasoning graph** (control plane) over a persistent
**knowledge projection** (data plane); every fact has provenance; recompute is **surgical**;
LLM proposes, deterministic/ML verify. Reviewed and corrected (no Kuzu bet; interface-first;
fail-closed certification; safe-aggregate learning parked).

| Doc | Role |
|---|---|
| [HEADWATER_REASONING_ENGINE_PLAN.md](HEADWATER_REASONING_ENGINE_PLAN.md) | **Architecture & roadmap.** The two graphs, KnowledgeProjection abstraction, ontology, fail-closed cert, PII trust boundary, P1–P4 phases with acceptance proofs. Start here. |
| [HEADWATER_REASONING_ENGINE_SPEC.md](HEADWATER_REASONING_ENGINE_SPEC.md) | **Implementation spec.** P1 to the file/function level — package layout, node/runner/ledger, SQLite schema, the goal-aware-question vertical, the readiness fail-closed edits, the 5-PR sequence, the test suite. |

Engine vocabulary never leaks to the UI: users see "what I think this column means / why
this question / what this answer depends on / what would break certification." The five-stage
stepper (Frame → Understand → Resolve → Readiness → Answer & Share) is unchanged — the engine
changes what is *behind* each screen. Stepper/flows: [HEADWATER2_UX.md](HEADWATER2_UX.md).

## Reference (still load-bearing)

| Doc | Why keep |
|---|---|
| [HEADWATER2_UX.md](HEADWATER2_UX.md) | Guided workflow, wireframes, anti-overload rules. The engine builds behind these screens. |
| [HEADWATER2_PERSONAS.md](HEADWATER2_PERSONAS.md) | Who we build for (analyst / analytics engineer / consultant beachhead). |
| [HEADWATER2_AUDIT.md](HEADWATER2_AUDIT.md) | Keep/mine/cut of the H1 spine — the strangler salvage map (connectors, profiler, executor, generator, quality). |
| [HEADWATER2_INSIGHT_PAGE.md](HEADWATER2_INSIGHT_PAGE.md) | Insight-page proposal; informs the P3 insight battery / "what this answer depends on" surface. |
| [h1-capabilities-to-reuse.md](h1-capabilities-to-reuse.md) | H1 inspection powers kept as source-level tools. |
| [H1_REMOVAL.md](H1_REMOVAL.md) | Record of the H1 removal (2026-06-02). |

## Superseded

Archived 2026-06-05 to `../archives/h2_design_2026-06-05/` (see its `ARCHIVED.md`):
`HEADWATER_NEXT_DIRECTION.md` → PLAN; `HEADWATER2_BUILD.md` → SPEC §1 + PLAN roadmap;
`HEADWATER2_REMEDIATION_PLAN.md` → done (S1–S10), engine arc is the successor.

Kept in place but superseded for generation: [HEADWATER2_QUESTION_COMPREHENSION.md](HEADWATER2_QUESTION_COMPREHENSION.md)
— absorbed by PLAN §3.4 + SPEC §8, but its §4 I-3-safe schema brief is still the canonical
reference the SPEC cites.

## Test bed

- `../data/radiology/` — Radiology Workflow Simulation (CC BY 4.0). Naked CSVs; provided
  docs withheld to `../data/_answer_key/` as a grading key (naked-data / design-first rule).
- `../data/media/` — MovieLens (gitignored; no redistribution).

## Status

Implementation on `feat/cdx_revision_hw`. The five-stage workflow, two-factor certification,
and the linear recompute spine exist; the reasoning-engine arc (PLAN/SPEC) replaces the
linear recompute with the surgical node graph and fixes the fail-open certification.
Superseded design generations live in `../archives/`.
