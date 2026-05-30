# Headwater 2 — Design Reference

Home for the renewed-architecture / design / UX work. The design was vetted first
(design-first-workflow); **implementation is now underway** on `feat/cdx_revision_hw` —
see the Status section and the two **Living** docs below for current build state.

## Documents

| File | What |
|---|---|
| [HEADWATER2_VISION.md](HEADWATER2_VISION.md) | Product vision — problem-centric pivot, gated 4-stage lifecycle, the readiness "truth" wedge, GTM |
| [HEADWATER2_PERSONAS.md](HEADWATER2_PERSONAS.md) | Persona problems mapped to lifecycle + modules, externally validated |
| [HEADWATER2_AUDIT.md](HEADWATER2_AUDIT.md) | H1 code audit — keep / mine / cut against the H2 model |
| [HEADWATER2_BUILD.md](HEADWATER2_BUILD.md) | Package layout, test bed, staged build plan (gated), information architecture |
| [HEADWATER2_IMPLEMENTATION_PLAN.md](HEADWATER2_IMPLEMENTATION_PLAN.md) | Execution plan — architecture rules, data model, stages, verification gates, dataset-agnostic safeguards |
| [HEADWATER2_UX.md](HEADWATER2_UX.md) | Guided workflow + UX flows, wireframes, IA, anti-overload rules |
| [HEADWATER2_BUILD_STATE.md](HEADWATER2_BUILD_STATE.md) | **Living.** What is actually built on `feat/cdx_revision_hw` and where it's broken — deep problem analysis + the 2026-05-30 corruption-recovery record |
| [HEADWATER2_REMEDIATION_PLAN.md](HEADWATER2_REMEDIATION_PLAN.md) | **Living.** Connect-and-complete design + step-by-step roadmap (checklist + progress log) + decisions |
| [h1-capabilities-to-reuse.md](h1-capabilities-to-reuse.md) | Which H1 inspection capabilities to bring forward, and status |
| [LEGACY_DOCS_REMOVED.md](LEGACY_DOCS_REMOVED.md) | Record of the H1/legacy docs removed 2026-05-30 and where their value now lives |
| [prototype/Headwater-handoff_update.zip](prototype/Headwater-handoff_update.zip) | Design handoff bundle (latest). Build from the `hw2-*` source inside it. The older `Headwater-handoff.zip` and the static `Headwater2_refined.html` were removed once implementation began. |

## Test bed

- `../data/radiology/` — Radiology Workflow Simulation (CC BY 4.0). Naked CSVs; docs
  withheld to `../data/_answer_key/` as a grading key.
- `../data/media/` — MovieLens (gitignored; no redistribution).

## Status

Design (vision, personas, audit, build plan, UX) drafted and the refined prototype iterated.
**Implementation is underway on branch `feat/cdx_revision_hw`** — the five-stage workflow,
two-factor certification, and recompute spine are built but not yet coherent end to end. The
current build is tracked in **HEADWATER2_BUILD_STATE.md**; the connect-and-complete work in
**HEADWATER2_REMEDIATION_PLAN.md** (start there for live status). Superseded design
generations live in `../archives/h2_design_2026-05-28/`.

> 2026-05-30: recovered 27 NUL-corrupted source files (filesystem event; restored from git
> HEAD, verified). Details in HEADWATER2_BUILD_STATE.md §0.
