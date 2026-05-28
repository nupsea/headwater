# Headwater 2 — Design Reference

Home for the renewed-architecture / design / UX work. Implementation is gated until
these are vetted (see memory: design-first-workflow).

## Documents

| File | What |
|---|---|
| [HEADWATER2_VISION.md](HEADWATER2_VISION.md) | Product vision — problem-centric pivot, gated 4-stage lifecycle, the readiness "truth" wedge, GTM |
| [HEADWATER2_PERSONAS.md](HEADWATER2_PERSONAS.md) | Persona problems mapped to lifecycle + modules, externally validated |
| [HEADWATER2_AUDIT.md](HEADWATER2_AUDIT.md) | H1 code audit — keep / mine / cut against the H2 model |
| [HEADWATER2_BUILD.md](HEADWATER2_BUILD.md) | Package layout, test bed, staged build plan (gated), information architecture |
| [HEADWATER2_UX.md](HEADWATER2_UX.md) | Guided workflow + UX flows, wireframes, IA, anti-overload rules |
| [h1-capabilities-to-reuse.md](h1-capabilities-to-reuse.md) | Which H1 inspection capabilities to bring forward, and status |
| [prototype/Headwater2_refined.html](prototype/Headwater2_refined.html) | Current clickable prototype — refined readiness workspace (open directly, no build step) |
| [prototype/Headwater-handoff.zip](prototype/Headwater-handoff.zip) | Design handoff bundle. Build from the `hw2-*` source inside it; ignore the `hw-*` / PR / INTEGRATION files (a superseded design generation, copied to `archives/h2_design_2026-05-28/`) |

## Test bed

- `../data/radiology/` — Radiology Workflow Simulation (CC BY 4.0). Naked CSVs; docs
  withheld to `../data/_answer_key/` as a grading key.
- `../data/media/` — MovieLens (gitignored; no redistribution).

## Status

Vision (updated 2026-05-28: continuous certification, evidence-derived per-answer
certification, proposed questions), personas, audit, build plan, UX flows, and the
refined prototype drafted. Iterating the prototype before locking architecture and
starting implementation (S0). Superseded design generations live in
`../archives/h2_design_2026-05-28/`.
