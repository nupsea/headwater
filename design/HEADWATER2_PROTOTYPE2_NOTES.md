# Headwater 2 — Prototype 2 UX Notes

Status: companion note for `prototype/headwater2-prototype2.html`.

## What changed from prototype 1

Prototype 1 proved the Apple-like shell and the high-level areas: Overview, Explore,
Workflow, Deliver. Prototype 2 makes the product sharper: Headwater is a **readiness
workspace for one business question**, not a collection of data tools.

The main object on screen is now the question plus its live trust contract:

1. **Compass** — frame the decision, metric, sources, and next best action.
2. **Source Lab** — catalog, profile, metadata locks, and SQL scratchpad in one
   inspection surface.
3. **Resolve** — impact-ranked questions only; every unknown becomes either locked
   truth or an explicit gap.
4. **Readiness** — the defendable truth ledger: have, trust, risk, gap, misleading.
5. **Answer Pack** — query, chart, audit report, and draft/certified stamp.

## H1 gaps addressed

- H1 spreads the user across `Health`, `Sources`, `Discovery`, `Models`, `Quality`,
  `Insights`, `Explore`, and `Data`. Prototype 2 collapses those into one loop.
- Review queues in H1 are operational counts. Prototype 2 ranks ambiguity by business
  impact on the answer.
- H1 has useful source/catalog/query tools, but they compete with the workflow.
  Prototype 2 makes them support surfaces inside Source Lab.
- H1 maturity is source/model oriented. Prototype 2 makes readiness goal-scoped: the
  same source can be ready for registration wait and unready for modality utilization.
- H1 can produce insights and questions, but the user must infer whether the answer is
  safe. Prototype 2 keeps the trust badge and evidence drawer visible throughout.

## Research cues folded in

- Apple HIG: clarity and deference drove the restrained shell, persistent context, and
  progressive disclosure.
- SageMaker Unified Studio: query/editor/catalog work belongs inside the active project
  instead of as a separate detached tool.
- Databricks Catalog Explorer: source browsing, object search, and query handoff are
  expected data-app ergonomics.

## Product stance

The prototype deliberately avoids building a BI tool. The chart and dashboard are the
payoff of the readiness verdict. Uncertified outputs are allowed, but they cannot look
certified. This preserves the wedge: Headwater's value is the defensible truth behind
the answer.
