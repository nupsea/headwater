# Legacy docs removed — 2026-05-30

H1-era plans and low-value docs were removed to keep the tree clean and stop stale plans
from misleading future work. This is the compressed record of what was discarded and where
its value now lives. Authoritative current docs: `design/HEADWATER2_*` (vision, audit, plan,
UX, build-state, remediation).

The `archives/` items were **gitignored** (not in git history) — gone from disk, captured
here only by title. The `headwater/` items were **tracked** and remain recoverable from git
history at commit `0cce1a2` (pre-cleanup).

## archives/ (gitignored — disk-only, now deleted)
- `IDEA.md` (38K) — Auto Data Manager (ADM) original idea
- `guide.md` (53K) — ADM Adoption Guide
- `plan.md` (43K) — ADM Implementation Plan
- `review.md` (17K) — ADM Architecture Review
- `expert_review.md` (9K) — Expert CTO Review (v2.0)
- `expert_review2.md` (8K) — Brutal Expert Critique
- `IMPLEMENTATION_PLAN_legacy_2026-04-28.md` (121K) — Headwater Strategic Implementation Plan
- `v2_plan.md` (41K) — Headwater v2: Semantic Intelligence & UX Overhaul → distilled in memory + `HEADWATER2_VISION.md`
- `v3_plan.md` (36K) — V3: Guided Advisory Experience → distilled in memory + `HEADWATER2_VISION.md`

## headwater/ (tracked — recoverable from git history @ 0cce1a2)
- `archives/IMPLEMENTATION_PLAN_2026-04-30_pre_docs_reset.md` (37K) — pre-docs-reset H1 plan
- `docs/ARCHITECTURE.md` (9K) — H1 architecture (context-suite era; superseded by `.claude/rules/architecture.md`)
- `docs/insights_cto_directive.md` (6K) — H1 domain-aware insight directive
- `docs/insights_ds_review_tlc.md` (4K) — H1 NYC-TLC insight review
- `press_release.md` (10K) — H1 working-backwards press release → superseded by `HEADWATER2_VISION.md`
- `rfp.md` (21K) — H1 Refactor RFP → superseded by `HEADWATER2_VISION.md` + `HEADWATER2_IMPLEMENTATION_PLAN.md`

Kept on purpose: both `README.md` files (repo + package), and `archives/h2_design_2026-05-28/`
(the immediate H2 predecessor snapshot that the live design docs still reference).
