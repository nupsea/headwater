# Headwater 2 — Build State & Deep Problem Analysis

Status: Living. Companion to `HEADWATER2_VISION.md` (the north star), `HEADWATER2_AUDIT.md`
(H1 keep/mine/cut), and `HEADWATER2_IMPLEMENTATION_PLAN.md` (original build plan). Those
describe what H2 *should* be; **this describes what is actually built on branch
`feat/cdx_revision_hw` and where it is broken.** Evidence cites `path:line` relative to
repo root (Python under `headwater/headwater/`, UI under `headwater/ui/`).

Headline: **the architecture matches the vision and most pieces exist; the failure is
coherence and half-wiring, not absence.** H2 has not "moved backwards from H1" — it is an
almost-complete skeleton whose joints don't connect into one trustworthy loop. The fix is
**connect-and-complete, not rebuild.**

---

## 0. Recovery incident — 2026-05-30 (resolved)

During analysis, `services/h2_project_relevance.py` was found to be 1,167 lines of **NUL
bytes** yet still importing — Python was running off a stale `.pyc` (May 29). A full scan
found **27 NUL-corrupted source files** (most of `analyzer/`, much of H2 `services/`, a few
UI files). All 27 were **clean in git HEAD** and **none overlapped the in-progress edits**
(the session-start modified/untracked set). Recovery: `git checkout HEAD -- <27 files>`,
cleared `__pycache__`/`.ruff_cache`/`.pytest_cache`. **Verified: 0 corrupted files remain;
H2 + core test suites pass.**

Signature (data blocks zeroed, inode mtimes preserved while the newer `.pyc` survived)
points to a **filesystem/power event**, not a bad edit. It can recur — commit often, watch
disk health. This corruption is the likely hidden cause of much "it's clumsy / reverts /
behaves oddly" pain: a quarter of the backend was zeroed and only alive as cached bytecode.

Known separate issue (out of H2 scope): ~280 failing tests confined to the **H1 `insights`
suite** (KeyErrors on scoring keys). Pre-existing on this branch; not touched.

---

## 1. What actually exists (inventory)

- **Two-factor certification** is real and well-structured: statistical readiness contracts
  + LLM judge, neither weakening the other, holds at `doubtful` when the judge is
  unavailable. `services/h2_pipeline.py:92-181` (`finalize_project_answers`), `_final_state`
  `:350-358`. Matches VISION §"Certification: evidence-derived, per-answer".
- **Recompute spine with input fingerprinting**: `project_input_fingerprint`
  (`h2_pipeline.py:466-522`) hashes scope + column metadata + semantic claims + resolve
  dispositions; `get_project_state` (`:534-547`) drives a staleness banner; `recompute_project`
  (`:550-578`).
- **Fast path vs. certify split** (matches the remediation decision to keep the slow local
  LLM step user-triggered): `POST /answer` drafts+executes, `run_judge=False`
  (`api/routes/h2.py:606-624`); `POST /answer/certify` runs the judge (`:627-643`).
- **Answer UI is strong**: certified/doubtful/pending/cannot pills, two-factor judge panel,
  chart (recharts) + data table + SQL, audit-report export (`ui/.../answer/page.tsx`).
- **Readiness UI**: per-question verdicts, contracts, data-model diagram, can't-answer hero
  (`ui/.../readiness/page.tsx`).
- **Inputs backend complete but unused by workflow UI**: `GET/POST /projects/{id}/resources`
  (`api/routes/h2.py:728-776`), service `services/h2_resource.py`.
- **Schema/meaning editing, relationships, generated descriptions**: `api/routes/h2.py:148-260`,
  `SchemaEditor`, `DataModelDiagram`.

## 2. Root causes (the two that matter)

### Root cause 1 — No "project home," and inputs are a dead end (breaks VISION Stage 1 "Frame")
- `/h2/projects/[id]` **silently redirects to `understand`** (`ui/.../projects/[id]/page.tsx:11-13`).
  No surface shows, in one place: the goal, the inputs/resources considered, the current verdict.
- **Resources can only be added on the *new project* page** (`ui/.../projects/new/page.tsx:597-678`,
  ingested once at creation `:146-158`). After creation there is **no UI to view or add inputs**,
  though the backend fully supports it. For an existing project the fed inputs are **invisible**
  and **cannot be extended**.
- The **"Frame" stepper step routes an existing project to `/projects/new`** (`ui/.../h2/layout.tsx:772`)
  — i.e. tries to create a *new* project, losing context.

### Root cause 2 — "Refresh from the beginning" is only a partial refresh (breaks VISION §"a loop, not a line")
- `recompute_project` → `finalize_project_answers` re-runs **draft → readiness → execute →
  (judge)** but **never re-runs `propose_relevance`** (relevant columns + proposed questions).
  See `h2_pipeline.py:128-133`. So correcting a column meaning or adding a data dictionary
  **does not regenerate which questions are answerable or which columns are relevant** — the
  earliest logical steps are skipped.
- The fingerprint *does* include the inputs that drive relevance, so staleness is detected
  correctly; only the recompute that follows is incomplete.
- The recompute banner does a full **`window.location.reload()`** (`layout.tsx:677`) — heavy.
- `notifyInputChanged()` (`h2api.ts:340-344`) is wired in some paths but **not guaranteed on
  every input path** (resource ingest, some schema edits) — needs an audit.

## 3. Specific half-wiring / bugs (secondary to the root causes)

1. **Readiness "Why not" is duplicated boilerplate** — `CannotAnswerBlock` renders `q.summary`
   as both heading and reason (`readiness/page.tsx:294, :304`); closing guidance is generic,
   not "add column X / confirm meaning of Y." VISION Stage 4 demands the honest, *specific*
   negative as a hero state.
2. **Answer SQL "Run" is fake** — `SqlCard` Run is a `setTimeout` that does nothing
   (`answer/page.tsx:237-240`); a real `POST /h2/query` exists.
3. **Question curation feeds nothing** — kept/curated set is local React state only
   (`understand/page.tsx:482, 536-552`); never persisted. VISION Stage 1 says "keep, edit,
   add, drop" must matter.
4. **`understand` re-runs relevance on every mount** (`:554-558`), overwriting persisted state.
5. **Answer page auto-executes SQL on mount** (`:787-792`).
6. **Layout readout uses `q.status === "certified"`** (`layout.tsx:40-43`) while verdicts live
   in answer artifacts — rail badge can disagree with readiness/answer pages.

## 4. Why it *feels* like a regression (synthesis)

The user walks Frame → Understand → Resolve → Readiness → Answer, but the goal/inputs have no
home, added context can't be fed after creation, and when something *is* changed the refresh
doesn't reach the early stages — so the workflow appears not to listen. With cosmetic-only
curation and a fake Run button on top, the whole loop reads as half-responsive. Connect the
loop and complete the refresh; do not rebuild.

→ Remediation design, roadmap, and decisions: `HEADWATER2_REMEDIATION_PLAN.md`.
