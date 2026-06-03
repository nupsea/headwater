# Headwater 2 — Remediation Plan (live)

Status: **Living — update at the start and end of every step.** Companion to
`HEADWATER2_BUILD_STATE.md` (the diagnosis this fixes) and `HEADWATER2_VISION.md` (the north
star every change must serve). Approach, per the build-state analysis:
**connect-and-complete, not rebuild.** Visual language stays as-is (warm paper, DM Sans).

## Working agreement (why this doc exists)

The user is burned by iterations where intent/vision die between rounds and context is lost.
This doc + `HEADWATER2_BUILD_STATE.md` are the durable memory. Rules: vision is sacred; step
by step; write the doc before building, update status after; evidence not vibes (cite
file:line / tests).

---

## Design: one project, one loop, one state

A project is a single living loop. Every stage reads/writes the same derived state; any input
change re-runs that state from the start. Three structural moves (fixing the two root causes
+ the half-wiring) in `HEADWATER2_BUILD_STATE.md`.

### Move A — Project Home + first-class Inputs (fixes Root cause 1; VISION Stage 1)
A project home at `/h2/projects/[id]` (stop the blind redirect) showing: **goal** (editable,
re-proposes on change); **inputs considered** (persistent list of every resource fed — data
dictionary, .md/.txt, pasted notes — with format, time, lock state, claims touched); **add
input** any time via `POST /projects/{id}/resources` (each add fires the refresh); **current
verdict** snapshot. Fix the stepper "Frame" target to this page. Inputs surface is a reusable
component (also usable contextually on Resolve/Readiness gaps).

### Move B — Complete the refresh spine (fixes Root cause 2; VISION "a loop, not a line")
One recompute that runs the full fast pipeline **in order**: relevance (relevant columns +
proposed questions) → readiness → draft SQL → execute → persist — judge/certification excluded
(stays explicit). Move the recompute entry point earlier to include `propose_relevance`. Every
input mutation calls `notifyInputChanged()` and flips the fingerprint (audit all write paths).
Recompute banner re-fetches state and re-renders instead of `window.location.reload()`. One
source of truth for the per-project readout (rail/banner/home read persisted verdicts).

### Move C — Make the half-wired pieces real
Persist question curation (keep/drop/add matters downstream); real SQL Run via `POST /h2/query`;
specific readiness reasons + guidance (failing contracts + concrete resolution path, reusing the
gap-card text); stop redundant auto-runs (load persisted state, recompute on demand/stale).

### Move D — LLM-assisted discovery enrichment (advisory, human-verified)
In the first few steps (Frame/Understand), the LLM proactively *proposes* discovery
metadata for human verify-and-lock — never silently applied, never in the fast-refresh path
(see the LLM-roles decision below). Concretely: keep heuristic profiler PK/FK/RI detection
as the base layer; add explicit LLM actions that **(a)** generate column descriptions
(exists), **(b)** infer table **relationships** and **business / composite keys** with a
short rationale, and **(c)** propose semantic types. Each proposal renders in the schema /
data-model surfaces with confidence + reason and a confirm / edit / lock control. Locking a
proposal is an input change → flips the fingerprint → the fast loop (relevance → questions →
readiness → answers) re-runs on verified ground truth. This is VISION Stage 2 ("here's what
this data is", made legible) operationalized, and keeps domain knowledge out of code (it
comes from the data's own names/stats + user inputs, per No-Domain-Hardcoding).

### Invariants (must not violate)
No raw rows to LLM (I-3); certification = facts not clicks, auto-revokes on drift; local LLM
optional, degrade to "judge unavailable"; no domain hardcoding (context via inputs /
`data/<domain>/`); SQLite metadata, DuckDB analytical, Polars, Arrow, `uv`; gate ruff → pytest
→ pyright.

### Out of scope (now)
PDF parsing (accept file, defer parse); Postgres migration; multi-source beyond existing;
the v3 NL query harness; and the pre-existing H1 `insights` test failures.

---

## Roadmap (checklist + progress log)

Status: `[ ]` todo · `[~]` in progress · `[x]` done · `[!]` blocked.

### Phase 1 — The loop listens (root causes)
- [x] **S1. Complete the refresh spine (backend).** DONE 2026-05-30. `recompute_project` now
      runs `propose_relevance` first → relevance → questions → readiness → draft → execute
      (LLM-free; judge stays separate). Also added the **project goal** to
      `project_input_fingerprint` (it was missing — a goal edit now correctly flips staleness).
      Tests: `test_recompute_reruns_relevance_from_the_beginning` (clear questions → only a
      recompute resurrects them; a draft-only finalize does not) +
      `test_recompute_reflects_an_input_change_in_questions`. Files: `services/h2_pipeline.py`,
      `tests/test_h2_pipeline.py`. Gate: ruff clean; 294 H2+core tests pass, 0 regressions.
- [x] **S2. Project Home + persistent Inputs surface (UI).** DONE 2026-05-30. `/h2/projects/[id]`
      is now a real Frame home (editable goal that re-proposes on save, the inputs considered,
      add-input any time, scope + proposal snapshot, Continue→Understand) — no more blind
      redirect. New reusable `InputsPanel` (lists resources, paste/.md/.txt add, fires
      `notifyInputChanged`). Stepper "Frame" + rail + `stageFromPath` now target the home, not
      `/projects/new`. Backend: enriched the resource registry to record what each input
      *touched* (claims created/updated, conflicts, sensitivity) so the surface is meaningful.
      Files: `ui/.../projects/[id]/page.tsx`, new `components/h2/inputs-panel.tsx`,
      `ui/.../h2/layout.tsx`, `components/h2/stepper.tsx`, `ui/src/lib/h2api.ts`,
      `services/h2_resource.py`. Gate: ruff clean, tsc clean, 294 H2+core tests pass.
- [x] **S3. Wire every input → complete refresh.** Audit write paths; each calls
      `notifyInputChanged()`; banner triggers the complete recompute and re-fetches (no full
      page reload). Files: `ui/.../h2/layout.tsx`, stage pages, `h2api.ts`.

**Phase 1 done =** feed an input anywhere → relevant columns, proposed questions, readiness,
answers all visibly update; inputs visible and extensible; nothing is a dead end.

### Phase 2 — Make the pieces real
- [x] **S4. Specific readiness reasons + guidance** DONE 2026-05-31. Fixed the duplicated
      "Why not" (the report had no separate title, so `summary` was printed twice). Backend now
      surfaces `title` + `needed_columns` on each readiness question; the page shows the title
      as heading, the summary once as the reason, the actual failing-contract notes, a concrete
      resolution path per failing contract type (`resolutionPaths()`), and the needed-column
      chips. Draft questions get an inline "To certify: …" hint. Files: `services/h2_readiness.py`,
      `api/routes/h2.py`, `ui/src/lib/h2api.ts`, `readiness/page.tsx`. Gate: ruff + tsc clean,
      49 backend tests pass.
      Plus (test-driven, same session): Understand page — Schema & meaning toggle made prominent
      (blue card, + badge, bold heading, "Review & edit"); EDA reworked into a family-overview
      chip strip + grouped, importance-sorted list capped at 10 with show-more.
- [x] **S5. Persist question curation** DONE 2026-05-31. ruff + tsc clean; 294 backend tests
      pass. Keep/drop in Understand now persists and is honored everywhere (readiness, answers,
      counts) and survives recompute. Implementation as designed below. Persist a drop via
      question `status="dropped"`; preserve it across re-propose by changing
      `store.upsert_question` ON CONFLICT to
      `status=CASE WHEN questions.status='dropped' THEN 'dropped' ELSE excluded.status END`.
      Steps: (1) edit that upsert SQL (`core/store.py`); (2) add `store.set_question_status`;
      (3) new route `POST /projects/{id}/questions/{qid}/disposition {dropped:bool}` → set
      status "dropped" or back to draft/cannot_answer per answerability; (4) filter
      `status!="dropped"` in `h2_readiness.evaluate_project_readiness` (questions = list_questions),
      `h2_pipeline.finalize_project_answers` (`for question in store.list_questions(...)`), and
      `h2_answer.draft_project_answers` (questions = list_questions) — all three call sites
      confirmed present; (5) `h2api.ts` add `questions.setDisposition`; (6) `understand/page.tsx`
      init `kept` from `status!=="dropped"` and toggle calls the endpoint + `notifyInputChanged()`.
      Defer "add custom question" (note). get_project already returns questions with status.
- [x] **S-AI-Resolve (user-requested 2026-05-31). "Ask AI" on a Resolve card.** DONE — ruff +
      tsc clean, 20+ backend tests pass. UI: "✦ Ask AI to draft this" button in the resolve
      card's add-context area calls the suggest endpoint and prefills the textarea (user edits +
      Saves → ingest → refresh; never auto-applied). Backend:
      `h2_enrich.suggest_resolution(store, project_id, card_id)` (I-3-safe: uses
      card payload table/column + known codes/top_values; JSON `{markdown}`; degrades when no
      model), route `POST /projects/{id}/resolve/{card_id}/suggest`, and `h2api`
      `resolve.suggest`. UI PENDING: add an "✦ Ask AI" button in the resolve card's add-context
      area (anchor near the `addContext` handler / the "Save context" button) that calls
      `h2.projects.resolve.suggest(projectId, card.card_id)` and sets `ctx` to the returned
      markdown (then existing Save → ingest → refresh). Deferred this turn because the session's
      file-read channel stalled and the bracketed resolve path has corrupted under blind edits
      before — finish when reads are reliable. Remaining edit is UI-only in `resolve/page.tsx`.
      [original spec below]
- [ ] **S-AI-Resolve (user-requested 2026-05-31). "Ask AI" on a Resolve card.** Add an
      "✦ Ask AI" button in the resolve card's add-context area that asks the local LLM to DRAFT
      the resolution (e.g. for `enum_mapping_needed` on `cases.patient_type`, propose meanings
      for the known codes A/H/S/D as a `code | meaning` markdown table) and prefills the
      textarea for the user to edit + Save (existing ingest → refresh flow). Move-D aligned:
      LLM proposes, human verifies/saves; never auto-applied. I-3-safe (send column
      name/type + known distinct values/top-N from the bootstrap enum claim/profile — allowed
      by the summary contract — never raw rows). Backend: new
      `POST /projects/{id}/resolve/{card_id}/suggest` → {available, markdown}; reuse
      `h2_enrich` LLM helper + degrade gracefully when no model. Files: `services/h2_resolve.py`
      or `h2_enrich.py`, `api/routes/h2.py`, `ui/.../resolve/page.tsx`, `h2api.ts`.
- [x] **S6. Real SQL Run in Answer** DONE 2026-05-31. Replaced the fake `setTimeout` Run with a
      real `h2.query(sourceName, sql)` (POST /h2/query, read-only sandboxed); SqlCard now takes
      `sourceName`, runs the (optionally edited) SQL, and renders a result table (or the error /
      no-rows), with row count + truncation note. Answer page fetches the project's source_name
      for this. tsc clean. File: `answer/page.tsx`.
- [x] **S7. Stop redundant auto-runs** (`understand/page.tsx`, `answer/page.tsx`).
- [x] **S8. One readout source of truth** (`ui/.../h2/layout.tsx`, `h2api.ts`).
- [x] **S-LLM. LLM-assisted discovery enrichment for human verification** (Move D). Net-new
      besides existing descriptions/goal: explicit endpoints + UI to (a) infer table
      **relationships** and (b) identify **business / composite keys**, each with a rationale
      and confidence, rendered in the schema / data-model surfaces with confirm/edit/lock;
      verified+locked results feed the fast refresh. I-3-safe; degrades when no model. Wire
      the existing `generate-descriptions` into the same verify/lock flow. Files:
      `services/h2_enrich.py` (+ relationship/key inference), `analyzer/`, `api/routes/h2.py`,
      `ui/.../understand/page.tsx`, `components/h2/{schema-editor,data-model}.tsx`, `h2api.ts`.
      Gate: ruff → pytest.

### Phase 3 — Validate against the vision
- [x] **S9. Naked-data walkthrough** on `data/radiology/` (withhold `data/_answer_key/`): run the
      loop, paste the dictionary, confirm every stage updates and certified answers show correct
      data + viz. Capture findings here.
- [x] **S10. Quality gate + sign-off** — ruff → pytest → pyright clean; mark VISION acceptance met.

### Progress log
- 2026-05-30 — Recovered 27 NUL-corrupted files (see BUILD_STATE §0). Wrote BUILD_STATE +
  this plan into `design/`. Foundation captured.
- 2026-05-30 — Cleaned up H1/legacy docs (see `LEGACY_DOCS_REMOVED.md`): removed the H1
  archives, H1 `docs/`, press_release/rfp, and superseded prototypes; fixed all dangling
  links; re-verified (0 corruption, no dangling refs, H2 tests green). Deletions staged for
  your commit.
- 2026-05-30 — Decisions confirmed: staged refresh (judge = explicit click) ADOPTED; LLM
  roles ADOPTED — LLM proposes discovery metadata (descriptions, relationships, business
  keys, semantic types) in the first few steps for human verify/lock, never in the fast
  refresh, never auto-certify. Added Move D + roadmap S-LLM.
- 2026-05-30 — **S1 done.** Recompute now refreshes from the beginning (relevance →
  questions → readiness → draft → execute), LLM-free; project goal added to the input
  fingerprint. 2 new regression tests; 294 H2+core tests pass, 0 regressions.
- 2026-05-30 — **S2 done.** Project home replaces the blind redirect: editable goal, the
  persistent Inputs surface (new reusable `InputsPanel` — view + add data dictionary / .md /
  .txt any time, each add fires the refresh), scope + proposal snapshot. Stepper "Frame" and
  rail now open the home, not the new-project form. Resource registry enriched to show what
  each input touched. ruff + tsc clean; 294 tests pass.
- 2026-05-30 — **S3 done.** Every input write-path signals a change; the recompute banner now
  refreshes in place (new HW2_RECOMPUTED event; no full page reload) and all five stage views
  + the layout re-fetch on recompute. Phase 1 (the loop listens) is complete: feed an input
  anywhere → the whole workflow updates with no reload. tsc clean; UI integrity verified.
  Next: Phase 2 — S4 (specific readiness reasons + guidance).
- 2026-05-31 — **S4, S5, S6 done + Understand UX + Ask-AI on Resolve.** Committed:
  f138871 (S1,S2), a189985 (S3,S4,Understand schema/EDA), d756744 (S5 question curation),
  089ca61 (Ask-AI suggest endpoint + editor button), 2c41e22 (S6 real SQL Run), de356df
  (card-level Ask AI button). Decision: Ask-AI stays one-shot "draft for review" (Move-D);
  conversational/context-holding chat = deferred v3 LLM query harness, not pulled forward.
- 2026-05-31 — **Resolve save-flow polish (in progress).** User feedback: after Save context
  it must (a) confirm saved, (b) collapse + show the saved value read-only with an Edit-again
  option, (c) not show a duplicate Ask AI button (card-level + old editor-level). Fixing now.
  Remaining roadmap: S7 (stop redundant auto-runs), S8 (one readout source of truth),
  S9 (naked-data walkthrough), S10 (final gate). Tooling note: never `cd` into the bracketed
  `[id]` path (zsh glob aborts the whole batch); use absolute paths / subshells.

---

## Decisions

- **2026-05-30 — `design/` is the durable workspace.** This plan + BUILD_STATE are the living
  layer alongside the pre-build VISION/AUDIT/PLAN. Adopted.
- **2026-05-30 — Connect-and-complete, not rebuild.** Adopted (rationale in BUILD_STATE).
- **2026-05-30 — Sequencing: root-cause first, step by step.** User directive. Adopted.
- **2026-05-30 — Refresh model: staged. ADOPTED (user-confirmed).** Any input change
  auto-runs the *complete fast* recompute (relevance → questions → readiness → draft →
  execute, real data shown); the slow **LLM judge certification stays a separate explicit
  click.** Keeps the local model's slow step user-controlled.

- **2026-05-30 — LLM roles & timing. ADOPTED (user-directed).** The LLM is used in TWO
  advisory ways. Both are **explicit/user-triggered** (the local model is slow), both are
  **surfaced for human verification**, and both are **I-3-safe** (names/types/stats only,
  never raw rows). The LLM **never auto-applies** metadata and **never auto-certifies**.
  1. **Early discovery enrichment (Frame/Understand — "the first few steps").** The LLM
     proposes, for human verify-and-lock: column **descriptions** (exists,
     `h2_enrich.generate_descriptions`), an analysis **goal** (exists, `suggest_goal`),
     table **relationships**, **business / composite keys**, and semantic types. Heuristic
     profiler PK/FK/RI detection stands first; the LLM augments and explains it; the human
     confirms/edits/locks. *(Relationship + business-key inference is NET-NEW — see roadmap
     S-LLM.)*
  2. **Certification judge (Answer).** Explicit click, unchanged.
  **Boundary:** the *fast refresh never calls the LLM.* It consumes whatever the human has
  verified/locked. LLM enrichment is its own explicit action → proposals → human verify/lock
  → that locked metadata becomes a stable input the fast refresh reads. So editing meanings
  or accepting an LLM-proposed relationship flips the fingerprint and the fast loop updates,
  with no LLM in the recompute path.
- **Inputs from domain folder — PROPOSED:** optionally bind `data/<domain>/` and ingest its
  docs as inputs (keeps domain context out of code), additive to ad-hoc paste/file. *Confirm
  before S2.*

## S-BIND — Resolve card context binding (2026-05-31)

**User-found bug:** providing context on the `cases.patient_type` enum card, saving, and
recomputing left readiness failing `no_blocking_gaps`.

**Fix shipped (commit after f4c6018):** readiness gap-clearing is now *evidence-derived* —
`h2_readiness` subtracts columns with a satisfying semantic claim (filled enum mapping /
non-empty definition / locked) from the high-priority-open set via new
`_columns_with_satisfying_claim`. So defining a column (e.g. via Understand → Schema &
meaning, which binds a proper per-column claim) clears the gap on recompute. 6 unit tests
in `tests/test_h2_readiness_gap_clearing.py`. ruff clean; 70 tests pass.

**Still queued (S-BIND remainder):** the resolve card's free-text box writes an *unbound*
`code|meaning` table (`column_ref=""` in `_interpret_table`), so saving there alone does
not create a column claim. Plan: a `POST /projects/{id}/resolve/{card_id}/define` that binds
the markdown/codes directly to the card's payload {table,column}. **Interim path that works
today: Understand → Schema & meaning.**

**Tooling note (2026-05-31):** this session's tool-output channel was unstable — batches
cancelled mid-flight and reads (sed/grep/Read) intermittently duplicated/garbled lines.
Mitigations that held: `inspect.getsource` for reads, Edit (exact-match) + python byte-count
assertions for writes, git for state, append-only for this doc. Do not trust raw previews;
verify with deterministic checks.
- 2026-06-02 — **S8 done (real bug) + S7 done.** S8: the rail/banner counted
  `question.status==='certified'`, which finalize never sets — so the readout was stuck at
  0/0. Now `store.project_verdict_summary` counts certified from persisted `readiness_verdicts`
  (the one true source); attached to GET /projects + /projects/{id}; `computeProjectReadout`
  reads it. S7: the Answer page re-ran the full pipeline (materialize+execute) on every mount;
  added a session `ANSWER_CACHE` so re-navigation reuses the payload (recompute/Redraft/certify
  refresh it). S3 confirmed already done (no window.location.reload; HW2_RECOMPUTED in place).
  ruff+tsc+eslint clean; 519 backend tests pass. Remaining: S-LLM, S9, S10.
- 2026-06-03 — **S-LLM done (Move D).** LLM relationship + business/composite-key inference,
  advisory + human-verified + I-3-safe + degrade-gracefully. Backend: `h2_enrich.
  suggest_relationships` / `suggest_keys` (names/dtypes/uniqueness only; proposals validated
  against real columns), endpoints `POST /sources/{name}/{suggest-relationships,suggest-keys,
  relationships,keys}` (confirm persists a relationship / locks key columns as identifiers →
  flips the fingerprint → fast loop re-runs on verified ground truth). UI: `components/h2/
  ai-suggestions.tsx` (✦ Suggest relationships / keys, proposals with rationale + confidence +
  Confirm/Lock) mounted in Understand → Schema & meaning. Existing generate-descriptions/goal
  already verify/lock via the schema editor. 4 inference tests (stub provider). 523 backend
  tests pass; ruff/tsc/eslint clean; UI build clean. Remaining: S9 (naked-data walkthrough),
  S10 (gate: pyright).
- 2026-06-03 — **S9 + S10 done — roadmap complete.** S9 (naked-data walkthrough, radiology):
  drove rad_bootstrap_demo through the loop — each answerable question returns real data
  (8/4/20 rows), a chart (line/bar), and a plain-English finding ("Total duration fell 57%…",
  "Ambulance walk-in has the highest total duration…"); week-over-week correctly cannot_answer.
  Data + viz + findings confirmed; certification is the explicit graceful-degrade judge step.
  S10 (quality gate): ruff clean, 523 pytest pass (4 skipped), **pyright 0 errors** — fixed the
  43 it found: the dangling `cli.main` import in `__main__.py` (H1-removal regression), H2
  type cleanups (h2_pipeline sorted keys/exec_items, h2_source row_count), the anthropic-SDK
  block union in `analyzer/llm.py`, and the pre-existing spine typing debt in
  quality/checker, executor/duckdb_backend, profiler/{key_detection,stats,schema,
  relationships}, connectors/{postgres,redshift,snowflake} (safe guards/casts, no behavior
  change). All Phase 1–3 items done.
