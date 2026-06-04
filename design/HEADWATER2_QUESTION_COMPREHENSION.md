# Goal-Aware Question Generation — Design (for review)

Status: PROPOSED — not yet built. Awaiting sign-off.

## 1. The defect this fixes

Three projects on the same `radiology` source, three different goals, near-identical
questions:

| Goal | Questions generated |
|---|---|
| "registration workflow bottlenecks and wait time" | duration by patient type; exam count over time; highest exam count |
| "Analyse efficiency" | duration over time; highest duration; workflow step |
| "where delays occur **in hours** for different radiology visits" | duration by patient type; exam count week-over-week; exam count over time; highest exam count |

The goal does not shape the questions. Root cause: `_build_question_proposals`
(`services/h2_project_relevance.py`) is **pure heuristic templates** — it finds "a time
column, a numeric measure, a category, an entity" by name/role hints and fills four
fixed sentences. The goal text only nudges *which* column is favored. **No step ever
shows the goal to a model and asks "what should we ask?"** The LLM is used elsewhere
(descriptions, relationship/key hints, the judge, Resolve suggestions) but never here.

So the system does not comprehend the goal. It pattern-fills. That is the core gap.

## 2. Principle — where the LLM belongs

Split the two things that are currently conflated:

- **Comprehension (LLM):** read the goal + the real schema and decide *what to ask* —
  which columns, what analytical intent, whether a join is needed. This is the analyst's
  judgment, and it is exactly what an LLM is good at.
- **SQL generation (deterministic, unchanged):** turn the chosen columns + intent into
  safe SQL via the existing templates (`_build_sql_and_chart`, including the join path).

This keeps SQL injection-free and stats-grounded (no LLM-authored SQL) while making the
*questions* genuinely goal-driven. It honors the project invariants:
- **I-3:** the model sees only names, types, roles, stats, top-N distinct values, and
  relationships — never raw rows.
- **Advisory-only:** proposals remain proposals the analyst keeps/drops/edits.
- **No domain hardcoding:** nothing about radiology/registration in code; the model
  infers from the goal and the data's own names/stats.

## 3. Data flow

```
goal (statement, decision, target_metric, entities, time_horizon)
  + schema brief (I-3-safe)  +  relationships
        │
        ▼
  propose_goal_questions()  ── LLM ──▶  JSON: [{title, intent, columns[], reason}]
        │
        ▼
  validate & repair  (drop hallucinated columns / unsafe joins / bad intents)
        │
        ▼
  persist as questions  (needed_columns, col_roles, question_type, title, reason, source="llm")
        │
        ▼
  EXISTING pipeline unchanged:
  draft SQL (templates, incl. join) → execute → finding → readiness + judge → confidence
```

Heuristic generator stays as the **fallback** (no model / invalid output / 0 survivors).

## 4. The schema brief (exact inputs, I-3-safe)

Built from the project's selected tables only:

- **Per column:** `table`, `column`, `dtype`, `semantic_role`, `distinct_count`,
  `null_fraction`, `min`/`max`/`mean` (numeric/temporal), and up to **6** top distinct
  values for low-cardinality categoricals (already-surfaced metadata; allowed by I-3).
- **Relationships** between selected tables: `from`, `to` columns, `confidence`.
- **Coverage window:** min/max of the primary time column (so "in hours / per visit /
  week-over-week" framing is grounded).
- **Goal block:** statement + any of decision / target_metric / entities / time_horizon.

Explicitly excluded: raw rows, full value lists, anything PII-bearing.

## 5. Prompt design

**System:** "You are a senior data analyst. Given a GOAL and a DATA SCHEMA with stats,
propose precise analytical questions that DIRECTLY serve the goal. Each must be
answerable from the listed columns ONLY. Reflect the goal's specific intent
(where / when / which / how much / trend). Use a JOIN across related tables when the goal
needs a measure from one table grouped by a dimension in another. NEVER invent columns
or codes. Respond as strict JSON only."

**User:** GOAL block, SCHEMA block (compact one-line-per-column), RELATIONSHIPS block,
then the output contract.

**Output contract** (JSON array, 3–6 items):
```json
{
  "title": "Which workflow step has the longest delay, in hours?",
  "intent": "ranking",                       // temporal | segment | ranking | coverage
  "columns": [
    {"ref": "events.activity", "role": "categorical"},
    {"ref": "events.total_duration", "role": "measure"}
  ],
  "reason": "The goal asks WHERE delays occur; ranking steps by duration answers it."
}
```

## 6. Validation & repair (the guardrails that make a weak model safe)

1. Parse JSON; on failure → heuristic fallback.
2. For each question: every `ref` must exist in the catalog (case-insensitive). Drop
   unknown refs (this kills hallucinated columns).
3. Require a valid **measure** for `temporal`/`segment`/`ranking`; `coverage` may be
   measure-less. Drop questions left with nothing usable.
4. Clamp `intent` to the allowed set (default `segment`).
5. **Join safety:** if a question's columns span two tables, require an existing
   relationship with confidence ≥ 0.80 (reuse the rel map); otherwise drop it (never
   fabricate a join).
6. De-dupe by (intent, sorted column set); cap at 6.
7. If **0** valid questions survive → heuristic fallback, so the screen is never empty.

## 7. Persistence & integration (minimal new surface)

- Reuse `_persist_question`. `question_id = f"{project}:ask{i}"`; payload carries
  `needed_columns`, `col_roles`, `question_type` (= intent), `title`, `reason`,
  `source="llm"`.
- One small change in the answer builder: `_draft_answer` uses
  `question_type = payload.get("question_type") or _detect_question_type(qid, title)`.
  Everything else — single-table SQL, the join path, duration derivation, readiness,
  judge, finding, confidence — is **already in place and unchanged**.

## 8. Fallback & model dependency

- `NoLLMProvider` / Ollama down / unparseable / 0 survivors → current heuristic generator.
- Question *quality* scales with the model. Local `gemma4` / `llama3.2` are weak;
  `qwen2.5:14b-instruct` is the intended analyst/judge model. Validation guarantees the
  questions are always **schema-valid and safe**, even on a weak model — just less sharp.

## 9. Mixing policy (open decision — see §12)

Proposed default: LLM is primary. If it yields **< 2** valid goal-questions, top up with
heuristic ones to reach a minimum of 3, so the user always has a usable set.

## 10. How we'll prove it works (naked-data test)

Per the design-first / naked-data principle (no provided domain docs fed in):

- **The regression that matters:** two *different* goals on the *same* schema must yield
  *different* question sets (with a stub provider returning goal-conditioned JSON). This
  is the exact failure you caught, encoded as a test.
- Unit tests: validation drops hallucinated columns; a 2-table question without a
  confident relationship is dropped; `NoLLMProvider` → heuristic fallback fires; intent
  clamping; min-questions top-up.
- Because real LLM output is non-deterministic, the chain (validate → persist → build SQL)
  is tested with a **stub provider**; a separate manually-run check exercises the real
  model on the radiology goals and shows the before/after.

## 11. Explicitly NOT doing

- No hardcoded domain values/meanings (radiology, registration, anything).
- No goal→question keyword hacks (that's just more templates).
- No LLM-authored SQL — SQL stays deterministic and safe (unless you chose option B).

## 12. Decisions I need from you

1. **Cap on number of questions** — default 6. OK?
2. **Mixing** — LLM primary with heuristic top-up to a minimum of 3 (§9)? Or LLM-only
   when a model is present?
3. **Reason line** — show the one-line "why this serves the goal" on each question card?
   (I think yes — it's how you'll judge whether it actually comprehended.)
4. **Scope of this pass** — just the question-generation comprehension step? Or also
   revisit relevance scoring (which columns are "relevant") with the same lens?
