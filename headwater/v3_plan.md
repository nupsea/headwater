# V3 Plan: Guided Advisory Experience

## Context

V2 built the semantic catalog backend (LanceDB, Kuzu, 3-strategy decomposer) and a first-pass frontend. But the app still feels like a collection of pages showing metadata, not a **guided advisory tool**. Independent reviewer feedback confirms: there's no navigable guided flow, no resolution paths for recommendations, duplicated info without added value, and the press release promise of "20 minutes to a complete map" isn't realized.

**Core problem**: The app recommends things but gives no way to act on them. It shows data but doesn't guide the user through a journey. V3 fixes this by making every screen answer: "What should I do next, and how do I do it here?"

### Reviewer feedback (verbatim, addressed below)

1. No way to add projects or see project names
2. Pipeline progress looks like colored lines, not a pipeline
3. Review items scattered with no grouping or stage awareness
4. Dictionary descriptions = column names, no LLM inference
5. Descriptions not editable, PK changes not persisted, no FK management
6. App should auto-detect PK/FK from data and propose for confirmation
7. Model questions (e.g. "What time granularity?") have no resolution UI
8. Unnecessary repetition of metadata across stages
9. Quality page needs grouped visual display
10. Settings don't persist, no LLM re-discovery trigger
11. Changes should reflect across dashboard KPIs continuously

### Press release promises not yet delivered

- "Complete map in 20 minutes" -- no guided onboarding flow
- "Tracks its own accuracy" -- confidence metrics computed but not surfaced
- "It proposes, you decide" -- questions/recommendations shown but unresolvable
- "Clarifying questions" -- displayed but no UI to answer them

---

## Architecture Changes

### New backend endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/projects` | POST | Create project with name, source path, description |
| `/api/projects/{id}/rename` | PATCH | Rename project display_name |
| `/api/tables/{table}/pk-fk-suggestions` | GET | Auto-detect PK/FK candidates from data patterns |
| `/api/tables/{table}/keys` | PATCH | Confirm/reject PK/FK suggestions, persist to metadata |
| `/api/models/{name}/answers` | POST | Submit answers to model clarifying questions |
| `/api/settings/llm` | PUT | Persist settings to disk (`.headwater/settings.json`) + clear cache |
| `/api/pipeline/re-enrich` | POST | Re-run LLM enrichment on existing discovery (no re-ingest) |
| `/api/activity` | GET | Recent activity log (last N actions with timestamps) |

### Modified backend files

| File | Change |
|------|--------|
| `core/metadata.py` | `persist_pk_fk()`, `save_question_answers()`, `log_activity()` |
| `profiler/key_detection.py` | NEW: PK/FK candidate detection from uniqueness, naming, cardinality |
| `api/routes/project.py` | POST endpoint, PATCH rename |
| `api/routes/models.py` | POST answers endpoint, store answers in metadata |
| `api/routes/settings.py` | Persist to file, trigger re-enrichment option |
| `api/routes/pipeline.py` | `/re-enrich` endpoint: re-run analyzer on existing discovery |
| `api/app.py` | Register new routes |

### New frontend components

| Component | Purpose |
|-----------|---------|
| `components/pipeline-stepper.tsx` | Visual pipeline as connected boxes with status |
| `components/review-queue.tsx` | Grouped review items by stage with resolution actions |
| `components/question-resolver.tsx` | UI to answer model/dictionary clarifying questions |
| `components/pk-fk-manager.tsx` | Propose/confirm/reject PK/FK assignments |
| `components/activity-feed.tsx` | Recent actions timeline |
| `components/create-project-dialog.tsx` | Modal to create/name new projects |

---

## Phases

### Phase 1: Project Identity & Creation

**Problem**: Only one project visible. No way to create others. Names missing.

**Files**: `api/routes/project.py`, `components/create-project-dialog.tsx`, `components/project-sidebar.tsx`, `app/page.tsx`

1. **POST `/api/projects`** -- accepts `{ display_name, source_path?, description? }`. Calls `store.upsert_project()` which already exists. Returns created project.

2. **PATCH `/api/projects/{id}/rename`** -- update `display_name`. Uses existing `store.upsert_project()`.

3. **`create-project-dialog.tsx`** -- modal with name input + optional source path + optional description. Triggered from sidebar "[+ New Project]" button.

4. **Update `project-sidebar.tsx`**:
   - Add "[+ New]" button at top
   - Show `display_name` prominently (currently shown but may show slug)
   - Active project highlighted, clickable to switch context
   - Each project shows maturity badge + 1-line progress summary

5. **Update `app/page.tsx`**: Show active project name in header. If no projects exist, show onboarding: "Create your first project to get started."

### Phase 2: Pipeline as a Visual Journey

**Problem**: Colored progress bars don't look like a pipeline. No guided steps.

**Files**: `components/pipeline-stepper.tsx`, `app/page.tsx`, `components/workflow-progress.tsx`

1. **`pipeline-stepper.tsx`** -- replace the segmented bar with a horizontal stepper:
   ```
   [ Discover ] --> [ Profile ] --> [ Review ] --> [ Model ] --> [ Quality ]
       done           done          3 items        2 pending      not started
   ```
   - Each step is a rounded box with an icon/label
   - Connecting arrows between boxes (CSS `::after` pseudo-element or SVG)
   - Status: green check (done), pulsing blue ring (active/has items), gray (pending)
   - Below each box: count of pending items or "done" text
   - **Clickable**: each box links to its page (`/discovery`, `/dictionary`, `/models`, `/quality`)
   - Active step expanded slightly with a "Start here" or "N items need review" callout

2. **Replace `<WorkflowProgress>` usage** on dashboard with `<PipelineStepper>`

3. **Stage-wise review counts**: Each step shows items needing attention:
   - Discover: "8 tables found" or "Run discovery"
   - Review: "3 tables pending review"
   - Model: "2 marts awaiting approval"
   - Quality: "5 contracts in observation"

### Phase 3: Grouped Review Queue

**Problem**: Review items scattered across pages with no grouping. No way to know what needs attention.

**Files**: `components/review-queue.tsx`, `app/page.tsx`, API changes

1. **`review-queue.tsx`** -- a single component showing ALL pending review items grouped by stage:
   ```
   Dictionary Review (3 items)
   +-----------------------------------------+
   | zones          | 12 columns | Pending   | [Review ->]
   | inspections    | 8 columns  | Pending   | [Review ->]
   | complaints     | 6 columns  | Pending   | [Review ->]
   +-----------------------------------------+

   Model Review (2 items)
   +-----------------------------------------+
   | mart_compliance_summary | 3 questions | [Review ->]
   | mart_inspection_trends  | 2 questions | [Review ->]
   +-----------------------------------------+

   Quality Review (5 items)
   +-----------------------------------------+
   | 5 contracts in observation mode         | [Review ->]
   +-----------------------------------------+
   ```
   - Each group has a stage header with count badge
   - Each item has a brief summary + "[Review ->]" link to the exact page/section
   - Empty groups show a green checkmark: "All reviewed"

2. **Dashboard integration**: Replace the scattered "Attention Needed" section with `<ReviewQueue>`. This becomes the primary call-to-action on the dashboard.

3. **Data source**: Aggregate from existing API data:
   - Dictionary: tables where `review_status !== "reviewed"` from `/api/dictionary/summary`
   - Models: marts where `status === "proposed"` from `/api/models`
   - Quality: contracts where `status === "observing"` from `/api/quality`

### Phase 4: LLM-Powered Descriptions & Smart Key Detection

**Problem**: Descriptions are just column names repeated. No LLM inference. PK/FK not auto-detected from data.

**Files**: `profiler/key_detection.py` (NEW), `api/routes/data.py`, `api/routes/dictionary.py`, `analyzer/semantic.py`, `components/pk-fk-manager.tsx`

#### 4A: LLM-enriched descriptions

1. **Fix `analyzer/semantic.py`**: Ensure the LLM enrichment pipeline is called during discovery when an LLM provider is configured. Currently heuristic descriptions just echo the column name. When Ollama or Anthropic is configured:
   - Send column name + dtype + sample values + table context to LLM
   - Get back: business-friendly description, semantic type, suggested role
   - Persist enriched descriptions to metadata

2. **Re-enrichment endpoint** (`POST /api/pipeline/re-enrich`):
   - Takes existing discovery (don't re-ingest data)
   - Re-runs `analyze()` with current LLM settings
   - Updates column descriptions, semantic types, roles in metadata
   - Returns count of columns enriched
   - Frontend shows progress: "Re-enriching 64 columns with Ollama..."

3. **Settings page trigger**: After saving LLM settings, show a prompt: "LLM provider changed. Re-run enrichment to update descriptions?" with a button that calls `/api/pipeline/re-enrich`.

#### 4B: PK/FK auto-detection

4. **`profiler/key_detection.py`** (NEW module):
   - `suggest_primary_keys(table, profiles) -> list[PKCandidate]`:
     - Uniqueness ratio == 1.0 + low null rate = strong PK candidate
     - Column name ends in `_id` or `id` + unique = strong candidate
     - Composite key detection: pairs of columns whose combination is unique
     - Returns candidates with confidence score and reasoning
   - `suggest_foreign_keys(tables, profiles, existing_relationships) -> list[FKCandidate]`:
     - Name matching: `table_a.zone_id` matches `zones.id` or `zones.zone_id`
     - Value overlap: check if values in candidate FK column are a subset of target PK
     - Cardinality: FK column has lower cardinality than rows (many-to-one)
     - Returns candidates with confidence, matching %, and reasoning

5. **GET `/api/tables/{table}/pk-fk-suggestions`**: Calls detection module, returns `{ pk_candidates: [...], fk_candidates: [...] }`.

6. **PATCH `/api/tables/{table}/keys`**: Accepts `{ confirm_pks: [col_names], reject_pks: [col_names], confirm_fks: [{from_col, to_table, to_col}], reject_fks: [...] }`. Persists to metadata. Updates discovery state.

7. **`pk-fk-manager.tsx`**: Renders PK/FK suggestions as confirm/reject cards:
   ```
   Suggested Primary Key                          [Confirm] [Reject]
   zone_id  (100% unique, 0% null, name pattern)

   Suggested Foreign Key                          [Confirm] [Reject]
   zone_id -> zones.zone_id  (98% match, 12k rows)
   ```
   - Integrated into dictionary page's table detail view
   - Shows reasoning for each suggestion (uniqueness ratio, naming pattern, match %)

### Phase 5: Question Resolution System

**Problem**: Model questions ("What time granularity?") and dictionary questions are displayed but have no resolution path.

**Files**: `components/question-resolver.tsx`, `api/routes/models.py`, `app/models/page.tsx`, `app/dictionary/page.tsx`

1. **`question-resolver.tsx`**: Reusable component for answering questions:
   ```
   ? What time granularity should 'year' be truncated to?

   [ ] day    [ ] week    [x] month    [ ] custom: [________]

   [Submit Answer]
   ```
   - Props: `questions: Question[]`, `onAnswer: (questionId, answer) => void`
   - Question types:
     - **Choice**: predefined options extracted from question text (e.g. "day/week/month")
     - **Text**: free-form answer input
     - **Confirm/Deny**: yes/no toggle
   - Each answered question shows a green check + the answer
   - Unanswered questions highlighted with amber indicator

2. **POST `/api/models/{name}/answers`**: Accepts `{ answers: [{question_index, answer}] }`. Stores in metadata. Optionally re-generates model SQL incorporating the answer.

3. **Backend `core/metadata.py`**: Add `save_model_answers(model_name, answers)` and `get_model_answers(model_name)` methods.

4. **Models page integration**:
   - "Questions for Review" section replaced with `<QuestionResolver>`
   - When all questions answered, show "All questions resolved" with option to regenerate model
   - Model can be approved even with unanswered questions, but UI nudges: "2 questions unanswered -- answers improve model quality"

5. **Dictionary page integration**:
   - "Needs Clarification" section uses same `<QuestionResolver>` component
   - Answers stored per table in metadata

### Phase 6: Settings Persistence & LLM Integration

**Problem**: Settings don't persist across restarts. Changing LLM provider doesn't trigger re-enrichment.

**Files**: `api/routes/settings.py`, `core/config.py`, `app/settings/page.tsx`

1. **Persist settings to file**: On save, write to `~/.headwater/settings.json`:
   ```json
   {
     "llm_provider": "ollama",
     "ollama_model": "llama3.2",
     "ollama_url": "http://localhost:11434"
   }
   ```
   Load on startup in `get_settings()` -- file values override env defaults.

2. **Settings page UX**:
   - On save: verify LLM connectivity (call `/api/settings/verify-llm`)
   - Show verification result: "Connected to Ollama (llama3.2)" or "Connection failed: ..."
   - If provider changed from previous: show prompt: "Re-run enrichment with new LLM? This will update descriptions for all 64 columns." with [Re-enrich Now] / [Later] buttons
   - Show last-saved timestamp: "Settings saved Apr 16, 2026"

3. **GET `/api/settings/verify-llm`**: Test LLM connectivity:
   - Ollama: call `/api/tags` endpoint
   - Anthropic: make a minimal API call
   - Return `{ status: "ok", model: "llama3.2", latency_ms: 45 }` or `{ status: "error", detail: "..." }`

### Phase 7: Deduplicate Metadata Display

**Problem**: Column names and descriptions repeated across discovery, dictionary, models without added value.

**Principle**: Each page shows metadata at a DIFFERENT level of detail appropriate to its purpose:

| Page | What it shows | Why it's different |
|------|---------------|-------------------|
| **Discovery** | Schema + profile stats (null rate, cardinality, patterns) | Profiling perspective: data quality at column level |
| **Dictionary** | Schema + descriptions + roles + PK/FK + **editable** | Governance perspective: enrich and confirm metadata |
| **Models** | Only columns relevant to the model + SQL + lineage | Modeling perspective: what's used and how |
| **Quality** | Only columns with issues + contract status | Quality perspective: what needs fixing |

**Changes**:
- **Discovery**: Remove description column from Full Schema (it's just the column name until enriched). Show ONLY profiling data: dtype, null rate, distinct count, pattern, sample values. Add "View in Dictionary" link per table.
- **Dictionary**: This is THE place for descriptions. Show enriched descriptions (or "Not yet enriched -- configure LLM in Settings" if still raw). Editable.
- **Models**: Don't repeat full column schema. Show only: model SQL, source tables (as links), lineage graph, questions. Remove duplicate column listing.
- **Quality**: Show columns only when they have issues. Group by issue type, not by table.

### Phase 8: Quality Page Visual Redesign

**Problem**: Quality page needs grouped visual display.

**Files**: `app/quality/page.tsx`

1. **Visual groups** (not tabs, not lists -- card groups with headers):
   ```
   [COMPLETENESS]                [DATA QUALITY]              [CATALOG HEALTH]
    94.2%                         87% pass rate                72% coverage
    ████████████░░  bar           12 pass / 2 fail            18/25 columns
    3 high-null columns           0 critical                   6 confirmed metrics

   ── Needs Attention ──────────────────────────────────────────────────────

   Critical (0)  |  Warning (3)  |  Info (2)

   [Warning] complaints.zone_id -- 28% null, affects 3 mart models  [Fix in Dictionary ->]
   [Warning] sites.latitude     -- 15% null, no FK usage             [Review ->]
   [Warning] zones -> sites     -- 72% integrity, JOINs lose 28%    [View Relationship ->]

   ── Contracts ────────────────────────────────────────────────────────────

   Enforcing (0)   Observing (12)   Proposed (0)

   [table: grouped contract rows with status pills]

   ── Detailed Analysis (collapsed) ──────────────────────────────────────
   [click to expand null heatmap, uniqueness, patterns]
   ```

2. **Each issue links to its resolution page** -- not just "go to dictionary" but "go to dictionary > zones table > zone_id column"

### Phase 9: Dashboard as Continuous Advisory

**Problem**: Dashboard doesn't reflect ongoing state changes. Doesn't feel like a continuous improvement tool.

**Files**: `app/page.tsx`, `components/activity-feed.tsx`, `api/routes/insights.py`

1. **Activity feed** at bottom of dashboard:
   ```
   Recent Activity
   ├ 10m ago  Confirmed 3 tables in dictionary
   ├ 25m ago  Approved mart_compliance_summary
   ├ 1h ago   Discovery completed: 8 tables, 64 columns
   └ 1h ago   Project "Riverton Env Health" created
   ```
   Backend: `store.log_activity(action, detail, timestamp)` called from review/approve/discover endpoints. GET `/api/activity?limit=10` returns recent entries.

2. **Dashboard KPI refresh**: After any mutation (review, approve, re-enrich), the dashboard refetches insights. Currently requires page refresh -- add polling (every 30s when tab is active) or trigger refetch on navigation.

3. **Maturity journey visualization**: Replace project summary maturity gauge with a timeline showing WHEN each maturity level was reached:
   ```
   raw ──[Apr 16 10:00]──> profiled ──[Apr 16 10:02]──> documented ──[...]──> modeled
   ```
   This shows continuous improvement over time.

### Phase 10: Editable Dictionary with Persistence

**Problem**: Descriptions not editable in some views. PK changes not persisted.

---

### Phase 11: Explorer Verification -- Complex Query & Visualization Accuracy

**Problem**: Auto-generated queries handle only simple single-table GROUP BY and basic 1-2 hop JOINs. Medium-to-high complexity insights fail both SQL-wise (incorrect queries, missing patterns) and visually (wrong chart type, misclassified columns). The press release promises "clarifying questions" and accurate exploration, but the current explorer can't correctly represent multi-metric trends, temporal aggregations with custom granularity, comparative analyses, or correlated breakdowns.

**Root cause analysis**:
1. `decomposition.py._build_sql()` generates flat `SELECT metrics GROUP BY dimensions` -- no window functions, HAVING, subqueries, or computed columns
2. `nl_to_sql.py._heuristic_sql()` has 5 patterns (trend, breakdown, top/ranking, count, fallback) but they're all single-aggregation, single-dimension
3. `visualization.py` classifies columns by name-pattern regex only (e.g. `_date` -> temporal), not from catalog semantic types or metric/dimension roles
4. Auto-generated suggested questions produce SQL hints at catalog-build time that may be stale after schema changes or re-enrichment
5. No validation that a generated chart actually renders the insight the user asked for

**Files**: `explorer/decomposition.py`, `explorer/nl_to_sql.py`, `explorer/visualization.py`, `explorer/query_patterns.py` (NEW), `components/result-chart.tsx`, `app/explore/page.tsx`

#### 11A: Extended SQL Generation Patterns

1. **`explorer/query_patterns.py`** (NEW module): Pattern library for complex queries
   - `TemporalAggregation`: DATE_TRUNC at configurable granularity (day/week/month/quarter/year), period-over-period comparison
   - `MultiMetricQuery`: Multiple aggregation functions in one SELECT (SUM + AVG + COUNT)
   - `WindowQuery`: ROW_NUMBER, RANK, LAG/LEAD for running totals, moving averages, period comparison
   - `FilteredAggregation`: HAVING clauses, WHERE with computed thresholds (e.g. "above average")
   - `SubqueryPattern`: Correlated subqueries for "tables where X > (avg of X across all tables)"
   - `CompositeBreakdown`: Multiple GROUP BY dimensions with rollup semantics
   - Each pattern exposes `matches(decomposed_query) -> bool` and `build_sql(context) -> str`

2. **Enhance `decomposition.py._build_sql()`**:
   - Route decomposed queries through pattern library before falling back to flat GROUP BY
   - Support `computed_columns`: expressions like `EXTRACT(YEAR FROM date_col)` derived from temporal intent
   - Support `having_clause`: post-aggregation filters parsed from user intent ("more than 10", "above average")
   - Support `order_by` with direction: already partially exists but not wired to all patterns

3. **Enhance `nl_to_sql.py._heuristic_sql()`**:
   - Add patterns: `comparison` (side-by-side metrics across dimension values), `correlation` (two metrics scatter), `distribution` (histogram buckets), `cumulative` (running sum/count)
   - Multi-dimension support: `GROUP BY dim1, dim2` when query mentions two categorical columns
   - Temporal + dimension: `GROUP BY DATE_TRUNC('month', date_col), category` for "monthly trend by category"
   - Improve JOIN logic: support 3-hop JOINs through intermediate tables when catalog graph shows a path

#### 11B: Catalog-Aware Visualization

4. **Enhance `visualization.py`**:
   - Replace name-pattern regex with catalog-first classification:
     - Check column's `semantic_type` from catalog (metric, dimension, temporal, identifier)
     - Check column's `role` (pk, fk, measure, attribute)
     - Fall back to name patterns only when catalog metadata is absent
   - New chart selection rules:
     - Grouped bar: 2 dimensions + 1 metric (e.g. zone + year + count)
     - Stacked area: temporal + 1 dimension + 1 metric with `group_by`
     - Dual-axis: 2 metrics with different scales on same temporal axis
     - Histogram: single numeric column with no GROUP BY
   - Add `confidence` field to recommendation: how certain the recommender is about the chart type

5. **Enhance `result-chart.tsx`**:
   - Add `GroupedBarChart` variant (side-by-side bars for multi-dimension breakdowns)
   - Add `StackedAreaChart` variant (temporal trend with dimensional breakdown)
   - Add `HistogramChart` variant (bucket distribution of single metric)
   - Improve axis labeling: use catalog display names, format numbers (k/M/B suffixes), date formatting by granularity

#### 11C: Query Hint Validation

6. **Suggestion SQL refresh**: When catalog is re-enriched (Phase 4A), regenerate SQL hints for all suggested questions. Old hints are invalidated, new ones generated against current schema.

7. **Pre-flight SQL validation**: Before executing any generated SQL:
   - Parse with DuckDB's `EXPLAIN` to catch syntax errors without running
   - Verify all referenced tables/columns exist in current schema
   - If validation fails, attempt auto-repair (fix column names, table aliases) before falling back to error

#### 11D: Verification Suite

8. **Test queries against sample dataset** (8 tables, 59.9K records, environmental health domain):
   - Simple: "How many inspections?" -- single COUNT, KPI chart
   - Medium: "Monthly inspection trend by zone" -- DATE_TRUNC + GROUP BY zone, grouped line chart
   - Medium: "Top 5 zones by complaint count" -- TOP N with ORDER BY, horizontal bar
   - Complex: "Compare violation rates across zones, year over year" -- multi-dimension temporal, grouped bar
   - Complex: "Which zones have above-average inspection failure rates?" -- subquery with HAVING, filtered table
   - Complex: "Running total of complaints over time" -- window function (cumulative sum), area chart
   - Each test verifies: SQL executes without error, result shape matches expected, chart type is appropriate

**Files**: `app/dictionary/page.tsx`, `api/routes/dictionary.py`

1. **Always-editable descriptions**: When viewing a table in dictionary, descriptions should be editable regardless of review status. The "reviewed" status means "confirmed as correct" -- it doesn't lock editing. Remove the `isLocked` gate on description input fields.

2. **PK toggle persistence**: When user toggles a column as PK in dictionary, the change must:
   - Call `api.reviewTable()` with `is_primary_key` change
   - Persist to `metadata.db` via `store.update_column_key_status()`
   - Reflect immediately in discovery and models pages

3. **FK management UI**: In dictionary table detail, add a "Relationships" section:
   - Show detected FKs with confirm/reject
   - "[+ Add FK]" button: select from_column -> to_table.to_column dropdown
   - Calls existing `POST /api/dictionary/relationships` endpoint
   - Show integrity % for each relationship

---

## File Change Summary

### Backend (Python)

| File | Action | Key changes |
|------|--------|-------------|
| `profiler/key_detection.py` | CREATE | PK/FK candidate detection algorithm |
| `api/routes/project.py` | MODIFY | POST create, PATCH rename |
| `api/routes/models.py` | MODIFY | POST answers for questions |
| `api/routes/settings.py` | MODIFY | File persistence, verify-llm, re-enrich trigger |
| `api/routes/pipeline.py` | MODIFY | `/re-enrich` endpoint |
| `api/routes/data.py` | MODIFY | PK/FK suggestion + confirmation endpoints |
| `api/routes/dictionary.py` | MODIFY | Editable descriptions, PK persistence |
| `core/metadata.py` | MODIFY | `save_model_answers`, `log_activity`, `persist_pk_fk` |
| `core/config.py` | MODIFY | Load settings from `settings.json` file |
| `analyzer/semantic.py` | MODIFY | Ensure LLM enrichment runs when provider configured |
| `explorer/query_patterns.py` | CREATE | Pattern library for complex SQL (window, HAVING, subquery, temporal) |
| `explorer/decomposition.py` | MODIFY | Route through pattern library, computed columns, HAVING support |
| `explorer/nl_to_sql.py` | MODIFY | New heuristic patterns (comparison, correlation, distribution, cumulative), 3-hop JOINs |
| `explorer/visualization.py` | MODIFY | Catalog-first column classification, grouped bar/stacked area/histogram rules |

### Frontend (TypeScript/React)

| File | Action | Key changes |
|------|--------|-------------|
| `components/pipeline-stepper.tsx` | CREATE | Connected-box pipeline visualization |
| `components/review-queue.tsx` | CREATE | Grouped review items by stage |
| `components/question-resolver.tsx` | CREATE | Answer model/dictionary questions |
| `components/pk-fk-manager.tsx` | CREATE | PK/FK suggestion confirm/reject UI |
| `components/activity-feed.tsx` | CREATE | Recent actions timeline |
| `components/create-project-dialog.tsx` | CREATE | New project modal |
| `app/page.tsx` | MODIFY | Pipeline stepper, review queue, activity feed |
| `app/dictionary/page.tsx` | MODIFY | Always-editable, PK/FK manager, question resolver |
| `app/models/page.tsx` | MODIFY | Relationships first tab, question resolver |
| `app/quality/page.tsx` | MODIFY | Visual groups with resolution links |
| `app/discovery/page.tsx` | MODIFY | Remove duplicate descriptions, profile-only view |
| `app/settings/page.tsx` | MODIFY | Persist indicator, verify LLM, re-enrich prompt |
| `components/project-sidebar.tsx` | MODIFY | New project button, active project highlight |
| `lib/api.ts` | MODIFY | New endpoint types and calls |
| `components/result-chart.tsx` | MODIFY | Grouped bar, stacked area, histogram variants; improved axis labels |
| `app/explore/page.tsx` | MODIFY | Chart confidence indicator, improved result metadata |

---

## Implementation Order

| Order | Phase | Dependencies | Effort |
|-------|-------|-------------|--------|
| 1 | Phase 1: Project Identity | None | S |
| 2 | Phase 6: Settings Persistence | None | S |
| 3 | Phase 4A: LLM Descriptions | Phase 6 (settings) | M |
| 4 | Phase 4B: PK/FK Detection | None | M |
| 5 | Phase 2: Pipeline Stepper | None | S |
| 6 | Phase 3: Review Queue | Phase 1 (projects) | M |
| 7 | Phase 5: Question Resolution | None | M |
| 8 | Phase 10: Editable Dictionary | Phase 4B (PK/FK) | S |
| 9 | Phase 7: Deduplicate Metadata | None | S |
| 10 | Phase 8: Quality Visual | None | M |
| 11 | Phase 9: Dashboard Advisory | Phases 1-8 | M |
| 12 | Phase 11: Explorer Verification | Phase 4A (catalog semantics) | L |

S = Small (1-2 files), M = Medium (3-5 files), L = Large (6+ files)

---

## Verification

1. **Project creation**: Sidebar shows "[+ New]", creating a project shows it immediately with display name. Multiple projects visible.
2. **Pipeline stepper**: Dashboard shows connected boxes (Discover -> Profile -> Review -> Model -> Quality) with status icons and pending counts. Each box links to its page.
3. **Review queue**: Dashboard groups all review items by stage. Each item has a resolution link that navigates to the exact context.
4. **LLM descriptions**: After configuring Ollama/Anthropic in settings and re-enriching, column descriptions are business-friendly (not just column names).
5. **PK/FK suggestions**: Dictionary shows "Suggested PK: zone_id (100% unique)" with Confirm/Reject. FK suggestions show match %.
6. **Question resolution**: Model page questions have answer inputs. Answering stores response and optionally regenerates SQL.
7. **Settings persistence**: Restart server -> settings preserved. Changing LLM provider shows "Re-enrich?" prompt.
8. **No duplicate metadata**: Discovery shows profile data only. Dictionary shows editable descriptions. Models show SQL + lineage only. Quality shows issues only.
9. **Quality visual**: Scorecard cards, grouped issues by severity with resolution links, contracts by status.
10. **Activity feed**: Dashboard shows recent actions with timestamps.
11. **Explorer verification**: "Monthly inspection trend by zone" produces correct DATE_TRUNC + GROUP BY SQL and renders as grouped line chart. "Top 5 zones by complaint count" renders as horizontal bar. Complex queries (year-over-year comparison, above-average filter, running totals) execute without error and select appropriate chart types.
12. **Build**: `cd headwater/ui && npm run build` -- zero errors. `cd headwater && uv run ruff check .` -- zero lint.
