# H1 Removal — what was kept, salvaged, and removed

Date: 2026-06-02. Branch: `chore/h1-removal`. The app is now Headwater 2 only.

The H2 stack was already fully decoupled from H1 (it imported none of `explorer/`,
`services/context_*`, or `core/metadata.py`), so the legacy stack was removed
wholesale after salvaging the few genuinely reusable pieces.

## Kept (the spine + H2)
- **Connectors / profiler / executor**: used directly by H2 (`h2_source`,
  `h2_execute`) for ingestion and analytical execution.
- **Generator / quality**: self-contained, retained for the planned export path
  (SQL/dbt models, data contracts). Not yet wired into H2.
- **`analyzer/{judge, llm, ollama}`**: the LLM provider abstraction + the
  certification judge. H2's only analyzer dependency.
- **`core/{config, models, store, exceptions, redaction, classification, types}`**.
- All `services/h2_*`, `api/routes/{h2, health}`, `cli/hw2`, and the `/h2` UI.

## Salvaged + modularized
- **`analyzer/llm.py` + `ollama.py`** were coupled to `core/metadata.MetadataStore`
  (the H1 audit/cache store) via a type hint. Replaced with a local `LLMAuditStore`
  Protocol, so the providers now depend on no concrete store — any object with
  `insert_llm_audit` / `get_llm_token_usage` works, and `None` disables auditing.
- **Health endpoint** rewritten to a stateless liveness + provider probe (it used
  to reach into the removed app-state stores).

## Removed (H1-only)
- `explorer/` (NL-to-SQL, query planner, EDA — the NL-to-SQL harness is deferred to
  v3; H2 has its own statistical kernel in `h2_eda`).
- `services/context_*` and the H1 orchestration services (pipeline_runner,
  source_sync, model_*, rerun_planner, resource_safety, source_evaluation, etc.).
- `core/metadata.py` (the 47-table store, superseded by `core/store.py`),
  `context_confidence`, `draft_secrets`, `events`, `graph_store`, `runtime_state`,
  `vector_store`.
- `analyzer/{catalog, companion, eval, heuristics, metadata_retrieval, semantic,
  semantic_schema, semantic_types}` (typing/definitions superseded by `h2_semantics`).
- All H1 API routers + `api/project_scope.py`; `app.py` rewritten to mount only
  `health` + `h2`.
- `cli/main.py` (the `headwater` script; `hw2` remains).
- H1 UI pages (`data, dictionary, discovery, explore, insights, models, quality,
  settings, sources, projects, health`) and all top-level H1 components +
  `lib/{api, project-context}`. Root layout no longer wraps in `H1Shell`; root page
  already redirects to `/h2`.
- ~27 H1 test modules. `test_connectors` and `test_ollama` were trimmed of their H1
  couplings rather than deleted (spine + provider coverage retained).

Anything not ported is recoverable from git history; the NL-to-SQL query harness is
the main deferred item (v3, per `HEADWATER2_VISION.md`).
