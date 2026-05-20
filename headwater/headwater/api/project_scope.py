"""Helpers for resolving project-scoped API state."""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import HTTPException, Request

from headwater.core.models import (
    ContractRule,
    DimensionDefinition,
    EntityDefinition,
    ExecutionResult,
    GeneratedModel,
    MetricDefinition,
    SemanticCatalog,
)
from headwater.core.runtime_state import PipelineRuntimeState, get_runtime_state


def resolve_project(store, project_id: str) -> dict | None:
    get_project = getattr(store, "get_project", None)
    project = get_project(project_id) if callable(get_project) else None
    if project:
        return project
    get_source = getattr(store, "get_source", None)
    if not callable(get_source):
        return None
    source = get_source(project_id)
    if not source:
        return None
    return _source_backed_project(source)


def project_sources(project: dict, store, *, sources: list[dict] | None = None) -> list[str]:
    explicit = [
        str(source)
        for source in project.get("sources", [])
        if isinstance(source, str) and source.strip()
    ]
    if explicit:
        return explicit

    project_id = project.get("id")
    sources = list(sources) if sources is not None else store.list_sources()
    if project_id and any(source.get("name") == project_id for source in sources):
        return [project_id]

    alias_matches = _source_names_from_alias_context(project, store, sources)
    if alias_matches:
        return alias_matches

    if len(sources) == 1:
        return [sources[0]["name"]]

    return [project_id] if project_id else []


def project_for_source(
    store,
    source_name: str,
    *,
    projects: list[dict] | None = None,
) -> dict | None:
    """Return the real project linked to a source, if one exists."""
    linked = []
    for project in (projects if projects is not None else store.list_projects()):
        if project.get("id") == source_name:
            continue
        explicit = project.get("sources") or []
        if source_name in explicit or source_name in _source_names_from_alias_context(
            project,
            store,
        ):
            linked.append(project)
    return linked[0] if linked else None


def visible_projects(
    store,
    *,
    projects: list[dict] | None = None,
    sources: list[dict] | None = None,
) -> list[dict]:
    """Hide legacy source-name shadow projects when a real project links them."""
    projects = list(projects) if projects is not None else store.list_projects()
    sources = list(sources) if sources is not None else store.list_sources()
    linked_sources = {
        source
        for project in projects
        for source in (project.get("sources") or [])
        if project.get("id") != source
    }
    source_names = {source["name"] for source in sources}
    for source_name in source_names:
        if project_for_source(store, source_name, projects=projects):
            linked_sources.add(source_name)
    visible = [project for project in projects if project.get("id") not in linked_sources]
    visible_ids = {project.get("id") for project in visible}
    for source in sources:
        source_name = source.get("name")
        if not source_name or source_name in visible_ids or source_name in linked_sources:
            continue
        visible.append(_source_backed_project(source))
    return visible


def catalog_ids_for_project(project: dict, store) -> list[str]:
    """Catalog rows may be keyed by real project id or legacy source id."""
    ids = [project["id"], *project_sources(project, store)]
    deduped = []
    for id_ in ids:
        if id_ and id_ not in deduped:
            deduped.append(id_)
    return deduped


def scoped_pipeline(request: Request, project_id: str | None = None) -> PipelineRuntimeState:
    """Return runtime pipeline state for a project, falling back to app state."""
    if not project_id:
        runtime_state = get_runtime_state(request)
        store = getattr(request.app.state, "metadata_store", None)
        active_source_name = runtime_state.active_source_name()
        if store is None or not active_source_name:
            return runtime_state
        hydrated = _pipeline_for_source(
            store,
            active_source_name,
            runtime_state=runtime_state,
        )
        if hydrated is None:
            return runtime_state
        return hydrated

    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    source_names = project_sources(project, store)
    if not source_names:
        return PipelineRuntimeState(
            project=project,
            source_names=[],
            discovery=None,
            staging_models=[],
            mart_models=[],
            contracts=[],
            execution_results=[],
            quality_report=None,
        )

    source_name = source_names[0]
    runtime_state = _pipeline_for_source(store, source_name)
    if runtime_state is None:
        raise HTTPException(
            status_code=404,
            detail=f"No persisted pipeline state found for source '{source_name}'.",
        )
    runtime_state.project = project
    runtime_state.source_names = source_names

    table_names = _project_table_names(project, runtime_state.discovery, store=store)
    if table_names is not None:
        runtime_state.discovery = _filter_discovery(runtime_state.discovery, table_names)
        runtime_state.staging_models = _filter_models(runtime_state.staging_models, table_names)
        runtime_state.mart_models = _filter_models(runtime_state.mart_models, table_names)
        model_names = {
            model.name
            for model in runtime_state.staging_models + runtime_state.mart_models
        }
        runtime_state.contracts = _contracts_for_models(store, model_names)
        runtime_state.execution_results = _execution_results_for_models(store, model_names)
        runtime_state.quality_report = _quality_report_for_source(store, source_name, model_names)
    runtime_state.table_names = sorted(table_names) if table_names is not None else None
    return runtime_state


def _pipeline_for_source(
    store,
    source_name: str,
    *,
    runtime_state: PipelineRuntimeState | None = None,
) -> PipelineRuntimeState | None:
    try:
        discovery = store.rebuild_discovery(source_name)
    except Exception:
        return None
    if discovery is None:
        return None

    staging, marts = _models_for_source(store, source_name)
    model_names = {model.name for model in staging + marts}
    contracts = _contracts_for_models(store, model_names)
    execution_results = _execution_results_for_models(store, model_names)
    quality_report = _quality_report_for_source(store, source_name, model_names)
    catalog = _catalog_for_source(store, source_name)

    return PipelineRuntimeState(
        discovery=discovery,
        catalog=catalog,
        staging_models=staging,
        mart_models=marts,
        contracts=contracts,
        execution_results=execution_results,
        quality_report=quality_report,
        graph_store=runtime_state.graph_store if runtime_state is not None else None,
        vector_store=runtime_state.vector_store if runtime_state is not None else None,
        source_names=[source_name],
    )


def _catalog_for_source(store, source_name: str) -> SemanticCatalog | None:
    get_catalog_metrics = getattr(store, "get_catalog_metrics", None)
    get_catalog_dimensions = getattr(store, "get_catalog_dimensions", None)
    get_catalog_entities = getattr(store, "get_catalog_entities", None)
    if not (
        callable(get_catalog_metrics)
        and callable(get_catalog_dimensions)
        and callable(get_catalog_entities)
    ):
        return None
    ids = []
    linked_project = project_for_source(store, source_name)
    if linked_project:
        ids.append(linked_project["id"])
    ids.append(source_name)

    for project_id in ids:
        metrics_raw = get_catalog_metrics(project_id)
        dims_raw = get_catalog_dimensions(project_id)
        ents_raw = get_catalog_entities(project_id)
        if not metrics_raw and not dims_raw and not ents_raw:
            continue
        return SemanticCatalog(
            metrics=[MetricDefinition(**_remap_catalog_row(row)) for row in metrics_raw],
            dimensions=[DimensionDefinition(**_remap_catalog_row(row)) for row in dims_raw],
            entities=[EntityDefinition(**_remap_catalog_row(row)) for row in ents_raw],
        )
    return None


def _remap_catalog_row(row: dict) -> dict:
    out = dict(row)
    if "column_name" in out:
        out["column"] = out.pop("column_name")
    if "table_name" in out:
        out["table"] = out.pop("table_name")
    out.pop("project_id", None)
    return out


def primary_source_for_project(request: Request, project_id: str) -> str:
    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")
    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")
    sources = project_sources(project, store)
    if not sources:
        raise HTTPException(status_code=400, detail="Project has no linked source.")
    return sources[0]


def _models_for_source(
    store,
    source_name: str,
) -> tuple[list[GeneratedModel], list[GeneratedModel]]:
    staging: list[GeneratedModel] = []
    marts: list[GeneratedModel] = []
    for row in store.get_models(source_name):
        model = GeneratedModel(
            name=row["name"],
            model_type=row["model_type"],
            sql=row["sql_text"],
            description=row.get("description", ""),
            source_tables=_json_list(row.get("source_tables")),
            depends_on=_json_list(row.get("depends_on")),
            status=row.get("status", "proposed"),
            assumptions=_json_list(row.get("assumptions")),
            questions=_json_list(row.get("questions")),
        )
        if model.model_type == "staging":
            staging.append(model)
        else:
            marts.append(model)
    return staging, marts


def _filter_discovery(discovery, table_names: set[str]):
    tables = [table for table in discovery.tables if table.name in table_names]
    profiles = [profile for profile in discovery.profiles if profile.table_name in table_names]
    relationships = [
        rel
        for rel in discovery.relationships
        if rel.from_table in table_names and rel.to_table in table_names
    ]
    return discovery.model_copy(
        update={
            "tables": tables,
            "profiles": profiles,
            "relationships": relationships,
        }
    )


def _filter_models(models: list[GeneratedModel], table_names: set[str]) -> list[GeneratedModel]:
    return [
        model
        for model in models
        if not model.source_tables or any(source in table_names for source in model.source_tables)
    ]


def _project_table_names(project: dict, discovery, *, store=None) -> set[str] | None:
    explicit = [
        str(table)
        for table in project.get("tables", [])
        if isinstance(table, str) and table.strip()
    ]
    if explicit:
        return set(explicit)

    if store is None:
        return None
    matches = _table_names_from_alias_context(project, store, discovery)
    return matches or None


def _table_search_text(table) -> str:
    return _slugify(
        " ".join(
            str(getattr(table, field, "") or "")
            for field in ("name", "domain", "description")
        )
    )


def _source_names_from_alias_context(
    project: dict,
    store,
    sources: list[dict] | None = None,
) -> list[str]:
    sources = list(sources) if sources is not None else store.list_sources()
    source_names = {source.get("name") for source in sources if source.get("name")}
    matches: list[str] = []
    for item in _project_context_items(store, project.get("id"), item_type="source_alias"):
        value = item.get("value") or {}
        candidates = _string_values(value.get("source_names"))
        candidates.extend(_string_values(value.get("sources")))
        candidates.extend(_string_values(value.get("source_name")))
        if item.get("source_name"):
            candidates.append(str(item["source_name"]))

        aliases = _alias_values(item)
        for source in sources:
            source_name = source.get("name")
            if not source_name:
                continue
            if source_name in candidates or _source_matches_alias(source, aliases):
                matches.append(source_name)
    return _dedupe([name for name in matches if name in source_names])


def _table_names_from_alias_context(project: dict, store, discovery) -> set[str]:
    discovery_tables = {table.name: table for table in discovery.tables}
    matches: set[str] = set()
    for item in _project_context_items(store, project.get("id"), item_type="table_alias"):
        value = item.get("value") or {}
        candidates = _string_values(value.get("table_names"))
        candidates.extend(_string_values(value.get("tables")))
        candidates.extend(_string_values(value.get("table_name")))
        if item.get("table_name"):
            candidates.append(str(item["table_name"]))

        for table_name in candidates:
            if table_name in discovery_tables:
                matches.add(table_name)

        aliases = _alias_values(item)
        if not aliases:
            continue
        for table in discovery.tables:
            if _alias_matches(_table_search_text(table), aliases):
                matches.add(table.name)
    return matches


def _project_context_items(store, project_id: str | None, *, item_type: str) -> list[dict]:
    if not project_id:
        return []
    list_items = getattr(store, "list_project_context_items", None)
    if not callable(list_items):
        return []
    try:
        items = list_items(project_id, item_type=item_type)
    except TypeError:
        items = [
            item
            for item in list_items(project_id)
            if item.get("item_type") == item_type
        ]
    return [item for item in items if item.get("status") != "rejected"]


def _source_matches_alias(source: dict, aliases: set[str]) -> bool:
    if not aliases:
        return False
    return _alias_matches(
        _slugify(
            " ".join(
                str(source.get(field) or "")
                for field in ("name", "display_name", "host")
            )
        ),
        aliases,
    )


def _alias_matches(search_text: str, aliases: set[str]) -> bool:
    normalized = _slugify(search_text)
    tokens = set(normalized.split("-"))
    return any(alias == normalized or alias in tokens for alias in aliases)


def _alias_values(item: dict) -> set[str]:
    value = item.get("value") or {}
    aliases = _string_values(value.get("aliases"))
    aliases.extend(_string_values(value.get("alias")))
    if item.get("name"):
        aliases.append(str(item["name"]))
    return {_slugify(alias) for alias in aliases if _slugify(alias)}


def _string_values(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _contracts_for_models(store, model_names: set[str]) -> list[ContractRule]:
    get_contracts = getattr(store, "get_contracts", None)
    if not callable(get_contracts):
        return []
    normalized_names = {name.split(".", 1)[-1] for name in model_names}
    contracts: list[ContractRule] = []
    for row in get_contracts():
        row_model_name = row["model_name"]
        row_base_name = row_model_name.split(".", 1)[-1]
        if row_model_name not in model_names and row_base_name not in normalized_names:
            continue
        contracts.append(
            ContractRule(
                id=row["id"],
                model_name=row_model_name,
                column_name=row.get("column_name"),
                rule_type=row["rule_type"],
                expression=row["expression"],
                severity=row.get("severity", "warning"),
                description=row.get("description", ""),
                confidence=row.get("confidence", 0.8),
                status=row.get("status", "proposed"),
            )
        )
    return contracts


def _execution_results_for_models(store, model_names: set[str]) -> list[ExecutionResult]:
    get_execution_results = getattr(store, "get_execution_results", None)
    if not callable(get_execution_results):
        return []
    results: list[ExecutionResult] = []
    for row in get_execution_results():
        if row["model_name"] not in model_names:
            continue
        results.append(
            ExecutionResult(
                model_name=row["model_name"],
                success=bool(row["success"]),
                row_count=row.get("row_count"),
                execution_time_ms=row.get("execution_time_ms", 0.0),
                error=row.get("error"),
            )
        )
    return results


def _quality_report_for_source(store, source_name: str, model_names: set[str]):
    get_latest_quality_report = getattr(store, "get_latest_quality_report", None)
    if not callable(get_latest_quality_report):
        return None
    row = get_latest_quality_report(source_name)
    if not row:
        return None
    filtered_results = [
        result
        for result in row.get("results", [])
        if not model_names or result.get("model_name") in model_names
    ]
    passed = sum(
        1
        for result in filtered_results
        if result.get("passed") and not result.get("skipped")
    )
    skipped = sum(1 for result in filtered_results if result.get("skipped"))
    failed = sum(
        1
        for result in filtered_results
        if not result.get("passed") and not result.get("skipped")
    )
    return SimpleNamespace(
        total_contracts=len(filtered_results) if model_names else row.get("total_contracts", 0),
        passed=passed if model_names else row.get("passed", 0),
        failed=failed if model_names else row.get("failed", 0),
        skipped=skipped if model_names else row.get("skipped", 0),
        results=[
            SimpleNamespace(
                rule_id=result.get("rule_id"),
                model_name=result.get("model_name"),
                passed=bool(result.get("passed")),
                skipped=bool(result.get("skipped")),
                message=result.get("message", ""),
            )
            for result in filtered_results
        ],
    )


def _json_list(value) -> list:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        import json

        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _source_backed_project(source: dict) -> dict:
    source_name = str(source.get("name") or "")
    display_name = str(source.get("display_name") or source_name)
    created_at = str(source.get("created_at") or "")
    updated_at = str(source.get("last_sync_at") or source.get("created_at") or "")
    return {
        "id": source_name,
        "slug": _slugify(display_name or source_name),
        "display_name": display_name or source_name,
        "description": "",
        "maturity": "raw",
        "maturity_score": 0.0,
        "catalog_confidence": 0.0,
        "created_at": created_at,
        "updated_at": updated_at,
        "sources": [source_name],
    }


def _slugify(name: str) -> str:
    import re

    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9\s_-]", "", slug)
    slug = re.sub(r"[\s_-]+", "-", slug)
    return re.sub(r"-+", "-", slug).strip("-")
