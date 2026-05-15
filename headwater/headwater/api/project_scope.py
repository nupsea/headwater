"""Helpers for resolving project-scoped API state."""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import HTTPException, Request

from headwater.core.models import ContractRule, ExecutionResult, GeneratedModel


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


def project_sources(project: dict, store) -> list[str]:
    explicit = [
        str(source)
        for source in project.get("sources", [])
        if isinstance(source, str) and source.strip()
    ]
    if explicit:
        return explicit

    project_id = project.get("id")
    sources = store.list_sources()
    if project_id and any(source.get("name") == project_id for source in sources):
        return [project_id]
    if len(sources) == 1:
        return [sources[0]["name"]]

    project_slug = project.get("slug") or _slugify(project.get("display_name", ""))
    matches = []
    for source in sources:
        source_name = source.get("name") or ""
        source_display = source.get("display_name") or ""
        source_slug = _slugify(f"{source_name} {source_display}")
        if _slug_matches(project_slug, source_slug):
            matches.append(source_name)
    return matches or ([project_id] if project_id else [])


def project_for_source(store, source_name: str) -> dict | None:
    """Return the real project linked to a source, if one exists."""
    linked = []
    source_slug = _slugify(source_name)
    for project in store.list_projects():
        if project.get("id") == source_name:
            continue
        explicit = project.get("sources") or []
        project_slug = project.get("slug") or _slugify(project.get("display_name", ""))
        if source_name in explicit or _slug_matches(project_slug, source_slug):
            linked.append(project)
    return linked[0] if linked else None


def visible_projects(store) -> list[dict]:
    """Hide legacy source-name shadow projects when a real project links them."""
    projects = store.list_projects()
    linked_sources = {
        source
        for project in projects
        for source in (project.get("sources") or [])
        if project.get("id") != source
    }
    source_names = {source["name"] for source in store.list_sources()}
    for source_name in source_names:
        if project_for_source(store, source_name):
            linked_sources.add(source_name)
    visible = [project for project in projects if project.get("id") not in linked_sources]
    visible_ids = {project.get("id") for project in visible}
    for source in store.list_sources():
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


def scoped_pipeline(request: Request, project_id: str | None = None) -> dict:
    """Return pipeline-like state for a project, falling back to app state."""
    if not project_id:
        return request.app.state.pipeline

    store = getattr(request.app.state, "metadata_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Metadata store not available.")

    project = resolve_project(store, project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found.")

    source_names = project_sources(project, store)
    if not source_names:
        return {
            "project": project,
            "source_names": [],
            "discovery": None,
            "staging_models": [],
            "mart_models": [],
            "contracts": [],
            "execution_results": [],
            "quality_report": None,
        }

    source_name = source_names[0]
    try:
        discovery = store.rebuild_discovery(source_name)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load project source '{source_name}': {exc}",
        ) from exc

    table_names = _project_table_names(project, discovery)
    if table_names is not None:
        discovery = _filter_discovery(discovery, table_names)

    staging, marts = _models_for_source(store, source_name)
    if table_names is not None:
        staging = _filter_models(staging, table_names)
        marts = _filter_models(marts, table_names)
    model_names = {model.name for model in staging + marts}
    contracts = _contracts_for_models(store, model_names)
    execution_results = _execution_results_for_models(store, model_names)
    quality_report = _quality_report_for_source(store, source_name, model_names)

    return {
        "project": project,
        "source_names": source_names,
        "table_names": sorted(table_names) if table_names is not None else None,
        "discovery": discovery,
        "staging_models": staging,
        "mart_models": marts,
        "contracts": contracts,
        "execution_results": execution_results,
        "quality_report": quality_report,
    }


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


def _models_for_source(store, source_name: str) -> tuple[list[GeneratedModel], list[GeneratedModel]]:
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


def _project_table_names(project: dict, discovery) -> set[str] | None:
    explicit = [
        str(table)
        for table in project.get("tables", [])
        if isinstance(table, str) and table.strip()
    ]
    if explicit:
        return set(explicit)

    aliases = _project_table_aliases(project)
    if not aliases:
        return None

    matches = {
        table.name
        for table in discovery.tables
        if any(alias in _table_search_text(table) for alias in aliases)
    }
    return matches or None


def _project_table_aliases(project: dict) -> set[str]:
    text = " ".join(
        str(project.get(field) or "")
        for field in ("slug", "display_name", "description")
    ).lower()
    tokens = set(_slugify(text).split("-"))
    aliases: set[str] = set()
    if "taxi" in tokens:
        aliases.update({"taxi", "tlc", "trip", "tripdata", "yellow", "green", "fhv", "fhvhv"})
    return aliases


def _table_search_text(table) -> str:
    return _slugify(
        " ".join(
            str(getattr(table, field, "") or "")
            for field in ("name", "domain", "description")
        )
    )


def _contracts_for_models(store, model_names: set[str]) -> list[ContractRule]:
    contracts: list[ContractRule] = []
    for model_name in sorted(model_names):
        for row in store.get_contracts(model_name):
            contracts.append(
                ContractRule(
                    id=row["id"],
                    model_name=row["model_name"],
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
    results: list[ExecutionResult] = []
    for row in store.get_execution_results():
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
    row = store.get_latest_quality_report(source_name)
    if not row:
        return None
    filtered_results = [
        result
        for result in row.get("results", [])
        if not model_names or result.get("model_name") in model_names
    ]
    passed = sum(1 for result in filtered_results if result.get("passed") and not result.get("skipped"))
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


def _slug_matches(project_slug: str, source_slug: str) -> bool:
    if not project_slug or not source_slug:
        return False
    if project_slug in source_slug or source_slug.startswith(project_slug):
        return True
    return project_slug.replace("-", "") in source_slug.replace("-", "")
