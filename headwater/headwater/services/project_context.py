"""Helpers for loading canonical project context into runtime consumers."""

from __future__ import annotations

from headwater.analyzer.metadata_retrieval import RetrievedMetadata, retrieve_metadata
from headwater.core.models import DatasetContext, DiscoveryResult, ProjectContextBundle


def load_project_context_bundle(
    store,
    discovery: DiscoveryResult,
    *,
    project_id: str | None = None,
) -> ProjectContextBundle:
    """Load canonical project context records relevant to a discovery result."""
    source_name = discovery.source.name
    ids = _context_ids(store, source_name, project_id)
    dataset_contexts: list[DatasetContext] = []
    context_row = store.get_dataset_context(source_name)
    if context_row:
        dataset_contexts.append(DatasetContext(**context_row))

    items = []
    resources = []
    seen_item_ids: set[str] = set()
    seen_resource_ids: set[str] = set()
    for context_id in ids:
        for item in store.list_project_context_items(context_id):
            item_id = item.get("id")
            if item_id in seen_item_ids:
                continue
            seen_item_ids.add(item_id)
            items.append(item)
        for resource in store.list_project_context_resources(context_id):
            resource_id = resource.get("id")
            if resource_id in seen_resource_ids:
                continue
            seen_resource_ids.add(resource_id)
            resources.append(resource)

    return ProjectContextBundle(
        project_id=ids[0] if ids else (project_id or source_name),
        source_names=[source_name],
        dataset_contexts=dataset_contexts,
        items=items,
        resources=resources,
    )


def load_retrieved_metadata(
    store,
    discovery: DiscoveryResult,
    *,
    project_id: str | None = None,
) -> RetrievedMetadata:
    """Load deterministic retrieval metadata augmented with canonical context."""
    bundle = load_project_context_bundle(store, discovery, project_id=project_id)
    context = bundle.dataset_contexts[0] if bundle.dataset_contexts else None
    return retrieve_metadata(
        discovery,
        context,
        context_items=bundle.items,
        resources=bundle.resources,
    )


def dataset_context_for_project(
    store,
    discovery: DiscoveryResult,
    *,
    project_id: str | None = None,
) -> DatasetContext | None:
    """Return the primary dataset context for a project/source if it exists."""
    bundle = load_project_context_bundle(store, discovery, project_id=project_id)
    return bundle.dataset_contexts[0] if bundle.dataset_contexts else None


def _context_ids(store, source_name: str, project_id: str | None) -> list[str]:
    ids: list[str] = []

    def add(value: str | None) -> None:
        if value and value not in ids:
            ids.append(value)

    add(project_id)
    linked = _linked_project_for_source(store, source_name)
    add(linked)
    add(source_name)
    return ids


def _linked_project_for_source(store, source_name: str) -> str | None:
    source_slug = _slugify(source_name)
    for project in store.list_projects():
        project_id = project.get("id")
        if project_id == source_name:
            continue
        explicit_sources = project.get("sources") or []
        project_slug = project.get("slug") or _slugify(project.get("display_name", ""))
        if source_name in explicit_sources:
            return project_id
        if (
            project_slug == source_slug
            or project_slug in source_slug
            or source_slug in project_slug
        ):
            return project_id
    return None


def _slugify(text: str) -> str:
    normalized = "".join(char.lower() if char.isalnum() else "-" for char in text)
    return "-".join(part for part in normalized.split("-") if part)
