"""Helpers for loading canonical project context into runtime consumers."""

from __future__ import annotations

from typing import Any

from headwater.analyzer.metadata_retrieval import RetrievedMetadata, retrieve_metadata
from headwater.core.models import DatasetContext, DiscoveryResult, ProjectContextBundle


class ProjectContextProvider:
    """Typed accessors over canonical project-context items.

    Runtime consumers should use this provider instead of walking arbitrary item
    lists. By default it returns approved/locked items; callers building review
    UI can opt into proposed items.
    """

    def __init__(self, items: list[dict | Any]):
        self._items = [_as_item_dict(item) for item in items]

    def items(
        self,
        item_type: str,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        include_proposed: bool = False,
    ) -> list[dict]:
        return [
            item
            for item in self._items
            if item.get("item_type") == item_type
            and _status_matches(item, include_proposed=include_proposed)
            and (table_name is None or item.get("table_name") == table_name)
            and (column_name is None or item.get("column_name") == column_name)
        ]

    def row_grain(self, table_name: str, *, include_proposed: bool = False) -> dict | None:
        return _first(
            self.items("row_grain", table_name=table_name, include_proposed=include_proposed)
        )

    def row_entity(self, table_name: str, *, include_proposed: bool = False) -> dict | None:
        return _first(
            self.items("row_entity", table_name=table_name, include_proposed=include_proposed)
        )

    def time_anchor(self, table_name: str, *, include_proposed: bool = False) -> dict | None:
        return _first(
            self.items("time_anchor", table_name=table_name, include_proposed=include_proposed)
        )

    def pk_candidates(
        self,
        table_name: str | None = None,
        *,
        include_proposed: bool = False,
    ) -> list[dict]:
        return self.items("pk_candidate", table_name=table_name, include_proposed=include_proposed)

    def fk_candidates(
        self,
        table_name: str | None = None,
        *,
        include_proposed: bool = False,
    ) -> list[dict]:
        return self.items("fk_candidate", table_name=table_name, include_proposed=include_proposed)

    def relationship_hints(
        self,
        table_name: str | None = None,
        *,
        include_proposed: bool = False,
    ) -> list[dict]:
        return self.items(
            "relationship_hint",
            table_name=table_name,
            include_proposed=include_proposed,
        )

    def aliases(self, kind: str | None = None, *, include_proposed: bool = False) -> list[str]:
        item_types = {
            None: ("project_alias", "source_alias", "table_alias"),
            "project": ("project_alias",),
            "source": ("source_alias",),
            "table": ("table_alias",),
        }.get(kind)
        if item_types is None:
            return []
        aliases: list[str] = []
        for item_type in item_types:
            for item in self.items(item_type, include_proposed=include_proposed):
                value = item.get("value") or {}
                aliases.extend(str(alias) for alias in value.get("aliases") or [])
                alias = value.get("alias")
                if alias:
                    aliases.append(str(alias))
                name = item.get("name")
                if name:
                    aliases.append(str(name))
        return _dedupe(aliases)

    def enum_mappings(
        self,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        include_proposed: bool = False,
    ) -> list[dict]:
        return self.items(
            "enum_mapping",
            table_name=table_name,
            column_name=column_name,
            include_proposed=include_proposed,
        )

    def value_labels(
        self,
        table_name: str,
        column_name: str,
        *,
        include_proposed: bool = False,
    ) -> dict[str, str]:
        labels: dict[str, str] = {}
        for item in self.enum_mappings(
            table_name=table_name,
            column_name=column_name,
            include_proposed=include_proposed,
        ):
            value = item.get("value") or {}
            labels.update(
                {
                    str(key): str(label)
                    for key, label in (value.get("labels") or {}).items()
                }
            )
        return labels

    def column_policies(
        self,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        include_proposed: bool = False,
    ) -> list[dict]:
        return self.items(
            "column_policy",
            table_name=table_name,
            column_name=column_name,
            include_proposed=include_proposed,
        )

    def low_signal_columns(self, *, include_proposed: bool = False) -> set[tuple[str, str]]:
        columns: set[tuple[str, str]] = set()
        for item in self.column_policies(include_proposed=include_proposed):
            value = item.get("value") or {}
            if value.get("low_signal") or value.get("policy") == "low_signal":
                table_name = item.get("table_name")
                column_name = item.get("column_name")
                if table_name and column_name:
                    columns.add((table_name, column_name))
        return columns

    def preferred_dimensions(self, *, include_proposed: bool = False) -> list[dict]:
        preferred: list[dict] = []
        for item in self.column_policies(include_proposed=include_proposed):
            value = item.get("value") or {}
            if value.get("preferred_dimension") or value.get("policy") == "preferred_dimension":
                preferred.append(item)
        return preferred

    def business_lenses(self, *, include_proposed: bool = False) -> list[dict]:
        return self.items("business_lens", include_proposed=include_proposed)

    def insight_family_configs(self, *, include_proposed: bool = False) -> list[dict]:
        return self.items("insight_family", include_proposed=include_proposed)

    def question_templates(self, *, include_proposed: bool = False) -> list[dict]:
        return self.items("question_template", include_proposed=include_proposed)

    def visualization_hints(
        self,
        *,
        table_name: str | None = None,
        column_name: str | None = None,
        include_proposed: bool = False,
    ) -> list[dict]:
        return self.items(
            "visualization_hint",
            table_name=table_name,
            column_name=column_name,
            include_proposed=include_proposed,
        )

    def derived_fields(
        self,
        table_name: str | None = None,
        *,
        include_proposed: bool = False,
    ) -> list[dict]:
        return self.items("derived_field", table_name=table_name, include_proposed=include_proposed)

    def advisor_packs(self, *, include_proposed: bool = False) -> list[dict]:
        return self.items("advisor_pack", include_proposed=include_proposed)


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


def project_context_provider(bundle: ProjectContextBundle) -> ProjectContextProvider:
    """Build provider accessors for a loaded context bundle."""
    return ProjectContextProvider(bundle.items)


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


def _as_item_dict(item: dict | Any) -> dict:
    if isinstance(item, dict):
        return item
    if hasattr(item, "model_dump"):
        return item.model_dump()
    return dict(item)


def _status_matches(item: dict, *, include_proposed: bool) -> bool:
    status = item.get("status")
    if status == "rejected":
        return False
    if include_proposed:
        return True
    return status in {"approved", "locked"}


def _first(items: list[dict]) -> dict | None:
    return items[0] if items else None


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique
