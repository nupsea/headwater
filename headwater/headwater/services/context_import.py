"""Import project context projections back into the canonical metadata store."""

from __future__ import annotations

import re
from collections import OrderedDict

import yaml

_CONTEXT_SECTIONS = {
    "row_grains": "row_grain",
    "row_entities": "row_entity",
    "time_anchors": "time_anchor",
    "pk_candidates": "pk_candidate",
    "fk_candidates": "fk_candidate",
    "project_aliases": "project_alias",
    "source_aliases": "source_alias",
    "table_aliases": "table_alias",
}

_TYPED_FILE_SECTIONS = {
    "derived_fields.yaml": {"derived_fields": "derived_field"},
    "insight_families.yaml": {
        "insight_families": "insight_family",
        "insight_priorities": "insight_priority",
    },
    "business_lenses.yaml": {"business_lenses": "business_lens"},
    "presentation.yaml": {"visualization_hints": "visualization_hint"},
    "question_templates.yaml": {"question_templates": "question_template"},
    "column_policies.yaml": {"column_policies": "column_policy"},
    "relationship_hints.yaml": {
        "relationships": "relationship",
        "relationship_hints": "relationship_hint",
        "fk_candidates": "fk_candidate",
    },
    "advisor_packs.yaml": {"advisor_packs": "advisor_pack"},
}


def import_context_exports(
    store,
    project: dict,
    *,
    files: dict[str, str],
    source_name: str | None = None,
) -> dict:
    """Merge exported context files into the canonical store."""
    source_name = source_name or _default_source_name(project)
    parsed = {
        name: (yaml.safe_load(content) or {})
        for name, content in files.items()
        if name.lower().endswith((".yaml", ".yml"))
    }
    items = OrderedDict()
    dataset_contexts_updated = 0
    resources_upserted = 0

    for context in parsed.get("context.yaml", {}).get("dataset_contexts", []):
        payload = dict(context or {})
        payload["source_name"] = payload.get("source_name") or source_name or project["id"]
        store.upsert_dataset_context(payload["source_name"], payload)
        dataset_contexts_updated += 1

    dataset_summary = parsed.get("context.yaml", {}).get("dataset_summary") or {}
    if dataset_summary:
        item_id = f"dataset_summary:{source_name or project['id']}"
        items[item_id] = {
            "id": item_id,
            "project_id": project["id"],
            "source_name": source_name,
            "item_type": "dataset_summary",
            "scope": "project",
            "name": "dataset_summary",
            "title": "Dataset summary",
            "value": dataset_summary,
            "status": "approved",
            "confidence": 1.0,
            "source": "import",
            "evidence": [],
        }

    for question in parsed.get("context.yaml", {}).get("open_questions", []):
        item_id = question.get("id") or _slugged_id(
            "question",
            question.get("table"),
            question.get("column"),
            question.get("name") or question.get("title") or "open_question",
        )
        items[item_id] = {
            "id": item_id,
            "project_id": project["id"],
            "source_name": source_name,
            "item_type": "open_question",
            "scope": question.get("scope") or "project",
            "name": question.get("name") or "open_question",
            "title": question.get("title"),
            "table_name": question.get("table"),
            "column_name": question.get("column"),
            "value": question.get("value") or {},
            "status": question.get("status") or "approved",
            "confidence": float(question.get("confidence") or 1.0),
            "source": "import",
            "evidence": question.get("evidence") or [],
        }

    context_doc = parsed.get("context.yaml", {})
    for section, item_type in _CONTEXT_SECTIONS.items():
        for entry in context_doc.get(section, []):
            _merge_generic_item(
                items,
                project_id=project["id"],
                source_name=source_name,
                item_type=item_type,
                entry=entry,
            )

    for role in parsed.get("semantic_schema.yaml", {}).get("roles", []):
        if role.get("item_type") == "semantic_role":
            _merge_generic_item(
                items,
                project_id=project["id"],
                source_name=source_name,
                item_type="semantic_role",
                entry=role,
            )
        else:
            _merge_column_semantics(
                items,
                project_id=project["id"],
                source_name=source_name,
                entry=role,
            )

    for column in parsed.get("semantic_types.yaml", {}).get("columns", []):
        _merge_column_semantics(
            items,
            project_id=project["id"],
            source_name=source_name,
            entry=column,
        )

    for term in parsed.get("glossary.yaml", {}).get("terms", []):
        target = _parse_term_target(term.get("term"))
        if target is None:
            glossary_id = f"glossary:{_slugged_id('term', term.get('term') or 'glossary')}"
            items[glossary_id] = {
                "id": glossary_id,
                "project_id": project["id"],
                "source_name": source_name,
                "item_type": "glossary_term",
                "scope": "project",
                "name": term.get("term") or "glossary_term",
                "title": f"Glossary term: {term.get('term') or 'glossary'}",
                "value": {
                    "definition": term.get("definition"),
                },
                "status": term.get("status") or "approved",
                "confidence": float(term.get("confidence") or 1.0),
                "source": "import",
                "evidence": [],
            }
            continue
        table_name, column_name = target
        _merge_column_semantics(
            items,
            project_id=project["id"],
            source_name=source_name,
            entry={
                "table": table_name,
                "column": column_name,
                "description": term.get("definition"),
                "semantic_type": term.get("semantic_type"),
                "role": term.get("role"),
                "status": term.get("status"),
                "confidence": term.get("confidence"),
                "source": term.get("source"),
            },
        )

    for lookup in parsed.get("lookups.yaml", {}).get("lookups", []):
        table_name = lookup.get("table")
        if not table_name:
            continue
        item_id = f"lookup:{table_name}"
        items[item_id] = {
            "id": item_id,
            "project_id": project["id"],
            "source_name": source_name,
            "item_type": "lookup",
            "scope": "table",
            "name": lookup.get("name") or table_name,
            "title": f"Lookup candidate: {table_name}",
            "table_name": table_name,
            "value": {
                key: value
                for key, value in lookup.items()
                if key
                not in {"table", "name", "status", "confidence", "source"}
            },
            "status": lookup.get("status") or "approved",
            "confidence": float(lookup.get("confidence") or 1.0),
            "source": "import",
            "evidence": [],
        }

    for enum_mapping in parsed.get("lookups.yaml", {}).get("enum_mappings", []):
        _merge_generic_item(
            items,
            project_id=project["id"],
            source_name=source_name,
            item_type="enum_mapping",
            entry=enum_mapping,
        )

    for file_name, sections in _TYPED_FILE_SECTIONS.items():
        doc = parsed.get(file_name, {})
        for section, item_type in sections.items():
            for entry in doc.get(section, []):
                _merge_generic_item(
                    items,
                    project_id=project["id"],
                    source_name=source_name,
                    item_type=item_type,
                    entry=entry,
                )

    for resource in parsed.get("resources.yaml", {}).get("resources", []):
        resource_id = resource.get("id") or _slugged_id(
            "resource",
            project["id"],
            resource.get("title") or resource.get("type") or "resource",
        )
        store.upsert_project_context_resource(
            id=resource_id,
            project_id=project["id"],
            source_name=source_name,
            resource_type=resource.get("type") or "unknown",
            title=resource.get("title") or resource_id,
            location=resource.get("location"),
            status=resource.get("status") or "active",
            source=resource.get("source") or "import",
            metadata=resource.get("metadata") or {},
        )
        resources_upserted += 1

    for item in items.values():
        existing = store.get_project_context_item(item["id"], project_id=project["id"])
        if existing is None:
            store.upsert_project_context_item(**item)
            continue
        store.update_project_context_item(
            item["id"],
            project_id=project["id"],
            status=item["status"],
            value=item["value"],
            name=item["name"],
            title=item.get("title"),
            confidence=item["confidence"],
            source=item["source"],
            evidence=item.get("evidence") or [],
        )

    return {
        "project_id": project["id"],
        "source_name": source_name,
        "items_upserted": len(items),
        "resources_upserted": resources_upserted,
        "dataset_contexts_updated": dataset_contexts_updated,
        "files_processed": sorted(parsed),
    }


def _merge_generic_item(
    items: OrderedDict,
    *,
    project_id: str,
    source_name: str | None,
    item_type: str,
    entry: dict,
) -> None:
    if not isinstance(entry, dict):
        return
    table_name = entry.get("table") or entry.get("table_name")
    column_name = entry.get("column") or entry.get("column_name")
    value = entry.get("value")
    if value is None:
        value = {
            key: payload
            for key, payload in entry.items()
            if key
            not in {
                "id",
                "name",
                "title",
                "scope",
                "table",
                "table_name",
                "column",
                "column_name",
                "item_type",
                "status",
                "confidence",
                "source",
                "evidence",
            }
        }
    item_id = entry.get("id") or _slugged_id(
        item_type,
        table_name,
        column_name,
        entry.get("name") or entry.get("title") or item_type,
    )
    items[item_id] = {
        "id": item_id,
        "project_id": project_id,
        "source_name": source_name,
        "item_type": item_type,
        "scope": entry.get("scope") or _scope_for(table_name, column_name),
        "name": entry.get("name") or str(value.get("name") or item_type),
        "title": entry.get("title"),
        "table_name": table_name,
        "column_name": column_name,
        "value": value or {},
        "status": entry.get("status") or "approved",
        "confidence": float(entry.get("confidence") or 1.0),
        "source": "import",
        "evidence": entry.get("evidence") or [],
    }


def _merge_column_semantics(
    items: OrderedDict,
    *,
    project_id: str,
    source_name: str | None,
    entry: dict,
) -> None:
    table_name = entry.get("table")
    column_name = entry.get("column")
    if not table_name or not column_name:
        return
    item_id = f"column_semantics:{table_name}.{column_name}"
    existing = items.get(item_id)
    base_value = dict((existing or {}).get("value") or {})
    for key in (
        "semantic_type",
        "role",
        "description",
        "dtype",
        "nullable",
        "is_primary_key",
    ):
        if key in entry and entry.get(key) is not None:
            base_value[key] = entry.get(key)
    item = {
        "id": item_id,
        "project_id": project_id,
        "source_name": source_name,
        "item_type": "column_semantics",
        "scope": "column",
        "name": column_name,
        "title": f"Column semantics: {table_name}.{column_name}",
        "table_name": table_name,
        "column_name": column_name,
        "value": base_value,
        "status": entry.get("status") or (existing or {}).get("status") or "approved",
        "confidence": float(
            entry.get("confidence")
            or (existing or {}).get("confidence")
            or 1.0
        ),
        "source": "import",
        "evidence": entry.get("evidence") or (existing or {}).get("evidence") or [],
    }
    items[item_id] = item


def _parse_term_target(term: str | None) -> tuple[str, str] | None:
    if not term or "." not in term:
        return None
    table_name, column_name = term.split(".", 1)
    if not table_name or not column_name:
        return None
    return table_name, column_name


def _default_source_name(project: dict) -> str | None:
    sources = project.get("sources") or []
    if sources:
        return sources[0]
    return project.get("id")


def _scope_for(table_name: str | None, column_name: str | None) -> str:
    if column_name:
        return "column"
    if table_name:
        return "table"
    return "project"


def _slugged_id(prefix: str, *parts: str | None) -> str:
    tokens = []
    for part in parts:
        if not part:
            continue
        normalized = re.sub(r"[^a-z0-9]+", "-", str(part).lower()).strip("-")
        if normalized:
            tokens.append(normalized)
    return f"{prefix}:{':'.join(tokens) if tokens else 'item'}"
