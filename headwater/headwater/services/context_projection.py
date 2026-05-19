"""Project context export helpers built from the canonical metadata store."""

from __future__ import annotations

from collections import Counter

import yaml


def build_context_exports(payload: dict, *, include_proposed: bool = True) -> dict[str, str]:
    """Render canonical project context into machine and review projections."""
    items = [
        item
        for item in payload.get("items", [])
        if _include_item(item, include_proposed=include_proposed)
    ]
    resources = payload.get("resources", [])
    dataset_contexts = payload.get("dataset_contexts", [])
    project_id = payload.get("project_id")

    context_doc = {
        "version": 1,
        "project_id": project_id,
        "source_names": payload.get("source_names", []),
        "dataset_contexts": dataset_contexts,
        "summary": _summary(items),
        "dataset_summary": _first_item_value(items, "dataset_summary"),
        "open_questions": [
            _item_entry(item)
            for item in items
            if item.get("item_type") == "open_question"
        ],
    }
    semantic_types_doc = {
        "version": 1,
        "project_id": project_id,
        "columns": [
            _semantic_type_entry(item)
            for item in items
            if item.get("item_type") == "column_semantics"
        ],
    }
    semantic_schema_doc = {
        "version": 1,
        "project_id": project_id,
        "roles": [
            _semantic_role_entry(item)
            for item in items
            if item.get("item_type") == "column_semantics" and item.get("value", {}).get("role")
        ],
    }
    lookups_doc = {
        "version": 1,
        "project_id": project_id,
        "lookups": [_lookup_entry(item) for item in items if item.get("item_type") == "lookup"],
    }
    glossary_doc = {
        "version": 1,
        "project_id": project_id,
        "terms": [
            _glossary_entry(item)
            for item in items
            if item.get("item_type") == "column_semantics"
            and item.get("value", {}).get("description")
        ],
    }
    resources_doc = {
        "version": 1,
        "project_id": project_id,
        "resources": [_resource_entry(resource) for resource in resources],
    }

    return {
        "context.yaml": _yaml(context_doc),
        "semantic_types.yaml": _yaml(semantic_types_doc),
        "semantic_schema.yaml": _yaml(semantic_schema_doc),
        "lookups.yaml": _yaml(lookups_doc),
        "glossary.yaml": _yaml(glossary_doc),
        "resources.yaml": _yaml(resources_doc),
        "REVIEW.md": _review_markdown(
            project_id=project_id,
            dataset_contexts=dataset_contexts,
            items=items,
            resources=resources,
        ),
    }


def _include_item(item: dict, *, include_proposed: bool) -> bool:
    if include_proposed:
        return item.get("status") != "rejected"
    return item.get("status") in {"approved", "locked"}


def _summary(items: list[dict]) -> dict:
    item_types = Counter(item.get("item_type") or "unknown" for item in items)
    status_counts = Counter(item.get("status") or "unknown" for item in items)
    return {
        "item_count": len(items),
        "item_types": dict(sorted(item_types.items())),
        "status_counts": dict(sorted(status_counts.items())),
    }


def _first_item_value(items: list[dict], item_type: str) -> dict:
    for item in items:
        if item.get("item_type") == item_type:
            return item.get("value") or {}
    return {}


def _item_entry(item: dict) -> dict:
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "title": item.get("title"),
        "scope": item.get("scope"),
        "table": item.get("table_name"),
        "column": item.get("column_name"),
        "status": item.get("status"),
        "confidence": item.get("confidence"),
        "source": item.get("source"),
        "value": item.get("value") or {},
        "evidence": item.get("evidence") or [],
    }


def _semantic_type_entry(item: dict) -> dict:
    value = item.get("value") or {}
    return {
        "table": item.get("table_name"),
        "column": item.get("column_name"),
        "semantic_type": value.get("semantic_type"),
        "role": value.get("role"),
        "description": value.get("description"),
        "dtype": value.get("dtype"),
        "nullable": value.get("nullable"),
        "is_primary_key": value.get("is_primary_key"),
        "status": item.get("status"),
        "confidence": item.get("confidence"),
        "source": item.get("source"),
    }


def _semantic_role_entry(item: dict) -> dict:
    value = item.get("value") or {}
    return {
        "table": item.get("table_name"),
        "column": item.get("column_name"),
        "role": value.get("role"),
        "semantic_type": value.get("semantic_type"),
        "description": value.get("description"),
        "status": item.get("status"),
        "confidence": item.get("confidence"),
        "source": item.get("source"),
    }


def _lookup_entry(item: dict) -> dict:
    value = item.get("value") or {}
    return {
        "table": item.get("table_name"),
        "name": item.get("name"),
        "status": item.get("status"),
        "confidence": item.get("confidence"),
        "source": item.get("source"),
        **value,
    }


def _glossary_entry(item: dict) -> dict:
    value = item.get("value") or {}
    column_name = item.get("column_name") or item.get("name")
    return {
        "term": f"{item.get('table_name')}.{column_name}",
        "definition": value.get("description"),
        "semantic_type": value.get("semantic_type"),
        "role": value.get("role"),
        "status": item.get("status"),
        "confidence": item.get("confidence"),
        "source": item.get("source"),
    }


def _resource_entry(resource: dict) -> dict:
    return {
        "id": resource.get("id"),
        "type": resource.get("resource_type"),
        "title": resource.get("title"),
        "location": resource.get("location"),
        "status": resource.get("status"),
        "source": resource.get("source"),
        "metadata": resource.get("metadata") or {},
    }


def _yaml(doc: dict) -> str:
    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=False)


def _review_markdown(
    *,
    project_id: str | None,
    dataset_contexts: list[dict],
    items: list[dict],
    resources: list[dict],
) -> str:
    summary = _summary(items)
    lines = [
        f"# Project Context Review: {project_id}",
        "",
        "## Summary",
        f"- Item count: {summary['item_count']}",
        f"- Resource count: {len(resources)}",
        (
            "- Status counts: "
            + (
                ", ".join(
                    f"{key}={value}" for key, value in summary["status_counts"].items()
                )
                or "none"
            )
        ),
        "",
    ]

    if dataset_contexts:
        lines.extend(["## Dataset Context", ""])
        for context in dataset_contexts:
            source_name = context.get("source_name") or "unknown"
            row_represents = context.get("row_represents") or "not set"
            time_grain = context.get("time_grain") or "not set"
            lines.append(f"- {source_name}: row={row_represents}; time_grain={time_grain}")
        lines.append("")

    open_questions = [item for item in items if item.get("item_type") == "open_question"]
    lines.extend(["## Open Questions", ""])
    if open_questions:
        for item in open_questions:
            question = (
                (item.get("value") or {}).get("question")
                or item.get("title")
                or item.get("name")
            )
            lines.append(
                f"- [{item.get('status')}] {question} "
                f"(confidence={item.get('confidence', 0.0):.2f})"
            )
    else:
        lines.append("- None")
    lines.append("")

    semantic_items = [
        item for item in items if item.get("item_type") == "column_semantics"
    ]
    lines.extend(
        [
            "## Proposed Column Semantics",
            "",
            "| Table | Column | Semantic Type | Role | Status | Confidence |",
            "|---|---|---|---|---|---|",
        ]
    )
    for item in semantic_items[:50]:
        value = item.get("value") or {}
        lines.append(
            (
                "| {table} | {column} | {semantic_type} | {role} | "
                "{status} | {confidence:.2f} |"
            ).format(
                table=item.get("table_name") or "",
                column=item.get("column_name") or "",
                semantic_type=value.get("semantic_type") or "",
                role=value.get("role") or "",
                status=item.get("status") or "",
                confidence=float(item.get("confidence") or 0.0),
            )
        )
    if not semantic_items:
        lines.append("|  |  |  |  |  |  |")
    lines.append("")

    lines.extend(["## Resources", ""])
    if resources:
        for resource in resources:
            lines.append(
                f"- [{resource.get('status')}] {resource.get('title')} "
                f"({resource.get('resource_type')})"
            )
    else:
        lines.append("- None")
    lines.append("")
    return "\n".join(lines)
