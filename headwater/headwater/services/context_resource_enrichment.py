"""Deterministic enrichment of project context from attached resources."""

from __future__ import annotations

import csv
import io
import re
from pathlib import Path

import yaml

from headwater.analyzer.companion import match_docs_to_tables
from headwater.core.models import CompanionDoc
from headwater.services.resource_safety import classified_resource_metadata

_DOC_TYPE_BY_SUFFIX = {
    ".md": "markdown",
    ".markdown": "markdown",
    ".txt": "text",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".csv": "csv",
}
_RESOURCE_SOURCES = {"user", "import", "bootstrap"}


def enrich_project_context_resource(
    store,
    project_id: str,
    resource: dict,
) -> dict:
    """Extract context proposals from a stored project resource."""
    resource = _with_resource_safety_metadata(resource)
    items = store.list_project_context_items(project_id)
    content = _resource_content(resource)
    if not content:
        return {"resource": resource, "items_created": 0, "questions_created": 0}

    table_names = sorted(
        {
            item.get("table_name")
            for item in items
            if item.get("table_name")
        }
    )
    doc = CompanionDoc(
        filename=_resource_filename(resource),
        content=content,
        doc_type=_resource_doc_type(resource),
    )
    match_docs_to_tables([doc], table_names)

    column_index: dict[str, list[tuple[str, str]]] = {}
    existing_by_id = {item["id"]: item for item in items}
    for item in items:
        if item.get("item_type") != "column_semantics":
            continue
        table_name = item.get("table_name")
        column_name = item.get("column_name")
        if not table_name or not column_name:
            continue
        column_index.setdefault(column_name.lower(), []).append((table_name, column_name))

    created = 0
    questions_created = 0
    enriched_ids: list[str] = []
    conflict_ids: list[str] = []

    for term, definition in _extract_glossary_terms(content, doc.doc_type, table_names):
        target = _resolve_column_target(term, column_index)
        if target is None:
            item_id = f"glossary:{_slug(term)}"
            payload = {
                "id": item_id,
                "project_id": project_id,
                "source_name": resource.get("source_name"),
                "item_type": "glossary_term",
                "scope": "project",
                "name": term,
                "title": f"Glossary term: {term}",
                "value": {
                    "definition": definition,
                    "resource_id": resource.get("id"),
                },
                "status": "proposed",
                "confidence": 0.74,
                "source": "resource_enrichment",
                "evidence": [
                    {
                        "evidence_type": "resource",
                        "source": resource.get("title") or resource.get("id") or "resource",
                        "summary": f"Extracted glossary term '{term}' from attached resource.",
                        "payload": {"resource_id": resource.get("id")},
                    }
                ],
            }
            if _upsert_item(store, existing_by_id, payload):
                created += 1
                enriched_ids.append(item_id)
            continue

        table_name, column_name = target
        item_id = f"column_semantics:{table_name}.{column_name}"
        existing = existing_by_id.get(item_id)
        if existing and existing.get("status") in {"approved", "locked"}:
            conflict_id = _conflict_question_id(resource, table_name, column_name)
            if _upsert_item(
                store,
                existing_by_id,
                {
                    "id": conflict_id,
                    "project_id": project_id,
                    "source_name": resource.get("source_name"),
                    "item_type": "open_question",
                    "scope": "column",
                    "name": f"{column_name}_resource_conflict",
                    "title": f"Review resource conflict for {table_name}.{column_name}",
                    "table_name": table_name,
                    "column_name": column_name,
                    "value": {
                        "question": (
                            f"Resource '{resource.get('title') or resource.get('id')}' suggests "
                            f"a different meaning for {table_name}.{column_name}. Confirm whether "
                            "the reviewed description should change."
                        ),
                        "suggested_description": definition,
                        "resource_id": resource.get("id"),
                    },
                    "status": "needs_review",
                    "confidence": 0.55,
                    "source": "resource_enrichment",
                    "evidence": [
                        {
                            "evidence_type": "resource_conflict",
                            "source": resource.get("title") or resource.get("id") or "resource",
                            "summary": (
                                "Attached resource conflicts with approved "
                                "or locked context."
                            ),
                            "payload": {"resource_id": resource.get("id")},
                        }
                    ],
                },
            ):
                questions_created += 1
                conflict_ids.append(conflict_id)
            continue

        current_value = dict((existing or {}).get("value") or {})
        current_value["description"] = definition
        current_value["resource_id"] = resource.get("id")
        payload = {
            "id": item_id,
            "project_id": project_id,
            "source_name": resource.get("source_name"),
            "item_type": "column_semantics",
            "scope": "column",
            "name": column_name,
            "title": f"Column semantics: {table_name}.{column_name}",
            "table_name": table_name,
            "column_name": column_name,
            "value": current_value,
            "status": "proposed",
            "confidence": 0.83,
            "source": "resource_enrichment",
            "evidence": [
                {
                    "evidence_type": "resource",
                    "source": resource.get("title") or resource.get("id") or "resource",
                    "summary": (
                        f"Description for {table_name}.{column_name} "
                        "extracted from attached resource."
                    ),
                    "payload": {"resource_id": resource.get("id")},
                }
            ],
        }
        if _upsert_item(store, existing_by_id, payload):
            created += 1
            enriched_ids.append(item_id)

    for question in _extract_open_questions(content, table_names):
        item_id = (
            f"resource-question:{_slug(resource.get('id') or 'resource')}:"
            f"{_slug(question['question'])}"
        )
        payload = {
            "id": item_id,
            "project_id": project_id,
            "source_name": resource.get("source_name"),
            "item_type": "open_question",
            "scope": "table" if question.get("table_name") else "project",
            "name": item_id.rsplit(":", 1)[-1],
            "title": "Question extracted from attached resource",
            "table_name": question.get("table_name"),
            "value": {
                "question": question["question"],
                "resource_id": resource.get("id"),
            },
            "status": "needs_review",
            "confidence": 0.52,
            "source": "resource_enrichment",
            "evidence": [
                {
                    "evidence_type": "resource_question",
                    "source": resource.get("title") or resource.get("id") or "resource",
                    "summary": "Question extracted from attached resource.",
                    "payload": {"resource_id": resource.get("id")},
                }
            ],
        }
        if _upsert_item(store, existing_by_id, payload):
            questions_created += 1

    metadata = dict(resource.get("metadata") or {})
    metadata["matched_tables"] = doc.matched_tables
    metadata["resource_doc_type"] = doc.doc_type
    metadata["enrichment"] = {
        "items_created": created,
        "questions_created": questions_created,
        "enriched_item_ids": enriched_ids,
        "conflict_item_ids": conflict_ids,
    }
    store.upsert_project_context_resource(
        id=resource["id"],
        project_id=resource["project_id"],
        source_name=resource.get("source_name"),
        resource_type=resource["resource_type"],
        title=resource["title"],
        location=resource.get("location"),
        status=resource.get("status") or "active",
        source=resource.get("source") or "user",
        metadata=metadata,
    )
    updated_resource = next(
        (
            entry
            for entry in store.list_project_context_resources(project_id)
            if entry["id"] == resource["id"]
        ),
        resource,
    )
    return {
        "resource": updated_resource,
        "items_created": created,
        "questions_created": questions_created,
    }


def _with_resource_safety_metadata(resource: dict) -> dict:
    metadata = classified_resource_metadata(
        resource.get("metadata") or {},
        content=(resource.get("metadata") or {}).get("content"),
    )
    if metadata == (resource.get("metadata") or {}):
        return resource
    updated = dict(resource)
    updated["metadata"] = metadata
    return updated


def _upsert_item(store, existing_by_id: dict[str, dict], payload: dict) -> bool:
    existing = existing_by_id.get(payload["id"])
    if existing is None:
        store.upsert_project_context_item(**payload)
        existing_by_id[payload["id"]] = store.get_project_context_item(
            payload["id"],
            project_id=payload["project_id"],
        ) or payload
        return True

    next_status = existing.get("status")
    if next_status in {"approved", "locked"} and payload["item_type"] != "open_question":
        return False
    next_value = dict(existing.get("value") or {})
    next_value.update(payload.get("value") or {})
    store.update_project_context_item(
        payload["id"],
        project_id=payload["project_id"],
        status=payload.get("status") or existing.get("status"),
        value=next_value,
        name=payload.get("name") or existing.get("name"),
        title=payload.get("title") or existing.get("title"),
        confidence=max(
            float(existing.get("confidence") or 0.0),
            float(payload.get("confidence") or 0.0),
        ),
        source=payload.get("source") or existing.get("source"),
        evidence=payload.get("evidence") or existing.get("evidence") or [],
    )
    existing_by_id[payload["id"]] = store.get_project_context_item(
        payload["id"],
        project_id=payload["project_id"],
    ) or payload
    return True


def _resource_content(resource: dict) -> str | None:
    metadata = resource.get("metadata") or {}
    if isinstance(metadata.get("content"), str) and metadata["content"].strip():
        return metadata["content"]

    location = resource.get("location")
    if not isinstance(location, str) or not location.strip():
        return None

    path = Path(location).expanduser()
    if not path.exists() or not path.is_file():
        return None
    return path.read_text(encoding="utf-8", errors="replace")


def _resource_doc_type(resource: dict) -> str:
    resource_type = str(resource.get("resource_type") or "").lower()
    if resource_type in {"markdown", "text", "yaml", "csv"}:
        return resource_type
    location = str(resource.get("location") or "")
    return _DOC_TYPE_BY_SUFFIX.get(Path(location).suffix.lower(), "text")


def _resource_filename(resource: dict) -> str:
    location = str(resource.get("location") or "")
    if location:
        return Path(location).name
    return str(resource.get("title") or resource.get("id") or "resource")


def _extract_glossary_terms(
    content: str,
    doc_type: str,
    table_names: list[str],
) -> list[tuple[str, str]]:
    if doc_type == "csv":
        return _glossary_terms_from_csv(content)
    if doc_type == "yaml":
        return _glossary_terms_from_yaml(content)
    return _glossary_terms_from_text(content, table_names)


def _glossary_terms_from_csv(content: str) -> list[tuple[str, str]]:
    reader = csv.DictReader(io.StringIO(content))
    pairs: list[tuple[str, str]] = []
    for row in reader:
        lowered = {str(key).strip().lower(): value for key, value in row.items()}
        table_name = lowered.get("table") or lowered.get("table_name") or lowered.get("model")
        term = (
            lowered.get("column_name")
            or lowered.get("field")
            or lowered.get("field_name")
            or lowered.get("term")
        )
        definition = (
            lowered.get("description")
            or lowered.get("definition")
            or lowered.get("meaning")
            or lowered.get("notes")
        )
        if _valid_term(term, definition):
            normalized_term = str(term).strip()
            if table_name:
                normalized_term = f"{str(table_name).strip()}.{normalized_term}"
            pairs.append((normalized_term, _normalize_definition(str(definition))))
    return pairs


def _glossary_terms_from_yaml(content: str) -> list[tuple[str, str]]:
    try:
        parsed = yaml.safe_load(content) or {}
    except yaml.YAMLError:
        return []
    pairs: list[tuple[str, str]] = []
    if _extract_structured_yaml_pairs(parsed, pairs):
        return pairs
    _walk_yaml_pairs(parsed, pairs)
    return pairs


def _walk_yaml_pairs(value, pairs: list[tuple[str, str]], prefix: str = "") -> None:
    if isinstance(value, dict):
        description = value.get("description") or value.get("definition") or value.get("meaning")
        if prefix and isinstance(description, str) and description.strip():
            pairs.append((prefix, _normalize_definition(description)))
        for key, nested in value.items():
            next_prefix = str(key) if not prefix else f"{prefix}.{key}"
            _walk_yaml_pairs(nested, pairs, next_prefix)
    elif isinstance(value, list):
        for entry in value:
            _walk_yaml_pairs(entry, pairs, prefix)


def _glossary_terms_from_text(content: str, table_names: list[str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    current_table: str | None = None
    for line in content.splitlines():
        stripped = line.strip().strip("|")
        if not stripped or len(stripped) > 300:
            continue
        if stripped.startswith("#"):
            heading = stripped.lstrip("#").strip().lower()
            current_table = next(
                (table_name for table_name in table_names if table_name.lower() == heading),
                None,
            )
            continue
        match = re.match(r"([A-Za-z_][\w .-]{1,80})\s*[:\-]\s*(.{8,240})$", stripped)
        if match:
            term = match.group(1).strip()
            definition = _normalize_definition(match.group(2))
            if _valid_term(term, definition):
                if "." not in term and current_table:
                    term = f"{current_table}.{term}"
                pairs.append((term, definition))
    return pairs


def _extract_structured_yaml_pairs(parsed, pairs: list[tuple[str, str]]) -> bool:
    found = False
    if isinstance(parsed, dict) and isinstance(parsed.get("models"), list):
        for model in parsed["models"]:
            found |= _extract_table_yaml_pairs(model, pairs)
    elif isinstance(parsed, dict):
        for table_name, value in parsed.items():
            if not isinstance(value, dict):
                continue
            found |= _extract_table_yaml_pairs({"name": table_name, **value}, pairs)
    elif isinstance(parsed, list):
        for entry in parsed:
            found |= _extract_table_yaml_pairs(entry, pairs)
    return found


def _extract_table_yaml_pairs(entry, pairs: list[tuple[str, str]]) -> bool:
    if not isinstance(entry, dict):
        return False
    table_name = entry.get("name") or entry.get("table") or entry.get("table_name")
    columns = entry.get("columns")
    if not table_name or not columns:
        return False

    found = False
    if isinstance(columns, list):
        for column in columns:
            if not isinstance(column, dict):
                continue
            column_name = column.get("name") or column.get("column")
            description = (
                column.get("description")
                or column.get("definition")
                or column.get("meaning")
            )
            if _valid_term(column_name, description):
                pairs.append(
                    (
                        f"{str(table_name).strip()}.{str(column_name).strip()}",
                        _normalize_definition(str(description)),
                    )
                )
                found = True
    elif isinstance(columns, dict):
        for column_name, value in columns.items():
            if not isinstance(value, dict):
                continue
            description = (
                value.get("description")
                or value.get("definition")
                or value.get("meaning")
            )
            if _valid_term(column_name, description):
                pairs.append(
                    (
                        f"{str(table_name).strip()}.{str(column_name).strip()}",
                        _normalize_definition(str(description)),
                    )
                )
                found = True
    return found


def _valid_term(term, definition) -> bool:
    if not term or not definition:
        return False
    normalized = str(term).strip()
    if not normalized or len(normalized) > 120:
        return False
    return len(str(definition).strip()) >= 8


def _extract_open_questions(content: str, table_names: list[str]) -> list[dict]:
    questions: list[dict] = []
    for line in content.splitlines():
        stripped = line.strip().lstrip("-*").strip()
        if not stripped:
            continue
        if "?" not in stripped and not stripped.lower().startswith(
            ("confirm ", "clarify ", "verify ")
        ):
            continue
        question = stripped if stripped.endswith("?") else f"{stripped}?"
        table_name = next((name for name in table_names if name.lower() in question.lower()), None)
        questions.append({"question": question[:240], "table_name": table_name})
    return questions[:5]


def _resolve_column_target(
    term: str,
    column_index: dict[str, list[tuple[str, str]]],
) -> tuple[str, str] | None:
    normalized = term.strip().lower()
    if "." in normalized:
        table_name, column_name = normalized.split(".", 1)
        candidates = column_index.get(column_name)
        if not candidates:
            return None
        for candidate in candidates:
            if candidate[0].lower() == table_name:
                return candidate
        return None
    candidates = column_index.get(normalized)
    if not candidates or len(candidates) != 1:
        return None
    return candidates[0]


def _normalize_definition(value: str) -> str:
    return " ".join(value.strip().split())


def _slug(value: str) -> str:
    tokens = [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]
    return "-".join(tokens) or "item"


def _conflict_question_id(resource: dict, table_name: str, column_name: str) -> str:
    return (
        f"resource-conflict:{_slug(resource.get('id') or 'resource')}:"
        f"{table_name}.{column_name}"
    )
