"""Bootstrap canonical project context from discovery results."""

from __future__ import annotations

import re
from collections import Counter

from headwater.core.models import (
    ColumnProfile,
    DiscoveryResult,
    ProjectContextBundle,
    ProjectContextItem,
    ProjectContextResource,
)


def bootstrap_project_context(
    discovery: DiscoveryResult,
    *,
    project_id: str | None = None,
) -> ProjectContextBundle:
    """Translate discovery output into project-scoped context proposals."""
    project_id = project_id or discovery.source.name
    source_name = discovery.source.name
    profiles = {(p.table_name, p.column_name): p for p in discovery.profiles}
    lookup_tables = {}
    for table in discovery.tables:
        lookup = _lookup_summary(table, profiles)
        if lookup is not None:
            lookup_tables[table.name] = lookup

    items: list[ProjectContextItem] = [
        ProjectContextItem(
            id=f"dataset_summary:{source_name}",
            project_id=project_id,
            source_name=source_name,
            item_type="dataset_summary",
            scope="project",
            name="dataset_summary",
            title="Dataset summary",
            confidence=0.95,
            value={
                "table_count": len(discovery.tables),
                "profile_count": len(discovery.profiles),
                "relationship_count": len(discovery.relationships),
                "largest_tables": [
                    {"table_name": table.name, "row_count": table.row_count}
                    for table in sorted(
                        discovery.tables,
                        key=lambda table: table.row_count,
                        reverse=True,
                    )[:5]
                ],
                "lookup_candidates": sorted(lookup_tables),
            },
            evidence=[
                {
                    "evidence_type": "discovery",
                    "source": "bootstrap",
                    "summary": "Derived from discovered tables, profiles, and relationships.",
                    "payload": {
                        "tables": len(discovery.tables),
                        "profiles": len(discovery.profiles),
                        "relationships": len(discovery.relationships),
                    },
                }
            ],
        )
    ]

    for table in discovery.tables:
        table_profiles = {
            column.name: profiles.get((table.name, column.name))
            for column in table.columns
        }
        temporal_columns = [
            column.name for column in table.columns if _is_temporal_dtype(column.dtype)
        ]
        string_columns = [column.name for column in table.columns if _is_string_dtype(column.dtype)]
        numeric_columns = [
            column.name for column in table.columns if _is_numeric_dtype(column.dtype)
        ]
        identifier_columns = [
            column.name
            for column in table.columns
            if column.is_primary_key or (column.semantic_type in {"id", "foreign_key"})
        ]
        items.append(
            ProjectContextItem(
                id=f"table_profile:{table.name}",
                project_id=project_id,
                source_name=source_name,
                item_type="table_profile",
                scope="table",
                name=table.name,
                title=f"Table profile: {table.name}",
                table_name=table.name,
                confidence=0.92,
                value={
                    "row_count": table.row_count,
                    "column_count": len(table.columns),
                    "temporal_columns": temporal_columns,
                    "string_columns": string_columns,
                    "numeric_columns": numeric_columns,
                    "identifier_columns": identifier_columns,
                    "lookup_candidate": table.name in lookup_tables,
                    "review_status": table.review_status,
                },
                evidence=[
                    {
                        "evidence_type": "table_shape",
                        "source": "bootstrap",
                        "summary": "Summarized from table schema and profile coverage.",
                        "payload": {
                            "row_count": table.row_count,
                            "column_count": len(table.columns),
                        },
                    }
                ],
            )
        )

        if table.name in lookup_tables:
            lookup = lookup_tables[table.name]
            items.append(
                ProjectContextItem(
                    id=f"lookup:{table.name}",
                    project_id=project_id,
                    source_name=source_name,
                    item_type="lookup",
                    scope="table",
                    name=table.name,
                    title=f"Lookup candidate: {table.name}",
                    table_name=table.name,
                    confidence=lookup["confidence"],
                    value=lookup,
                    evidence=[
                        {
                            "evidence_type": "lookup_shape",
                            "source": "bootstrap",
                            "summary": "Small table with identifier and label-like columns.",
                            "payload": lookup,
                        }
                    ],
                )
            )

        if not any(column.is_primary_key for column in table.columns):
            items.append(
                ProjectContextItem(
                    id=f"question:grain:{table.name}",
                    project_id=project_id,
                    source_name=source_name,
                    item_type="open_question",
                    scope="table",
                    name=f"{table.name}_grain",
                    title=f"Confirm row grain for {table.name}",
                    table_name=table.name,
                    confidence=0.4,
                    value={
                        "question": f"Which columns define the business grain for '{table.name}'?",
                        "reason": "No explicit primary key was detected during discovery.",
                    },
                    evidence=[
                        {
                            "evidence_type": "missing_primary_key",
                            "source": "bootstrap",
                            "summary": "No primary key is marked on this table.",
                            "payload": {"table_name": table.name},
                        }
                    ],
                )
            )

        if len(temporal_columns) > 1:
            items.append(
                ProjectContextItem(
                    id=f"question:temporal:{table.name}",
                    project_id=project_id,
                    source_name=source_name,
                    item_type="open_question",
                    scope="table",
                    name=f"{table.name}_canonical_time",
                    title=f"Confirm canonical time for {table.name}",
                    table_name=table.name,
                    confidence=0.45,
                    value={
                        "question": (
                            "Which temporal column should be treated as the "
                            f"canonical event time for '{table.name}'?"
                        ),
                        "candidates": temporal_columns,
                    },
                    evidence=[
                        {
                            "evidence_type": "multiple_temporal_columns",
                            "source": "bootstrap",
                            "summary": "More than one temporal column was detected on the table.",
                            "payload": {"columns": temporal_columns},
                        }
                    ],
                )
            )

        for column in table.columns:
            profile = table_profiles.get(column.name)
            items.append(
                ProjectContextItem(
                    id=f"column_semantics:{table.name}.{column.name}",
                    project_id=project_id,
                    source_name=source_name,
                    item_type="column_semantics",
                    scope="column",
                    name=column.name,
                    title=f"Column semantics: {table.name}.{column.name}",
                    table_name=table.name,
                    column_name=column.name,
                    confidence=_column_confidence(column, profile),
                    value={
                        "dtype": column.dtype,
                        "nullable": column.nullable,
                        "is_primary_key": column.is_primary_key,
                        "semantic_type": column.semantic_type,
                        "role": column.role,
                        "description": column.description,
                        "profile": _profile_payload(profile),
                    },
                    evidence=_column_evidence(column, profile),
                )
            )

    for relationship in discovery.relationships:
        items.append(
            ProjectContextItem(
                id=(
                    "relationship:"
                    f"{relationship.from_table}.{relationship.from_column}"
                    f"->{relationship.to_table}.{relationship.to_column}"
                ),
                project_id=project_id,
                source_name=source_name,
                item_type="relationship",
                scope="relationship",
                name=(
                    f"{relationship.from_table}.{relationship.from_column}"
                    f"->{relationship.to_table}.{relationship.to_column}"
                ),
                title="Relationship proposal",
                confidence=relationship.confidence,
                value={
                    "from_table": relationship.from_table,
                    "from_column": relationship.from_column,
                    "to_table": relationship.to_table,
                    "to_column": relationship.to_column,
                    "relationship_type": relationship.type,
                    "referential_integrity": relationship.referential_integrity,
                    "detection_source": relationship.source,
                },
                evidence=[
                    {
                        "evidence_type": "relationship",
                        "source": relationship.source,
                        "summary": "Detected relationship from schema or value overlap.",
                        "payload": {
                            "confidence": relationship.confidence,
                            "referential_integrity": relationship.referential_integrity,
                        },
                    }
                ],
            )
        )

    resources = [
        ProjectContextResource(
            id=f"resource:{source_name}:{_slug(doc.filename)}",
            project_id=project_id,
            source_name=source_name,
            resource_type=doc.doc_type,
            title=doc.filename,
            location=doc.filename,
            metadata={
                "matched_tables": doc.matched_tables,
                "confidence": doc.confidence,
            },
        )
        for doc in discovery.companion_docs
    ]

    return ProjectContextBundle(
        project_id=project_id,
        source_names=[source_name],
        items=items,
        resources=resources,
    )


def _column_confidence(column, profile: ColumnProfile | None) -> float:
    confidence = float(column.confidence or 0.0)
    if column.is_primary_key:
        confidence = max(confidence, 0.95)
    elif column.semantic_type or column.role:
        confidence = max(confidence, 0.65)
    elif profile and profile.detected_pattern:
        confidence = max(confidence, 0.55)
    elif profile and profile.uniqueness_ratio >= 0.98:
        confidence = max(confidence, 0.5)
    return min(confidence, 1.0)


def _column_evidence(column, profile: ColumnProfile | None) -> list[dict]:
    evidence = [
        {
            "evidence_type": "schema",
            "source": "bootstrap",
            "summary": "Derived from the physical column definition.",
            "payload": {
                "dtype": column.dtype,
                "nullable": column.nullable,
                "is_primary_key": column.is_primary_key,
            },
        }
    ]
    if column.semantic_type or column.role:
        evidence.append(
            {
                "evidence_type": "classification",
                "source": "analyzer",
                "summary": "Generic analyzer classification from structural and profile signals.",
                "payload": {
                    "semantic_type": column.semantic_type,
                    "role": column.role,
                    "confidence": column.confidence,
                },
            }
        )
    if profile is not None:
        evidence.append(
            {
                "evidence_type": "profile",
                "source": "profiler",
                "summary": "Observed value distribution and completeness statistics.",
                "payload": _profile_payload(profile),
            }
        )
    return evidence


def _profile_payload(profile: ColumnProfile | None) -> dict:
    if profile is None:
        return {}
    top_values = profile.top_values[:5] if profile.top_values else []
    return {
        "null_rate": profile.null_rate,
        "distinct_count": profile.distinct_count,
        "uniqueness_ratio": profile.uniqueness_ratio,
        "detected_pattern": profile.detected_pattern,
        "min_value": profile.min_value,
        "max_value": profile.max_value,
        "min_date": profile.min_date,
        "max_date": profile.max_date,
        "top_values": top_values,
    }


def _lookup_summary(table, profiles: dict[tuple[str, str], ColumnProfile | None]) -> dict | None:
    if not (1 < len(table.columns) <= 4):
        return None
    id_candidates = [
        column.name
        for column in table.columns
        if column.is_primary_key or column.semantic_type in {"id", "foreign_key"}
    ]
    if len(id_candidates) != 1:
        return None
    label_candidates = [
        column.name
        for column in table.columns
        if _is_string_dtype(column.dtype)
        and column.name != id_candidates[0]
        and _looks_like_label_profile(profiles.get((table.name, column.name)))
    ]
    if not label_candidates:
        return None
    return {
        "key_column": id_candidates[0],
        "label_column": label_candidates[0],
        "row_count": table.row_count,
        "column_count": len(table.columns),
        "confidence": 0.82 if table.row_count <= 10_000 else 0.72,
    }


def _looks_like_label_profile(profile: ColumnProfile | None) -> bool:
    if profile is None:
        return True
    if profile.detected_pattern in {"email", "url", "phone"}:
        return False
    avg_length = profile.avg_length or 0.0
    if avg_length and avg_length > 120:
        return False
    return not (profile.distinct_count and profile.distinct_count <= 1)


def _is_numeric_dtype(dtype: str | None) -> bool:
    normalized = (dtype or "").lower()
    return any(token in normalized for token in ("int", "float", "double", "decimal", "numeric"))


def _is_string_dtype(dtype: str | None) -> bool:
    normalized = (dtype or "").lower()
    return any(token in normalized for token in ("char", "text", "string", "varchar"))


def _is_temporal_dtype(dtype: str | None) -> bool:
    normalized = (dtype or "").lower()
    return any(token in normalized for token in ("date", "time", "timestamp"))


def _slug(value: str) -> str:
    tokens = [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]
    counts = Counter(tokens)
    return "-".join(token for token in tokens if counts[token] >= 1) or "resource"
