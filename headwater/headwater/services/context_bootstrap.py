"""Bootstrap canonical project context from discovery results."""

from __future__ import annotations

import re
from collections import Counter

from headwater.analyzer.semantic_types import SemanticTypeEvidence, detect_semantic_types
from headwater.core.models import (
    ColumnProfile,
    DiscoveryResult,
    ProjectContextBundle,
    ProjectContextItem,
    ProjectContextResource,
)
from headwater.services.resource_safety import classified_resource_metadata


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
    items.append(
        ProjectContextItem(
            id=f"source_alias:{source_name}",
            project_id=project_id,
            source_name=source_name,
            item_type="source_alias",
            scope="source",
            name=source_name,
            title=f"Source alias: {source_name}",
            confidence=0.9,
            value={
                "source_names": [source_name],
                "aliases": [source_name],
            },
            evidence=[
                {
                    "evidence_type": "source_name",
                    "source": "bootstrap",
                    "summary": "Initial source alias from the discovered source name.",
                    "payload": {"source_name": source_name},
                }
            ],
        )
    )

    for table in discovery.tables:
        table_profiles = {
            column.name: profiles.get((table.name, column.name))
            for column in table.columns
        }
        temporal_columns = [
            column.name for column in table.columns if _is_temporal_column(column)
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
        pk_candidates = _primary_key_candidates(table, table_profiles)
        best_pk = pk_candidates[0] if pk_candidates else None
        row_grain_value = {
            "table": table.name,
            "columns": best_pk["columns"] if best_pk else [],
            "grain_type": "keyed_table" if best_pk else "unknown",
            "reason": (
                "Best key candidate from declared key or uniqueness profile."
                if best_pk
                else "No strong primary key candidate was detected."
            ),
            "candidate_count": len(pk_candidates),
        }
        items.append(
            ProjectContextItem(
                id=f"row_grain:{table.name}",
                project_id=project_id,
                source_name=source_name,
                item_type="row_grain",
                scope="table",
                name=table.name,
                title=f"Row grain proposal: {table.name}",
                table_name=table.name,
                confidence=best_pk["confidence"] if best_pk else 0.35,
                value=row_grain_value,
                evidence=[
                    {
                        "evidence_type": "row_grain",
                        "source": "bootstrap",
                        "summary": "Derived from declared keys and uniqueness profiles.",
                        "payload": row_grain_value,
                    }
                ],
            )
        )
        row_entity_value = {
            "table": table.name,
            "entity": _row_entity_name(table.name),
            "source": "table_name",
            "review_required": True,
        }
        items.append(
            ProjectContextItem(
                id=f"row_entity:{table.name}",
                project_id=project_id,
                source_name=source_name,
                item_type="row_entity",
                scope="table",
                name=table.name,
                title=f"Row entity proposal: {table.name}",
                table_name=table.name,
                confidence=0.42,
                value=row_entity_value,
                evidence=[
                    {
                        "evidence_type": "table_name",
                        "source": "bootstrap",
                        "summary": "Weak entity proposal from the table name.",
                        "payload": row_entity_value,
                    }
                ],
            )
        )
        for candidate in pk_candidates:
            column_key = "__".join(candidate["columns"])
            items.append(
                ProjectContextItem(
                    id=f"pk_candidate:{table.name}.{column_key}",
                    project_id=project_id,
                    source_name=source_name,
                    item_type="pk_candidate",
                    scope="table",
                    name=f"{table.name}.{column_key}",
                    title=f"Primary key candidate: {table.name}.{column_key}",
                    table_name=table.name,
                    column_name=candidate["columns"][0] if len(candidate["columns"]) == 1 else None,
                    confidence=candidate["confidence"],
                    value=candidate,
                    evidence=[
                        {
                            "evidence_type": "key_candidate",
                            "source": "bootstrap",
                            "summary": "Derived from declared key or uniqueness profile.",
                            "payload": candidate,
                        }
                    ],
                )
            )
        if temporal_columns:
            anchor_column = _time_anchor_column(table, table_profiles, temporal_columns)
            time_anchor_value = {
                "table": table.name,
                "column": anchor_column,
                "candidates": temporal_columns,
                "ambiguous": len(temporal_columns) > 1,
            }
            items.append(
                ProjectContextItem(
                    id=f"time_anchor:{table.name}",
                    project_id=project_id,
                    source_name=source_name,
                    item_type="time_anchor",
                    scope="table",
                    name=anchor_column,
                    title=f"Time anchor proposal: {table.name}.{anchor_column}",
                    table_name=table.name,
                    column_name=anchor_column,
                    confidence=0.72 if len(temporal_columns) == 1 else 0.55,
                    value=time_anchor_value,
                    evidence=[
                        {
                            "evidence_type": "temporal_column",
                            "source": "bootstrap",
                            "summary": "Derived from timestamp-like column types.",
                            "payload": time_anchor_value,
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
            semantic_evidence = detect_semantic_types(column.name, column.dtype, profile)
            enum_candidate = _enum_mapping_candidate(table.name, column, profile)
            if enum_candidate is not None:
                items.append(
                    ProjectContextItem(
                        id=f"enum_mapping:{table.name}.{column.name}",
                        project_id=project_id,
                        source_name=source_name,
                        item_type="enum_mapping",
                        scope="column",
                        name=column.name,
                        title=f"Enum mapping candidate: {table.name}.{column.name}",
                        table_name=table.name,
                        column_name=column.name,
                        confidence=enum_candidate["confidence"],
                        value=enum_candidate,
                        evidence=[
                            {
                                "evidence_type": "enum_shape",
                                "source": "bootstrap",
                                "summary": "Low-cardinality code-like column values need labels.",
                                "payload": enum_candidate,
                            }
                        ],
                    )
                )
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
                    confidence=_column_confidence(column, profile, semantic_evidence),
                    value={
                        "dtype": column.dtype,
                        "nullable": column.nullable,
                        "is_primary_key": column.is_primary_key,
                        "semantic_type": column.semantic_type,
                        "role": column.role,
                        "description": column.description,
                        "profile": _profile_payload(
                            profile,
                            redact_values=_has_sensitive_detection(semantic_evidence),
                        ),
                        "semantic_type_evidence": [
                            evidence.model_dump() for evidence in semantic_evidence
                        ],
                    },
                    evidence=_column_evidence(column, profile, semantic_evidence),
                )
            )
            policy = _sensitive_column_policy(table.name, column.name, semantic_evidence)
            if policy is not None:
                items.append(
                    ProjectContextItem(
                        id=f"column_policy:{table.name}.{column.name}",
                        project_id=project_id,
                        source_name=source_name,
                        item_type="column_policy",
                        scope="column",
                        name=column.name,
                        title=f"Column policy proposal: {table.name}.{column.name}",
                        table_name=table.name,
                        column_name=column.name,
                        confidence=policy["confidence"],
                        value=policy,
                        evidence=[
                            {
                                "evidence_type": "semantic_type_detection",
                                "source": "semantic_type_library",
                                "summary": (
                                    "Generic detector identified a likely sensitive "
                                    "format without storing raw sample values."
                                ),
                                "payload": {
                                    "semantic_type": policy["semantic_type"],
                                    "confidence": policy["confidence"],
                                },
                            }
                        ],
                    )
                )

    for relationship in discovery.relationships:
        fk_value = {
            "from_table": relationship.from_table,
            "from_column": relationship.from_column,
            "to_table": relationship.to_table,
            "to_column": relationship.to_column,
            "relationship_type": relationship.type,
            "referential_integrity": relationship.referential_integrity,
            "detection_source": relationship.source,
        }
        items.append(
            ProjectContextItem(
                id=(
                    "fk_candidate:"
                    f"{relationship.from_table}.{relationship.from_column}"
                    f"->{relationship.to_table}.{relationship.to_column}"
                ),
                project_id=project_id,
                source_name=source_name,
                item_type="fk_candidate",
                scope="relationship",
                name=(
                    f"{relationship.from_table}.{relationship.from_column}"
                    f"->{relationship.to_table}.{relationship.to_column}"
                ),
                title="Foreign key candidate",
                table_name=relationship.from_table,
                column_name=relationship.from_column,
                confidence=relationship.confidence,
                value=fk_value,
                evidence=[
                    {
                        "evidence_type": "relationship",
                        "source": relationship.source,
                        "summary": "Detected foreign-key-like relationship.",
                        "payload": fk_value,
                    }
                ],
            )
        )
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

    items.append(
        _cold_start_summary_item(
            discovery,
            project_id=project_id,
            source_name=source_name,
            profiles=profiles,
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
                **classified_resource_metadata({}),
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


def _column_confidence(
    column,
    profile: ColumnProfile | None,
    semantic_evidence: list[SemanticTypeEvidence] | None = None,
) -> float:
    confidence = float(column.confidence or 0.0)
    best_semantic = semantic_evidence[0] if semantic_evidence else None
    if column.is_primary_key:
        confidence = max(confidence, 0.95)
    elif column.semantic_type or column.role:
        confidence = max(confidence, 0.65)
    elif best_semantic is not None:
        confidence = max(confidence, min(best_semantic.confidence, 0.9))
    elif profile and profile.detected_pattern:
        confidence = max(confidence, 0.55)
    elif profile and profile.uniqueness_ratio >= 0.98:
        confidence = max(confidence, 0.5)
    return min(confidence, 1.0)


def _column_evidence(
    column,
    profile: ColumnProfile | None,
    semantic_evidence: list[SemanticTypeEvidence] | None = None,
) -> list[dict]:
    redact_values = _has_sensitive_detection(semantic_evidence or [])
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
                "payload": _profile_payload(profile, redact_values=redact_values),
            }
        )
    if semantic_evidence:
        evidence.append(
            {
                "evidence_type": "semantic_type_detection",
                "source": "semantic_type_library",
                "summary": "Generic format and distribution detectors.",
                "payload": {
                    "detections": [
                        detection.model_dump() for detection in semantic_evidence
                    ]
                },
            }
        )
    return evidence


def _profile_payload(profile: ColumnProfile | None, *, redact_values: bool = False) -> dict:
    if profile is None:
        return {}
    top_values = []
    if profile.top_values:
        top_values = (
            [{"redacted": True, "count": count} for _value, count in profile.top_values[:5]]
            if redact_values
            else profile.top_values[:5]
        )
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


def _has_sensitive_detection(semantic_evidence: list[SemanticTypeEvidence]) -> bool:
    return any(evidence.sensitive and evidence.confidence >= 0.75 for evidence in semantic_evidence)


def _sensitive_column_policy(
    table_name: str,
    column_name: str,
    semantic_evidence: list[SemanticTypeEvidence],
) -> dict | None:
    sensitive = [
        evidence
        for evidence in semantic_evidence
        if evidence.sensitive and evidence.confidence >= 0.75
    ]
    if not sensitive:
        return None
    best = sensitive[0]
    return {
        "table_name": table_name,
        "column_name": column_name,
        "policy": "sensitive",
        "semantic_type": best.semantic_type,
        "redaction": "mask",
        "allow_llm": False,
        "confidence": best.confidence,
        "reason": "Generic semantic type detector marked likely sensitive.",
    }


def _cold_start_summary_item(
    discovery: DiscoveryResult,
    *,
    project_id: str,
    source_name: str,
    profiles: dict[tuple[str, str], ColumnProfile],
) -> ProjectContextItem:
    summary = _cold_start_summary(discovery, profiles)
    return ProjectContextItem(
        id=f"cold_start_summary:{source_name}",
        project_id=project_id,
        source_name=source_name,
        item_type="cold_start_summary",
        scope="project",
        name="cold_start_summary",
        title="Cold-start summary",
        confidence=0.78,
        value=summary,
        evidence=[
            {
                "evidence_type": "cold_start",
                "source": "bootstrap",
                "summary": "Generic day-one summary from schema, profiles, and detectors.",
                "payload": {
                    "table_count": len(discovery.tables),
                    "dimension_candidates": len(summary["top_dimensions"]),
                    "measure_candidates": len(summary["top_measures"]),
                    "fallback_questions": len(summary["fallback_questions"]),
                },
            }
        ],
    )


def _cold_start_summary(
    discovery: DiscoveryResult,
    profiles: dict[tuple[str, str], ColumnProfile],
) -> dict:
    dimensions: list[dict] = []
    measures: list[dict] = []
    distributional_facts: list[dict] = []
    quality_risks: list[dict] = []
    sensitive_columns: list[dict] = []

    for table in discovery.tables:
        for column in table.columns:
            profile = profiles.get((table.name, column.name))
            semantic_evidence = detect_semantic_types(column.name, column.dtype, profile)
            best_semantic = semantic_evidence[0].semantic_type if semantic_evidence else None
            sensitive = _has_sensitive_detection(semantic_evidence)
            if sensitive:
                sensitive_columns.append(
                    {
                        "table_name": table.name,
                        "column_name": column.name,
                        "semantic_type": best_semantic,
                    }
                )
            if _is_dimension_candidate(column, profile, sensitive):
                dimensions.append(
                    {
                        "table_name": table.name,
                        "column_name": column.name,
                        "distinct_count": profile.distinct_count if profile else None,
                        "semantic_type": column.semantic_type or best_semantic,
                        "confidence": _dimension_confidence(column, profile, best_semantic),
                    }
                )
                distributional_facts.extend(
                    _distributional_facts(table.name, column.name, profile)
                )
            if _is_measure_candidate(column, profile, best_semantic):
                measures.append(
                    {
                        "table_name": table.name,
                        "column_name": column.name,
                        "semantic_type": column.semantic_type or best_semantic or "measure",
                        "min_value": profile.min_value if profile else None,
                        "max_value": profile.max_value if profile else None,
                        "confidence": _measure_confidence(column, profile, best_semantic),
                    }
                )
            quality_risks.extend(_quality_risks(table.name, column.name, profile))

    return {
        "top_dimensions": sorted(
            dimensions,
            key=lambda item: (-float(item["confidence"]), item["table_name"], item["column_name"]),
        )[:5],
        "top_measures": sorted(
            measures,
            key=lambda item: (-float(item["confidence"]), item["table_name"], item["column_name"]),
        )[:5],
        "distributional_facts": distributional_facts[:5],
        "quality_risks": quality_risks[:5],
        "sensitive_columns": sensitive_columns[:5],
        "fallback_questions": _fallback_questions(discovery, dimensions, measures),
    }


def _is_dimension_candidate(column, profile: ColumnProfile | None, sensitive: bool) -> bool:
    if sensitive or column.is_primary_key or column.semantic_type in {"id", "foreign_key"}:
        return False
    if column.semantic_type == "dimension" or column.role == "dimension":
        return True
    if not _is_string_dtype(column.dtype):
        return False
    if profile is None:
        return True
    return 1 < profile.distinct_count <= 100


def _is_measure_candidate(
    column,
    profile: ColumnProfile | None,
    semantic_type: str | None,
) -> bool:
    if column.is_primary_key or column.semantic_type in {"id", "foreign_key", "dimension"}:
        return False
    if semantic_type in {"latitude", "longitude", "postal_code", "country_code"}:
        return False
    if column.semantic_type == "metric" or column.role == "metric":
        return True
    return _is_numeric_dtype(column.dtype) and not _looks_like_identifier_name(column.name)


def _dimension_confidence(
    column,
    profile: ColumnProfile | None,
    semantic_type: str | None,
) -> float:
    confidence = 0.55
    if column.semantic_type == "dimension" or column.role == "dimension":
        confidence = 0.78
    if semantic_type in {"currency_code", "country_code", "postal_code"}:
        confidence = max(confidence, 0.72)
    if profile and 1 < profile.distinct_count <= 30:
        confidence = max(confidence, 0.7)
    return round(confidence, 3)


def _measure_confidence(column, profile: ColumnProfile | None, semantic_type: str | None) -> float:
    confidence = 0.62
    if column.semantic_type == "metric" or column.role == "metric":
        confidence = 0.82
    if semantic_type == "monetary_amount":
        confidence = max(confidence, 0.74)
    if profile and profile.distinct_count > 5:
        confidence = max(confidence, 0.68)
    return round(confidence, 3)


def _distributional_facts(
    table_name: str,
    column_name: str,
    profile: ColumnProfile | None,
) -> list[dict]:
    if profile is None or not profile.top_values:
        return []
    sample_size = sum(int(count) for _value, count in profile.top_values)
    if sample_size <= 0:
        return []
    value, count = profile.top_values[0]
    return [
        {
            "table_name": table_name,
            "column_name": column_name,
            "fact_type": "top_value",
            "value": str(value),
            "count": int(count),
            "share": round(int(count) / sample_size, 3),
        }
    ]


def _quality_risks(
    table_name: str,
    column_name: str,
    profile: ColumnProfile | None,
) -> list[dict]:
    if profile is None:
        return []
    risks: list[dict] = []
    if profile.null_rate >= 0.2:
        risks.append(
            {
                "table_name": table_name,
                "column_name": column_name,
                "risk_type": "high_null_rate",
                "observed_value": profile.null_rate,
            }
        )
    if profile.distinct_count == 1:
        risks.append(
            {
                "table_name": table_name,
                "column_name": column_name,
                "risk_type": "constant_column",
                "observed_value": profile.distinct_count,
            }
        )
    return risks


def _fallback_questions(
    discovery: DiscoveryResult,
    dimensions: list[dict],
    measures: list[dict],
) -> list[str]:
    primary_table = (
        max(discovery.tables, key=lambda table: table.row_count).name
        if discovery.tables
        else "records"
    )
    questions = [
        f"What should one row in '{primary_table}' represent?",
        f"Which column should be the primary time anchor for '{primary_table}'?",
    ]
    if dimensions:
        questions.append("Which dimensions are most useful for slicing this dataset?")
    if measures:
        questions.append("Which measures should be monitored over time?")
    questions.append("Are any detected sensitive columns allowed outside local analysis?")
    return questions[:5]


def _looks_like_identifier_name(name: str) -> bool:
    return bool(re.search(r"(^id$|_id$|key$|code$|uuid$)", name, re.I))


def _primary_key_candidates(table, table_profiles: dict[str, ColumnProfile | None]) -> list[dict]:
    candidates: list[dict] = []
    for column in table.columns:
        profile = table_profiles.get(column.name)
        if column.is_primary_key:
            candidates.append(
                {
                    "table": table.name,
                    "columns": [column.name],
                    "declared": True,
                    "uniqueness_ratio": _profile_uniqueness(profile),
                    "null_rate": profile.null_rate if profile else 0.0,
                    "distinct_count": profile.distinct_count if profile else None,
                    "confidence": 0.98,
                }
            )
            continue
        if profile is None:
            continue
        if _is_temporal_column(column):
            continue
        if profile.distinct_count <= 1 or profile.null_rate > 0.02:
            continue
        if profile.uniqueness_ratio >= 0.995:
            confidence = 0.9
        elif profile.uniqueness_ratio >= 0.98:
            confidence = 0.78
        else:
            continue
        candidates.append(
            {
                "table": table.name,
                "columns": [column.name],
                "declared": False,
                "uniqueness_ratio": profile.uniqueness_ratio,
                "null_rate": profile.null_rate,
                "distinct_count": profile.distinct_count,
                "confidence": confidence,
            }
        )
    candidates.sort(
        key=lambda candidate: (
            candidate["declared"],
            candidate["confidence"],
            candidate.get("uniqueness_ratio") or 0.0,
        ),
        reverse=True,
    )
    return candidates[:5]


def _profile_uniqueness(profile: ColumnProfile | None) -> float | None:
    return profile.uniqueness_ratio if profile else None


def _row_entity_name(table_name: str) -> str:
    tokens = [token for token in re.split(r"[^a-z0-9]+", table_name.lower()) if token]
    if not tokens:
        return "record"
    last = tokens[-1]
    if last.endswith("ies") and len(last) > 3:
        tokens[-1] = f"{last[:-3]}y"
    elif last.endswith("s") and not last.endswith("ss") and len(last) > 1:
        tokens[-1] = last[:-1]
    return " ".join(tokens)


def _time_anchor_column(
    table,
    table_profiles: dict[str, ColumnProfile | None],
    temporal_columns: list[str],
) -> str:
    primary_temporal = [
        column.name
        for column in table.columns
        if column.name in temporal_columns and column.role == "temporal"
    ]
    if primary_temporal:
        return primary_temporal[0]
    populated = [
        column
        for column in temporal_columns
        if (table_profiles.get(column) is None or table_profiles[column].null_rate < 0.5)
    ]
    return (populated or temporal_columns)[0]


def _enum_mapping_candidate(table_name: str, column, profile: ColumnProfile | None) -> dict | None:
    if profile is None or not profile.top_values:
        return None
    if profile.distinct_count <= 1 or profile.distinct_count > 50:
        return None
    if _is_temporal_column(column) or _is_numeric_measure(column):
        return None
    code_like = _is_code_like_name(column.name) or _has_code_like_values(profile.top_values)
    if not code_like:
        return None
    values = [str(value) for value, _count in profile.top_values[:25]]
    return {
        "table": table_name,
        "column": column.name,
        "values": values,
        "labels": {},
        "needs_labels": True,
        "distinct_count": profile.distinct_count,
        "confidence": 0.62,
    }


def _is_numeric_measure(column) -> bool:
    return column.semantic_type == "metric" or column.role == "metric"


def _is_code_like_name(column_name: str) -> bool:
    lower = column_name.lower()
    return lower.endswith(("_code", "code", "_type", "type", "_status", "status"))


def _has_code_like_values(top_values: list[tuple[str, int]]) -> bool:
    values = [str(value).strip() for value, _count in top_values[:10]]
    if not values:
        return False
    code_like = 0
    for value in values:
        if not value:
            continue
        if value.isdigit() or len(value) <= 3 or re.match(r"^[A-Z0-9_-]{1,8}$", value):
            code_like += 1
    return code_like / max(len(values), 1) >= 0.6


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


def _is_temporal_column(column) -> bool:
    if _is_temporal_dtype(column.dtype):
        return True
    return column.role == "temporal" or column.semantic_type in {"date", "datetime", "timestamp"}


def _slug(value: str) -> str:
    tokens = [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]
    counts = Counter(tokens)
    return "-".join(token for token in tokens if counts[token] >= 1) or "resource"
