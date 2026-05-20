"""Persistence helpers for discovery and catalog artifacts."""

from __future__ import annotations

import json
import logging

from headwater.core.events import EventType
from headwater.core.redaction import redact_secrets
from headwater.drift.schema import build_snapshot_from_discovery, compare_schemas
from headwater.services.model_impacts import (
    compute_schema_drift_model_impacts,
    invalidated_model_names,
)

logger = logging.getLogger(__name__)


def persist_discovery_data(store, discovery, source_name: str) -> int | None:
    """Persist tables, columns, profiles, and relationships."""
    if store is None:
        logger.warning("persist_discovery_data: no metadata store available, skipping")
        return None

    source = discovery.source
    logger.info(
        "Persisting source: name=%s, type=%s, path=%s, uri=%s, mode=%s",
        source_name,
        source.type,
        redact_secrets(source.path),
        redact_secrets(source.uri),
        source.mode,
    )
    store.upsert_source(source_name, source.type, source.path, source.uri, mode=source.mode)
    run_id = store.start_run(source_name)
    logger.info("Started discovery run_id=%d", run_id)

    total_cols = 0
    for table in discovery.tables:
        store.upsert_table(
            table.name,
            source_name,
            schema_name=table.schema_name,
            row_count=table.row_count,
            description=table.description,
            domain=table.domain,
            tags=table.tags,
            run_id=run_id,
        )
        for i, col in enumerate(table.columns):
            store.upsert_column(
                table.name,
                source_name,
                col.name,
                col.dtype,
                nullable=col.nullable,
                is_primary_key=col.is_primary_key,
                description=col.description,
                semantic_type=col.semantic_type,
                role=col.role,
                confidence=col.confidence,
                ordinal=i,
            )
            total_cols += 1
    logger.info("Persisted %d tables, %d columns", len(discovery.tables), total_cols)

    for profile in discovery.profiles:
        stats = profile.model_dump(exclude={"table_name", "column_name", "dtype"})
        store.upsert_profile(
            profile.table_name,
            profile.column_name,
            source_name,
            profile.dtype,
            stats,
            run_id=run_id,
        )
    logger.info("Persisted %d profiles", len(discovery.profiles))

    cleared = store.clear_relationships(source_name)
    logger.info("Cleared %d old relationships", cleared)
    for rel in discovery.relationships:
        rel_id = store.insert_relationship(
            source_name,
            rel.from_table,
            rel.from_column,
            rel.to_table,
            rel.to_column,
            rel.type,
            rel.confidence,
            rel.referential_integrity,
            rel.source,
            run_id=run_id,
        )
        rel.id = rel_id
    logger.info("Persisted %d relationships", len(discovery.relationships))

    removed = store.mark_removed_tables(source_name, [t.name for t in discovery.tables], run_id)
    if removed:
        logger.info("Marked %d tables as removed: %s", len(removed), removed)
    _persist_schema_drift(store, discovery, source_name, run_id)
    store.finish_run(run_id, table_count=len(discovery.tables))
    logger.info("Discovery run_id=%d finished", run_id)
    return run_id


def persist_semantic_data(store, discovery, source_name: str) -> None:
    """Persist semantic details and companion docs."""
    if store is None:
        return

    for table in discovery.tables:
        if table.semantic_detail:
            store.upsert_semantic_detail(
                table.name,
                source_name,
                table.semantic_detail.model_dump(),
            )

    for doc in discovery.companion_docs:
        store.upsert_companion_doc(
            source_name=source_name,
            filename=doc.filename,
            content=doc.content,
            doc_type=doc.doc_type,
            matched_tables=doc.matched_tables,
            confidence=doc.confidence,
        )


def persist_catalog_data(store, catalog, evaluation, source_name: str) -> None:
    """Persist catalog metrics, dimensions, and entities."""
    if store is None:
        return

    linked_project = _project_for_source(store, source_name)
    project_id = linked_project["id"] if linked_project else source_name

    if linked_project:
        store.upsert_project(
            id_=project_id,
            slug=linked_project["slug"],
            display_name=linked_project["display_name"],
            description=linked_project.get("description", ""),
            sources_json=json.dumps(linked_project.get("sources") or []),
            maturity="profiled",
            catalog_confidence=evaluation.confidence,
        )
    else:
        store.upsert_project(
            id_=project_id,
            slug=project_id,
            display_name=project_id,
            maturity="profiled",
            catalog_confidence=evaluation.confidence,
        )

    store.clear_catalog(project_id)

    for metric in catalog.metrics:
        store.upsert_catalog_metric(
            project_id=project_id,
            name=metric.name,
            display_name=metric.display_name,
            description=metric.description,
            expression=metric.expression,
            table_name=metric.table,
            agg_type=metric.agg_type,
            column_name=metric.column,
            synonyms=metric.synonyms,
            confidence=metric.confidence,
            status=metric.status,
            source=metric.source,
        )

    for dimension in catalog.dimensions:
        store.upsert_catalog_dimension(
            project_id=project_id,
            name=dimension.name,
            display_name=dimension.display_name,
            description=dimension.description,
            column_name=dimension.column,
            table_name=dimension.table,
            dtype=dimension.dtype,
            synonyms=dimension.synonyms,
            sample_values=dimension.sample_values,
            cardinality=dimension.cardinality,
            confidence=dimension.confidence,
            status=dimension.status,
            source=dimension.source,
            join_path=dimension.join_path,
            join_nullable=dimension.join_nullable,
        )

    for entity in catalog.entities:
        store.upsert_catalog_entity(
            project_id=project_id,
            name=entity.name,
            display_name=entity.display_name,
            description=entity.description,
            table_name=entity.table,
            row_semantics=entity.row_semantics,
            metrics=entity.metrics,
            dimensions=entity.dimensions,
            temporal_grain=entity.temporal_grain,
        )

    logger.info(
        "Persisted catalog: %d metrics, %d dimensions, %d entities",
        len(catalog.metrics),
        len(catalog.dimensions),
        len(catalog.entities),
    )


def _persist_schema_drift(store, discovery, source_name: str, run_id: int) -> None:
    current_snapshot = build_snapshot_from_discovery(discovery)
    previous_record = store.get_latest_snapshot_record(source_name, before_run_id=run_id)
    store.save_snapshot(run_id, source_name, current_snapshot)

    if previous_record is None:
        logger.info("Schema drift baseline saved for source '%s'", source_name)
        return

    diff = compare_schemas(
        previous_record["snapshot"],
        current_snapshot,
        source_name,
        run_id_from=previous_record["run_id"],
        run_id_to=run_id,
    )
    report_id = store.save_drift_report(
        source_name,
        previous_record["run_id"],
        run_id,
        diff.model_dump(),
    )
    if not diff.no_changes:
        _persist_model_impacts_for_drift(store, source_name, report_id, diff.model_dump())
        try:
            store.insert_event(
                EventType.SCHEMA_DRIFT_DETECTED,
                "Schema drift detected",
                source_name=source_name,
                severity="warning",
                artifact_type="source",
                artifact_id=source_name,
                payload={"report_id": report_id, **diff.model_dump()},
                invalidates=["sources", "briefing", "health", "insights", "models"],
            )
        except Exception:
            logger.exception("Failed to write normalized drift event for '%s'", source_name)


def _persist_model_impacts_for_drift(
    store,
    source_name: str,
    drift_report_id: int,
    diff: dict,
) -> None:
    models = store.get_models(source_name)
    if not models:
        return
    impacts = compute_schema_drift_model_impacts(
        source_name=source_name,
        drift_report_id=drift_report_id,
        diff=diff,
        models=models,
    )
    if not impacts:
        return

    impact_ids = store.save_model_impacts(impacts)
    invalidated = invalidated_model_names(impacts)
    for model_name in invalidated:
        store.update_model_status(model_name, "invalidated")
        try:
            store.insert_event(
                EventType.MODEL_IMPACTED,
                f"Model '{model_name}' impacted by schema drift",
                source_name=source_name,
                severity="warning",
                artifact_type="model",
                artifact_id=model_name,
                payload={
                    "drift_report_id": drift_report_id,
                    "impact_ids": [
                        impact_id
                        for impact_id, impact in zip(impact_ids, impacts, strict=False)
                        if impact["model_name"] == model_name
                    ],
                },
                invalidates=["models", "briefing", "health"],
            )
        except Exception:
            logger.exception("Failed to write model impact event for '%s'", model_name)


def _project_for_source(store, source_name: str) -> dict | None:
    source_slug = _slugify(source_name)
    linked = []
    for project in store.list_projects():
        if project.get("id") == source_name:
            continue
        explicit = project.get("sources") or []
        project_slug = project.get("slug") or _slugify(project.get("display_name", ""))
        if source_name in explicit or _slug_matches(project_slug, source_slug):
            linked.append(project)
    return linked[0] if linked else None


def _slugify(text: str) -> str:
    normalized = "".join(c.lower() if c.isalnum() else "-" for c in text)
    return "-".join(part for part in normalized.split("-") if part)


def _slug_matches(project_slug: str, source_slug: str) -> bool:
    return project_slug == source_slug or project_slug in source_slug or source_slug in project_slug
