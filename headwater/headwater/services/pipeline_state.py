"""Persistence helpers for generated pipeline outputs."""

from __future__ import annotations

import logging

from headwater.core.events import EventType

logger = logging.getLogger(__name__)


def persist_models(store: object, models: list, source_name: str) -> None:
    """Persist generated models to SQLite."""
    for model in models:
        try:
            store.upsert_model(  # type: ignore[union-attr]
                name=model.name,
                source_name=source_name,
                model_type=model.model_type,
                sql_text=model.sql,
                description=model.description,
                source_tables=model.source_tables,
                depends_on=model.depends_on,
                status=model.status,
                assumptions=getattr(model, "assumptions", []),
                questions=getattr(model, "questions", []),
            )
        except Exception:
            logger.exception("Failed to persist model %s", model.name)
    logger.info("Persisted %d models to metadata store", len(models))


def persist_contracts(store: object, contracts: list) -> None:
    """Persist generated contracts to SQLite."""
    for contract in contracts:
        try:
            contract_id = (
                contract.id
                or f"{contract.model_name}_{contract.rule_type}_{contract.column_name}"
            )
            store.upsert_contract(  # type: ignore[union-attr]
                id_=contract_id,
                model_name=contract.model_name,
                rule_type=contract.rule_type,
                expression=contract.expression,
                column_name=contract.column_name,
                severity=contract.severity,
                description=contract.description,
                confidence=contract.confidence,
                status=contract.status,
            )
        except Exception:
            logger.exception("Failed to persist contract %s", contract.id)
    logger.info("Persisted %d contracts to metadata store", len(contracts))


def persist_execution_results(store: object, results: list) -> None:
    """Persist execution results to SQLite."""
    for result in results:
        try:
            store.save_execution_result(  # type: ignore[union-attr]
                model_name=result.model_name,
                success=result.success,
                row_count=result.row_count,
                execution_time_ms=result.execution_time_ms,
                error=result.error,
            )
        except Exception:
            logger.exception("Failed to persist exec result for %s", result.model_name)
    logger.info("Persisted %d execution results", len(results))


def persist_quality_report(
    store: object,
    source_name: str,
    report,
    sync_run_id: int | None = None,
):
    """Persist quality report and emit quality-related events."""
    run_id = store.save_quality_report(  # type: ignore[union-attr]
        source_name,
        report,
        sync_run_id=sync_run_id,
    )
    if report.failed:
        try:
            store.insert_event(  # type: ignore[union-attr]
                EventType.QUALITY_CHECKS_FAILED,
                f"{report.failed} quality contract(s) failed",
                source_name=source_name,
                severity="warning",
                artifact_type="quality_run",
                artifact_id=str(run_id),
                payload={
                    "quality_run_id": run_id,
                    "total": report.total_contracts,
                    "passed": report.passed,
                    "failed": report.failed,
                },
                invalidates=["sources", "briefing", "health", "insights", "quality"],
            )
        except Exception:
            logger.exception("Failed to emit quality event for '%s'", source_name)
    elif getattr(report, "previous_failed", 0):
        try:
            store.insert_event(  # type: ignore[union-attr]
                EventType.QUALITY_CHECKS_RECOVERED,
                "Quality contracts recovered",
                source_name=source_name,
                severity="info",
                artifact_type="quality_run",
                artifact_id=str(run_id),
                payload={
                    "quality_run_id": run_id,
                    "total": report.total_contracts,
                    "passed": report.passed,
                    "failed": report.failed,
                    "previous_failed": getattr(report, "previous_failed", 0),
                },
                invalidates=["sources", "briefing", "health", "insights", "quality"],
            )
        except Exception:
            logger.exception("Failed to emit quality recovery event for '%s'", source_name)
    logger.info("Persisted quality run %s for source '%s'", run_id, source_name)
    return run_id
