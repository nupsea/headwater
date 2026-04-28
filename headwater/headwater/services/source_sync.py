"""Source sync orchestration service."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import duckdb

from headwater.connectors.registry import get_connector
from headwater.core.config import get_settings
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig

logger = logging.getLogger(__name__)


class SourceNotFoundError(Exception):
    """Raised when a requested source does not exist."""


class SourceSyncService:
    """Run source connection tests and full syncs from persisted source config."""

    def __init__(self, request) -> None:
        self.request = request
        self.store = request.app.state.metadata_store

    def test(self, name: str) -> dict:
        """Test connectivity for a persisted source without discovery/profiling."""
        row = self._source_row(name)
        try:
            connector = get_connector(row["type"])
            config = SourceConfig(
                name=row["name"],
                type=row["type"],
                path=row.get("path"),
                uri=row.get("uri"),
            )
            connector.connect(config)
            table_count = None
            if hasattr(connector, "list_tables"):
                try:
                    table_count = len(connector.list_tables())
                except Exception:
                    table_count = None
            if hasattr(connector, "close"):
                connector.close()
            self.record_event(
                "connection_tested",
                "Connection verified",
                source_name=name,
                payload={"table_count": table_count},
                invalidates=["sources"],
            )
            return {"name": name, "status": "ok", "tables": table_count}
        except ConnectorError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Connection test failed for source '%s'", name)
            raise ConnectorError(str(exc)) from exc

    def sync(self, name: str) -> dict:
        """Run the full source-scoped discovery/model/quality pipeline."""
        row = self._source_row(name)
        run_id = self.store.start_sync_run(name, mode="full")
        self.store.upsert_source_meta(name, status="syncing", last_sync_at=_now_iso())
        self.record_event(
            "sync_started",
            "Source sync started",
            source_name=name,
            payload={"run_id": run_id},
            invalidates=["sources", "briefing"],
        )

        try:
            result = self._run_pipeline(row, name)
            latest = self.store.get_source(name) or row
            drift_count = latest.get("drift_count") or 0
            drift_health = max(0, 100 - 5 * drift_count)
            quality_failed = int(result.get("quality_failed") or 0)
            quality_score = int(round(result.get("quality_score", 100)))
            health = min(drift_health, quality_score)
            final_status = "warning" if drift_count or quality_failed else "healthy"
            quality_run_id = result.get("quality_run_id")
            if quality_run_id:
                self.store.attach_quality_run_to_sync(int(quality_run_id), run_id)
            self.store.finish_sync_run(
                run_id,
                tables_seen=result.get("tables_discovered", 0),
                profiles_written=result.get("profiles", 0),
                contracts_checked=result.get("quality_total", 0),
                payload=result,
            )
            self.store.upsert_source_meta(
                name,
                status=final_status,
                health=health,
                last_sync_at=_now_iso(),
            )
            self.record_event(
                "sync_completed",
                f"Source sync completed: {result.get('tables_discovered', 0)} table(s) discovered",
                source_name=name,
                payload={"run_id": run_id, **result},
                invalidates=["sources", "briefing", "health", "insights", "models", "quality"],
            )
            return {
                "name": name,
                "status": final_status,
                "health": health,
                "run_id": run_id,
                **result,
            }
        except ConnectorError as exc:
            self._record_sync_failure(name, run_id, str(exc))
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Sync failed for source '%s'", name)
            self._record_sync_failure(name, run_id, str(exc))
            raise ConnectorError(str(exc)) from exc

    def record_event(
        self,
        event_type: str,
        summary: str,
        *,
        source_name: str,
        severity: str = "info",
        payload: dict | None = None,
        invalidates: list[str] | None = None,
        artifact_type: str | None = None,
        artifact_id: str | None = None,
        detail: str | None = None,
    ) -> None:
        """Write normalized and legacy source events."""
        try:
            self.store.insert_event(
                event_type,
                summary,
                source_name=source_name,
                severity=severity,
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                detail=detail,
                payload=payload,
                invalidates=invalidates,
            )
        except Exception:
            logger.exception("Failed to write normalized event '%s'", event_type)
        try:
            self.store.insert_sync_event(
                source_name,
                event_type,
                summary,
                severity=severity,
                payload=payload,
            )
        except Exception:
            logger.exception("Failed to write legacy sync_event '%s'", event_type)

    def _source_row(self, name: str) -> dict:
        row = self.store.get_source(name)
        if not row:
            raise SourceNotFoundError(f"Source '{name}' not found.")
        return row

    def _run_pipeline(self, row: dict, name: str) -> dict:
        from headwater.api.routes.pipeline import _run_pipeline_inner

        source_path = _source_value(row)
        pipeline = self.request.app.state.pipeline
        source_schema = _default_source_schema(row)
        target_schema = "staging"

        if getattr(self.request.app.state, "_in_memory", False):
            return _run_pipeline_inner(
                self.request.app.state.duckdb_con,
                self.request,
                pipeline,
                source_path,
                row["type"],
                name,
                source_schema,
                target_schema,
            )

        settings = get_settings()
        con = duckdb.connect(str(settings.analytical_db_path))
        try:
            return _run_pipeline_inner(
                con,
                self.request,
                pipeline,
                source_path,
                row["type"],
                name,
                source_schema,
                target_schema,
            )
        finally:
            con.close()

    def _record_sync_failure(self, name: str, run_id: int, error: str) -> None:
        self.store.fail_sync_run(run_id, error)
        self.store.upsert_source_meta(name, status="error", health=0, last_sync_at=_now_iso())
        self.record_event(
            "sync_failed",
            error,
            source_name=name,
            severity="error",
            payload={"run_id": run_id},
            invalidates=["sources", "briefing"],
        )


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _source_value(row: dict) -> str:
    value = row.get("uri") or row.get("path")
    if not value:
        raise ConnectorError("Source has no uri or path configured.")
    return value


def _default_source_schema(row: dict) -> str:
    return "public" if row.get("uri") else "env_health"
