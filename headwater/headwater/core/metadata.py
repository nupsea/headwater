"""SQLite metadata store -- schema DDL, init, and CRUD helpers.

Metadata is always SQLite (POC) or Postgres (Phase 1+). Never DuckDB.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

from headwater.core.exceptions import MetadataError

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sources (
    name        TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    path        TEXT,
    uri         TEXT,
    mode        TEXT NOT NULL DEFAULT 'generate',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS discovery_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL REFERENCES sources(name),
    started_at  TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    table_count INTEGER DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS tables (
    name        TEXT NOT NULL,
    source_name TEXT NOT NULL REFERENCES sources(name),
    schema_name TEXT,
    row_count   INTEGER DEFAULT 0,
    description TEXT,
    domain      TEXT,
    tags        TEXT DEFAULT '[]',
    run_id      INTEGER REFERENCES discovery_runs(id),
    locked      INTEGER NOT NULL DEFAULT 0,
    locked_at   TEXT,
    removed_in_run_id INTEGER,
    PRIMARY KEY (name, source_name)
);

CREATE TABLE IF NOT EXISTS columns (
    table_name  TEXT NOT NULL,
    source_name TEXT NOT NULL,
    name        TEXT NOT NULL,
    dtype       TEXT NOT NULL,
    nullable    INTEGER DEFAULT 1,
    is_primary_key INTEGER DEFAULT 0,
    description TEXT,
    semantic_type TEXT,
    ordinal     INTEGER DEFAULT 0,
    locked      INTEGER NOT NULL DEFAULT 0,
    locked_at   TEXT,
    PRIMARY KEY (table_name, source_name, name),
    FOREIGN KEY (table_name, source_name) REFERENCES tables(name, source_name)
);

CREATE TABLE IF NOT EXISTS profiles (
    table_name  TEXT NOT NULL,
    column_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    dtype       TEXT NOT NULL,
    stats_json  TEXT NOT NULL,
    run_id      INTEGER REFERENCES discovery_runs(id),
    PRIMARY KEY (table_name, column_name, source_name)
);

CREATE TABLE IF NOT EXISTS relationships (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name     TEXT NOT NULL,
    from_table      TEXT NOT NULL,
    from_column     TEXT NOT NULL,
    to_table        TEXT NOT NULL,
    to_column       TEXT NOT NULL,
    rel_type        TEXT NOT NULL,
    confidence      REAL NOT NULL,
    ref_integrity   REAL NOT NULL,
    detection_source TEXT NOT NULL,
    run_id          INTEGER REFERENCES discovery_runs(id)
);

CREATE TABLE IF NOT EXISTS models (
    name        TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    model_type  TEXT NOT NULL,
    sql_text    TEXT NOT NULL,
    description TEXT,
    source_tables TEXT DEFAULT '[]',
    depends_on  TEXT DEFAULT '[]',
    status      TEXT NOT NULL DEFAULT 'proposed',
    assumptions TEXT DEFAULT '[]',
    questions   TEXT DEFAULT '[]',
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS model_reviews (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name  TEXT NOT NULL,
    source_name TEXT,
    reviewer    TEXT,
    decision    TEXT NOT NULL,
    reason      TEXT,
    diff_summary TEXT,
    payload_json TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS model_impacts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    drift_report_id INTEGER,
    model_name  TEXT NOT NULL,
    impact_type TEXT NOT NULL,
    severity    TEXT NOT NULL,
    source_table TEXT,
    source_column TEXT,
    contract_id TEXT,
    reason      TEXT NOT NULL,
    payload_json TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS contracts (
    id          TEXT PRIMARY KEY,
    model_name  TEXT NOT NULL,
    column_name TEXT,
    rule_type   TEXT NOT NULL,
    expression  TEXT NOT NULL,
    severity    TEXT NOT NULL DEFAULT 'warning',
    description TEXT DEFAULT '',
    confidence  REAL DEFAULT 0.8,
    status      TEXT NOT NULL DEFAULT 'proposed',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS decisions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_type TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    action      TEXT NOT NULL,
    reason      TEXT,
    payload_json TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS llm_audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL,
    model       TEXT NOT NULL,
    prompt_hash TEXT,
    prompt_text TEXT NOT NULL,
    response_text TEXT,
    tokens_in   INTEGER DEFAULT 0,
    tokens_out  INTEGER DEFAULT 0,
    cached      INTEGER DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS schema_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      INTEGER NOT NULL,
    source_name TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES discovery_runs(id)
);

CREATE TABLE IF NOT EXISTS drift_reports (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    run_id_from INTEGER,
    run_id_to   INTEGER NOT NULL,
    diff_json   TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    acknowledged INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS table_semantic_details (
    table_name  TEXT NOT NULL,
    source_name TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    run_id      INTEGER REFERENCES discovery_runs(id),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (table_name, source_name)
);

CREATE TABLE IF NOT EXISTS companion_docs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name     TEXT NOT NULL REFERENCES sources(name),
    filename        TEXT NOT NULL,
    content         TEXT NOT NULL,
    doc_type        TEXT NOT NULL DEFAULT 'unknown',
    matched_tables  TEXT DEFAULT '[]',
    confidence      REAL DEFAULT 0.5,
    run_id          INTEGER REFERENCES discovery_runs(id),
    UNIQUE(source_name, filename)
);

CREATE TABLE IF NOT EXISTS projects (
    id              TEXT PRIMARY KEY,
    slug            TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    description     TEXT DEFAULT '',
    sources_json    TEXT DEFAULT '[]',
    maturity        TEXT NOT NULL DEFAULT 'raw',
    maturity_score  REAL DEFAULT 0.0,
    catalog_confidence REAL DEFAULT 0.0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS catalog_metrics (
    name            TEXT NOT NULL,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    display_name    TEXT NOT NULL,
    description     TEXT NOT NULL,
    expression      TEXT NOT NULL,
    column_name     TEXT,
    table_name      TEXT NOT NULL,
    agg_type        TEXT NOT NULL,
    filters_json    TEXT DEFAULT '[]',
    synonyms_json   TEXT DEFAULT '[]',
    confidence      REAL DEFAULT 0.5,
    status          TEXT NOT NULL DEFAULT 'proposed',
    source          TEXT NOT NULL DEFAULT 'heuristic',
    PRIMARY KEY (name, project_id)
);

CREATE TABLE IF NOT EXISTS catalog_dimensions (
    name            TEXT NOT NULL,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    display_name    TEXT NOT NULL,
    description     TEXT NOT NULL,
    column_name     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    dtype           TEXT NOT NULL,
    expression      TEXT,
    synonyms_json   TEXT DEFAULT '[]',
    hierarchy_json  TEXT DEFAULT '[]',
    sample_values_json TEXT DEFAULT '[]',
    cardinality     INTEGER DEFAULT 0,
    confidence      REAL DEFAULT 0.5,
    status          TEXT NOT NULL DEFAULT 'proposed',
    source          TEXT NOT NULL DEFAULT 'heuristic',
    join_path       TEXT,
    join_nullable   INTEGER DEFAULT 0,
    PRIMARY KEY (name, project_id)
);

CREATE TABLE IF NOT EXISTS catalog_entities (
    name            TEXT NOT NULL,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    display_name    TEXT NOT NULL,
    description     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    row_semantics   TEXT NOT NULL,
    metrics_json    TEXT DEFAULT '[]',
    dimensions_json TEXT DEFAULT '[]',
    temporal_grain  TEXT,
    synonyms_json   TEXT DEFAULT '[]',
    PRIMARY KEY (name, project_id)
);

CREATE TABLE IF NOT EXISTS activity_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    action      TEXT NOT NULL,
    detail      TEXT,
    artifact_type TEXT,
    artifact_id TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS model_answers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name  TEXT NOT NULL,
    question_index INTEGER NOT NULL,
    answer      TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(model_name, question_index)
);

CREATE TABLE IF NOT EXISTS execution_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name      TEXT NOT NULL,
    success         INTEGER NOT NULL DEFAULT 0,
    row_count       INTEGER,
    execution_time_ms REAL DEFAULT 0.0,
    error           TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS quality_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name     TEXT,
    sync_run_id     INTEGER,
    total_contracts INTEGER NOT NULL DEFAULT 0,
    passed          INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    skipped         INTEGER NOT NULL DEFAULT 0,
    score           REAL NOT NULL DEFAULT 100.0,
    status          TEXT NOT NULL DEFAULT 'passing',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS quality_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES quality_runs(id),
    source_name     TEXT,
    rule_id         TEXT NOT NULL,
    model_name      TEXT NOT NULL,
    passed          INTEGER NOT NULL DEFAULT 0,
    observed_value_json TEXT,
    message         TEXT DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


class MetadataStore:
    """SQLite-backed metadata store with WAL mode for read concurrency."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._db_path = str(db_path)
        self._con: sqlite3.Connection | None = None

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    @property
    def con(self) -> sqlite3.Connection:
        if self._con is None:
            self._con = self._connect()
        return self._con

    def init(self) -> None:
        """Create all tables if they don't exist, then apply any pending migrations."""
        self.con.executescript(_SCHEMA_SQL)
        self._migrate()

    def _migrate(self) -> None:
        """Apply incremental schema migrations that are safe to run repeatedly."""
        migrations = [
            # US-100: add mode column to sources (default 'generate')
            "ALTER TABLE sources ADD COLUMN mode TEXT NOT NULL DEFAULT 'generate'",
            # US-301: add payload_json column to decisions
            "ALTER TABLE decisions ADD COLUMN payload_json TEXT",
            # US-201: add locked columns to tables
            "ALTER TABLE tables ADD COLUMN locked INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE tables ADD COLUMN locked_at TEXT",
            # US-201: add locked columns to columns
            "ALTER TABLE columns ADD COLUMN locked INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE columns ADD COLUMN locked_at TEXT",
            # US-203: add removed_in_run_id to tables
            "ALTER TABLE tables ADD COLUMN removed_in_run_id INTEGER",
            # Data dictionary: review status on tables
            "ALTER TABLE tables ADD COLUMN review_status TEXT NOT NULL DEFAULT 'pending'",
            "ALTER TABLE tables ADD COLUMN reviewed_at TEXT",
            # Data dictionary: confidence and role on columns
            "ALTER TABLE columns ADD COLUMN confidence REAL DEFAULT 0.0",
            "ALTER TABLE columns ADD COLUMN role TEXT",
            # Multi-source: extend sources for the Sources page
            "ALTER TABLE sources ADD COLUMN display_name TEXT",
            "ALTER TABLE sources ADD COLUMN host TEXT",
            "ALTER TABLE sources ADD COLUMN config_json TEXT",
            "ALTER TABLE sources ADD COLUMN status TEXT NOT NULL DEFAULT 'idle'",
            "ALTER TABLE sources ADD COLUMN health INTEGER",
            "ALTER TABLE sources ADD COLUMN last_sync_at TEXT",
            "ALTER TABLE sources ADD COLUMN drift_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE sources ADD COLUMN auto_sync INTEGER NOT NULL DEFAULT 0",
        ]
        # sync_events table for source activity history (briefing + sources page)
        self.con.executescript(
            """
CREATE TABLE IF NOT EXISTS sync_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    event_type  TEXT NOT NULL,
                severity    TEXT NOT NULL DEFAULT 'info',
                detail      TEXT,
                payload     TEXT,
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_sync_events_source
    ON sync_events(source_name, created_at DESC);

CREATE TABLE IF NOT EXISTS sync_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    mode        TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    duration_ms INTEGER,
    tables_seen INTEGER DEFAULT 0,
    profiles_written INTEGER DEFAULT 0,
    contracts_checked INTEGER DEFAULT 0,
    error       TEXT,
    payload_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_sync_runs_source
    ON sync_runs(source_name, started_at DESC);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT,
    event_type  TEXT NOT NULL,
    severity    TEXT NOT NULL DEFAULT 'info',
    artifact_type TEXT,
    artifact_id TEXT,
    summary     TEXT NOT NULL,
    detail      TEXT,
    payload_json TEXT,
    invalidates_json TEXT DEFAULT '[]',
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    acknowledged_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_source
    ON events(source_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_type
    ON events(event_type, created_at DESC);

CREATE TABLE IF NOT EXISTS quality_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name     TEXT,
    sync_run_id     INTEGER,
    total_contracts INTEGER NOT NULL DEFAULT 0,
    passed          INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    skipped         INTEGER NOT NULL DEFAULT 0,
    score           REAL NOT NULL DEFAULT 100.0,
    status          TEXT NOT NULL DEFAULT 'passing',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_quality_runs_source
    ON quality_runs(source_name, created_at DESC);

CREATE TABLE IF NOT EXISTS quality_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES quality_runs(id),
    source_name     TEXT,
    rule_id         TEXT NOT NULL,
    model_name      TEXT NOT NULL,
    passed          INTEGER NOT NULL DEFAULT 0,
    observed_value_json TEXT,
    message         TEXT DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_quality_results_run
    ON quality_results(run_id);

CREATE TABLE IF NOT EXISTS model_reviews (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name  TEXT NOT NULL,
    source_name TEXT,
    reviewer    TEXT,
    decision    TEXT NOT NULL,
    reason      TEXT,
    diff_summary TEXT,
    payload_json TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_model_reviews_model
    ON model_reviews(model_name, created_at DESC);

CREATE TABLE IF NOT EXISTS model_impacts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    drift_report_id INTEGER,
    model_name  TEXT NOT NULL,
    impact_type TEXT NOT NULL,
    severity    TEXT NOT NULL,
    source_table TEXT,
    source_column TEXT,
    contract_id TEXT,
    reason      TEXT NOT NULL,
    payload_json TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_model_impacts_source
    ON model_impacts(source_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_model_impacts_model
    ON model_impacts(model_name, created_at DESC);
"""
        )
        self.con.commit()
        for sql in migrations:
            try:
                self.con.execute(sql)
                self.con.commit()
            except Exception:
                # Column already exists -- migration already applied
                pass

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    # -- Sources -----------------------------------------------------------

    def upsert_source(
        self,
        name: str,
        type_: str,
        path: str | None,
        uri: str | None,
        mode: str = "generate",
    ) -> None:
        # ON CONFLICT preserves the multi-source columns (status, health, etc.)
        # that upsert_source_meta may have populated.
        self.con.execute(
            """
            INSERT INTO sources (name, type, path, uri, mode)
                 VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                type = excluded.type,
                path = excluded.path,
                uri  = excluded.uri,
                mode = excluded.mode
            """,
            (name, type_, path, uri, mode),
        )
        self.con.commit()

    def upsert_source_meta(
        self,
        name: str,
        *,
        display_name: str | None = None,
        host: str | None = None,
        config: dict | None = None,
        status: str | None = None,
        health: int | None = None,
        last_sync_at: str | None = None,
        drift_count: int | None = None,
        auto_sync: bool | None = None,
    ) -> None:
        """Update the multi-source metadata for an existing source row."""
        fields: list[str] = []
        params: list = []
        if display_name is not None:
            fields.append("display_name = ?")
            params.append(display_name)
        if host is not None:
            fields.append("host = ?")
            params.append(host)
        if config is not None:
            fields.append("config_json = ?")
            params.append(json.dumps(config))
        if status is not None:
            fields.append("status = ?")
            params.append(status)
        if health is not None:
            fields.append("health = ?")
            params.append(int(health))
        if last_sync_at is not None:
            fields.append("last_sync_at = ?")
            params.append(last_sync_at)
        if drift_count is not None:
            fields.append("drift_count = ?")
            params.append(int(drift_count))
        if auto_sync is not None:
            fields.append("auto_sync = ?")
            params.append(1 if auto_sync else 0)
        if not fields:
            return
        params.append(name)
        self.con.execute(f"UPDATE sources SET {', '.join(fields)} WHERE name = ?", params)
        self.con.commit()

    def delete_source(self, name: str) -> bool:
        """Remove a source and its sync events. Returns True if a row was deleted."""
        cur = self.con.execute("DELETE FROM sources WHERE name = ?", (name,))
        self.con.execute("DELETE FROM sync_events WHERE source_name = ?", (name,))
        self.con.commit()
        return cur.rowcount > 0

    def get_source(self, name: str) -> dict | None:
        row = self.con.execute("SELECT * FROM sources WHERE name = ?", (name,)).fetchone()
        return dict(row) if row else None

    def list_sources(self) -> list[dict]:
        return [dict(r) for r in self.con.execute("SELECT * FROM sources").fetchall()]

    # -- Sync events -------------------------------------------------------

    def insert_sync_event(
        self,
        source_name: str,
        event_type: str,
        detail: str,
        *,
        severity: str = "info",
        payload: dict | None = None,
    ) -> int:
        cur = self.con.execute(
            "INSERT INTO sync_events (source_name, event_type, severity, detail, payload) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                source_name,
                event_type,
                severity,
                detail,
                json.dumps(payload) if payload is not None else None,
            ),
        )
        self.con.commit()
        return cur.lastrowid or 0

    def list_sync_events(
        self,
        source_name: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        if source_name:
            rows = self.con.execute(
                "SELECT * FROM sync_events WHERE source_name = ? "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                (source_name, limit),
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT * FROM sync_events ORDER BY created_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        import contextlib

        out = []
        for r in rows:
            d = dict(r)
            if d.get("payload"):
                with contextlib.suppress(ValueError, TypeError):
                    d["payload"] = json.loads(d["payload"])
            out.append(d)
        return out

    # -- Sync runs ---------------------------------------------------------

    def start_sync_run(self, source_name: str, mode: str = "full") -> int:
        """Create a sync_runs row and return its id."""
        cur = self.con.execute(
            "INSERT INTO sync_runs (source_name, mode, status) VALUES (?, ?, 'running')",
            (source_name, mode),
        )
        self.con.commit()
        if cur.lastrowid is None:
            raise MetadataError("Failed to create sync run")
        return cur.lastrowid

    def finish_sync_run(
        self,
        run_id: int,
        *,
        tables_seen: int = 0,
        profiles_written: int = 0,
        contracts_checked: int = 0,
        payload: dict | None = None,
    ) -> None:
        """Mark a sync run successful."""
        self.con.execute(
            """
            UPDATE sync_runs
               SET status = 'succeeded',
                   finished_at = datetime('now'),
                   duration_ms = CAST(
                       (julianday(datetime('now')) - julianday(started_at)) * 86400000
                       AS INTEGER
                   ),
                   tables_seen = ?,
                   profiles_written = ?,
                   contracts_checked = ?,
                   payload_json = ?
             WHERE id = ?
            """,
            (
                tables_seen,
                profiles_written,
                contracts_checked,
                json.dumps(payload) if payload is not None else None,
                run_id,
            ),
        )
        self.con.commit()

    def fail_sync_run(self, run_id: int, error: str, payload: dict | None = None) -> None:
        """Mark a sync run failed."""
        self.con.execute(
            """
            UPDATE sync_runs
               SET status = 'failed',
                   finished_at = datetime('now'),
                   duration_ms = CAST(
                       (julianday(datetime('now')) - julianday(started_at)) * 86400000
                       AS INTEGER
                   ),
                   error = ?,
                   payload_json = ?
             WHERE id = ?
            """,
            (error, json.dumps(payload) if payload is not None else None, run_id),
        )
        self.con.commit()

    def list_sync_runs(self, source_name: str | None = None, limit: int = 20) -> list[dict]:
        """Return recent sync runs."""
        if source_name:
            rows = self.con.execute(
                "SELECT * FROM sync_runs WHERE source_name = ? "
                "ORDER BY started_at DESC, id DESC LIMIT ?",
                (source_name, limit),
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT * FROM sync_runs ORDER BY started_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            if d.get("payload_json"):
                try:
                    d["payload"] = json.loads(d["payload_json"])
                except (TypeError, ValueError):
                    d["payload"] = None
            out.append(d)
        return out

    def get_latest_sync_run(self, source_name: str) -> dict | None:
        """Return the most recent sync run for a source."""
        row = self.con.execute(
            "SELECT * FROM sync_runs WHERE source_name = ? "
            "ORDER BY started_at DESC, id DESC LIMIT 1",
            (source_name,),
        ).fetchone()
        return dict(row) if row else None

    # -- Normalized events -------------------------------------------------

    def insert_event(
        self,
        event_type: str,
        summary: str,
        *,
        source_name: str | None = None,
        severity: str = "info",
        artifact_type: str | None = None,
        artifact_id: str | None = None,
        detail: str | None = None,
        payload: dict | None = None,
        invalidates: list[str] | None = None,
    ) -> int:
        """Insert a normalized event and return its id."""
        cur = self.con.execute(
            """
            INSERT INTO events
                (source_name, event_type, severity, artifact_type, artifact_id,
                 summary, detail, payload_json, invalidates_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_name,
                event_type,
                severity,
                artifact_type,
                artifact_id,
                summary,
                detail,
                json.dumps(payload) if payload is not None else None,
                json.dumps(invalidates or []),
            ),
        )
        self.con.commit()
        return cur.lastrowid or 0

    def list_events(
        self,
        source_name: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return recent normalized events."""
        if source_name:
            rows = self.con.execute(
                "SELECT * FROM events WHERE source_name = ? "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                (source_name, limit),
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT * FROM events ORDER BY created_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            for db_key, out_key, default in (
                ("payload_json", "payload", None),
                ("invalidates_json", "invalidates", []),
            ):
                if d.get(db_key):
                    try:
                        d[out_key] = json.loads(d[db_key])
                    except (TypeError, ValueError):
                        d[out_key] = default
                else:
                    d[out_key] = default
            out.append(d)
        return out

    # -- Discovery runs ----------------------------------------------------

    def start_run(self, source_name: str) -> int:
        cur = self.con.execute(
            "INSERT INTO discovery_runs (source_name) VALUES (?)", (source_name,)
        )
        self.con.commit()
        if cur.lastrowid is None:
            raise MetadataError("Failed to create discovery run")
        return cur.lastrowid

    def finish_run(self, run_id: int, table_count: int, status: str = "completed") -> None:
        self.con.execute(
            "UPDATE discovery_runs SET finished_at = datetime('now'), table_count = ?, status = ? "
            "WHERE id = ?",
            (table_count, status, run_id),
        )
        self.con.commit()

    # -- Tables & Columns --------------------------------------------------

    def upsert_table(
        self,
        name: str,
        source_name: str,
        *,
        schema_name: str | None = None,
        row_count: int = 0,
        description: str | None = None,
        domain: str | None = None,
        tags: list[str] | None = None,
        run_id: int | None = None,
        review_status: str | None = None,
    ) -> None:
        # INSERT OR IGNORE to preserve existing lock state on re-runs.
        self.con.execute(
            "INSERT OR IGNORE INTO tables "
            "(name, source_name, schema_name, row_count, description, domain, tags, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                name,
                source_name,
                schema_name,
                row_count,
                description,
                domain,
                json.dumps(tags or []),
                run_id,
            ),
        )
        # Update non-locked fields; if locked=1 skip description/domain update.
        # Also clear removed_in_run_id since the table is present again.
        # Preserve review_status if already reviewed (don't regress on re-run).
        review_clause = ""
        review_params: list = []
        if review_status is not None:
            review_clause = (
                ", review_status = CASE WHEN review_status = 'reviewed' "
                "THEN review_status ELSE ? END"
            )
            review_params = [review_status]

        self.con.execute(
            "UPDATE tables SET "
            "schema_name = ?, row_count = ?, tags = ?, run_id = ?, "
            "removed_in_run_id = NULL, "
            "description = CASE WHEN locked = 1 THEN description ELSE ? END, "
            f"domain = CASE WHEN locked = 1 THEN domain ELSE ? END{review_clause} "
            "WHERE name = ? AND source_name = ?",
            [schema_name, row_count, json.dumps(tags or []), run_id, description, domain]
            + review_params
            + [name, source_name],
        )
        self.con.commit()

    def upsert_column(
        self,
        table_name: str,
        source_name: str,
        name: str,
        dtype: str,
        *,
        nullable: bool = True,
        is_primary_key: bool = False,
        description: str | None = None,
        semantic_type: str | None = None,
        role: str | None = None,
        confidence: float = 0.0,
        ordinal: int = 0,
    ) -> None:
        # INSERT OR IGNORE to preserve existing lock state on re-runs.
        # Then UPDATE non-lock fields only (description/semantic_type honored if not locked).
        self.con.execute(
            "INSERT OR IGNORE INTO columns "
            "(table_name, source_name, name, dtype, nullable, is_primary_key, "
            "description, semantic_type, role, confidence, ordinal) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                table_name,
                source_name,
                name,
                dtype,
                int(nullable),
                int(is_primary_key),
                description,
                semantic_type,
                role,
                confidence,
                ordinal,
            ),
        )
        # Update non-locked columns; if locked=1 skip description/semantic_type/role update
        self.con.execute(
            "UPDATE columns SET "
            "dtype = ?, nullable = ?, is_primary_key = ?, ordinal = ?, "
            "description = CASE WHEN locked = 1 THEN description ELSE ? END, "
            "semantic_type = CASE WHEN locked = 1 THEN semantic_type ELSE ? END, "
            "role = CASE WHEN locked = 1 THEN role ELSE ? END, "
            "confidence = CASE WHEN locked = 1 THEN confidence ELSE ? END "
            "WHERE table_name = ? AND source_name = ? AND name = ?",
            (
                dtype,
                int(nullable),
                int(is_primary_key),
                ordinal,
                description,
                semantic_type,
                role,
                confidence,
                table_name,
                source_name,
                name,
            ),
        )
        self.con.commit()

    def lock_column(
        self,
        table_name: str,
        source_name: str,
        name: str,
        *,
        locked: bool,
        description: str | None = None,
    ) -> None:
        """Set or clear the lock on a column. Setting locked=True also updates description."""
        if locked:
            self.con.execute(
                "UPDATE columns SET locked = 1, locked_at = datetime('now')"
                + (", description = ?" if description is not None else "")
                + " WHERE table_name = ? AND source_name = ? AND name = ?",
                ([description] if description is not None else [])
                + [table_name, source_name, name],
            )
        else:
            self.con.execute(
                "UPDATE columns SET locked = 0, locked_at = NULL "
                "WHERE table_name = ? AND source_name = ? AND name = ?",
                (table_name, source_name, name),
            )
        self.con.commit()

    def get_tables(self, source_name: str) -> list[dict]:
        return [
            dict(r)
            for r in self.con.execute(
                "SELECT * FROM tables WHERE source_name = ?", (source_name,)
            ).fetchall()
        ]

    def get_columns(self, table_name: str, source_name: str) -> list[dict]:
        return [
            dict(r)
            for r in self.con.execute(
                "SELECT * FROM columns WHERE table_name = ? AND source_name = ? ORDER BY ordinal",
                (table_name, source_name),
            ).fetchall()
        ]

    def mark_removed_tables(
        self,
        source_name: str,
        current_table_names: list[str],
        run_id: int,
    ) -> list[str]:
        """Mark tables not in current_table_names as removed.

        Returns list of table names that were marked as removed.
        """
        existing = self.get_tables(source_name)
        current_set = set(current_table_names)
        removed: list[str] = []
        for t in existing:
            if t["name"] not in current_set and t.get("removed_in_run_id") is None:
                self.con.execute(
                    "UPDATE tables SET removed_in_run_id = ? WHERE name = ? AND source_name = ?",
                    (run_id, t["name"], source_name),
                )
                removed.append(t["name"])
        if removed:
            self.con.commit()
        return removed

    def get_active_tables(self, source_name: str) -> list[dict]:
        """Return tables that have not been marked as removed."""
        return [
            dict(r)
            for r in self.con.execute(
                "SELECT * FROM tables WHERE source_name = ? AND removed_in_run_id IS NULL",
                (source_name,),
            ).fetchall()
        ]

    # -- Data Dictionary review ------------------------------------------------

    def update_table_review_status(
        self,
        name: str,
        source_name: str,
        status: str,
    ) -> None:
        """Set review_status on a table. 'reviewed' also sets reviewed_at."""
        if status == "reviewed":
            self.con.execute(
                "UPDATE tables SET review_status = ?, reviewed_at = datetime('now') "
                "WHERE name = ? AND source_name = ?",
                (status, name, source_name),
            )
        else:
            self.con.execute(
                "UPDATE tables SET review_status = ? WHERE name = ? AND source_name = ?",
                (status, name, source_name),
            )
        self.con.commit()

    def bulk_update_columns(
        self,
        table_name: str,
        source_name: str,
        updates: list[dict],
        lock: bool = False,
    ) -> None:
        """Bulk update columns for a table during review.

        Each dict in *updates* may contain: name (required), description,
        semantic_type, role, is_primary_key, confidence.
        If *lock* is True, all updated columns get locked=1.
        """
        for col in updates:
            col_name = col["name"]
            sets: list[str] = []
            params: list = []

            for field in ("description", "semantic_type", "role"):
                if field in col and col[field] is not None:
                    sets.append(f"{field} = ?")
                    params.append(col[field])

            if "confidence" in col:
                sets.append("confidence = ?")
                params.append(col["confidence"])

            if "is_primary_key" in col and col["is_primary_key"] is not None:
                sets.append("is_primary_key = ?")
                params.append(int(col["is_primary_key"]))

            if lock:
                sets.append("locked = 1")
                sets.append("locked_at = datetime('now')")

            if not sets:
                continue

            sql = (
                f"UPDATE columns SET {', '.join(sets)} "
                "WHERE table_name = ? AND source_name = ? AND name = ?"
            )
            params.extend([table_name, source_name, col_name])
            self.con.execute(sql, params)

        self.con.commit()

    def get_reviewed_tables(self, source_name: str) -> list[dict]:
        """Return tables where review_status is 'reviewed' or 'skipped'."""
        return [
            dict(r)
            for r in self.con.execute(
                "SELECT * FROM tables WHERE source_name = ? "
                "AND review_status IN ('reviewed', 'skipped') "
                "AND removed_in_run_id IS NULL",
                (source_name,),
            ).fetchall()
        ]

    def get_review_summary(self, source_name: str) -> dict:
        """Return review progress counts for a source."""
        rows = self.con.execute(
            "SELECT review_status, COUNT(*) as cnt FROM tables "
            "WHERE source_name = ? AND removed_in_run_id IS NULL "
            "GROUP BY review_status",
            (source_name,),
        ).fetchall()
        counts = {r["review_status"]: r["cnt"] for r in rows}
        total = sum(counts.values())
        reviewed = counts.get("reviewed", 0)
        return {
            "total": total,
            "reviewed": reviewed,
            "pending": counts.get("pending", 0),
            "in_review": counts.get("in_review", 0),
            "skipped": counts.get("skipped", 0),
            "pct_complete": round(reviewed / total * 100, 1) if total > 0 else 0.0,
        }

    def compute_rerun_summary(
        self,
        source_name: str,
        current_table_names: list[str],
        previous_table_names: list[str],
    ) -> dict:
        """Compute re-run summary: unchanged, updated, added, removed counts.

        Args:
            source_name: The source being re-run.
            current_table_names: Table names discovered in the current run.
            previous_table_names: Table names from the previous run (active only).

        Returns:
            dict with keys: unchanged, updated, added, removed.
        """
        current_set = set(current_table_names)
        previous_set = set(previous_table_names)

        added = current_set - previous_set
        removed = previous_set - current_set
        # Tables in both runs are either unchanged or updated (we treat them
        # as "updated" since profiles/stats may have changed).
        common = current_set & previous_set

        return {
            "unchanged": 0,  # All common tables get re-profiled so they are "updated"
            "updated": len(common),
            "added": len(added),
            "removed": len(removed),
        }

    # -- Confidence metrics (US-302, US-303) --------------------------------

    def get_description_acceptance_rate(
        self,
        source_name: str | None = None,
        min_decisions: int = 5,
    ) -> dict:
        """Compute description acceptance rate from decisions.

        Looks at decisions where artifact_type='column' and action in
        ('locked', 'description_accepted', 'description_edited').
        An 'accepted' action means the auto-generated description was kept as-is.
        An 'edited' or 'locked' action with a changed description means it was modified.

        Returns:
            dict with keys: acceptance_rate (float|None), sample_size, reason (str|None)
        """
        if source_name:
            rows = self.con.execute(
                "SELECT action, payload_json FROM decisions "
                "WHERE artifact_type = 'column' AND artifact_id LIKE ? "
                "AND action IN ('locked', 'description_accepted', 'description_edited') "
                "ORDER BY created_at",
                (f"{source_name}.%",),
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT action, payload_json FROM decisions "
                "WHERE artifact_type = 'column' "
                "AND action IN ('locked', 'description_accepted', 'description_edited') "
                "ORDER BY created_at",
            ).fetchall()

        total = len(rows)
        if total < min_decisions:
            return {
                "acceptance_rate": None,
                "sample_size": total,
                "reason": f"Below minimum threshold ({total}/{min_decisions} decisions)",
            }

        accepted = sum(1 for r in rows if r["action"] == "description_accepted")
        return {
            "acceptance_rate": round(accepted / total, 4) if total > 0 else 0.0,
            "sample_size": total,
            "reason": None,
        }

    def get_model_edit_distance_avg(self, source_name: str | None = None) -> dict:
        """Compute average model edit distance from decisions.

        Looks at decisions with artifact_type='model' and action='edited'
        that include 'edit_distance' in payload_json.

        Returns:
            dict with keys: edit_distance_avg (float|None), sample_size
        """
        if source_name:
            rows = self.con.execute(
                "SELECT payload_json FROM decisions "
                "WHERE artifact_type = 'model' AND action = 'edited' "
                "AND artifact_id LIKE ?",
                (f"{source_name}%",),
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT payload_json FROM decisions "
                "WHERE artifact_type = 'model' AND action = 'edited'",
            ).fetchall()

        distances = []
        for r in rows:
            if r["payload_json"]:
                payload = json.loads(r["payload_json"])
                if "edit_distance" in payload:
                    distances.append(payload["edit_distance"])

        if not distances:
            return {"edit_distance_avg": None, "sample_size": 0}

        avg = round(sum(distances) / len(distances), 4)
        return {"edit_distance_avg": avg, "sample_size": len(distances)}

    def get_contract_precision(self, source_name: str | None = None) -> dict:
        """Compute contract precision from decisions.

        Precision = true alerts / (true alerts + false positives).
        Looks at decisions with artifact_type='contract'.

        Returns:
            dict with keys: precision (float|None), sample_size
        """
        if source_name:
            rows = self.con.execute(
                "SELECT action FROM decisions "
                "WHERE artifact_type = 'contract' "
                "AND action IN ('false_positive', 'true_positive', 'acknowledged') "
                "AND artifact_id IN ("
                "  SELECT id FROM contracts WHERE model_name LIKE ?"
                ")",
                (f"%{source_name}%",),
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT action FROM decisions "
                "WHERE artifact_type = 'contract' "
                "AND action IN ('false_positive', 'true_positive', 'acknowledged')",
            ).fetchall()

        total = len(rows)
        if total == 0:
            return {"precision": None, "sample_size": 0}

        false_positives = sum(1 for r in rows if r["action"] == "false_positive")
        true_positives = total - false_positives
        precision = round(true_positives / total, 4) if total > 0 else None
        return {"precision": precision, "sample_size": total}

    # -- Schema snapshot building -------------------------------------------

    def build_snapshot_from_tables(self, source_name: str) -> dict:
        """Build a schema snapshot dict from current metadata for a source.

        Returns:
            dict mapping table_name -> {columns: [...], row_count: int}
        """
        snapshot: dict = {}
        tables = self.get_active_tables(source_name)
        for t in tables:
            columns = self.get_columns(t["name"], source_name)
            snapshot[t["name"]] = {
                "columns": [
                    {
                        "name": c["name"],
                        "dtype": c["dtype"],
                        "nullable": bool(c["nullable"]),
                    }
                    for c in columns
                ],
                "row_count": t["row_count"],
            }
        return snapshot

    # -- Drift reports (list all) -------------------------------------------

    def get_drift_reports(
        self,
        source_name: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return drift reports, optionally filtered by source."""
        if source_name:
            rows = self.con.execute(
                "SELECT * FROM drift_reports WHERE source_name = ? ORDER BY id DESC LIMIT ?",
                (source_name, limit),
            ).fetchall()
        else:
            rows = self.con.execute(
                "SELECT * FROM drift_reports ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        results = []
        for row in rows:
            r = dict(row)
            r["diff"] = json.loads(r["diff_json"])
            results.append(r)
        return results

    # -- Profiles ----------------------------------------------------------

    def upsert_profile(
        self,
        table_name: str,
        column_name: str,
        source_name: str,
        dtype: str,
        stats: dict,
        run_id: int | None = None,
    ) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO profiles "
            "(table_name, column_name, source_name, dtype, stats_json, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (table_name, column_name, source_name, dtype, json.dumps(stats), run_id),
        )
        self.con.commit()

    # -- Relationships -----------------------------------------------------

    def insert_relationship(
        self,
        source_name: str,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: str,
        rel_type: str,
        confidence: float,
        ref_integrity: float,
        detection_source: str,
        run_id: int | None = None,
    ) -> int:
        """Insert a relationship and return its id."""
        cur = self.con.execute(
            "INSERT INTO relationships "
            "(source_name, from_table, from_column, to_table, to_column, "
            "rel_type, confidence, ref_integrity, detection_source, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                source_name,
                from_table,
                from_column,
                to_table,
                to_column,
                rel_type,
                confidence,
                ref_integrity,
                detection_source,
                run_id,
            ),
        )
        self.con.commit()
        return cur.lastrowid or 0

    def get_relationships(self, source_name: str) -> list[dict]:
        return [
            dict(r)
            for r in self.con.execute(
                "SELECT * FROM relationships WHERE source_name = ?", (source_name,)
            ).fetchall()
        ]

    def clear_relationships(self, source_name: str) -> int:
        """Delete all relationships for a source. Returns count deleted."""
        cur = self.con.execute("DELETE FROM relationships WHERE source_name = ?", (source_name,))
        self.con.commit()
        return cur.rowcount

    def get_relationship(self, relationship_id: int) -> dict | None:
        row = self.con.execute(
            "SELECT * FROM relationships WHERE id = ?", (relationship_id,)
        ).fetchone()
        return dict(row) if row else None

    def delete_relationship(self, relationship_id: int) -> bool:
        cur = self.con.execute("DELETE FROM relationships WHERE id = ?", (relationship_id,))
        self.con.commit()
        return cur.rowcount > 0

    # -- Profiles (read-back) ------------------------------------------------

    def get_profiles(self, source_name: str) -> list[dict]:
        """Return all profiles for a source with parsed stats."""
        rows = self.con.execute(
            "SELECT * FROM profiles WHERE source_name = ?", (source_name,)
        ).fetchall()
        results = []
        for row in rows:
            r = dict(row)
            r["stats"] = json.loads(r["stats_json"])
            del r["stats_json"]
            results.append(r)
        return results

    # -- Rebuild discovery from persisted state --------------------------------

    def rebuild_discovery(self, source_name: str):
        """Reconstruct a DiscoveryResult from persisted metadata.

        Returns None if no source or tables are found.
        """
        from headwater.core.models import (
            ColumnInfo,
            ColumnProfile,
            CompanionDoc,
            DiscoveryResult,
            Relationship,
            SourceConfig,
            TableInfo,
            TableSemanticDetail,
        )

        logger.info("rebuild_discovery: looking up source '%s'", source_name)
        source_row = self.get_source(source_name)
        if source_row is None:
            logger.warning(
                "rebuild_discovery: source '%s' not found in metadata store",
                source_name,
            )
            return None

        logger.info(
            "rebuild_discovery: source found -- type=%s, path=%s, uri=%s",
            source_row.get("type"),
            source_row.get("path"),
            source_row.get("uri"),
        )
        source = SourceConfig(
            name=source_row["name"],
            type=source_row["type"],
            path=source_row.get("path"),
            uri=source_row.get("uri"),
            mode=source_row.get("mode", "generate"),
        )

        table_rows = self.get_active_tables(source_name)
        if not table_rows:
            logger.warning("rebuild_discovery: no active tables for source '%s'", source_name)
            return None
        logger.info("rebuild_discovery: found %d active tables", len(table_rows))

        tables: list[TableInfo] = []
        for trow in table_rows:
            col_rows = self.get_columns(trow["name"], source_name)
            columns = [
                ColumnInfo(
                    name=c["name"],
                    dtype=c["dtype"],
                    nullable=bool(c["nullable"]),
                    is_primary_key=bool(c["is_primary_key"]),
                    description=c.get("description"),
                    semantic_type=c.get("semantic_type"),
                    role=c.get("role"),
                    confidence=c.get("confidence", 0.0),
                    locked=bool(c.get("locked", 0)),
                )
                for c in col_rows
            ]

            semantic_detail = None
            sd_row = self.get_semantic_detail(trow["name"], source_name)
            if sd_row:
                semantic_detail = TableSemanticDetail(**sd_row)

            tables.append(
                TableInfo(
                    name=trow["name"],
                    schema_name=trow.get("schema_name"),
                    row_count=trow.get("row_count", 0),
                    columns=columns,
                    description=trow.get("description"),
                    domain=trow.get("domain"),
                    tags=json.loads(trow.get("tags", "[]")),
                    locked=bool(trow.get("locked", 0)),
                    review_status=trow.get("review_status", "pending"),
                    reviewed_at=trow.get("reviewed_at"),
                    semantic_detail=semantic_detail,
                )
            )

        # Rebuild profiles
        profile_rows = self.get_profiles(source_name)
        logger.info("rebuild_discovery: found %d profile rows", len(profile_rows))
        profiles: list[ColumnProfile] = []
        for prow in profile_rows:
            stats = prow["stats"]
            profiles.append(
                ColumnProfile(
                    table_name=prow["table_name"],
                    column_name=prow["column_name"],
                    dtype=prow["dtype"],
                    **stats,
                )
            )

        # Rebuild relationships
        rel_rows = self.get_relationships(source_name)
        logger.info("rebuild_discovery: found %d relationship rows", len(rel_rows))
        relationships: list[Relationship] = []
        for rrow in rel_rows:
            relationships.append(
                Relationship(
                    id=rrow["id"],
                    from_table=rrow["from_table"],
                    from_column=rrow["from_column"],
                    to_table=rrow["to_table"],
                    to_column=rrow["to_column"],
                    type=rrow["rel_type"],
                    confidence=rrow["confidence"],
                    referential_integrity=rrow["ref_integrity"],
                    source=rrow["detection_source"],
                )
            )

        # Rebuild companion docs
        doc_rows = self.get_companion_docs(source_name)
        logger.info("rebuild_discovery: found %d companion docs", len(doc_rows))
        companion_docs: list[CompanionDoc] = []
        for drow in doc_rows:
            matched = drow.get("matched_tables")
            if isinstance(matched, str):
                matched = json.loads(matched)
            companion_docs.append(
                CompanionDoc(
                    filename=drow["filename"],
                    content=drow["content"],
                    doc_type=drow.get("doc_type", "unknown"),
                    matched_tables=matched or [],
                    confidence=drow.get("confidence", 0.5),
                )
            )

        # Log per-table detail
        for t in tables:
            reviewed_cols = sum(1 for c in t.columns if c.locked)
            logger.info(
                "rebuild_discovery: table=%s, review_status=%s, cols=%d, locked_cols=%d",
                t.name,
                t.review_status,
                len(t.columns),
                reviewed_cols,
            )

        logger.info(
            "rebuild_discovery: complete -- %d tables, %d profiles, %d relationships, %d docs",
            len(tables),
            len(profiles),
            len(relationships),
            len(companion_docs),
        )

        return DiscoveryResult(
            source=source,
            tables=tables,
            profiles=profiles,
            relationships=relationships,
            companion_docs=companion_docs,
        )

    # -- Models ------------------------------------------------------------

    def upsert_model(
        self,
        name: str,
        source_name: str,
        model_type: str,
        sql_text: str,
        description: str = "",
        source_tables: list[str] | None = None,
        depends_on: list[str] | None = None,
        status: str = "proposed",
        assumptions: list[str] | None = None,
        questions: list[str] | None = None,
    ) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO models "
            "(name, source_name, model_type, sql_text, description, source_tables, "
            "depends_on, status, assumptions, questions, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (
                name,
                source_name,
                model_type,
                sql_text,
                description,
                json.dumps(source_tables or []),
                json.dumps(depends_on or []),
                status,
                json.dumps(assumptions or []),
                json.dumps(questions or []),
            ),
        )
        self.con.commit()

    def get_models(self, source_name: str | None = None) -> list[dict]:
        if source_name:
            rows = self.con.execute(
                "SELECT * FROM models WHERE source_name = ?", (source_name,)
            ).fetchall()
        else:
            rows = self.con.execute("SELECT * FROM models").fetchall()
        return [dict(r) for r in rows]

    def update_model_status(self, name: str, status: str) -> None:
        self.con.execute(
            "UPDATE models SET status = ?, updated_at = datetime('now') WHERE name = ?",
            (status, name),
        )
        self.con.commit()

    def record_model_review(
        self,
        model_name: str,
        decision: str,
        *,
        source_name: str | None = None,
        reviewer: str | None = None,
        reason: str | None = None,
        diff_summary: str | None = None,
        payload: dict | None = None,
    ) -> int:
        """Persist an audit record for a model review decision."""
        cur = self.con.execute(
            """
            INSERT INTO model_reviews
                (model_name, source_name, reviewer, decision, reason,
                 diff_summary, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                model_name,
                source_name,
                reviewer,
                decision,
                reason,
                diff_summary,
                json.dumps(payload) if payload is not None else None,
            ),
        )
        self.con.commit()
        return cur.lastrowid or 0

    def list_model_reviews(
        self,
        model_name: str | None = None,
        *,
        source_name: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return model review records, newest first."""
        clauses = []
        params: list = []
        if model_name:
            clauses.append("model_name = ?")
            params.append(model_name)
        if source_name:
            clauses.append("source_name = ?")
            params.append(source_name)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self.con.execute(
            f"SELECT * FROM model_reviews {where} "
            "ORDER BY created_at DESC, id DESC LIMIT ?",
            params,
        ).fetchall()
        reviews = []
        for row in rows:
            review = dict(row)
            if review.get("payload_json"):
                try:
                    review["payload"] = json.loads(review["payload_json"])
                except (TypeError, ValueError):
                    review["payload"] = None
            else:
                review["payload"] = None
            reviews.append(review)
        return reviews

    def save_model_impacts(self, impacts: list[dict]) -> list[int]:
        """Persist computed model impacts and return inserted ids."""
        ids: list[int] = []
        for impact in impacts:
            cur = self.con.execute(
                """
                INSERT INTO model_impacts
                    (source_name, drift_report_id, model_name, impact_type,
                     severity, source_table, source_column, contract_id,
                     reason, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    impact["source_name"],
                    impact.get("drift_report_id"),
                    impact["model_name"],
                    impact["impact_type"],
                    impact["severity"],
                    impact.get("source_table"),
                    impact.get("source_column"),
                    impact.get("contract_id"),
                    impact["reason"],
                    json.dumps(impact.get("payload")) if impact.get("payload") else None,
                ),
            )
            ids.append(cur.lastrowid or 0)
        self.con.commit()
        return ids

    def list_model_impacts(
        self,
        *,
        source_name: str | None = None,
        model_name: str | None = None,
        drift_report_id: int | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Return persisted model impacts, newest first."""
        clauses = []
        params: list = []
        if source_name:
            clauses.append("source_name = ?")
            params.append(source_name)
        if model_name:
            clauses.append("model_name = ?")
            params.append(model_name)
        if drift_report_id is not None:
            clauses.append("drift_report_id = ?")
            params.append(drift_report_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self.con.execute(
            f"SELECT * FROM model_impacts {where} "
            "ORDER BY created_at DESC, id DESC LIMIT ?",
            params,
        ).fetchall()
        impacts = []
        for row in rows:
            impact = dict(row)
            if impact.get("payload_json"):
                try:
                    impact["payload"] = json.loads(impact["payload_json"])
                except (TypeError, ValueError):
                    impact["payload"] = None
            else:
                impact["payload"] = None
            impacts.append(impact)
        return impacts

    # -- Contracts ---------------------------------------------------------

    def upsert_contract(
        self,
        id_: str,
        model_name: str,
        rule_type: str,
        expression: str,
        *,
        column_name: str | None = None,
        severity: str = "warning",
        description: str = "",
        confidence: float = 0.8,
        status: str = "proposed",
    ) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO contracts "
            "(id, model_name, column_name, rule_type, expression, severity, "
            "description, confidence, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                id_,
                model_name,
                column_name,
                rule_type,
                expression,
                severity,
                description,
                confidence,
                status,
            ),
        )
        self.con.commit()

    def get_contracts(self, model_name: str | None = None) -> list[dict]:
        if model_name:
            rows = self.con.execute(
                "SELECT * FROM contracts WHERE model_name = ?", (model_name,)
            ).fetchall()
        else:
            rows = self.con.execute("SELECT * FROM contracts").fetchall()
        return [dict(r) for r in rows]

    # -- Quality Results ----------------------------------------------------

    def save_quality_report(
        self,
        source_name: str | None,
        report,
        sync_run_id: int | None = None,
    ) -> int:
        """Persist a quality report aggregate and its contract results."""
        total = int(getattr(report, "total_contracts", 0) or 0)
        passed = int(getattr(report, "passed", 0) or 0)
        failed = int(getattr(report, "failed", 0) or 0)
        skipped = int(getattr(report, "skipped", 0) or 0)
        score = round((passed / total) * 100, 2) if total else 100.0
        status = "failing" if failed else "passing"
        cur = self.con.execute(
            """
            INSERT INTO quality_runs
                (source_name, sync_run_id, total_contracts, passed, failed,
                 skipped, score, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (source_name, sync_run_id, total, passed, failed, skipped, score, status),
        )
        if cur.lastrowid is None:
            raise MetadataError("Failed to create quality run")
        run_id = cur.lastrowid
        for result in getattr(report, "results", []) or []:
            self.con.execute(
                """
                INSERT INTO quality_results
                    (run_id, source_name, rule_id, model_name, passed,
                     observed_value_json, message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source_name,
                    getattr(result, "rule_id", "") or "",
                    getattr(result, "model_name", "") or "",
                    int(bool(getattr(result, "passed", False))),
                    json.dumps(getattr(result, "observed_value", None), default=str),
                    getattr(result, "message", "") or "",
                ),
            )
        if source_name and failed:
            existing = self.con.execute(
                "SELECT drift_count FROM sources WHERE name = ?", (source_name,)
            ).fetchone()
            if existing is not None:
                drift_count = int(existing["drift_count"] or 0)
                health = max(0, min(int(round(score)), 100 - 5 * drift_count))
                self.con.execute(
                    "UPDATE sources SET status = 'warning', health = ? WHERE name = ?",
                    (health, source_name),
                )
        self.con.commit()
        return run_id

    def get_latest_quality_report(self, source_name: str | None = None) -> dict | None:
        """Return the latest persisted quality run and its result rows."""
        if source_name:
            row = self.con.execute(
                "SELECT * FROM quality_runs WHERE source_name = ? ORDER BY id DESC LIMIT 1",
                (source_name,),
            ).fetchone()
        else:
            row = self.con.execute(
                "SELECT * FROM quality_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        report = dict(row)
        rows = self.con.execute(
            "SELECT * FROM quality_results WHERE run_id = ? ORDER BY id",
            (report["id"],),
        ).fetchall()
        results = []
        for result_row in rows:
            result = dict(result_row)
            if result.get("observed_value_json") is not None:
                try:
                    result["observed_value"] = json.loads(result["observed_value_json"])
                except (TypeError, ValueError):
                    result["observed_value"] = None
            results.append(result)
        report["results"] = results
        return report

    def attach_quality_run_to_sync(self, quality_run_id: int, sync_run_id: int) -> None:
        """Associate a persisted quality run with its outer source sync run."""
        self.con.execute(
            "UPDATE quality_runs SET sync_run_id = ? WHERE id = ?",
            (sync_run_id, quality_run_id),
        )
        self.con.commit()

    # -- Execution Results --------------------------------------------------

    def save_execution_result(
        self,
        model_name: str,
        success: bool,
        row_count: int | None = None,
        execution_time_ms: float = 0.0,
        error: str | None = None,
    ) -> None:
        self.con.execute(
            "INSERT INTO execution_results "
            "(model_name, success, row_count, execution_time_ms, error) "
            "VALUES (?, ?, ?, ?, ?)",
            (model_name, int(success), row_count, execution_time_ms, error),
        )
        self.con.commit()

    def get_execution_results(self) -> list[dict]:
        """Return the most recent execution result per model."""
        rows = self.con.execute(
            "SELECT * FROM execution_results "
            "WHERE id IN ("
            "  SELECT MAX(id) FROM execution_results GROUP BY model_name"
            ") ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    # -- Decisions ---------------------------------------------------------

    def record_decision(
        self,
        artifact_type: str,
        artifact_id: str,
        action: str,
        *,
        reason: str | None = None,
        payload: dict | None = None,
    ) -> None:
        """Record a human review decision in the decisions table.

        Args:
            artifact_type: 'model', 'contract', 'column', etc.
            artifact_id:   Unique identifier of the artifact (name, id, etc.).
            action:        Human action taken ('approved', 'rejected', 'locked', etc.).
            reason:        Optional human-readable reason.
            payload:       Optional dict of before/after values or context data.
        """
        payload_json = json.dumps(payload) if payload is not None else None
        self.con.execute(
            "INSERT INTO decisions "
            "(artifact_type, artifact_id, action, reason, payload_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (artifact_type, artifact_id, action, reason, payload_json),
        )
        self.con.commit()

    def get_decisions(
        self,
        artifact_type: str | None = None,
        artifact_id: str | None = None,
    ) -> list[dict]:
        """Return decisions, optionally filtered by type and/or artifact id."""
        if artifact_type and artifact_id:
            rows = self.con.execute(
                "SELECT * FROM decisions WHERE artifact_type = ? AND artifact_id = ? "
                "ORDER BY created_at DESC",
                (artifact_type, artifact_id),
            ).fetchall()
        elif artifact_type:
            rows = self.con.execute(
                "SELECT * FROM decisions WHERE artifact_type = ? ORDER BY created_at DESC",
                (artifact_type,),
            ).fetchall()
        else:
            rows = self.con.execute("SELECT * FROM decisions ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]

    # -- LLM audit log -----------------------------------------------------

    def insert_llm_audit(
        self,
        provider: str,
        model: str,
        prompt_text: str,
        response_text: str,
        *,
        prompt_hash: str | None = None,
        tokens_in: int = 0,
        tokens_out: int = 0,
        cached: int = 0,
    ) -> None:
        """Write one row to the LLM audit log."""
        self.con.execute(
            "INSERT INTO llm_audit_log "
            "(provider, model, prompt_hash, prompt_text, response_text, "
            "tokens_in, tokens_out, cached) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                provider,
                model,
                prompt_hash,
                prompt_text,
                response_text,
                tokens_in,
                tokens_out,
                cached,
            ),
        )
        self.con.commit()

    def get_llm_audit_log(self, limit: int = 100) -> list[dict]:
        """Return the most recent LLM audit log entries."""
        rows = self.con.execute(
            "SELECT * FROM llm_audit_log ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    # -- Schema snapshots (US-401) -----------------------------------------

    def save_snapshot(self, run_id: int, source_name: str, snapshot: dict) -> None:
        """Persist a schema snapshot for the given run."""
        self.con.execute(
            "INSERT INTO schema_snapshots (run_id, source_name, snapshot_json, captured_at) "
            "VALUES (?, ?, ?, datetime('now'))",
            (run_id, source_name, json.dumps(snapshot)),
        )
        self.con.commit()

    def get_latest_snapshot(self, source_name: str, before_run_id: int) -> dict | None:
        """Return the most recent snapshot for source_name before the given run_id.

        Returns None if no prior snapshot exists (first run).
        """
        record = self.get_latest_snapshot_record(source_name, before_run_id)
        if record is None:
            return None
        return record["snapshot"]

    def get_latest_snapshot_record(
        self,
        source_name: str,
        before_run_id: int,
    ) -> dict | None:
        """Return run metadata and snapshot for the latest prior snapshot."""
        row = self.con.execute(
            "SELECT run_id, snapshot_json FROM schema_snapshots "
            "WHERE source_name = ? AND run_id < ? "
            "ORDER BY run_id DESC LIMIT 1",
            (source_name, before_run_id),
        ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row["run_id"],
            "snapshot": json.loads(row["snapshot_json"]),
        }

    # -- Drift reports (US-401) --------------------------------------------

    def save_drift_report(
        self,
        source_name: str,
        run_id_from: int | None,
        run_id_to: int,
        diff: dict,
    ) -> int:
        """Persist a drift report. Returns the new report id.

        If the diff describes real changes, also emits a sync_event and bumps
        the source's drift counter so the Briefing page can surface it.
        """
        cur = self.con.execute(
            "INSERT INTO drift_reports "
            "(source_name, run_id_from, run_id_to, diff_json, detected_at) "
            "VALUES (?, ?, ?, ?, datetime('now'))",
            (source_name, run_id_from, run_id_to, json.dumps(diff)),
        )
        self.con.commit()
        if cur.lastrowid is None:
            raise MetadataError("Failed to create drift report")

        # Skip event emission when nothing changed -- diff exists but is empty.
        if diff.get("no_changes"):
            return cur.lastrowid

        added = len(diff.get("tables_added") or [])
        removed = len(diff.get("tables_removed") or [])
        changed = len(diff.get("tables_changed") or [])
        bits = []
        if added:
            bits.append(f"{added} table(s) added")
        if removed:
            bits.append(f"{removed} table(s) removed")
        if changed:
            bits.append(f"{changed} table(s) changed")
        detail = "Drift detected -- " + ", ".join(bits) if bits else "Drift detected"

        try:
            self.insert_sync_event(
                source_name,
                "drift",
                detail,
                severity="warning",
                payload={
                    "report_id": cur.lastrowid,
                    "added": added,
                    "removed": removed,
                    "changed": changed,
                },
            )
            existing = self.con.execute(
                "SELECT drift_count FROM sources WHERE name = ?", (source_name,)
            ).fetchone()
            if existing is not None:
                new_count = (existing["drift_count"] or 0) + 1
                self.con.execute(
                    "UPDATE sources SET drift_count = ?, status = 'warning' WHERE name = ?",
                    (new_count, source_name),
                )
                self.con.commit()
        except Exception:
            logger.exception("Failed to emit sync_event for drift on '%s'", source_name)

        return cur.lastrowid

    def get_latest_drift_report(self, source_name: str | None = None) -> dict | None:
        """Return the most recent drift report, optionally filtered by source."""
        if source_name:
            row = self.con.execute(
                "SELECT * FROM drift_reports WHERE source_name = ? ORDER BY id DESC LIMIT 1",
                (source_name,),
            ).fetchone()
        else:
            row = self.con.execute(
                "SELECT * FROM drift_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["diff"] = json.loads(result["diff_json"])
        return result

    def acknowledge_drift_report(self, report_id: int) -> None:
        """Mark a drift report as acknowledged."""
        self.con.execute(
            "UPDATE drift_reports SET acknowledged = 1 WHERE id = ?",
            (report_id,),
        )
        self.con.commit()

    # -- Semantic details --------------------------------------------------

    def upsert_semantic_detail(
        self,
        table_name: str,
        source_name: str,
        detail: dict,
        run_id: int | None = None,
    ) -> None:
        """Persist a TableSemanticDetail as JSON for a table."""
        self.con.execute(
            "INSERT INTO table_semantic_details "
            "(table_name, source_name, detail_json, run_id, updated_at) "
            "VALUES (?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(table_name, source_name) DO UPDATE SET "
            "detail_json = excluded.detail_json, run_id = excluded.run_id, "
            "updated_at = datetime('now')",
            (table_name, source_name, json.dumps(detail), run_id),
        )
        self.con.commit()

    def get_semantic_detail(self, table_name: str, source_name: str) -> dict | None:
        """Return the semantic detail dict for a table, or None."""
        row = self.con.execute(
            "SELECT detail_json FROM table_semantic_details "
            "WHERE table_name = ? AND source_name = ?",
            (table_name, source_name),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["detail_json"])

    # -- Companion docs ----------------------------------------------------

    def upsert_companion_doc(
        self,
        source_name: str,
        filename: str,
        content: str,
        doc_type: str = "unknown",
        matched_tables: list[str] | None = None,
        confidence: float = 0.5,
        run_id: int | None = None,
    ) -> None:
        """Persist a companion documentation file."""
        self.con.execute(
            "INSERT INTO companion_docs "
            "(source_name, filename, content, doc_type, matched_tables, confidence, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(source_name, filename) DO UPDATE SET "
            "content = excluded.content, doc_type = excluded.doc_type, "
            "matched_tables = excluded.matched_tables, confidence = excluded.confidence, "
            "run_id = excluded.run_id",
            (
                source_name,
                filename,
                content,
                doc_type,
                json.dumps(matched_tables or []),
                confidence,
                run_id,
            ),
        )
        self.con.commit()

    def get_companion_docs(self, source_name: str) -> list[dict]:
        """Return all companion docs for a source."""
        rows = self.con.execute(
            "SELECT * FROM companion_docs WHERE source_name = ?",
            (source_name,),
        ).fetchall()
        results = []
        for row in rows:
            r = dict(row)
            r["matched_tables"] = json.loads(r["matched_tables"])
            results.append(r)
        return results

    # -- Projects (v2) -----------------------------------------------------

    def upsert_project(
        self,
        id_: str,
        slug: str,
        display_name: str,
        *,
        description: str = "",
        sources_json: str = "[]",
        maturity: str = "raw",
        maturity_score: float = 0.0,
        catalog_confidence: float = 0.0,
    ) -> None:
        self.con.execute(
            "INSERT INTO projects "
            "(id, slug, display_name, description, sources_json, maturity, "
            "maturity_score, catalog_confidence) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "slug = excluded.slug, display_name = excluded.display_name, "
            "description = excluded.description, sources_json = excluded.sources_json, "
            "maturity = excluded.maturity, maturity_score = excluded.maturity_score, "
            "catalog_confidence = excluded.catalog_confidence, "
            "updated_at = datetime('now')",
            (
                id_,
                slug,
                display_name,
                description,
                sources_json,
                maturity,
                maturity_score,
                catalog_confidence,
            ),
        )
        self.con.commit()

    def get_project(self, project_id: str) -> dict | None:
        row = self.con.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
        if row is None:
            return None
        r = dict(row)
        r["sources"] = json.loads(r.pop("sources_json", "[]"))
        return r

    def get_project_by_slug(self, slug: str) -> dict | None:
        row = self.con.execute("SELECT * FROM projects WHERE slug = ?", (slug,)).fetchone()
        if row is None:
            return None
        r = dict(row)
        r["sources"] = json.loads(r.pop("sources_json", "[]"))
        return r

    def list_projects(self) -> list[dict]:
        rows = self.con.execute("SELECT * FROM projects ORDER BY updated_at DESC").fetchall()
        results = []
        for row in rows:
            r = dict(row)
            r["sources"] = json.loads(r.pop("sources_json", "[]"))
            results.append(r)
        return results

    def update_project_maturity(
        self,
        project_id: str,
        maturity: str,
        maturity_score: float,
    ) -> None:
        self.con.execute(
            "UPDATE projects SET maturity = ?, maturity_score = ?, "
            "updated_at = datetime('now') WHERE id = ?",
            (maturity, maturity_score, project_id),
        )
        self.con.commit()

    def delete_project(self, project_id: str) -> bool:
        cur = self.con.execute("DELETE FROM projects WHERE id = ?", (project_id,))
        self.con.commit()
        return cur.rowcount > 0

    # -- Catalog metrics (v2) ----------------------------------------------

    def upsert_catalog_metric(
        self,
        project_id: str,
        name: str,
        display_name: str,
        description: str,
        expression: str,
        table_name: str,
        agg_type: str,
        *,
        column_name: str | None = None,
        filters: list[str] | None = None,
        synonyms: list[str] | None = None,
        confidence: float = 0.5,
        status: str = "proposed",
        source: str = "heuristic",
    ) -> None:
        self.con.execute(
            "INSERT INTO catalog_metrics "
            "(name, project_id, display_name, description, expression, "
            "column_name, table_name, agg_type, filters_json, synonyms_json, "
            "confidence, status, source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(name, project_id) DO UPDATE SET "
            "display_name = excluded.display_name, description = excluded.description, "
            "expression = excluded.expression, column_name = excluded.column_name, "
            "table_name = excluded.table_name, agg_type = excluded.agg_type, "
            "filters_json = excluded.filters_json, synonyms_json = excluded.synonyms_json, "
            "confidence = excluded.confidence, status = excluded.status, "
            "source = excluded.source",
            (
                name,
                project_id,
                display_name,
                description,
                expression,
                column_name,
                table_name,
                agg_type,
                json.dumps(filters or []),
                json.dumps(synonyms or []),
                confidence,
                status,
                source,
            ),
        )
        self.con.commit()

    def get_catalog_metrics(self, project_id: str) -> list[dict]:
        rows = self.con.execute(
            "SELECT * FROM catalog_metrics WHERE project_id = ?", (project_id,)
        ).fetchall()
        results = []
        for row in rows:
            r = dict(row)
            r["filters"] = json.loads(r.pop("filters_json", "[]"))
            r["synonyms"] = json.loads(r.pop("synonyms_json", "[]"))
            results.append(r)
        return results

    # -- Catalog dimensions (v2) -------------------------------------------

    def upsert_catalog_dimension(
        self,
        project_id: str,
        name: str,
        display_name: str,
        description: str,
        column_name: str,
        table_name: str,
        dtype: str,
        *,
        expression: str | None = None,
        synonyms: list[str] | None = None,
        hierarchy: list[str] | None = None,
        sample_values: list[str] | None = None,
        cardinality: int = 0,
        confidence: float = 0.5,
        status: str = "proposed",
        source: str = "heuristic",
        join_path: str | None = None,
        join_nullable: bool = False,
    ) -> None:
        self.con.execute(
            "INSERT INTO catalog_dimensions "
            "(name, project_id, display_name, description, column_name, "
            "table_name, dtype, expression, synonyms_json, hierarchy_json, "
            "sample_values_json, cardinality, confidence, status, source, "
            "join_path, join_nullable) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(name, project_id) DO UPDATE SET "
            "display_name = excluded.display_name, description = excluded.description, "
            "column_name = excluded.column_name, table_name = excluded.table_name, "
            "dtype = excluded.dtype, expression = excluded.expression, "
            "synonyms_json = excluded.synonyms_json, hierarchy_json = excluded.hierarchy_json, "
            "sample_values_json = excluded.sample_values_json, "
            "cardinality = excluded.cardinality, confidence = excluded.confidence, "
            "status = excluded.status, source = excluded.source, "
            "join_path = excluded.join_path, join_nullable = excluded.join_nullable",
            (
                name,
                project_id,
                display_name,
                description,
                column_name,
                table_name,
                dtype,
                expression,
                json.dumps(synonyms or []),
                json.dumps(hierarchy or []),
                json.dumps(sample_values or []),
                cardinality,
                confidence,
                status,
                source,
                join_path,
                int(join_nullable),
            ),
        )
        self.con.commit()

    def get_catalog_dimensions(self, project_id: str) -> list[dict]:
        rows = self.con.execute(
            "SELECT * FROM catalog_dimensions WHERE project_id = ?", (project_id,)
        ).fetchall()
        results = []
        for row in rows:
            r = dict(row)
            r["synonyms"] = json.loads(r.pop("synonyms_json", "[]"))
            r["hierarchy"] = json.loads(r.pop("hierarchy_json", "[]"))
            r["sample_values"] = json.loads(r.pop("sample_values_json", "[]"))
            r["join_nullable"] = bool(r.get("join_nullable", 0))
            results.append(r)
        return results

    # -- Catalog entities (v2) ---------------------------------------------

    def upsert_catalog_entity(
        self,
        project_id: str,
        name: str,
        display_name: str,
        description: str,
        table_name: str,
        row_semantics: str,
        *,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        temporal_grain: str | None = None,
        synonyms: list[str] | None = None,
    ) -> None:
        self.con.execute(
            "INSERT INTO catalog_entities "
            "(name, project_id, display_name, description, table_name, "
            "row_semantics, metrics_json, dimensions_json, temporal_grain, synonyms_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(name, project_id) DO UPDATE SET "
            "display_name = excluded.display_name, description = excluded.description, "
            "table_name = excluded.table_name, row_semantics = excluded.row_semantics, "
            "metrics_json = excluded.metrics_json, dimensions_json = excluded.dimensions_json, "
            "temporal_grain = excluded.temporal_grain, synonyms_json = excluded.synonyms_json",
            (
                name,
                project_id,
                display_name,
                description,
                table_name,
                row_semantics,
                json.dumps(metrics or []),
                json.dumps(dimensions or []),
                temporal_grain,
                json.dumps(synonyms or []),
            ),
        )
        self.con.commit()

    def get_catalog_entities(self, project_id: str) -> list[dict]:
        rows = self.con.execute(
            "SELECT * FROM catalog_entities WHERE project_id = ?", (project_id,)
        ).fetchall()
        results = []
        for row in rows:
            r = dict(row)
            r["metrics"] = json.loads(r.pop("metrics_json", "[]"))
            r["dimensions"] = json.loads(r.pop("dimensions_json", "[]"))
            r["synonyms"] = json.loads(r.pop("synonyms_json", "[]"))
            results.append(r)
        return results

    def update_catalog_metric_status(
        self,
        project_id: str,
        name: str,
        status: str,
        confidence: float | None = None,
        source: str | None = None,
    ) -> bool:
        """Update the status (and optionally confidence/source) of a catalog metric."""
        parts = ["status = ?"]
        params: list = [status]
        if confidence is not None:
            parts.append("confidence = ?")
            params.append(confidence)
        if source is not None:
            parts.append("source = ?")
            params.append(source)
        params.extend([name, project_id])
        cur = self.con.execute(
            f"UPDATE catalog_metrics SET {', '.join(parts)} WHERE name = ? AND project_id = ?",
            params,
        )
        self.con.commit()
        return cur.rowcount > 0

    def update_catalog_dimension_status(
        self,
        project_id: str,
        name: str,
        status: str,
        confidence: float | None = None,
        source: str | None = None,
        synonyms: list[str] | None = None,
    ) -> bool:
        """Update the status (and optionally confidence/source/synonyms) of a catalog dimension."""
        parts = ["status = ?"]
        params: list = [status]
        if confidence is not None:
            parts.append("confidence = ?")
            params.append(confidence)
        if source is not None:
            parts.append("source = ?")
            params.append(source)
        if synonyms is not None:
            parts.append("synonyms_json = ?")
            params.append(json.dumps(synonyms))
        params.extend([name, project_id])
        cur = self.con.execute(
            f"UPDATE catalog_dimensions SET {', '.join(parts)} WHERE name = ? AND project_id = ?",
            params,
        )
        self.con.commit()
        return cur.rowcount > 0

    def clear_catalog(self, project_id: str) -> None:
        """Delete all catalog entries for a project."""
        self.con.execute("DELETE FROM catalog_metrics WHERE project_id = ?", (project_id,))
        self.con.execute("DELETE FROM catalog_dimensions WHERE project_id = ?", (project_id,))
        self.con.execute("DELETE FROM catalog_entities WHERE project_id = ?", (project_id,))
        self.con.commit()

    # -- Activity log (v3) -------------------------------------------------

    def log_activity(
        self,
        action: str,
        detail: str | None = None,
        *,
        artifact_type: str | None = None,
        artifact_id: str | None = None,
    ) -> None:
        """Record a user or system action in the activity log."""
        self.con.execute(
            "INSERT INTO activity_log (action, detail, artifact_type, artifact_id) "
            "VALUES (?, ?, ?, ?)",
            (action, detail, artifact_type, artifact_id),
        )
        self.con.commit()

    def get_activity(self, limit: int = 20) -> list[dict]:
        """Return recent activity entries, newest first."""
        rows = self.con.execute(
            "SELECT * FROM activity_log ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    # -- Model answers (v3) ------------------------------------------------

    def save_model_answers(
        self,
        model_name: str,
        answers: list[dict],
    ) -> int:
        """Save answers to model clarifying questions.

        Each answer dict has keys: question_index (int), answer (str).
        Returns the number of answers saved.
        """
        count = 0
        for a in answers:
            self.con.execute(
                "INSERT INTO model_answers (model_name, question_index, answer) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(model_name, question_index) DO UPDATE SET "
                "answer = excluded.answer, created_at = datetime('now')",
                (model_name, a["question_index"], a["answer"]),
            )
            count += 1
        self.con.commit()
        return count

    def get_model_answers(self, model_name: str) -> list[dict]:
        """Return all answers for a model's clarifying questions."""
        rows = self.con.execute(
            "SELECT * FROM model_answers WHERE model_name = ? ORDER BY question_index",
            (model_name,),
        ).fetchall()
        return [dict(r) for r in rows]

    # -- PK/FK persistence (v3) --------------------------------------------

    def persist_pk_fk(
        self,
        table_name: str,
        source_name: str,
        *,
        confirm_pks: list[str] | None = None,
        reject_pks: list[str] | None = None,
        confirm_fks: list[dict] | None = None,
        reject_fk_ids: list[int] | None = None,
    ) -> dict:
        """Persist PK/FK confirmations and rejections.

        confirm_fks items: {from_col, to_table, to_col}
        Returns counts of changes made.
        """
        counts = {"pks_confirmed": 0, "pks_rejected": 0, "fks_confirmed": 0, "fks_rejected": 0}

        for col in confirm_pks or []:
            self.con.execute(
                "UPDATE columns SET is_primary_key = 1 "
                "WHERE table_name = ? AND source_name = ? AND name = ?",
                (table_name, source_name, col),
            )
            counts["pks_confirmed"] += 1

        for col in reject_pks or []:
            self.con.execute(
                "UPDATE columns SET is_primary_key = 0 "
                "WHERE table_name = ? AND source_name = ? AND name = ?",
                (table_name, source_name, col),
            )
            counts["pks_rejected"] += 1

        for fk in confirm_fks or []:
            self.con.execute(
                "INSERT OR REPLACE INTO relationships "
                "(source_name, from_table, from_column, to_table, to_column, "
                "rel_type, confidence, ref_integrity, detection_source) "
                "VALUES (?, ?, ?, ?, ?, 'fk', 1.0, 0.0, 'confirmed')",
                (source_name, table_name, fk["from_col"], fk["to_table"], fk["to_col"]),
            )
            counts["fks_confirmed"] += 1

        for fk_id in reject_fk_ids or []:
            self.con.execute("DELETE FROM relationships WHERE id = ?", (fk_id,))
            counts["fks_rejected"] += 1

        self.con.commit()
        return counts
