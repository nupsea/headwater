"""Lean project-centric SQLite store for Headwater 2."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sources (
    name TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    path TEXT,
    uri TEXT,
    latest_snapshot_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS source_snapshots (
    id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL REFERENCES sources(name),
    fingerprint TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    captured_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_source_snapshots_source
    ON source_snapshots(source_name, captured_at DESC);

CREATE TABLE IF NOT EXISTS tables (
    name TEXT NOT NULL,
    source_name TEXT NOT NULL REFERENCES sources(name),
    schema_name TEXT,
    row_count INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    domain TEXT,
    selected INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (name, source_name)
);

CREATE TABLE IF NOT EXISTS columns (
    table_name TEXT NOT NULL,
    source_name TEXT NOT NULL REFERENCES sources(name),
    name TEXT NOT NULL,
    dtype TEXT NOT NULL,
    nullable INTEGER NOT NULL DEFAULT 1,
    is_primary_key INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    semantic_type TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0,
    locked INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (table_name, source_name, name),
    FOREIGN KEY (table_name, source_name) REFERENCES tables(name, source_name)
);

CREATE TABLE IF NOT EXISTS profiles (
    table_name TEXT NOT NULL,
    source_name TEXT NOT NULL REFERENCES sources(name),
    column_name TEXT NOT NULL,
    snapshot_id TEXT,
    dtype TEXT NOT NULL,
    profile_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (table_name, source_name, column_name, snapshot_id)
);

CREATE TABLE IF NOT EXISTS relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL REFERENCES sources(name),
    from_table TEXT NOT NULL,
    from_column TEXT NOT NULL,
    to_table TEXT NOT NULL,
    to_column TEXT NOT NULL,
    rel_type TEXT NOT NULL,
    confidence REAL NOT NULL,
    referential_integrity REAL NOT NULL,
    snapshot_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    goal_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS project_sources (
    project_id TEXT NOT NULL REFERENCES projects(id),
    source_name TEXT NOT NULL REFERENCES sources(name),
    selected_tables_json TEXT NOT NULL DEFAULT '[]',
    scope_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (project_id, source_name)
);

CREATE TABLE IF NOT EXISTS semantic_claims (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    source_name TEXT REFERENCES sources(name),
    scope_type TEXT NOT NULL,
    table_name TEXT,
    column_name TEXT,
    claim_type TEXT NOT NULL,
    claim_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'proposed',
    confidence REAL NOT NULL DEFAULT 0.0,
    source TEXT NOT NULL DEFAULT 'bootstrap',
    locked INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_semantic_claims_project
    ON semantic_claims(project_id, status);

CREATE TABLE IF NOT EXISTS questions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    source_name TEXT REFERENCES sources(name),
    title TEXT NOT NULL,
    question_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'draft',
    answerability TEXT NOT NULL DEFAULT 'answerable',
    confidence REAL NOT NULL DEFAULT 0.0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_questions_project
    ON questions(project_id, status);

CREATE TABLE IF NOT EXISTS resolve_items (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    question_id TEXT REFERENCES questions(id),
    priority TEXT NOT NULL DEFAULT 'medium',
    issue_kind TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_resolve_items_project
    ON resolve_items(project_id, priority, status);

CREATE TABLE IF NOT EXISTS readiness_contracts (
    id TEXT PRIMARY KEY,
    question_id TEXT NOT NULL REFERENCES questions(id),
    contract_type TEXT NOT NULL,
    passed INTEGER NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    snapshot_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_readiness_contracts_question
    ON readiness_contracts(question_id, contract_type);

CREATE TABLE IF NOT EXISTS readiness_verdicts (
    id TEXT PRIMARY KEY,
    question_id TEXT NOT NULL REFERENCES questions(id),
    source_snapshot_id TEXT,
    state TEXT NOT NULL DEFAULT 'draft',
    readiness_pct INTEGER NOT NULL DEFAULT 0,
    trust_bucket TEXT NOT NULL DEFAULT 'not_started',
    summary TEXT NOT NULL DEFAULT '',
    freshness TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_readiness_verdicts_question
    ON readiness_verdicts(question_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS answer_artifacts (
    id TEXT PRIMARY KEY,
    question_id TEXT NOT NULL REFERENCES questions(id),
    sql_text TEXT,
    chart_spec_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL DEFAULT 'draft',
    certified_at TEXT,
    source_snapshot_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_answer_artifacts_question
    ON answer_artifacts(question_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_type TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_decisions_artifact
    ON decisions(artifact_type, artifact_id, created_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_state (
    project_id TEXT PRIMARY KEY REFERENCES projects(id),
    last_input_hash TEXT,
    last_recomputed_at TEXT,
    impacted_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Reasoning engine: node output cache, provenance ledger, knowledge projection.
CREATE TABLE IF NOT EXISTS node_cache (
    node_id     TEXT NOT NULL,
    input_hash  TEXT NOT NULL,
    output_json TEXT NOT NULL DEFAULT '{}',
    computed_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (node_id, input_hash)
);

CREATE TABLE IF NOT EXISTS node_provenance (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id     TEXT,
    produced_by TEXT NOT NULL,
    input_hash  TEXT NOT NULL,
    lane        TEXT NOT NULL,
    model_id    TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_node_provenance_fact ON node_provenance(fact_id);

CREATE TABLE IF NOT EXISTS graph_node (
    id         TEXT PRIMARY KEY,
    type       TEXT NOT NULL,
    props_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS graph_edge (
    src        TEXT NOT NULL,
    rel        TEXT NOT NULL,
    dst        TEXT NOT NULL,
    props_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (src, rel, dst)
);
CREATE INDEX IF NOT EXISTS idx_graph_edge_src ON graph_edge(src, rel);
CREATE INDEX IF NOT EXISTS idx_graph_edge_dst ON graph_edge(dst, rel);
"""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class HeadwaterStore:
    """SQLite-backed project store for Headwater 2."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._db_path = str(db_path)
        self._con: sqlite3.Connection | None = None

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("PRAGMA journal_mode=WAL")
        return con

    @property
    def con(self) -> sqlite3.Connection:
        if self._con is None:
            self._con = self._connect()
        return self._con

    def init(self) -> None:
        self.con.executescript(_SCHEMA_SQL)
        self.con.commit()

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def upsert_source(
        self,
        name: str,
        type_: str,
        path: str | None = None,
        uri: str | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO sources (name, type, path, uri)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                type = excluded.type,
                path = excluded.path,
                uri = excluded.uri,
                updated_at = datetime('now')
            """,
            (name, type_, path, uri),
        )
        self.con.commit()

    def get_source(self, name: str) -> dict[str, Any] | None:
        row = self.con.execute("SELECT * FROM sources WHERE name = ?", (name,)).fetchone()
        return dict(row) if row else None

    def delete_project(self, project_id: str) -> None:
        """Delete a project and everything derived from it."""
        qids = [
            r[0]
            for r in self.con.execute(
                "SELECT id FROM questions WHERE project_id = ?", (project_id,)
            ).fetchall()
        ]
        if qids:
            marks = ",".join("?" * len(qids))
            for tbl in ("readiness_contracts", "readiness_verdicts", "answer_artifacts"):
                self.con.execute(
                    f"DELETE FROM {tbl} WHERE question_id IN ({marks})", qids
                )
        # Order matters with foreign_keys=ON: resolve_items.question_id references
        # questions(id), so resolve_items must go before questions.
        for tbl in (
            "resolve_items",
            "questions",
            "semantic_claims",
            "pipeline_state",
            "project_sources",
        ):
            self.con.execute(f"DELETE FROM {tbl} WHERE project_id = ?", (project_id,))
        self.con.execute("DELETE FROM projects WHERE id = ?", (project_id,))
        self.con.commit()

    def delete_questions(self, question_ids: list[str]) -> None:
        """Delete specific questions and their derived verdicts/answers/resolve items.

        Used by the reasoning engine to replace a project's prior goal-aware
        question set (ids ``<project>:rq*``) so the questions stay current with the
        goal. Cascades the same dependents as :meth:`delete_project`.
        """
        if not question_ids:
            return
        marks = ",".join("?" * len(question_ids))
        for tbl in (
            "readiness_contracts",
            "readiness_verdicts",
            "answer_artifacts",
            "resolve_items",
        ):
            self.con.execute(
                f"DELETE FROM {tbl} WHERE question_id IN ({marks})", question_ids
            )
        self.con.execute(f"DELETE FROM questions WHERE id IN ({marks})", question_ids)
        self.con.commit()

    def delete_source(self, name: str) -> dict[str, Any]:
        """Delete a source, its catalog, and any project left with no source.

        Cascades: the source's snapshots/tables/columns/profiles/relationships,
        the source row, and project links to it.  A project that has no remaining
        source afterward is itself deleted (it can no longer function).
        """
        linked = [
            r[0]
            for r in self.con.execute(
                "SELECT project_id FROM project_sources WHERE source_name = ?", (name,)
            ).fetchall()
        ]
        # Resolve dependent projects first (so their rows that reference the
        # source are gone before we drop it): delete a project left with no other
        # source; just unlink one that keeps another.
        orphaned: list[str] = []
        for pid in linked:
            other = self.con.execute(
                "SELECT COUNT(*) FROM project_sources WHERE project_id = ? AND source_name != ?",
                (pid, name),
            ).fetchone()[0]
            if other == 0:
                self.delete_project(pid)
                orphaned.append(pid)
            else:
                self.con.execute(
                    "DELETE FROM project_sources WHERE project_id = ? AND source_name = ?",
                    (pid, name),
                )
        # Clear residual references to the source on any surviving project rows.
        for tbl in ("questions", "semantic_claims"):
            self.con.execute(
                f"UPDATE {tbl} SET source_name = NULL WHERE source_name = ?", (name,)
            )
        # Drop the catalog (children before parents), then the source row.
        for tbl in ("profiles", "columns", "tables", "relationships", "source_snapshots"):
            self.con.execute(f"DELETE FROM {tbl} WHERE source_name = ?", (name,))
        self.con.execute("DELETE FROM sources WHERE name = ?", (name,))
        self.con.commit()
        return {"source": name, "deleted_projects": orphaned}

    def record_source_snapshot(
        self,
        source_name: str,
        snapshot_id: str,
        *,
        fingerprint: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO source_snapshots (id, source_name, fingerprint, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (snapshot_id, source_name, fingerprint, _json(payload or {})),
        )
        self.con.execute(
            """
            UPDATE sources
               SET latest_snapshot_id = ?,
                   updated_at = datetime('now')
             WHERE name = ?
            """,
            (snapshot_id, source_name),
        )
        self.con.commit()

    def get_latest_source_snapshot(self, source_name: str) -> dict[str, Any] | None:
        row = self.con.execute(
            """
            SELECT *
              FROM source_snapshots
             WHERE source_name = ?
             ORDER BY captured_at DESC, id DESC
             LIMIT 1
            """,
            (source_name,),
        ).fetchone()
        if row is None:
            return None
        snapshot = dict(row)
        snapshot["payload"] = json.loads(snapshot.pop("payload_json") or "{}")
        return snapshot

    def upsert_table(
        self,
        source_name: str,
        table_name: str,
        *,
        schema_name: str | None = None,
        row_count: int = 0,
        description: str | None = None,
        domain: str | None = None,
        selected: bool = False,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO tables (
                name, source_name, schema_name, row_count, description, domain, selected
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name, source_name) DO UPDATE SET
                schema_name = excluded.schema_name,
                row_count = excluded.row_count,
                description = excluded.description,
                domain = excluded.domain,
                selected = excluded.selected,
                updated_at = datetime('now')
            """,
            (table_name, source_name, schema_name, row_count, description, domain, int(selected)),
        )
        self.con.commit()

    def delete_table(self, source_name: str, table_name: str) -> None:
        """Remove one table from a source's catalog.

        Cascades the table's columns, profiles, and any relationship that
        touches it, and prunes the table from every project's selected set so
        project scope stays consistent with the catalog.  Staleness detection
        picks up the change on the next recompute.
        """
        rows = self.con.execute(
            """
            SELECT project_id, selected_tables_json
              FROM project_sources
             WHERE source_name = ?
            """,
            (source_name,),
        ).fetchall()
        for project_id, selected_json in rows:
            selected = json.loads(selected_json or "[]")
            if table_name not in selected:
                continue
            self.con.execute(
                """
                UPDATE project_sources
                   SET selected_tables_json = ?, updated_at = datetime('now')
                 WHERE project_id = ? AND source_name = ?
                """,
                (_json([t for t in selected if t != table_name]), project_id, source_name),
            )
        self.con.execute(
            "DELETE FROM profiles WHERE source_name = ? AND table_name = ?",
            (source_name, table_name),
        )
        self.con.execute(
            "DELETE FROM columns WHERE source_name = ? AND table_name = ?",
            (source_name, table_name),
        )
        self.con.execute(
            """
            DELETE FROM relationships
             WHERE source_name = ? AND (from_table = ? OR to_table = ?)
            """,
            (source_name, table_name, table_name),
        )
        self.con.execute(
            "DELETE FROM tables WHERE source_name = ? AND name = ?",
            (source_name, table_name),
        )
        self.con.commit()

    def get_tables(self, source_name: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT * FROM tables WHERE source_name = ? ORDER BY name",
            (source_name,),
        ).fetchall()
        return [dict(row) for row in rows]

    def upsert_column(
        self,
        source_name: str,
        table_name: str,
        column_name: str,
        dtype: str,
        *,
        nullable: bool = True,
        is_primary_key: bool = False,
        description: str | None = None,
        semantic_type: str | None = None,
        ordinal: int = 0,
        locked: bool = False,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO columns (
                table_name, source_name, name, dtype, nullable, is_primary_key,
                description, semantic_type, ordinal, locked
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(table_name, source_name, name) DO UPDATE SET
                dtype = excluded.dtype,
                nullable = excluded.nullable,
                is_primary_key = excluded.is_primary_key,
                description = excluded.description,
                semantic_type = excluded.semantic_type,
                ordinal = excluded.ordinal,
                locked = excluded.locked,
                updated_at = datetime('now')
            """,
            (
                table_name,
                source_name,
                column_name,
                dtype,
                int(nullable),
                int(is_primary_key),
                description,
                semantic_type,
                ordinal,
                int(locked),
            ),
        )
        self.con.commit()

    def get_columns(self, source_name: str, table_name: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT *
              FROM columns
             WHERE source_name = ? AND table_name = ?
             ORDER BY ordinal, name
            """,
            (source_name, table_name),
        ).fetchall()
        return [dict(row) for row in rows]

    def upsert_profile(
        self,
        source_name: str,
        table_name: str,
        column_name: str,
        dtype: str,
        profile: dict[str, Any],
        *,
        snapshot_id: str | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO profiles (
                table_name, source_name, column_name, snapshot_id, dtype, profile_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(table_name, source_name, column_name, snapshot_id) DO UPDATE SET
                dtype = excluded.dtype,
                profile_json = excluded.profile_json
            """,
            (table_name, source_name, column_name, snapshot_id, dtype, _json(profile)),
        )
        self.con.commit()

    def get_profiles(self, source_name: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT * FROM profiles WHERE source_name = ? ORDER BY table_name, column_name",
            (source_name,),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["profile"] = json.loads(item.pop("profile_json") or "{}")
        return items

    def insert_relationship(
        self,
        source_name: str,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: str,
        rel_type: str,
        confidence: float,
        referential_integrity: float,
        *,
        snapshot_id: str | None = None,
    ) -> int:
        # Upsert semantics: one row per column pair. A re-discovery or a user
        # confirmation REPLACES the prior row (confirm carries confidence 1.0)
        # instead of stacking duplicates the UI and EDA would then repeat.
        self.con.execute(
            """
            DELETE FROM relationships
             WHERE source_name = ? AND from_table = ? AND from_column = ?
               AND to_table = ? AND to_column = ?
            """,
            (source_name, from_table, from_column, to_table, to_column),
        )
        cur = self.con.execute(
            """
            INSERT INTO relationships (
                source_name, from_table, from_column, to_table, to_column,
                rel_type, confidence, referential_integrity, snapshot_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_name,
                from_table,
                from_column,
                to_table,
                to_column,
                rel_type,
                confidence,
                referential_integrity,
                snapshot_id,
            ),
        )
        self.con.commit()
        assert cur.lastrowid is not None
        return int(cur.lastrowid)

    def get_relationships(self, source_name: str) -> list[dict[str, Any]]:
        # Defensive dedupe for rows written before insert_relationship gained
        # upsert semantics: keep the strongest row per column pair.
        rows = self.con.execute(
            """
            SELECT * FROM relationships
             WHERE source_name = ?
             ORDER BY confidence DESC, referential_integrity DESC, id DESC
            """,
            (source_name,),
        ).fetchall()
        seen: set[tuple[str, str, str, str]] = set()
        out: list[dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            key = (d["from_table"], d["from_column"], d["to_table"], d["to_column"])
            if key in seen:
                continue
            seen.add(key)
            out.append(d)
        return out

    def upsert_project(
        self,
        project_id: str,
        *,
        slug: str,
        display_name: str,
        description: str = "",
        goal: dict[str, Any] | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO projects (id, slug, display_name, description, goal_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                slug = excluded.slug,
                display_name = excluded.display_name,
                description = excluded.description,
                goal_json = excluded.goal_json,
                updated_at = datetime('now')
            """,
            (project_id, slug, display_name, description, _json(goal or {})),
        )
        self.con.commit()

    def get_project(self, project_id: str) -> dict[str, Any] | None:
        row = self.con.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
        if row is None:
            return None
        project = dict(row)
        project["goal"] = json.loads(project.pop("goal_json") or "{}")
        return project

    def list_projects(self) -> list[dict[str, Any]]:
        rows = self.con.execute("SELECT * FROM projects ORDER BY updated_at DESC, id").fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["goal"] = json.loads(item.pop("goal_json") or "{}")
        return items

    def project_verdict_summary(
        self, project_id: str, current_fingerprint: str | None = None
    ) -> dict[str, int]:
        """Certified / total counts from persisted verdicts (the one true source).

        Counts answerable, non-dropped questions as the total and those whose
        latest readiness verdict is ``certified`` — NOT ``question.status`` (which
        finalize never flips), so the rail/banner/home all read the real verdict.

        When ``current_fingerprint`` is given, a verdict only counts as certified
        if the judge produced it against the *current* inputs.  A verdict judged
        against older inputs is stale — not certified anymore — so this readout
        agrees with the Answer page instead of showing a misleading count.
        """
        total = 0
        certified = 0
        for q in self.list_questions(project_id):
            if q.get("answerability") == "cannot_answer" or q.get("status") == "dropped":
                continue
            total += 1
            verdict = self.get_readiness_verdict(f"{q['id']}:verdict:latest")
            if not (verdict and verdict.get("state") == "certified"):
                continue
            if current_fingerprint is not None:
                judged_fp = next(
                    (
                        (c.get("evidence") or {}).get("judged_fingerprint")
                        for c in self.list_readiness_contracts(q["id"])
                        if c.get("contract_type") == "judge_verdict"
                    ),
                    None,
                )
                if judged_fp != current_fingerprint:
                    continue  # stale — re-certification needed
            certified += 1
        return {"certified": certified, "total": total}

    def upsert_project_source(
        self,
        project_id: str,
        source_name: str,
        *,
        selected_tables: list[str] | None = None,
        scope: dict[str, Any] | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO project_sources (
                project_id, source_name, selected_tables_json, scope_json
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(project_id, source_name) DO UPDATE SET
                selected_tables_json = excluded.selected_tables_json,
                scope_json = excluded.scope_json,
                updated_at = datetime('now')
            """,
            (project_id, source_name, _json(selected_tables or []), _json(scope or {})),
        )
        self.con.commit()

    def get_project_sources(self, project_id: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT * FROM project_sources WHERE project_id = ? ORDER BY source_name",
            (project_id,),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["selected_tables"] = json.loads(item.pop("selected_tables_json") or "[]")
            item["scope"] = json.loads(item.pop("scope_json") or "{}")
        return items

    def set_question_status(self, question_id: str, status: str) -> None:
        """Set a question's status (e.g. user curation: 'dropped' or restored)."""
        self.con.execute(
            "UPDATE questions SET status=?, updated_at=datetime('now') WHERE id=?",
            (status, question_id),
        )
        self.con.commit()

    def list_questions(self, project_id: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT * FROM questions WHERE project_id = ? ORDER BY updated_at DESC, id",
            (project_id,),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["question"] = json.loads(item.pop("question_json") or "{}")
        return items

    def upsert_semantic_claim(
        self,
        claim_id: str,
        *,
        project_id: str,
        source_name: str | None = None,
        scope_type: str,
        claim_type: str,
        claim: dict[str, Any],
        table_name: str | None = None,
        column_name: str | None = None,
        status: str = "proposed",
        confidence: float = 0.0,
        source: str = "bootstrap",
        locked: bool = False,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO semantic_claims (
                id, project_id, source_name, scope_type, table_name, column_name,
                claim_type, claim_json, status, confidence, source, locked
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                project_id = excluded.project_id,
                source_name = excluded.source_name,
                scope_type = excluded.scope_type,
                table_name = excluded.table_name,
                column_name = excluded.column_name,
                claim_type = excluded.claim_type,
                claim_json = excluded.claim_json,
                status = excluded.status,
                confidence = excluded.confidence,
                source = excluded.source,
                locked = excluded.locked,
                updated_at = datetime('now')
            """,
            (
                claim_id,
                project_id,
                source_name,
                scope_type,
                table_name,
                column_name,
                claim_type,
                _json(claim),
                status,
                confidence,
                source,
                int(locked),
            ),
        )
        self.con.commit()

    def get_semantic_claim(self, claim_id: str) -> dict[str, Any] | None:
        row = self.con.execute(
            "SELECT * FROM semantic_claims WHERE id = ?",
            (claim_id,),
        ).fetchone()
        if row is None:
            return None
        claim = dict(row)
        claim["claim"] = json.loads(claim.pop("claim_json") or "{}")
        return claim

    def list_semantic_claims(self, project_id: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT * FROM semantic_claims WHERE project_id = ? ORDER BY updated_at DESC, id",
            (project_id,),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["claim"] = json.loads(item.pop("claim_json") or "{}")
        return items

    def upsert_question(
        self,
        question_id: str,
        *,
        project_id: str,
        title: str,
        question: dict[str, Any],
        source_name: str | None = None,
        status: str = "draft",
        answerability: str = "answerable",
        confidence: float = 0.0,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO questions (
                id, project_id, source_name, title, question_json, status,
                answerability, confidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                project_id = excluded.project_id,
                source_name = excluded.source_name,
                title = excluded.title,
                question_json = excluded.question_json,
                status = CASE WHEN questions.status='dropped'
                              THEN 'dropped' ELSE excluded.status END,
                answerability = excluded.answerability,
                confidence = excluded.confidence,
                updated_at = datetime('now')
            """,
            (
                question_id,
                project_id,
                source_name,
                title,
                _json(question),
                status,
                answerability,
                confidence,
            ),
        )
        self.con.commit()

    def get_question(self, question_id: str) -> dict[str, Any] | None:
        row = self.con.execute("SELECT * FROM questions WHERE id = ?", (question_id,)).fetchone()
        if row is None:
            return None
        question = dict(row)
        question["question"] = json.loads(question.pop("question_json") or "{}")
        return question

    def upsert_resolve_item(
        self,
        item_id: str,
        *,
        project_id: str,
        issue_kind: str,
        title: str,
        body: str = "",
        question_id: str | None = None,
        priority: str = "medium",
        status: str = "open",
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO resolve_items (
                id, project_id, question_id, priority, issue_kind, title,
                body, status, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                project_id = excluded.project_id,
                question_id = excluded.question_id,
                priority = excluded.priority,
                issue_kind = excluded.issue_kind,
                title = excluded.title,
                body = excluded.body,
                status = excluded.status,
                payload_json = excluded.payload_json,
                updated_at = datetime('now')
            """,
            (
                item_id,
                project_id,
                question_id,
                priority,
                issue_kind,
                title,
                body,
                status,
                _json(payload or {}),
            ),
        )
        self.con.commit()

    def set_resolve_item_status(self, item_id: str, status: str) -> None:
        self.con.execute(
            "UPDATE resolve_items SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, item_id),
        )
        self.con.commit()

    def delete_resolve_item(self, item_id: str) -> None:
        self.con.execute("DELETE FROM resolve_items WHERE id = ?", (item_id,))
        self.con.commit()

    def list_resolve_items(self, project_id: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT * FROM resolve_items WHERE project_id = ? ORDER BY priority, updated_at DESC",
            (project_id,),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
        return items

    def upsert_readiness_contract(
        self,
        contract_id: str,
        *,
        question_id: str,
        contract_type: str,
        passed: bool,
        note: str = "",
        evidence: dict[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO readiness_contracts (
                id, question_id, contract_type, passed, note, evidence_json, snapshot_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                question_id = excluded.question_id,
                contract_type = excluded.contract_type,
                passed = excluded.passed,
                note = excluded.note,
                evidence_json = excluded.evidence_json,
                snapshot_id = excluded.snapshot_id,
                updated_at = datetime('now')
            """,
            (
                contract_id,
                question_id,
                contract_type,
                int(passed),
                note,
                _json(evidence or {}),
                snapshot_id,
            ),
        )
        self.con.commit()

    def upsert_readiness_verdict(
        self,
        verdict_id: str,
        *,
        question_id: str,
        state: str,
        readiness_pct: int,
        trust_bucket: str,
        summary: str = "",
        source_snapshot_id: str | None = None,
        freshness: str | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO readiness_verdicts (
                id, question_id, source_snapshot_id, state, readiness_pct,
                trust_bucket, summary, freshness
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                question_id = excluded.question_id,
                source_snapshot_id = excluded.source_snapshot_id,
                state = excluded.state,
                readiness_pct = excluded.readiness_pct,
                trust_bucket = excluded.trust_bucket,
                summary = excluded.summary,
                freshness = excluded.freshness,
                updated_at = datetime('now')
            """,
            (
                verdict_id,
                question_id,
                source_snapshot_id,
                state,
                readiness_pct,
                trust_bucket,
                summary,
                freshness,
            ),
        )
        self.con.commit()

    def get_readiness_verdict(self, verdict_id: str) -> dict[str, Any] | None:
        row = self.con.execute(
            "SELECT * FROM readiness_verdicts WHERE id = ?",
            (verdict_id,),
        ).fetchone()
        return dict(row) if row else None

    def list_readiness_verdicts(self, question_id: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT * FROM readiness_verdicts WHERE question_id = ? ORDER BY updated_at DESC, id",
            (question_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def upsert_answer_artifact(
        self,
        artifact_id: str,
        *,
        question_id: str,
        sql_text: str | None = None,
        chart_spec: dict[str, Any] | None = None,
        state: str = "draft",
        certified_at: str | None = None,
        source_snapshot_id: str | None = None,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO answer_artifacts (
                id, question_id, sql_text, chart_spec_json, state,
                certified_at, source_snapshot_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                question_id = excluded.question_id,
                sql_text = excluded.sql_text,
                chart_spec_json = excluded.chart_spec_json,
                state = excluded.state,
                certified_at = excluded.certified_at,
                source_snapshot_id = excluded.source_snapshot_id,
                updated_at = datetime('now')
            """,
            (
                artifact_id,
                question_id,
                sql_text,
                _json(chart_spec or {}),
                state,
                certified_at,
                source_snapshot_id,
            ),
        )
        self.con.commit()

    def get_answer_artifact(self, artifact_id: str) -> dict[str, Any] | None:
        row = self.con.execute(
            "SELECT * FROM answer_artifacts WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        if row is None:
            return None
        artifact = dict(row)
        artifact["chart_spec"] = json.loads(artifact.pop("chart_spec_json") or "{}")
        return artifact

    def list_answer_artifacts(self, question_id: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            "SELECT * FROM answer_artifacts WHERE question_id = ? ORDER BY updated_at DESC, id",
            (question_id,),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["chart_spec"] = json.loads(item.pop("chart_spec_json") or "{}")
        return items

    def record_decision(
        self,
        artifact_type: str,
        artifact_id: str,
        action: str,
        *,
        reason: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        cur = self.con.execute(
            """
            INSERT INTO decisions (artifact_type, artifact_id, action, reason, payload_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (artifact_type, artifact_id, action, reason, _json(payload or {})),
        )
        self.con.commit()
        assert cur.lastrowid is not None
        return int(cur.lastrowid)

    def list_readiness_contracts(self, question_id: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT *
              FROM readiness_contracts
             WHERE question_id = ?
             ORDER BY contract_type
            """,
            (question_id,),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["evidence"] = json.loads(item.pop("evidence_json") or "{}")
        return items

    def get_pipeline_state(self, project_id: str) -> dict[str, Any] | None:
        row = self.con.execute(
            "SELECT * FROM pipeline_state WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        return dict(row) if row else None

    def set_pipeline_state(
        self,
        project_id: str,
        *,
        last_input_hash: str,
        impacted_count: int,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO pipeline_state (
                project_id, last_input_hash, last_recomputed_at, impacted_count
            )
            VALUES (?, ?, datetime('now'), ?)
            ON CONFLICT(project_id) DO UPDATE SET
                last_input_hash = excluded.last_input_hash,
                last_recomputed_at = excluded.last_recomputed_at,
                impacted_count = excluded.impacted_count,
                updated_at = datetime('now')
            """,
            (project_id, last_input_hash, impacted_count),
        )
        self.con.commit()

    def list_decisions(self, artifact_type: str, artifact_id: str) -> list[dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT *
              FROM decisions
             WHERE artifact_type = ? AND artifact_id = ?
             ORDER BY id
            """,
            (artifact_type, artifact_id),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
        return items
