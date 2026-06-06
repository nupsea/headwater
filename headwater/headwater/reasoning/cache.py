"""Node output cache, keyed by (node_id, input_hash).

A cache hit means the node's declared inputs are byte-identical to a previous run,
so its output can be reused — the memoization that makes expensive nodes
affordable. Backed by the SQLite ``node_cache`` table (system of record, I-1).
"""

from __future__ import annotations

import json
from typing import Any

from headwater.core.store import HeadwaterStore


class NodeCache:
    def __init__(self, store: HeadwaterStore) -> None:
        self._store = store

    def get(self, node_id: str, input_hash: str) -> Any | None:
        row = self._store.con.execute(
            "SELECT output_json FROM node_cache WHERE node_id = ? AND input_hash = ?",
            (node_id, input_hash),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["output_json"])

    def put(self, node_id: str, input_hash: str, output: Any) -> None:
        self._store.con.execute(
            """
            INSERT INTO node_cache (node_id, input_hash, output_json, computed_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(node_id, input_hash) DO UPDATE SET
                output_json = excluded.output_json,
                computed_at = datetime('now')
            """,
            (node_id, input_hash, json.dumps(output, default=str, sort_keys=True)),
        )
        self._store.con.commit()

    def invalidate(self, node_id: str, input_hash: str | None = None) -> None:
        """Drop a node's cached output (one entry, or all entries for the node)."""
        if input_hash is None:
            self._store.con.execute(
                "DELETE FROM node_cache WHERE node_id = ?", (node_id,)
            )
        else:
            self._store.con.execute(
                "DELETE FROM node_cache WHERE node_id = ? AND input_hash = ?",
                (node_id, input_hash),
            )
        self._store.con.commit()
