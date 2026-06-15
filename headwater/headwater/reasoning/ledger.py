"""Provenance ledger — the certification ledger substrate.

Every fact a node emits is recorded with what produced it, from which input hash,
on which lane, and (for L nodes) which model. This is the audit trail the vision
requires: "recomputable from facts, re-checked on every trigger."
"""

from __future__ import annotations

from headwater.core.store import HeadwaterStore
from headwater.reasoning.types import ProvenanceRef


class ProvenanceLedger:
    def __init__(self, store: HeadwaterStore) -> None:
        self._store = store

    def record(self, ref: ProvenanceRef) -> None:
        fact_ids = ref.fact_ids or (None,)
        self._store.con.executemany(
            """
            INSERT INTO node_provenance
                (fact_id, produced_by, input_hash, lane, model_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(fid, ref.produced_by, ref.input_hash, ref.lane, ref.model_id) for fid in fact_ids],
        )
        self._store.con.commit()

    def for_fact(self, fact_id: str) -> list[dict]:
        rows = self._store.con.execute(
            "SELECT * FROM node_provenance WHERE fact_id = ? ORDER BY id",
            (fact_id,),
        ).fetchall()
        return [dict(r) for r in rows]
