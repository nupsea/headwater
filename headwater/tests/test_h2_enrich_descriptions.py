"""Chunked, data-aware description generation (column + table level)."""

from __future__ import annotations

import duckdb
import pytest

from headwater.core.store import HeadwaterStore
from headwater.services.h2_enrich import _DESC_CHUNK_SIZE, generate_descriptions
from headwater.services.h2_source import ingest_tables


class FakeProvider:
    """Records every call; answers each column chunk and the table prompt."""

    def __init__(self, fail_chunks: int = 0) -> None:
        self.calls: list[str] = []
        self._fail_remaining = fail_chunks

    async def analyze(self, prompt: str, system: str) -> dict:
        self.calls.append(prompt)
        if prompt.startswith("TABLE:") and "Describe in ONE sentence" in prompt:
            return {"description": "One row per business record."}
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            return {}  # simulates a timeout/empty response
        out: dict[str, str] = {}
        for line in prompt.splitlines():
            if line.startswith("- "):
                name = line[2:].split(":", 1)[0]
                out[name] = f"Description of {name}."
        return out


@pytest.fixture()
def wide_source(tmp_path):
    """A source with one wide table (2.5 chunks worth of columns)."""
    n_cols = _DESC_CHUNK_SIZE * 2 + 8
    db = tmp_path / "wide.duckdb"
    con = duckdb.connect(str(db))
    cols = ", ".join(f"1 AS col_{i:02d}" for i in range(n_cols))
    con.execute(f"CREATE TABLE wide AS SELECT {cols}")
    con.close()

    store = HeadwaterStore(tmp_path / "h2_metadata.db")
    store.init()
    store.upsert_source("w", "duckdb", str(db), None)
    ingest_tables(store, "w", ["wide"])
    try:
        yield store, n_cols
    finally:
        store.close()


def test_descriptions_are_chunked_and_complete(wide_source):
    store, n_cols = wide_source
    provider = FakeProvider()
    result = generate_descriptions(store, "w", provider=provider)

    assert result["available"] is True
    assert result["updated"] == n_cols
    assert result["tables_updated"] == 1
    # ceil(n/_DESC_CHUNK_SIZE) column calls + 1 table-description call.
    expected_chunks = -(-n_cols // _DESC_CHUNK_SIZE)
    assert len(provider.calls) == expected_chunks + 1
    # No single call carries more than one chunk of columns.
    for call in provider.calls:
        col_lines = [ln for ln in call.splitlines() if ln.startswith("- ")]
        assert len(col_lines) <= _DESC_CHUNK_SIZE

    described = [
        c for c in store.get_columns("w", "wide") if (c.get("description") or "").strip()
    ]
    assert len(described) == n_cols
    table = store.get_tables("w")[0]
    assert table["description"] == "One row per business record."


def test_failed_chunk_is_reported_not_hidden(wide_source):
    store, n_cols = wide_source
    provider = FakeProvider(fail_chunks=1)
    result = generate_descriptions(store, "w", provider=provider)

    assert result["available"] is True
    assert result["updated"] == n_cols - _DESC_CHUNK_SIZE  # one chunk lost
    assert "failed" in result["note"]


def test_prompt_includes_profile_stats(wide_source):
    store, _ = wide_source
    provider = FakeProvider()
    generate_descriptions(store, "w", provider=provider)
    # Locally profiled source: at least one chunk line carries a stats hint.
    assert any("distinct" in call for call in provider.calls)
