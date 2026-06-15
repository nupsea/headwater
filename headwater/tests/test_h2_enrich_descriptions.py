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
    assert "timed out" in result["note"]


def test_prompt_includes_profile_stats(wide_source):
    store, _ = wide_source
    provider = FakeProvider()
    generate_descriptions(store, "w", provider=provider)
    # Locally profiled source: at least one chunk line carries a stats hint.
    assert any("distinct" in call for call in provider.calls)


# ── Robust response matching (silent-rejection bug: model replies, none match) ──

from headwater.services.h2_enrich import _match_descriptions  # noqa: E402


def test_match_exact_and_caseless_and_qualified():
    valid = {"martian_id", "account_name", "brand"}
    # Exact
    assert _match_descriptions({"martian_id": "an id"}, valid) == {"martian_id": "an id"}
    # Case difference
    assert _match_descriptions({"Account_Name": "a name"}, valid) == {
        "account_name": "a name"
    }
    # Qualified key -> matched by last component
    assert _match_descriptions(
        {"data.fct_subscription.brand": "the brand"}, valid
    ) == {"brand": "the brand"}


def test_match_unwraps_wrapper_key():
    valid = {"col_a", "col_b"}
    raw = {"descriptions": {"col_a": "desc a", "col_b": "desc b"}}
    assert _match_descriptions(raw, valid) == {"col_a": "desc a", "col_b": "desc b"}


def test_match_handles_list_of_objects():
    valid = {"col_a", "col_b"}
    raw = {"columns": [
        {"name": "col_a", "description": "desc a"},
        {"column": "col_b", "desc": "desc b"},
    ]}
    assert _match_descriptions(raw, valid) == {"col_a": "desc a", "col_b": "desc b"}


def test_match_ignores_unknown_and_blank():
    valid = {"col_a"}
    raw = {"col_a": "  ", "nope": "stray"}
    assert _match_descriptions(raw, valid) == {}


class QualifiedKeyProvider:
    """Returns descriptions keyed by FULLY-QUALIFIED column name (the real bug)."""

    def __init__(self, table: str) -> None:
        self.table = table

    async def analyze(self, prompt: str, system: str) -> dict:
        if "Describe in ONE sentence" in prompt:
            return {"description": "One row per record."}
        out: dict[str, str] = {}
        for line in prompt.splitlines():
            if line.startswith("- "):
                name = line[2:].split(":", 1)[0]
                out[f"{self.table}.{name}"] = f"Description of {name}."
        return out


def test_qualified_key_response_is_not_silently_dropped(wide_source):
    store, n_cols = wide_source
    result = generate_descriptions(store, "w", provider=QualifiedKeyProvider("w"))
    assert result["available"] is True
    assert result["updated"] == n_cols  # all matched despite qualified keys
    described = [
        c for c in store.get_columns("w", "wide") if (c.get("description") or "").strip()
    ]
    assert len(described) == n_cols


class UnparseableProvider:
    """Replies with a non-empty but unusable shape (no column names match)."""

    async def analyze(self, prompt: str, system: str) -> dict:
        if "Describe in ONE sentence" in prompt:
            return {}  # table desc also fails
        return {"unexpected": "totally wrong shape"}


def test_unparseable_response_reports_failure_not_success(wide_source):
    store, _ = wide_source
    result = generate_descriptions(store, "w", provider=UnparseableProvider())
    # Must NOT claim "no new descriptions to add" — it must say it couldn't parse.
    assert result["available"] is False
    assert result["updated"] == 0
    assert "format" in result["note"].lower() or "parse" in result["note"].lower()
