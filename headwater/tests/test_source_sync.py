"""Tests for the source sync service boundary."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from headwater.core.metadata import MetadataStore
from headwater.services.source_sync import SourceNotFoundError, SourceSyncService


@pytest.fixture()
def meta() -> MetadataStore:
    store = MetadataStore(":memory:")
    store.init()
    return store


def _request(store: MetadataStore) -> SimpleNamespace:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(metadata_store=store)))


def test_missing_source_raises_not_found(meta: MetadataStore):
    service = SourceSyncService(_request(meta))

    with pytest.raises(SourceNotFoundError):
        service.test("missing")


def test_record_event_writes_normalized_and_legacy_events(meta: MetadataStore):
    meta.upsert_source("src", "json", "/data", None)
    service = SourceSyncService(_request(meta))

    service.record_event(
        "sync_started",
        "Source sync started",
        source_name="src",
        payload={"run_id": 1},
        invalidates=["sources"],
    )

    normalized = meta.list_events("src")
    legacy = meta.list_sync_events("src")
    assert normalized[0]["event_type"] == "sync_started"
    assert normalized[0]["invalidates"] == ["sources"]
    assert legacy[0]["event_type"] == "sync_started"
    assert legacy[0]["payload"] == {"run_id": 1}
