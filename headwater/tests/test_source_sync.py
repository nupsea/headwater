"""Tests for the source sync service boundary."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from headwater.core.events import EventType
from headwater.core.metadata import MetadataStore
from headwater.services.source_sync import (
    SourceNotFoundError,
    SourceSyncService,
    _default_source_schema,
)


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
        EventType.SYNC_STARTED,
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


def test_record_event_redacts_secret_payloads(meta: MetadataStore):
    meta.upsert_source("src", "postgres", None, "postgresql://user:secret@localhost/db")
    service = SourceSyncService(_request(meta))

    service.record_event(
        EventType.CONNECTION_TEST_FAILED,
        "Failed postgresql://user:secret@localhost/db",
        source_name="src",
        severity="error",
        payload={"uri": "postgresql://user:secret@localhost/db", "password": "secret"},
    )

    event = meta.list_events("src")[0]
    assert "secret" not in event["summary"]
    assert event["payload"]["uri"] == "postgresql://user:***@localhost/db"
    assert event["payload"]["password"] == "***"


def test_default_source_schema_is_stable_and_source_specific():
    assert _default_source_schema({"name": "Lifestyle"}) == "src_lifestyle"
    assert _default_source_schema({"name": "Lifestyle Redshift"}) == "src_lifestyle_redshift"
    assert _default_source_schema({"name": "123 source"}) == "src_s_123_source"
