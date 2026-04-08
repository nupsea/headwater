"""Shared test fixtures."""

from __future__ import annotations

import pytest

from headwater.core.metadata import MetadataStore


@pytest.fixture()
def meta() -> MetadataStore:
    """In-memory SQLite metadata store, schema initialized."""
    store = MetadataStore(":memory:")
    store.init()
    return store
