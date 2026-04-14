"""Tests for deep semantic inference."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from headwater.analyzer.heuristics import (
    enrich_tables,
    generate_deep_table_description,
)
from headwater.analyzer.semantic import analyze
from headwater.connectors.json_loader import JsonLoader
from headwater.core.metadata import MetadataStore
from headwater.core.models import (
    ColumnInfo,
    CompanionDoc,
    DiscoveryResult,
    SourceConfig,
    TableInfo,
    TableSemanticDetail,
)
from headwater.profiler.engine import discover

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sample"


@pytest.fixture()
def sample_discovery() -> DiscoveryResult:
    """Full discovery result from sample data."""
    con = duckdb.connect(":memory:")
    loader = JsonLoader()
    source = SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR))
    loader.connect(source)
    loader.load_to_duckdb(con, "env_health")
    return discover(con, "env_health", source)


def test_deep_heuristic_produces_row_semantics(sample_discovery: DiscoveryResult) -> None:
    """Row semantics should be populated for each table."""
    enrich_tables(
        sample_discovery.tables,
        sample_discovery.profiles,
        sample_discovery.relationships,
    )

    for table in sample_discovery.tables:
        detail = generate_deep_table_description(
            table,
            [p for p in sample_discovery.profiles if p.table_name == table.name],
            sample_discovery.relationships,
        )
        assert detail.row_semantics is not None
        assert len(detail.row_semantics) > 10
        assert "each row" in detail.row_semantics.lower()


def test_deep_heuristic_identifies_key_metrics(sample_discovery: DiscoveryResult) -> None:
    """Key metrics should be identified from column classification."""
    enrich_tables(
        sample_discovery.tables,
        sample_discovery.profiles,
        sample_discovery.relationships,
    )

    # readings table should have metric columns
    readings = next(t for t in sample_discovery.tables if t.name == "readings")
    detail = generate_deep_table_description(
        readings,
        [p for p in sample_discovery.profiles if p.table_name == "readings"],
        sample_discovery.relationships,
    )
    assert len(detail.key_metrics) > 0


def test_deep_heuristic_groups_columns(sample_discovery: DiscoveryResult) -> None:
    """Column groups should organize columns by semantic type."""
    enrich_tables(
        sample_discovery.tables,
        sample_discovery.profiles,
        sample_discovery.relationships,
    )

    readings = next(t for t in sample_discovery.tables if t.name == "readings")
    detail = generate_deep_table_description(
        readings,
        [p for p in sample_discovery.profiles if p.table_name == "readings"],
        sample_discovery.relationships,
    )
    assert len(detail.column_groups) > 0
    # Should have at least identifiers and some other group
    all_grouped = set()
    for cols in detail.column_groups.values():
        all_grouped.update(cols)
    assert len(all_grouped) > 0


def test_deep_heuristic_detects_temporal_grain(sample_discovery: DiscoveryResult) -> None:
    """Tables with temporal columns should get a temporal grain."""
    enrich_tables(
        sample_discovery.tables,
        sample_discovery.profiles,
        sample_discovery.relationships,
    )

    readings = next(t for t in sample_discovery.tables if t.name == "readings")
    detail = generate_deep_table_description(
        readings,
        [p for p in sample_discovery.profiles if p.table_name == "readings"],
        sample_discovery.relationships,
    )
    # readings should have some temporal grain
    assert detail.temporal_grain is not None


def test_deep_heuristic_builds_narrative(sample_discovery: DiscoveryResult) -> None:
    """Narrative should be a multi-sentence description."""
    enrich_tables(
        sample_discovery.tables,
        sample_discovery.profiles,
        sample_discovery.relationships,
    )

    for table in sample_discovery.tables[:3]:
        detail = generate_deep_table_description(
            table,
            [p for p in sample_discovery.profiles if p.table_name == table.name],
            sample_discovery.relationships,
        )
        assert detail.narrative is not None
        # Should have multiple sentences
        assert detail.narrative.count(".") >= 2


def test_deep_heuristic_with_companion_context() -> None:
    """Companion context should be attached to the semantic detail."""
    table = TableInfo(
        name="sensors",
        row_count=100,
        columns=[
            ColumnInfo(name="sensor_id", dtype="varchar", role="identifier", semantic_type="id"),
            ColumnInfo(name="status", dtype="varchar", role="dimension", semantic_type="dimension"),
        ],
    )
    companion_ctx = "EPA air quality monitoring sensors deployed across the city."

    detail = generate_deep_table_description(table, [], [], companion_ctx)
    assert detail.companion_context == companion_ctx


def test_analyze_produces_semantic_detail(sample_discovery: DiscoveryResult) -> None:
    """The full analyze() flow should produce semantic detail on all tables."""
    analyze(sample_discovery)

    for table in sample_discovery.tables:
        assert table.semantic_detail is not None
        assert isinstance(table.semantic_detail, TableSemanticDetail)
        assert table.semantic_detail.row_semantics is not None


def test_analyze_with_companion_docs(sample_discovery: DiscoveryResult) -> None:
    """Companion docs should be integrated into semantic analysis."""
    sample_discovery.companion_docs = [
        CompanionDoc(
            filename="sensors.md",
            content="EPA air quality monitoring sensors.",
            doc_type="markdown",
            matched_tables=["sensors"],
            confidence=0.9,
        ),
    ]

    analyze(sample_discovery)

    sensors = next(t for t in sample_discovery.tables if t.name == "sensors")
    assert sensors.semantic_detail is not None
    assert sensors.semantic_detail.companion_context is not None
    assert "EPA" in sensors.semantic_detail.companion_context


def test_locked_table_preserves_existing_detail() -> None:
    """Locked tables with existing detail should not be overwritten."""
    existing_detail = TableSemanticDetail(
        narrative="Human-approved narrative",
        row_semantics="Each row is a verified sensor",
        inference_confidence=1.0,
    )
    table = TableInfo(
        name="sensors",
        row_count=10,
        locked=True,
        semantic_detail=existing_detail,
        columns=[
            ColumnInfo(name="id", dtype="int64", locked=True),
        ],
    )
    source = SourceConfig(name="test", type="json", path="/tmp")
    discovery = DiscoveryResult(source=source, tables=[table])

    analyze(discovery)

    assert table.semantic_detail is existing_detail
    assert table.semantic_detail.narrative == "Human-approved narrative"


def test_semantic_detail_persisted_to_metadata() -> None:
    """Semantic details should persist to and load from metadata store."""
    store = MetadataStore(":memory:")
    store.init()

    detail = {
        "narrative": "Test narrative.",
        "row_semantics": "Each row is a test record.",
        "temporal_grain": "daily",
        "key_dimensions": ["dim1"],
        "key_metrics": ["metric1"],
        "column_groups": {"test": ["col1"]},
        "inference_confidence": 0.8,
    }

    store.upsert_semantic_detail("test_table", "test_source", detail)
    loaded = store.get_semantic_detail("test_table", "test_source")

    assert loaded is not None
    assert loaded["narrative"] == "Test narrative."
    assert loaded["row_semantics"] == "Each row is a test record."
    assert loaded["temporal_grain"] == "daily"

    store.close()


def test_semantic_detail_upsert_updates() -> None:
    """Upserting semantic detail should update existing records."""
    store = MetadataStore(":memory:")
    store.init()

    store.upsert_semantic_detail("t", "s", {"narrative": "v1"})
    store.upsert_semantic_detail("t", "s", {"narrative": "v2"})

    loaded = store.get_semantic_detail("t", "s")
    assert loaded is not None
    assert loaded["narrative"] == "v2"

    store.close()


def test_semantic_detail_not_found() -> None:
    """Querying nonexistent semantic detail returns None."""
    store = MetadataStore(":memory:")
    store.init()

    assert store.get_semantic_detail("nope", "nope") is None
    store.close()


def test_column_semantic_detail_populated(sample_discovery: DiscoveryResult) -> None:
    """Per-column semantic details should be populated."""
    analyze(sample_discovery)

    readings = next(t for t in sample_discovery.tables if t.name == "readings")
    assert readings.semantic_detail is not None
    assert len(readings.semantic_detail.semantic_columns) > 0

    # Check a column has business_description
    for _col_name, col_detail in readings.semantic_detail.semantic_columns.items():
        assert col_detail.business_description is not None or col_detail.semantic_group is not None


def test_inference_confidence_is_heuristic() -> None:
    """Heuristic-only analysis should have confidence 0.4."""
    table = TableInfo(
        name="test",
        row_count=10,
        columns=[ColumnInfo(name="id", dtype="int64")],
    )
    detail = generate_deep_table_description(table, [], [])
    assert detail.inference_confidence == 0.4
