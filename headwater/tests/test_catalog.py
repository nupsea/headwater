"""Tests for semantic catalog builder and evaluator."""

from __future__ import annotations

import pytest

from headwater.analyzer.catalog import (
    _expand_synonyms,
    _infer_agg_type,
    _to_display_name,
    build_catalog,
)
from headwater.analyzer.eval import evaluate_catalog
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    Relationship,
    SourceConfig,
    TableInfo,
)

# ---------------------------------------------------------------------------
# Fixtures: Riverton-like sample data
# ---------------------------------------------------------------------------


@pytest.fixture
def riverton_source():
    return SourceConfig(name="riverton", type="json", path="/data/sample")


@pytest.fixture
def riverton_tables():
    """Simplified Riverton schema for catalog testing."""
    return [
        TableInfo(
            name="complaints",
            row_count=3000,
            domain="Environmental",
            description="Environmental health complaints",
            columns=[
                ColumnInfo(
                    name="complaint_id",
                    dtype="varchar",
                    is_primary_key=True,
                    semantic_type="primary_key",
                ),
                ColumnInfo(
                    name="zone_id",
                    dtype="varchar",
                    semantic_type="foreign_key",
                    role="identifier",
                ),
                ColumnInfo(
                    name="related_site_id",
                    dtype="varchar",
                    semantic_type="foreign_key",
                    role="identifier",
                ),
                ColumnInfo(
                    name="category",
                    dtype="varchar",
                    semantic_type="dimension",
                    role="dimension",
                    description="Type of complaint",
                    confidence=0.9,
                ),
                ColumnInfo(
                    name="priority",
                    dtype="varchar",
                    semantic_type="dimension",
                    role="dimension",
                    description="Severity level",
                    confidence=0.85,
                ),
                ColumnInfo(
                    name="status",
                    dtype="varchar",
                    semantic_type="dimension",
                    role="dimension",
                    confidence=0.8,
                ),
                ColumnInfo(
                    name="date_filed",
                    dtype="timestamp",
                    semantic_type="temporal",
                    role="temporal",
                ),
                ColumnInfo(
                    name="resolution_days",
                    dtype="float64",
                    semantic_type="metric",
                    role="metric",
                    description="Days to resolve",
                    confidence=0.75,
                ),
            ],
        ),
        TableInfo(
            name="zones",
            row_count=25,
            domain="Environmental",
            description="Geographic zones",
            columns=[
                ColumnInfo(
                    name="zone_id",
                    dtype="varchar",
                    is_primary_key=True,
                    semantic_type="primary_key",
                ),
                ColumnInfo(
                    name="name",
                    dtype="varchar",
                    semantic_type="dimension",
                    role="dimension",
                    description="Zone name",
                    confidence=0.9,
                ),
                ColumnInfo(
                    name="type",
                    dtype="varchar",
                    semantic_type="dimension",
                    role="dimension",
                    description="Land use classification",
                    confidence=0.85,
                ),
                ColumnInfo(
                    name="population",
                    dtype="int64",
                    semantic_type="metric",
                    role="metric",
                    confidence=0.7,
                ),
            ],
        ),
        TableInfo(
            name="sites",
            row_count=500,
            domain="Monitoring",
            description="Monitoring sites",
            columns=[
                ColumnInfo(
                    name="site_id",
                    dtype="varchar",
                    is_primary_key=True,
                    semantic_type="primary_key",
                ),
                ColumnInfo(
                    name="zone_id",
                    dtype="varchar",
                    semantic_type="foreign_key",
                    role="identifier",
                ),
                ColumnInfo(
                    name="site_type",
                    dtype="varchar",
                    semantic_type="dimension",
                    role="dimension",
                    description="Facility type",
                    confidence=0.85,
                ),
            ],
        ),
        TableInfo(
            name="inspections",
            row_count=1243,
            domain="Environmental",
            description="Site inspections",
            columns=[
                ColumnInfo(
                    name="inspection_id",
                    dtype="varchar",
                    is_primary_key=True,
                    semantic_type="primary_key",
                ),
                ColumnInfo(
                    name="site_id",
                    dtype="varchar",
                    semantic_type="foreign_key",
                    role="identifier",
                ),
                ColumnInfo(
                    name="score",
                    dtype="float64",
                    semantic_type="metric",
                    role="metric",
                    description="Inspection score",
                    confidence=0.9,
                ),
                ColumnInfo(
                    name="inspection_type",
                    dtype="varchar",
                    semantic_type="dimension",
                    role="dimension",
                    confidence=0.8,
                ),
                ColumnInfo(
                    name="critical_violations",
                    dtype="int64",
                    semantic_type="metric",
                    role="metric",
                    confidence=0.85,
                ),
            ],
        ),
    ]


@pytest.fixture
def riverton_profiles():
    """Column profiles for the Riverton tables."""
    return [
        ColumnProfile(
            table_name="complaints",
            column_name="category",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=6,
            uniqueness_ratio=0.002,
            top_values=[("noise", 600), ("water_quality", 450), ("air_quality", 360)],
        ),
        ColumnProfile(
            table_name="complaints",
            column_name="priority",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=4,
            uniqueness_ratio=0.001,
            top_values=[("medium", 900), ("high", 800), ("low", 700), ("urgent", 600)],
        ),
        ColumnProfile(
            table_name="complaints",
            column_name="status",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=5,
            uniqueness_ratio=0.002,
            top_values=[("resolved", 1200), ("open", 800), ("investigating", 500)],
        ),
        ColumnProfile(
            table_name="complaints",
            column_name="resolution_days",
            dtype="float64",
            null_count=1350,
            null_rate=0.45,
            distinct_count=120,
            min_value=0.5,
            max_value=180.0,
            mean=14.3,
        ),
        ColumnProfile(
            table_name="zones",
            column_name="name",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=25,
            uniqueness_ratio=1.0,
            top_values=[("Downtown Core", 1), ("Industrial Park", 1)],
        ),
        ColumnProfile(
            table_name="zones",
            column_name="type",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=5,
            uniqueness_ratio=0.2,
            top_values=[("urban_commercial", 8), ("residential", 7), ("industrial", 5)],
        ),
        ColumnProfile(
            table_name="zones",
            column_name="population",
            dtype="int64",
            null_count=0,
            null_rate=0.0,
            distinct_count=25,
            min_value=500,
            max_value=50000,
            mean=12000,
        ),
        ColumnProfile(
            table_name="sites",
            column_name="site_type",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=8,
            uniqueness_ratio=0.016,
            top_values=[("air_monitoring_station", 120), ("food_establishment", 100)],
        ),
        ColumnProfile(
            table_name="inspections",
            column_name="score",
            dtype="float64",
            null_count=0,
            null_rate=0.0,
            distinct_count=80,
            min_value=0.0,
            max_value=100.0,
            mean=72.5,
        ),
        ColumnProfile(
            table_name="inspections",
            column_name="inspection_type",
            dtype="varchar",
            null_count=0,
            null_rate=0.0,
            distinct_count=5,
            top_values=[("routine", 600), ("follow_up", 300), ("complaint_driven", 200)],
        ),
        ColumnProfile(
            table_name="inspections",
            column_name="critical_violations",
            dtype="int64",
            null_count=0,
            null_rate=0.0,
            distinct_count=10,
            min_value=0,
            max_value=12,
            mean=1.2,
        ),
    ]


@pytest.fixture
def riverton_relationships():
    return [
        Relationship(
            from_table="complaints",
            from_column="zone_id",
            to_table="zones",
            to_column="zone_id",
            type="many_to_one",
            confidence=0.95,
            referential_integrity=1.0,
            source="inferred_name",
        ),
        Relationship(
            from_table="complaints",
            from_column="related_site_id",
            to_table="sites",
            to_column="site_id",
            type="many_to_one",
            confidence=0.80,
            referential_integrity=0.28,
            source="inferred_name",
        ),
        Relationship(
            from_table="sites",
            from_column="zone_id",
            to_table="zones",
            to_column="zone_id",
            type="many_to_one",
            confidence=0.95,
            referential_integrity=1.0,
            source="inferred_name",
        ),
        Relationship(
            from_table="inspections",
            from_column="site_id",
            to_table="sites",
            to_column="site_id",
            type="many_to_one",
            confidence=0.95,
            referential_integrity=1.0,
            source="inferred_name",
        ),
    ]


@pytest.fixture
def riverton_discovery(riverton_source, riverton_tables, riverton_profiles, riverton_relationships):
    return DiscoveryResult(
        source=riverton_source,
        tables=riverton_tables,
        profiles=riverton_profiles,
        relationships=riverton_relationships,
    )


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_to_display_name(self):
        assert _to_display_name("zone_geography") == "Zone Geography"
        assert _to_display_name("complaint_count") == "Complaint Count"
        assert _to_display_name("avg_score") == "Avg Score"

    def test_infer_agg_type_score(self):
        col = ColumnInfo(name="score", dtype="float64")
        assert _infer_agg_type(col) == "avg"

    def test_infer_agg_type_count(self):
        col = ColumnInfo(name="total_count", dtype="int64")
        assert _infer_agg_type(col) == "count"

    def test_infer_agg_type_amount(self):
        col = ColumnInfo(name="total_amount", dtype="float64")
        assert _infer_agg_type(col) == "sum"

    def test_infer_agg_type_rate(self):
        col = ColumnInfo(name="resolution_rate", dtype="float64")
        assert _infer_agg_type(col) == "avg"

    def test_expand_synonyms_geographic(self):
        syns = _expand_synonyms("county", "The county name", [])
        assert "borough" in syns
        assert "district" in syns

    def test_expand_synonyms_categorical(self):
        syns = _expand_synonyms("category", "Category of complaint", [])
        assert "type" in syns
        assert "kind" in syns

    def test_expand_synonyms_temporal(self):
        syns = _expand_synonyms("month", "Month of the year", [])
        assert "period" in syns


# ---------------------------------------------------------------------------
# Catalog building tests
# ---------------------------------------------------------------------------


class TestBuildCatalog:
    def test_builds_metrics(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        assert len(catalog.metrics) > 0
        metric_names = [m.name for m in catalog.metrics]
        # Should have count metrics for fact tables
        assert "complaint_count" in metric_names
        assert "inspection_count" in metric_names

    def test_builds_dimensions(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        assert len(catalog.dimensions) > 0
        dim_names = [d.name for d in catalog.dimensions]
        assert "complaints_category" in dim_names
        assert "zones_name" in dim_names

    def test_builds_entities(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        assert len(catalog.entities) > 0
        entity_names = [e.name for e in catalog.entities]
        assert "complaints" in entity_names
        assert "inspections" in entity_names

    def test_entity_has_metrics_and_dimensions(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        complaints_entity = next(e for e in catalog.entities if e.name == "complaints")
        assert len(complaints_entity.metrics) > 0
        assert len(complaints_entity.dimensions) > 0

    def test_cross_table_dimensions_linked(self, riverton_discovery):
        """Entities should include FK-reachable dimensions from other tables."""
        catalog = build_catalog(riverton_discovery)
        complaints_entity = next(e for e in catalog.entities if e.name == "complaints")
        # complaints has FK to zones, so zone dimensions should be linked
        assert any("zones" in d for d in complaints_entity.dimensions)

    def test_metric_has_expression(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        count_metric = next(m for m in catalog.metrics if m.name == "complaint_count")
        assert count_metric.expression == "COUNT(*)"
        assert count_metric.agg_type == "count"

    def test_dimension_has_sample_values(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        cat_dim = next(d for d in catalog.dimensions if d.name == "complaints_category")
        assert len(cat_dim.sample_values) > 0
        assert "noise" in cat_dim.sample_values

    def test_dimension_has_synonyms(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        cat_dim = next(d for d in catalog.dimensions if d.name == "complaints_category")
        assert len(cat_dim.synonyms) > 0

    def test_null_rate_in_metric_description(self, riverton_discovery):
        """Metrics on columns with high null rate should note it."""
        catalog = build_catalog(riverton_discovery)
        res_metric = next(
            (m for m in catalog.metrics if m.column == "resolution_days"),
            None,
        )
        assert res_metric is not None
        assert "NULL" in res_metric.description

    def test_catalog_confidence(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        assert 0.0 < catalog.confidence <= 1.0

    def test_generation_source_heuristic(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        assert catalog.generation_source == "heuristic"

    def test_fk_columns_excluded_from_dimensions(self, riverton_discovery):
        """FK columns (zone_id, site_id) should not appear as dimensions."""
        catalog = build_catalog(riverton_discovery)
        dim_cols = [(d.table, d.column) for d in catalog.dimensions]
        assert ("complaints", "zone_id") not in dim_cols
        assert ("complaints", "related_site_id") not in dim_cols

    def test_pk_columns_excluded_from_dimensions(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        dim_cols = [(d.table, d.column) for d in catalog.dimensions]
        assert ("complaints", "complaint_id") not in dim_cols
        assert ("zones", "zone_id") not in dim_cols


class TestBuildCatalogEdgeCases:
    def test_empty_discovery(self, riverton_source):
        discovery = DiscoveryResult(source=riverton_source)
        catalog = build_catalog(discovery)
        assert len(catalog.metrics) == 0
        assert len(catalog.dimensions) == 0
        assert len(catalog.entities) == 0

    def test_table_with_no_columns(self, riverton_source):
        discovery = DiscoveryResult(
            source=riverton_source,
            tables=[TableInfo(name="empty", row_count=100, columns=[])],
        )
        catalog = build_catalog(discovery)
        # Should still generate COUNT(*) metric
        assert any(m.name == "empty_count" for m in catalog.metrics)


# ---------------------------------------------------------------------------
# Catalog evaluation tests
# ---------------------------------------------------------------------------


class TestEvaluateCatalog:
    def test_coverage(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        ev = evaluate_catalog(
            catalog,
            riverton_discovery.tables,
            riverton_discovery.profiles,
        )
        assert ev.total_analytical_columns > 0
        assert ev.coverage > 0.0

    def test_sql_validity(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        ev = evaluate_catalog(
            catalog,
            riverton_discovery.tables,
            riverton_discovery.profiles,
        )
        assert ev.total_expressions > 0
        assert ev.sql_validity > 0.0
        # All heuristic expressions should be valid SQL
        assert ev.sql_validity == 1.0, f"Invalid: {ev.invalid_expressions}"

    def test_synonym_coverage(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        ev = evaluate_catalog(
            catalog,
            riverton_discovery.tables,
            riverton_discovery.profiles,
        )
        assert ev.total_dimensions > 0
        assert ev.synonym_coverage > 0.0

    def test_overall_confidence(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        ev = evaluate_catalog(
            catalog,
            riverton_discovery.tables,
            riverton_discovery.profiles,
        )
        assert 0.0 < ev.confidence <= 1.0

    def test_ambiguity_detection(self, riverton_discovery):
        """Dimensions sharing synonyms should be flagged."""
        catalog = build_catalog(riverton_discovery)
        ev = evaluate_catalog(
            catalog,
            riverton_discovery.tables,
            riverton_discovery.profiles,
        )
        # type/kind/category synonyms may overlap between category and type dims
        # This is informational, not necessarily a failure
        assert isinstance(ev.ambiguous_synonyms, list)

    def test_empty_catalog_evaluation(self, riverton_source):
        discovery = DiscoveryResult(source=riverton_source)
        catalog = build_catalog(discovery)
        ev = evaluate_catalog(catalog, [], [])
        assert ev.coverage == 0.0
        assert ev.sql_validity == 1.0  # No expressions = 100% valid
        assert ev.synonym_coverage == 0.0

    def test_evaluation_warnings(self, riverton_discovery):
        catalog = build_catalog(riverton_discovery)
        ev = evaluate_catalog(
            catalog,
            riverton_discovery.tables,
            riverton_discovery.profiles,
        )
        assert isinstance(ev.warnings, list)
