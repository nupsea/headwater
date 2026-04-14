"""Tests for core Pydantic models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    ContractRule,
    DecompositionResult,
    DimensionDefinition,
    DimensionMatch,
    DimensionOption,
    DiscoveryResult,
    EntityDefinition,
    GeneratedModel,
    MetricDefinition,
    MetricMatch,
    Project,
    ProjectProgress,
    Relationship,
    SemanticCatalog,
    SourceConfig,
    TableInfo,
)


def test_source_config_json():
    sc = SourceConfig(name="test", type="json", path="/data/sample")
    assert sc.type == "json"
    assert sc.uri is None


def test_source_config_postgres():
    sc = SourceConfig(name="prod", type="postgres", uri="postgres://localhost/db")
    assert sc.path is None


def test_source_config_invalid_type():
    with pytest.raises(ValidationError):
        SourceConfig(name="bad", type="mongo")


def test_column_info_defaults():
    col = ColumnInfo(name="id", dtype="int64")
    assert col.nullable is True
    assert col.is_primary_key is False
    assert col.description is None


def test_table_info():
    ti = TableInfo(
        name="sites",
        row_count=500,
        columns=[ColumnInfo(name="site_id", dtype="varchar")],
    )
    assert len(ti.columns) == 1
    assert ti.domain is None


def test_column_profile():
    cp = ColumnProfile(
        table_name="sites",
        column_name="latitude",
        dtype="float64",
        null_count=0,
        null_rate=0.0,
        distinct_count=500,
        uniqueness_ratio=1.0,
        min_value=38.0,
        max_value=39.5,
    )
    assert cp.mean is None  # Not provided


def test_relationship_confidence_bounds():
    r = Relationship(
        from_table="sites",
        from_column="zone_id",
        to_table="zones",
        to_column="zone_id",
        type="many_to_one",
        confidence=0.95,
        referential_integrity=0.98,
        source="inferred_name",
    )
    assert r.confidence == 0.95

    with pytest.raises(ValidationError):
        Relationship(
            from_table="a",
            from_column="b",
            to_table="c",
            to_column="d",
            type="one_to_many",
            confidence=1.5,
            referential_integrity=0.5,
            source="declared",
        )


def test_generated_model():
    m = GeneratedModel(
        name="stg_sites",
        model_type="staging",
        sql="SELECT * FROM sites",
        description="Staging model for sites",
    )
    assert m.status == "proposed"
    assert m.questions == []


def test_contract_rule_defaults():
    c = ContractRule(
        model_name="stg_sites",
        column_name="site_id",
        rule_type="not_null",
        expression="site_id IS NOT NULL",
    )
    assert c.severity == "warning"
    assert c.status == "proposed"


def test_discovery_result():
    src = SourceConfig(name="sample", type="json", path="/data")
    dr = DiscoveryResult(source=src)
    assert dr.tables == []
    assert dr.relationships == []
    assert dr.discovered_at is not None


# ---------------------------------------------------------------------------
# v2 models
# ---------------------------------------------------------------------------


def test_project_defaults():
    p = Project(id="abc-123", slug="test-project", display_name="Test Project")
    assert p.maturity == "raw"
    assert p.maturity_score == 0.0
    assert p.catalog_confidence == 0.0
    assert p.progress.tables_discovered == 0


def test_project_progress():
    pp = ProjectProgress(
        tables_discovered=8,
        tables_profiled=8,
        columns_total=126,
        columns_described=89,
    )
    assert pp.tables_discovered == 8
    assert pp.columns_confirmed == 0  # default


def test_metric_definition():
    m = MetricDefinition(
        name="complaint_count",
        display_name="Total Complaints",
        description="Count of all complaint records",
        expression="COUNT(*)",
        table="complaints",
        agg_type="count",
        synonyms=["total complaints", "number of complaints"],
    )
    assert m.confidence == 0.5
    assert m.status == "proposed"
    assert len(m.synonyms) == 2


def test_metric_definition_confidence_bounds():
    with pytest.raises(ValidationError):
        MetricDefinition(
            name="bad",
            display_name="Bad",
            description="Bad metric",
            expression="COUNT(*)",
            table="t",
            agg_type="count",
            confidence=1.5,
        )


def test_dimension_definition():
    d = DimensionDefinition(
        name="zone_geography",
        display_name="Zone / Geographic Area",
        description="Administrative zones",
        column="name",
        table="zones",
        dtype="varchar",
        synonyms=["county", "district", "area"],
        sample_values=["Downtown Core", "Industrial Park"],
        join_path="complaints.zone_id -> zones.zone_id",
    )
    assert d.cardinality == 0
    assert d.join_nullable is False


def test_entity_definition():
    e = EntityDefinition(
        name="complaints",
        display_name="Complaints",
        description="Environmental health complaints from residents",
        table="complaints",
        row_semantics="Each row represents one complaint filing",
        metrics=["complaint_count", "resolution_rate"],
        dimensions=["complaint_category", "zone_geography"],
    )
    assert len(e.metrics) == 2
    assert e.temporal_grain is None


def test_semantic_catalog():
    cat = SemanticCatalog(
        metrics=[
            MetricDefinition(
                name="count",
                display_name="Count",
                description="Total count",
                expression="COUNT(*)",
                table="t",
                agg_type="count",
            ),
        ],
        dimensions=[],
        entities=[],
    )
    assert len(cat.metrics) == 1
    assert cat.generation_source == "heuristic"
    assert cat.confidence == 0.5


def test_metric_match():
    mm = MetricMatch(
        metric_name="complaint_count",
        display_name="Total Complaints",
        expression="COUNT(*)",
        table="complaints",
        confidence=0.85,
        strategy="keyword",
    )
    assert mm.strategy == "keyword"


def test_dimension_match():
    dm = DimensionMatch(
        dimension_name="zone_geography",
        display_name="Zone",
        column="name",
        table="zones",
        join_path="complaints.zone_id -> zones.zone_id",
        is_filter=False,
    )
    assert dm.filter_value is None


def test_dimension_option():
    opt = DimensionOption(
        dimension_name="zone_geography",
        display_name="Zone / Geographic Area",
        description="Administrative zones for monitoring",
        sample_values=["Downtown Core", "Industrial Park"],
        confidence=0.85,
    )
    assert len(opt.sample_values) == 2


def test_decomposition_result_resolved():
    dr = DecompositionResult(
        status="resolved",
        entity="complaints",
        sql="SELECT COUNT(*) FROM complaints",
        explanation="Counted all complaints",
        confidence=0.9,
        resolution_mode="catalog",
    )
    assert dr.status == "resolved"
    assert dr.warnings == []


def test_decomposition_result_options():
    dr = DecompositionResult(
        status="options",
        options=[
            DimensionOption(
                dimension_name="zone_geography",
                display_name="Zone Name",
                description="Geographic zone",
            ),
            DimensionOption(
                dimension_name="zone_type",
                display_name="Zone Type",
                description="Land use classification",
            ),
        ],
    )
    assert len(dr.options) == 2


def test_decomposition_result_outside_scope():
    dr = DecompositionResult(
        status="outside_scope",
        outside_catalog=["revenue", "profit"],
        explanation="These concepts are not in the dataset",
    )
    assert len(dr.outside_catalog) == 2
