"""Tests for core Pydantic models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    ContractRule,
    DiscoveryResult,
    GeneratedModel,
    Relationship,
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
            from_table="a", from_column="b", to_table="c", to_column="d",
            type="one_to_many", confidence=1.5, referential_integrity=0.5,
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
