"""Tests for companion documentation discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from headwater.analyzer.companion import (
    discover_companion_docs,
    extract_table_context,
    match_docs_to_tables,
    parse_doc_file,
)
from headwater.analyzer.metadata_retrieval import retrieve_metadata
from headwater.core.models import ColumnInfo, CompanionDoc, DiscoveryResult, SourceConfig, TableInfo
from headwater.explorer.readability import enum_dimension_label, enum_mapping_for_column


@pytest.fixture()
def source_dir(tmp_path: Path) -> Path:
    """Create a temp directory with data files and companion docs."""
    # Data files
    (tmp_path / "sensors.json").write_text('[{"id": 1}]\n')
    (tmp_path / "readings.json").write_text('[{"id": 1}]\n')

    # Companion docs
    (tmp_path / "README.md").write_text(
        "# Dataset Documentation\n\n"
        "This dataset contains environmental monitoring data.\n\n"
        "## sensors\n"
        "The sensors table contains sensor metadata.\n"
        "Each row represents a monitoring sensor deployed at a site.\n\n"
        "## readings\n"
        "The readings table contains measurement data.\n"
        "Each row represents a single pollutant reading.\n"
    )
    (tmp_path / "sensors.md").write_text(
        "# Sensors\n\n"
        "EPA-deployed air quality monitoring sensors.\n"
        "Columns: sensor_id, site_id, pollutant, status.\n"
    )
    return tmp_path


def test_discover_docs_finds_markdown(source_dir: Path) -> None:
    source = SourceConfig(name="test", type="json", path=str(source_dir))
    docs = discover_companion_docs(source)
    filenames = {d.filename for d in docs}
    assert "README.md" in filenames
    assert "sensors.md" in filenames


def test_discover_docs_ignores_data_files(source_dir: Path) -> None:
    source = SourceConfig(name="test", type="json", path=str(source_dir))
    docs = discover_companion_docs(source)
    filenames = {d.filename for d in docs}
    assert "sensors.json" not in filenames
    assert "readings.json" not in filenames


def test_database_source_returns_empty() -> None:
    source = SourceConfig(name="db", type="postgres", uri="postgresql://localhost/test")
    docs = discover_companion_docs(source)
    assert docs == []


def test_match_docs_by_filename(source_dir: Path) -> None:
    source = SourceConfig(name="test", type="json", path=str(source_dir))
    docs = discover_companion_docs(source)
    table_names = ["sensors", "readings", "sites"]
    match_docs_to_tables(docs, table_names)

    sensors_doc = next(d for d in docs if d.filename == "sensors.md")
    assert "sensors" in sensors_doc.matched_tables
    assert sensors_doc.confidence >= 0.9


def test_match_docs_by_content(source_dir: Path) -> None:
    source = SourceConfig(name="test", type="json", path=str(source_dir))
    docs = discover_companion_docs(source)
    table_names = ["sensors", "readings", "sites"]
    match_docs_to_tables(docs, table_names)

    readme = next(d for d in docs if d.filename == "README.md")
    # README mentions both sensors and readings multiple times
    assert "sensors" in readme.matched_tables
    assert "readings" in readme.matched_tables


def test_global_doc_matches_all_tables() -> None:
    doc = CompanionDoc(
        filename="README.md",
        content="General overview of the dataset.",
        doc_type="markdown",
    )
    table_names = ["sensors", "readings", "sites"]
    match_docs_to_tables([doc], table_names)
    # README is a global doc name, should match all
    assert set(doc.matched_tables) == {"sensors", "readings", "sites"}
    assert doc.confidence == 0.5  # Global docs get low confidence


def test_parse_markdown(tmp_path: Path) -> None:
    fp = tmp_path / "test.md"
    fp.write_text("# Title\n\nSome content.")
    content = parse_doc_file(fp)
    assert "# Title" in content
    assert "Some content" in content


def test_parse_text(tmp_path: Path) -> None:
    fp = tmp_path / "test.txt"
    fp.write_text("Plain text description.")
    content = parse_doc_file(fp)
    assert content == "Plain text description."


def test_parse_yaml(tmp_path: Path) -> None:
    fp = tmp_path / "schema.yml"
    fp.write_text(
        "sensors:\n  description: Sensor metadata\nreadings:\n  description: Measurements\n"
    )
    content = parse_doc_file(fp)
    assert "sensors" in content
    assert "readings" in content


def test_parse_csv_dictionary(tmp_path: Path) -> None:
    fp = tmp_path / "dictionary.csv"
    fp.write_text(
        "column_name,description\n"
        "sensor_id,Unique sensor identifier\n"
        "pollutant,Measured pollutant\n"
    )
    content = parse_doc_file(fp)
    assert "sensor_id" in content
    assert "Unique sensor identifier" in content


def test_extract_table_context() -> None:
    docs = [
        CompanionDoc(
            filename="sensors.md",
            content="Sensor deployment metadata.",
            doc_type="markdown",
            matched_tables=["sensors"],
            confidence=0.9,
        ),
        CompanionDoc(
            filename="README.md",
            content="# sensors\nGeneral sensor info.\n\n# readings\nReading data.",
            doc_type="markdown",
            matched_tables=["sensors", "readings"],
            confidence=0.5,
        ),
    ]

    ctx = extract_table_context(docs, "sensors")
    assert ctx is not None
    assert "Sensor deployment metadata" in ctx


def test_extract_table_context_no_match() -> None:
    docs = [
        CompanionDoc(
            filename="sensors.md",
            content="Sensor data.",
            doc_type="markdown",
            matched_tables=["sensors"],
            confidence=0.9,
        ),
    ]
    ctx = extract_table_context(docs, "nonexistent_table")
    assert ctx is None


def test_yaml_key_matching(tmp_path: Path) -> None:
    fp = tmp_path / "schema.yml"
    fp.write_text("sensors:\n  desc: Sensors\nreadings:\n  desc: Readings\n")
    source = SourceConfig(name="test", type="json", path=str(tmp_path))
    # Create a json file so the dir is a valid source
    (tmp_path / "data.json").write_text('[{"id": 1}]\n')

    docs = discover_companion_docs(source)
    yaml_doc = next((d for d in docs if d.filename == "schema.yml"), None)
    assert yaml_doc is not None

    match_docs_to_tables([yaml_doc], ["sensors", "readings", "zones"])
    assert "sensors" in yaml_doc.matched_tables
    assert "readings" in yaml_doc.matched_tables
    assert yaml_doc.confidence >= 0.85


def test_csv_dictionary_detection(tmp_path: Path) -> None:
    """A CSV with dictionary-like headers is detected as a doc."""
    fp = tmp_path / "data_dictionary.csv"
    fp.write_text(
        "table_name,column_name,description\n"
        "sensors,sensor_id,Unique ID\n"
        "sensors,status,Current status\n"
    )
    # Also create a data CSV that should NOT be picked up
    (tmp_path / "sales_data.csv").write_text(
        "date,amount,customer\n2024-01-01,100,acme\n2024-01-02,200,globex\n"
    )

    source = SourceConfig(name="test", type="json", path=str(tmp_path))
    docs = discover_companion_docs(source)
    filenames = {d.filename for d in docs}
    assert "data_dictionary.csv" in filenames
    # sales_data.csv should not be treated as a dictionary
    assert "sales_data.csv" not in filenames


def test_retrieve_metadata_extracts_enum_mappings_from_dictionary_rows() -> None:
    discovery = DiscoveryResult(
        source=SourceConfig(name="test", type="json", path="/data"),
        tables=[
            TableInfo(
                name="orders",
                row_count=10,
                columns=[ColumnInfo(name="status_code", dtype="varchar")],
            )
        ],
        companion_docs=[
            CompanionDoc(
                filename="dictionary.csv",
                content=(
                    "column_name: status_code | "
                    "description: order status. P=Pending; S=Shipped; C=Cancelled"
                ),
                doc_type="csv",
                matched_tables=["orders"],
                confidence=0.9,
            )
        ],
    )

    metadata = retrieve_metadata(discovery)

    assert metadata.glossary["status_code"] == "order status."
    assert metadata.enum_mappings["status_code"] == {
        "P": "Pending",
        "S": "Shipped",
        "C": "Cancelled",
    }
    assert enum_mapping_for_column("payment_type") is None
    assert enum_dimension_label("payment_type", "payment type") == "payment type"


def test_retrieve_metadata_merges_canonical_project_context_items() -> None:
    discovery = DiscoveryResult(
        source=SourceConfig(name="test", type="json", path="/data"),
        tables=[
            TableInfo(
                name="orders",
                row_count=10,
                columns=[ColumnInfo(name="status_code", dtype="varchar")],
            ),
            TableInfo(
                name="status_lookup",
                row_count=3,
                columns=[
                    ColumnInfo(name="status_code", dtype="varchar", semantic_type="id"),
                    ColumnInfo(name="status_label", dtype="varchar"),
                ],
            ),
        ],
    )

    metadata = retrieve_metadata(
        discovery,
        context_items=[
            {
                "id": "column_semantics:orders.status_code",
                "project_id": "test",
                "source_name": "test",
                "item_type": "column_semantics",
                "scope": "column",
                "name": "status_code",
                "table_name": "orders",
                "column_name": "status_code",
                "status": "approved",
                "confidence": 0.97,
                "value": {
                    "description": "Order lifecycle status.",
                    "role": "dimension",
                },
            },
            {
                "id": "lookup:status_lookup",
                "project_id": "test",
                "source_name": "test",
                "item_type": "lookup",
                "scope": "table",
                "name": "status_lookup",
                "table_name": "status_lookup",
                "value": {
                    "key_column": "status_code",
                    "label_column": "status_label",
                },
            },
            {
                "id": "enum_mapping:orders.status_code",
                "project_id": "test",
                "source_name": "test",
                "item_type": "enum_mapping",
                "scope": "column",
                "name": "status_code",
                "table_name": "orders",
                "column_name": "status_code",
                "status": "approved",
                "value": {
                    "label": "order status",
                    "labels": {
                        "P": "Pending",
                        "S": "Shipped",
                    }
                },
            },
            {
                "id": "insight_family:order_lifecycle",
                "project_id": "test",
                "source_name": "test",
                "item_type": "insight_family",
                "scope": "project",
                "name": "order_lifecycle",
                "status": "approved",
                "confidence": 0.92,
                "value": {
                    "required_roles": ["event_ts", "dimension"],
                    "priority": 11,
                },
            },
        ],
    )

    assert metadata.glossary["status_code"] == "Order lifecycle status."
    assert metadata.lookup_tables["status_lookup"] == {
        "id_column": "status_code",
        "label_column": "status_label",
    }
    assert metadata.enum_mappings["status_code"] == {
        "P": "Pending",
        "S": "Shipped",
    }
    assert metadata.glossary["status_code"] == "Order lifecycle status."
    assert metadata.locked_roles[("orders", "status_code")] == "dimension"
    assert metadata.insight_families == [
        {
            "key": "order_lifecycle",
            "required_roles": ["event_ts", "dimension"],
            "priority": 11,
            "source": "project_context",
            "context_item_id": "insight_family:order_lifecycle",
        }
    ]


def test_nonexistent_path_returns_empty() -> None:
    source = SourceConfig(name="test", type="json", path="/nonexistent/path")
    docs = discover_companion_docs(source)
    assert docs == []
