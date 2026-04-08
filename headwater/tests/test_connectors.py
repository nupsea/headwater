"""Tests for connectors and schema extraction."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from headwater.connectors.csv_loader import CsvLoader
from headwater.connectors.json_loader import JsonLoader
from headwater.connectors.registry import get_connector
from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig
from headwater.profiler.schema import extract_schema

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sample"


@pytest.fixture()
def ddb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


# -- JsonLoader -----------------------------------------------------------


class TestJsonLoader:
    def test_load_sample_data(self, ddb: duckdb.DuckDBPyConnection):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        tables = loader.load_to_duckdb(ddb, "env_health")

        assert len(tables) == 8
        assert "zones" in tables
        assert "readings" in tables

    def test_row_counts(self, ddb: duckdb.DuckDBPyConnection):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        loader.load_to_duckdb(ddb, "env_health")

        expected = {
            "zones": 25,
            "sites": 500,
            "sensors": 832,
            "readings": 49302,
            "inspections": 1243,
            "incidents": 5000,
            "complaints": 3000,
            "programs": 10,
        }
        for table, count in expected.items():
            result = ddb.execute(f"SELECT COUNT(*) FROM env_health.{table}").fetchone()
            assert result[0] == count, f"{table}: expected {count}, got {result[0]}"

    def test_connect_missing_path(self):
        loader = JsonLoader()
        with pytest.raises(ConnectorError):
            loader.connect(SourceConfig(name="bad", type="json", path="/nonexistent"))

    def test_connect_no_path(self):
        loader = JsonLoader()
        with pytest.raises(ConnectorError):
            loader.connect(SourceConfig(name="bad", type="json"))


# -- Registry --------------------------------------------------------------


class TestRegistry:
    def test_get_json(self):
        c = get_connector("json")
        assert isinstance(c, JsonLoader)

    def test_get_csv(self):
        c = get_connector("csv")
        assert isinstance(c, CsvLoader)

    def test_get_unknown(self):
        with pytest.raises(ConnectorError):
            get_connector("mongo")


# -- Schema extraction -----------------------------------------------------


class TestSchemaExtraction:
    def test_extract_schema(self, ddb: duckdb.DuckDBPyConnection):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        loader.load_to_duckdb(ddb, "env_health")

        tables = extract_schema(ddb, "env_health")
        assert len(tables) == 8

        table_names = {t.name for t in tables}
        assert "zones" in table_names
        assert "readings" in table_names

    def test_column_types_normalised(self, ddb: duckdb.DuckDBPyConnection):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        loader.load_to_duckdb(ddb, "env_health")

        tables = extract_schema(ddb, "env_health")
        zones = next(t for t in tables if t.name == "zones")

        # population should be numeric
        pop = next(c for c in zones.columns if c.name == "population")
        assert pop.dtype in ("int64", "float64")

        # zone_id should be varchar
        zid = next(c for c in zones.columns if c.name == "zone_id")
        assert zid.dtype == "varchar"

    def test_primary_key_detection(self, ddb: duckdb.DuckDBPyConnection):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        loader.load_to_duckdb(ddb, "env_health")

        tables = extract_schema(ddb, "env_health")
        zones = next(t for t in tables if t.name == "zones")
        zone_id = next(c for c in zones.columns if c.name == "zone_id")
        assert zone_id.is_primary_key is True

    def test_row_counts_in_schema(self, ddb: duckdb.DuckDBPyConnection):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        loader.load_to_duckdb(ddb, "env_health")

        tables = extract_schema(ddb, "env_health")
        readings = next(t for t in tables if t.name == "readings")
        assert readings.row_count == 49302
