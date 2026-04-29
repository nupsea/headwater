"""Tests for connectors and schema extraction."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pytest

from headwater.connectors.csv_loader import CsvLoader
from headwater.connectors.duckdb_loader import DuckDBConnector
from headwater.connectors.json_loader import JsonLoader
from headwater.connectors.registry import (
    connector_status,
    get_connector,
    get_connector_capabilities,
    list_connector_catalog,
)
from headwater.connectors.sqlite_loader import SQLiteConnector
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

    def test_get_duckdb(self):
        c = get_connector("duckdb")
        assert isinstance(c, DuckDBConnector)

    def test_get_sqlite(self):
        c = get_connector("sqlite")
        assert isinstance(c, SQLiteConnector)

    def test_get_unknown(self):
        with pytest.raises(ConnectorError):
            get_connector("mongo")

    def test_catalog_exposes_support_status(self):
        catalog = {c["id"]: c for c in list_connector_catalog()}
        assert catalog["json"]["status"] == "supported"
        assert catalog["csv"]["status"] == "supported"
        assert catalog["duckdb"]["status"] == "supported"
        assert catalog["sqlite"]["status"] == "supported"
        assert catalog["postgres"]["status"] == "supported"
        assert catalog["mysql"]["status"] == "planned"
        assert catalog["json"]["supported"] is True
        assert catalog["mysql"]["supported"] is False
        assert catalog["json"]["capabilities"]["list_tables"] is True
        assert catalog["duckdb"]["capabilities"]["execute_readonly"] is True
        assert catalog["sqlite"]["capabilities"]["execute_readonly"] is True
        assert catalog["json"]["capabilities"]["sample_arrow"] is True
        assert catalog["mysql"]["capabilities"]["test"] is False

    def test_connector_status_helper(self):
        assert connector_status("postgres") == "supported"
        assert connector_status("duckdb") == "supported"
        assert connector_status("sqlite") == "supported"
        assert connector_status("mysql") == "planned"
        assert connector_status("mongo") is None

    def test_connector_capabilities_helper(self):
        json_caps = get_connector_capabilities("json")
        duckdb_caps = get_connector_capabilities("duckdb")
        sqlite_caps = get_connector_capabilities("sqlite")
        postgres_caps = get_connector_capabilities("postgres")
        mysql_caps = get_connector_capabilities("mysql")

        assert json_caps.load_to_duckdb is True
        assert duckdb_caps.load_to_duckdb is True
        assert duckdb_caps.execute_readonly is True
        assert sqlite_caps.load_to_duckdb is True
        assert sqlite_caps.execute_readonly is True
        assert postgres_caps.execute_readonly is True
        assert postgres_caps.load_to_duckdb is False
        assert mysql_caps.test is False

    def test_get_planned_connector_explains_status(self):
        with pytest.raises(ConnectorError, match="planned"):
            get_connector("mysql")


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


class TestDuckDBConnector:
    def test_duckdb_load_to_duckdb_copies_source_tables(self, tmp_path: Path):
        db_path = tmp_path / "source.duckdb"
        source = duckdb.connect(str(db_path))
        try:
            source.execute("CREATE TABLE users (user_id INTEGER, email VARCHAR)")
            source.execute("INSERT INTO users VALUES (1, 'a@example.com'), (2, 'b@example.com')")
        finally:
            source.close()

        loader = DuckDBConnector()
        loader.connect(SourceConfig(name="sample_duckdb", type="duckdb", path=str(db_path)))
        target = duckdb.connect(":memory:")
        try:
            tables = loader.load_to_duckdb(target, "env_health")
            count = target.execute('SELECT COUNT(*) FROM "env_health"."users"').fetchone()[0]
        finally:
            target.close()
            loader.close()

        assert tables == ["users"]
        assert count == 2

    def test_duckdb_connect_missing_path(self, tmp_path: Path):
        loader = DuckDBConnector()
        with pytest.raises(ConnectorError):
            loader.connect(
                SourceConfig(
                    name="missing_duckdb",
                    type="duckdb",
                    path=str(tmp_path / "missing.duckdb"),
                )
            )


class TestSQLiteConnector:
    def test_sqlite_load_to_duckdb_copies_source_tables(self, tmp_path: Path):
        db_path = tmp_path / "source.sqlite"
        source = sqlite3.connect(db_path)
        try:
            source.execute("CREATE TABLE users (user_id INTEGER, email TEXT)")
            source.execute("INSERT INTO users VALUES (1, 'a@example.com'), (2, 'b@example.com')")
            source.commit()
        finally:
            source.close()

        loader = SQLiteConnector()
        loader.connect(SourceConfig(name="sample_sqlite", type="sqlite", path=str(db_path)))
        target = duckdb.connect(":memory:")
        try:
            tables = loader.load_to_duckdb(target, "env_health")
            count = target.execute('SELECT COUNT(*) FROM "env_health"."users"').fetchone()[0]
        finally:
            target.close()
            loader.close()

        assert tables == ["users"]
        assert count == 2

    def test_sqlite_connect_missing_path(self, tmp_path: Path):
        loader = SQLiteConnector()
        with pytest.raises(ConnectorError):
            loader.connect(
                SourceConfig(
                    name="missing_sqlite",
                    type="sqlite",
                    path=str(tmp_path / "missing.sqlite"),
                )
            )


# -- BaseConnector new methods (US-100) ------------------------------------


class TestJsonLoaderProfile:
    def test_profile_returns_stats_dict(self):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        stats = loader.profile("zones")
        assert isinstance(stats, dict)
        assert "zone_id" in stats
        assert stats["zone_id"]["count"] == 25
        assert stats["zone_id"]["null_count"] == 0
        assert stats["zone_id"]["distinct_count"] == 25

    def test_profile_numeric_min_max(self):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        stats = loader.profile("zones")
        # population is numeric -- should have min/max
        assert "population" in stats
        pop = stats["population"]
        assert "min" in pop
        assert "max" in pop
        assert pop["min"] >= 0

    def test_sample_returns_arrow_table(self):
        import pyarrow as pa

        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        table = loader.sample("zones")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 25  # zones has only 25 rows, all returned

    def test_sample_respects_n(self):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        # readings has 49302 rows; limit to 100
        table = loader.sample("readings", n=100)
        assert table.num_rows == 100

    def test_profile_after_load_to_duckdb(self, ddb: duckdb.DuckDBPyConnection):
        """profile() works after load_to_duckdb() using the cached frame."""
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))
        loader.load_to_duckdb(ddb, "env_health")
        stats = loader.profile("zones")
        assert stats["zone_id"]["count"] == 25

    def test_capabilities_and_listing(self):
        loader = JsonLoader()
        loader.connect(SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)))

        caps = loader.capabilities()
        columns = loader.list_columns("zones")

        assert caps.list_tables is True
        assert "zones" in loader.list_tables()
        assert columns[0]["name"] == "zone_id"


class TestSourceConfigMode:
    def test_default_mode_is_generate(self):
        cfg = SourceConfig(name="s", type="json", path="/data")
        assert cfg.mode == "generate"

    def test_observe_mode_accepted(self):
        cfg = SourceConfig(name="s", type="json", path="/data", mode="observe")
        assert cfg.mode == "observe"
