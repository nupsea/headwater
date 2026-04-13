"""Tests for the data viewer API routes (table preview + SQL query)."""

from __future__ import annotations

import duckdb
import pytest
from fastapi.testclient import TestClient

from headwater.core.metadata import MetadataStore
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    SourceConfig,
    TableInfo,
)


@pytest.fixture()
def client():
    """Create a test client with a DuckDB table loaded."""
    from headwater.api.app import create_app

    app = create_app()

    con = duckdb.connect(":memory:")
    # Create a staging table with sample data
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE staging.stg_readings (
            site_id INTEGER,
            value DOUBLE,
            status VARCHAR,
            reading_date DATE
        );
        INSERT INTO staging.stg_readings VALUES
            (1, 42.5, 'good', '2024-01-01'),
            (2, 88.3, 'moderate', '2024-01-01'),
            (3, 150.0, 'unhealthy', '2024-01-02'),
            (1, 35.0, 'good', '2024-01-02'),
            (2, 95.0, 'moderate', '2024-01-03');
    """)

    app.state.duckdb_con = con
    app.state.metadata_store = MetadataStore(":memory:")
    app.state.metadata_store.init()

    discovery = DiscoveryResult(
        source=SourceConfig(name="test_src", type="json", path="/data"),
        tables=[
            TableInfo(
                name="readings",
                row_count=5,
                columns=[
                    ColumnInfo(name="site_id", dtype="int64"),
                    ColumnInfo(name="value", dtype="float64"),
                    ColumnInfo(name="status", dtype="varchar"),
                    ColumnInfo(name="reading_date", dtype="date"),
                ],
            ),
        ],
        profiles=[
            ColumnProfile(
                table_name="readings", column_name="site_id",
                dtype="int64", distinct_count=3, uniqueness_ratio=0.6,
            ),
        ],
    )

    app.state.pipeline = {
        "discovery": discovery,
        "staging_models": [],
        "mart_models": [],
        "contracts": [],
        "execution_results": [],
        "quality_report": None,
    }

    client = TestClient(app, raise_server_exceptions=False)
    yield client
    con.close()


class TestDataCatalog:
    """Test GET /data/catalog."""

    def test_catalog_returns_all_tables(self, client):
        resp = client.get("/api/data/catalog")
        assert resp.status_code == 200
        data = resp.json()
        assert "schemas" in data
        assert "tables" in data
        assert "total" in data
        assert data["total"] >= 1
        # The staging.stg_readings table should appear
        names = [t["qualified_name"] for t in data["tables"]]
        assert "staging.stg_readings" in names

    def test_catalog_includes_columns(self, client):
        resp = client.get("/api/data/catalog")
        data = resp.json()
        stg = next(t for t in data["tables"] if t["table_name"] == "stg_readings")
        assert stg["schema"] == "staging"
        assert stg["column_count"] == 4
        assert len(stg["columns"]) == 4
        col_names = [c["name"] for c in stg["columns"]]
        assert "site_id" in col_names
        assert "value" in col_names

    def test_catalog_includes_row_count(self, client):
        resp = client.get("/api/data/catalog")
        data = resp.json()
        stg = next(t for t in data["tables"] if t["table_name"] == "stg_readings")
        assert stg["row_count"] == 5


class TestDataPreview:
    """Test GET /data/{table_name}/preview."""

    def test_preview_returns_data(self, client):
        resp = client.get("/api/data/readings/preview")
        assert resp.status_code == 200
        data = resp.json()
        assert "columns" in data
        assert "data" in data
        assert "row_count" in data
        assert "sql" in data
        assert data["row_count"] == 5
        assert len(data["data"]) == 5
        assert "site_id" in data["columns"]

    def test_preview_respects_limit(self, client):
        resp = client.get("/api/data/readings/preview?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 2
        assert len(data["data"]) == 2

    def test_preview_max_limit_capped(self, client):
        resp = client.get("/api/data/readings/preview?limit=9999")
        assert resp.status_code == 200
        # Should be capped, not fail
        data = resp.json()
        assert data["row_count"] <= 500

    def test_preview_nonexistent_table(self, client):
        resp = client.get("/api/data/nonexistent/preview")
        assert resp.status_code == 404

    def test_preview_no_discovery(self, client):
        client.app.state.pipeline["discovery"] = None
        resp = client.get("/api/data/readings/preview")
        assert resp.status_code == 400


class TestDataQuery:
    """Test POST /data/query."""

    def test_query_returns_data(self, client):
        resp = client.post("/api/data/query", json={
            "sql": "SELECT * FROM staging.stg_readings LIMIT 3",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 3
        assert len(data["data"]) == 3
        assert data["error"] is None

    def test_query_aggregation(self, client):
        resp = client.post("/api/data/query", json={
            "sql": "SELECT status, COUNT(*) AS cnt FROM staging.stg_readings GROUP BY status",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] > 0
        assert "status" in data["columns"]
        assert "cnt" in data["columns"]

    def test_query_rejects_write(self, client):
        for stmt in [
            "INSERT INTO staging.stg_readings VALUES (99, 1.0, 'x', '2024-01-01')",
            "DELETE FROM staging.stg_readings WHERE site_id = 1",
            "DROP TABLE staging.stg_readings",
            "UPDATE staging.stg_readings SET value = 0",
        ]:
            resp = client.post("/api/data/query", json={"sql": stmt})
            assert resp.status_code == 200
            data = resp.json()
            assert data["error"] is not None
            assert "blocked" in data["error"].lower() or "read-only" in data["error"].lower()

    def test_query_bad_sql(self, client):
        resp = client.post("/api/data/query", json={
            "sql": "SELECT * FROM nonexistent_table_xyz",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["error"] is not None

    def test_query_empty_sql(self, client):
        resp = client.post("/api/data/query", json={"sql": ""})
        assert resp.status_code == 200
        data = resp.json()
        assert data["error"] is not None

    def test_query_no_discovery(self, client):
        client.app.state.pipeline["discovery"] = None
        resp = client.post("/api/data/query", json={
            "sql": "SELECT 1",
        })
        assert resp.status_code == 400
