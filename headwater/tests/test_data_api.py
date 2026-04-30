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

    app = create_app(in_memory=True)

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
        CREATE TABLE sites (
            site_id INTEGER,
            name VARCHAR
        );
        INSERT INTO sites VALUES
            (1, 'North'),
            (2, 'South'),
            (3, 'West');
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
                    ColumnInfo(name="reading_date", dtype="date"),
                    ColumnInfo(name="value", dtype="float64"),
                    ColumnInfo(name="status", dtype="varchar"),
                ],
            ),
            TableInfo(
                name="sites",
                row_count=3,
                columns=[
                    ColumnInfo(name="site_id", dtype="int64"),
                    ColumnInfo(name="name", dtype="varchar"),
                ],
            ),
        ],
        profiles=[
            ColumnProfile(
                table_name="readings",
                column_name="site_id",
                dtype="int64",
                distinct_count=3,
                uniqueness_ratio=0.6,
            ),
            ColumnProfile(
                table_name="readings",
                column_name="reading_date",
                dtype="date",
                distinct_count=5,
                uniqueness_ratio=1.0,
            ),
            ColumnProfile(
                table_name="sites",
                column_name="site_id",
                dtype="int64",
                distinct_count=3,
                uniqueness_ratio=1.0,
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

    def test_preview_accepts_staging_table_name(self, client):
        resp = client.get("/api/data/stg_readings/preview")
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 5
        assert data["sql"] == "SELECT * FROM \"staging\".\"stg_readings\" LIMIT 100"

    def test_preview_accepts_qualified_table_name(self, client):
        resp = client.get("/api/data/staging.stg_readings/preview")
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 5
        assert data["sql"] == "SELECT * FROM \"staging\".\"stg_readings\" LIMIT 100"

    def test_preview_falls_back_from_missing_staging_model_to_source_table(self, client):
        resp = client.get("/api/data/stg_sites/preview")
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 3
        assert data["sql"] == "SELECT * FROM \"main\".\"sites\" LIMIT 100"

    def test_preview_falls_back_from_missing_qualified_staging_model(self, client):
        resp = client.get("/api/data/staging.stg_sites/preview")
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 3
        assert data["sql"] == "SELECT * FROM \"main\".\"sites\" LIMIT 100"

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
        resp = client.post(
            "/api/data/query",
            json={
                "sql": "SELECT * FROM staging.stg_readings LIMIT 3",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 3
        assert len(data["data"]) == 3
        assert data["error"] is None

    def test_query_aggregation(self, client):
        resp = client.post(
            "/api/data/query",
            json={
                "sql": "SELECT status, COUNT(*) AS cnt FROM staging.stg_readings GROUP BY status",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] > 0
        assert "status" in data["columns"]
        assert "cnt" in data["columns"]

    def test_query_normalizes_smart_quotes(self, client):
        resp = client.post(
            "/api/data/query",
            json={
                "sql": (
                    'SELECT “status”, COUNT(*) AS cnt '
                    'FROM staging.stg_readings GROUP BY "status"'
                ),
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["error"] is None
        assert data["row_count"] > 0
        assert data["sql"] == (
            'SELECT "status", COUNT(*) AS cnt '
            'FROM staging.stg_readings GROUP BY "status"'
        )

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
        resp = client.post(
            "/api/data/query",
            json={
                "sql": "SELECT * FROM nonexistent_table_xyz",
            },
        )
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
        resp = client.post(
            "/api/data/query",
            json={
                "sql": "SELECT 1",
            },
        )
        assert resp.status_code == 400


class TestKeyPersistence:
    def test_confirmed_keys_update_active_discovery(self, client):
        pk_resp = client.patch(
            "/api/tables/sites/keys",
            json={
                "confirm_pks": ["site_id"],
            },
        )
        assert pk_resp.status_code == 200

        fk_resp = client.patch(
            "/api/tables/readings/keys",
            json={
                "confirm_fks": [
                    {"from_col": "site_id", "to_table": "sites", "to_col": "site_id"}
                ],
            },
        )
        assert fk_resp.status_code == 200

        discovery = client.app.state.pipeline["discovery"]
        sites = next(t for t in discovery.tables if t.name == "sites")
        site_pk = next(c for c in sites.columns if c.name == "site_id")
        assert site_pk.is_primary_key is True

        readings = next(t for t in discovery.tables if t.name == "readings")
        site_id = next(c for c in readings.columns if c.name == "site_id")
        assert site_id.semantic_type == "foreign_key"
        assert any(
            r.from_table == "readings"
            and r.from_column == "site_id"
            and r.to_table == "sites"
            and r.to_column == "site_id"
            for r in discovery.relationships
        )

    def test_rejected_pk_candidate_is_removed_from_suggestions(self, client):
        before = client.get("/api/tables/readings/pk-fk-suggestions")
        assert before.status_code == 200
        assert any(
            pk["column"] == "reading_date"
            for pk in before.json()["pk_candidates"]
        )

        reject = client.patch(
            "/api/tables/readings/keys",
            json={"reject_pks": ["reading_date"]},
        )
        assert reject.status_code == 200

        after = client.get("/api/tables/readings/pk-fk-suggestions")
        assert after.status_code == 200
        assert all(
            pk["column"] != "reading_date"
            for pk in after.json()["pk_candidates"]
        )

    def test_confirmed_and_rejected_pks_survive_active_discovery_rerun(self, client):
        confirm = client.patch(
            "/api/tables/sites/keys",
            json={"confirm_pks": ["site_id"]},
        )
        assert confirm.status_code == 200

        reject = client.patch(
            "/api/tables/readings/keys",
            json={"reject_pks": ["reading_date"]},
        )
        assert reject.status_code == 200

        store = client.app.state.metadata_store
        discovery = client.app.state.pipeline["discovery"]

        # Simulate a fresh autodetection pass that disagrees with user choices.
        for table in discovery.tables:
            for col in table.columns:
                if table.name == "sites" and col.name == "site_id":
                    col.is_primary_key = False
                    col.semantic_type = None
                if table.name == "readings" and col.name == "reading_date":
                    col.is_primary_key = True
                    col.semantic_type = "primary_key"

        store.apply_key_decisions_to_discovery(discovery)

        sites = next(t for t in discovery.tables if t.name == "sites")
        site_id = next(c for c in sites.columns if c.name == "site_id")
        assert site_id.is_primary_key is True
        assert site_id.semantic_type == "primary_key"

        readings = next(t for t in discovery.tables if t.name == "readings")
        reading_date = next(c for c in readings.columns if c.name == "reading_date")
        assert reading_date.is_primary_key is False
        assert reading_date.semantic_type is None

        suggestions = client.get("/api/tables/readings/pk-fk-suggestions")
        assert suggestions.status_code == 200
        assert all(
            pk["column"] != "reading_date"
            for pk in suggestions.json()["pk_candidates"]
        )
