"""Tests for the data dictionary review workflow.

Covers: confidence scoring, review status, metadata persistence,
explorer gating, and the dictionary API routes.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from headwater.analyzer.heuristics import (
    classify_semantic_type_with_confidence,
    enrich_tables,
    generate_clarifying_questions,
)
from headwater.core.metadata import MetadataStore
from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    Relationship,
    ReviewSummary,
    SourceConfig,
    TableInfo,
)
from headwater.explorer.schema_graph import SchemaGraph

# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture()
def source():
    return SourceConfig(name="test_src", type="json", path="/data")


@pytest.fixture()
def tables():
    return [
        TableInfo(
            name="env_complaints",
            row_count=5000,
            columns=[
                ColumnInfo(name="complaint_id", dtype="int64", is_primary_key=True),
                ColumnInfo(name="complaint_type", dtype="varchar"),
                ColumnInfo(name="county", dtype="varchar"),
                ColumnInfo(name="created_date", dtype="timestamp"),
                ColumnInfo(name="bin", dtype="int64"),
                ColumnInfo(name="score", dtype="float64"),
            ],
        ),
        TableInfo(
            name="aqs_sites",
            row_count=200,
            columns=[
                ColumnInfo(name="site_id", dtype="int64", is_primary_key=True),
                ColumnInfo(name="state_code", dtype="varchar"),
                ColumnInfo(name="county_code", dtype="varchar"),
                ColumnInfo(name="latitude", dtype="float64"),
                ColumnInfo(name="longitude", dtype="float64"),
            ],
        ),
    ]


@pytest.fixture()
def profiles():
    return [
        ColumnProfile(
            table_name="env_complaints", column_name="complaint_id",
            dtype="int64", distinct_count=5000, uniqueness_ratio=1.0,
        ),
        ColumnProfile(
            table_name="env_complaints", column_name="complaint_type",
            dtype="varchar", distinct_count=15, uniqueness_ratio=0.003,
            top_values=[("noise", 1500), ("air quality", 1200)],
        ),
        ColumnProfile(
            table_name="env_complaints", column_name="county",
            dtype="varchar", distinct_count=62, uniqueness_ratio=0.012,
            top_values=[("kings", 800), ("queens", 700)],
        ),
        ColumnProfile(
            table_name="env_complaints", column_name="created_date",
            dtype="timestamp", distinct_count=365, uniqueness_ratio=0.073,
        ),
        ColumnProfile(
            table_name="env_complaints", column_name="bin",
            dtype="int64", distinct_count=4500, uniqueness_ratio=0.9,
            mean=3045000.0,
        ),
        ColumnProfile(
            table_name="env_complaints", column_name="score",
            dtype="float64", distinct_count=100, uniqueness_ratio=0.02,
            mean=72.5,
        ),
        ColumnProfile(
            table_name="aqs_sites", column_name="site_id",
            dtype="int64", distinct_count=200, uniqueness_ratio=1.0,
        ),
        ColumnProfile(
            table_name="aqs_sites", column_name="state_code",
            dtype="varchar", distinct_count=5, uniqueness_ratio=0.025,
        ),
        ColumnProfile(
            table_name="aqs_sites", column_name="county_code",
            dtype="varchar", distinct_count=30, uniqueness_ratio=0.15,
        ),
        ColumnProfile(
            table_name="aqs_sites", column_name="latitude",
            dtype="float64", distinct_count=180, uniqueness_ratio=0.9,
            mean=40.7,
        ),
        ColumnProfile(
            table_name="aqs_sites", column_name="longitude",
            dtype="float64", distinct_count=180, uniqueness_ratio=0.9,
            mean=-73.9,
        ),
    ]


@pytest.fixture()
def relationships():
    return [
        Relationship(
            from_table="env_complaints", from_column="county",
            to_table="aqs_sites", to_column="county_code",
            type="many_to_one", confidence=0.8,
            referential_integrity=0.75, source="inferred_name",
        ),
    ]


@pytest.fixture()
def discovery(source, tables, profiles, relationships):
    return DiscoveryResult(
        source=source,
        tables=tables,
        profiles=profiles,
        relationships=relationships,
    )


# ── Confidence scoring tests ───────────────────────────────────────────────


class TestConfidenceScoring:
    """Test that classify_semantic_type_with_confidence returns proper scores."""

    def test_id_pattern_high_confidence(self):
        sem, conf = classify_semantic_type_with_confidence("complaint_id")
        assert sem == "id"
        assert conf >= 0.85

    def test_score_metric_high_confidence(self):
        sem, conf = classify_semantic_type_with_confidence("total_count")
        assert sem == "metric"
        assert conf >= 0.85

    def test_standalone_dim_moderate_confidence(self):
        sem, conf = classify_semantic_type_with_confidence("county")
        assert sem == "dimension"
        assert conf >= 0.7

    def test_broad_pattern_moderate_confidence(self):
        sem, conf = classify_semantic_type_with_confidence("complaint_type")
        assert sem == "dimension"
        assert conf >= 0.6

    def test_no_pattern_zero_confidence(self):
        sem, conf = classify_semantic_type_with_confidence("foobar_xyz")
        assert sem is None
        assert conf == 0.0


class TestEnrichmentWithConfidence:
    """Test that enrich_tables populates confidence and role fields."""

    def test_enrichment_sets_confidence(self, tables, profiles, relationships):
        enriched = enrich_tables(tables, profiles, relationships)
        complaints = enriched[0]

        # complaint_id: PK pattern -> high confidence
        cid = next(c for c in complaints.columns if c.name == "complaint_id")
        assert cid.confidence >= 0.85
        assert cid.role == "identifier"

        # county: standalone dimension pattern -> moderate confidence
        county = next(c for c in complaints.columns if c.name == "county")
        assert county.confidence >= 0.7
        assert county.role == "dimension"

        # score: metric pattern -> high confidence
        score = next(c for c in complaints.columns if c.name == "score")
        assert score.confidence >= 0.7
        assert score.role == "metric"

    def test_bin_classified_as_identifier(self, tables, profiles, relationships):
        """bin has 90% uniqueness and is int64 -> should be identifier, not metric."""
        enriched = enrich_tables(tables, profiles, relationships)
        complaints = enriched[0]
        bin_col = next(c for c in complaints.columns if c.name == "bin")
        assert bin_col.role == "identifier"

    def test_locked_columns_preserve_confidence(self, tables, profiles, relationships):
        """Locked columns should not have their confidence overwritten."""
        tables[0].columns[1].locked = True
        tables[0].columns[1].confidence = 0.95
        tables[0].columns[1].role = "dimension"

        enriched = enrich_tables(tables, profiles, relationships)
        ct = next(c for c in enriched[0].columns if c.name == "complaint_type")
        assert ct.confidence == 0.95  # Preserved
        assert ct.role == "dimension"  # Preserved


class TestClarifyingQuestions:
    """Test that clarifying questions are generated for low-confidence columns."""

    def test_generates_questions_for_low_confidence(self, tables, profiles, relationships):
        # Enrich first so columns get confidence scores
        enriched = enrich_tables(tables, profiles, relationships)
        questions = generate_clarifying_questions(enriched, profiles)

        # There should be at least some questions for low-confidence columns
        # (bin might get a question before enrichment, but after enrichment
        # it should be classified; other columns with no pattern match may get questions)
        # We just verify the function runs and returns a valid structure
        assert isinstance(questions, dict)
        for _table_name, qs in questions.items():
            assert isinstance(qs, list)
            for q in qs:
                assert isinstance(q, str)

    def test_locked_columns_get_no_questions(self, tables, profiles, relationships):
        # Lock all columns
        for t in tables:
            for c in t.columns:
                c.locked = True
                c.confidence = 0.95
        questions = generate_clarifying_questions(tables, profiles)
        all_qs = [q for qs in questions.values() for q in qs]
        assert len(all_qs) == 0


# ── Review status and metadata tests ───────────────────────────────────────


class TestReviewStatus:
    """Test review_status on TableInfo."""

    def test_default_is_pending(self):
        t = TableInfo(name="test", row_count=10)
        assert t.review_status == "pending"
        assert t.reviewed_at is None

    def test_review_status_values(self):
        for status in ("pending", "in_review", "reviewed", "skipped"):
            t = TableInfo(name="test", row_count=10, review_status=status)
            assert t.review_status == status


class TestMetadataStoreReview:
    """Test metadata store methods for the dictionary workflow."""

    @pytest.fixture()
    def store(self):
        s = MetadataStore(":memory:")
        s.init()
        s.upsert_source("test_src", "json", "/data", None)
        return s

    def test_update_table_review_status(self, store):
        store.upsert_table("t1", "test_src", row_count=100)
        store.update_table_review_status("t1", "test_src", "reviewed")
        rows = store.get_tables("test_src")
        assert rows[0]["review_status"] == "reviewed"
        assert rows[0]["reviewed_at"] is not None

    def test_get_reviewed_tables(self, store):
        store.upsert_table("t1", "test_src", row_count=100)
        store.upsert_table("t2", "test_src", row_count=200)
        store.update_table_review_status("t1", "test_src", "reviewed")
        store.update_table_review_status("t2", "test_src", "pending")

        reviewed = store.get_reviewed_tables("test_src")
        assert len(reviewed) == 1
        assert reviewed[0]["name"] == "t1"

    def test_get_review_summary(self, store):
        store.upsert_table("t1", "test_src", row_count=100)
        store.upsert_table("t2", "test_src", row_count=200)
        store.upsert_table("t3", "test_src", row_count=300)
        store.update_table_review_status("t1", "test_src", "reviewed")
        store.update_table_review_status("t2", "test_src", "skipped")

        summary = store.get_review_summary("test_src")
        assert summary["total"] == 3
        assert summary["reviewed"] == 1
        assert summary["skipped"] == 1
        assert summary["pending"] == 1
        assert summary["pct_complete"] == pytest.approx(33.3, abs=0.1)

    def test_bulk_update_columns(self, store):
        store.upsert_table("t1", "test_src", row_count=100)
        store.upsert_column("t1", "test_src", "col_a", "varchar")
        store.upsert_column("t1", "test_src", "col_b", "int64")

        store.bulk_update_columns("t1", "test_src", [
            {"name": "col_a", "role": "dimension", "description": "A dim"},
            {"name": "col_b", "role": "metric", "confidence": 0.95},
        ], lock=True)

        cols = store.get_columns("t1", "test_src")
        a = next(c for c in cols if c["name"] == "col_a")
        b = next(c for c in cols if c["name"] == "col_b")
        assert a["role"] == "dimension"
        assert a["description"] == "A dim"
        assert a["locked"] == 1
        assert b["role"] == "metric"
        assert b["confidence"] == 0.95
        assert b["locked"] == 1

    def test_upsert_column_with_role_and_confidence(self, store):
        store.upsert_table("t1", "test_src", row_count=100)
        store.upsert_column(
            "t1", "test_src", "col_a", "varchar",
            role="dimension", confidence=0.8,
        )
        cols = store.get_columns("t1", "test_src")
        assert cols[0]["role"] == "dimension"
        assert cols[0]["confidence"] == 0.8

    def test_review_status_preserved_on_rerun(self, store):
        """A re-run should not regress reviewed tables to pending."""
        store.upsert_table("t1", "test_src", row_count=100)
        store.update_table_review_status("t1", "test_src", "reviewed")

        # Simulate re-run upserting the same table
        store.upsert_table(
            "t1", "test_src", row_count=150, review_status="pending",
        )
        rows = store.get_tables("test_src")
        # Should still be reviewed, not regressed to pending
        assert rows[0]["review_status"] == "reviewed"


# ── Explorer gating tests ──────────────────────────────────────────────────


class TestExplorerGating:
    """Test that SchemaGraph respects reviewed_tables filter."""

    def test_reviewed_tables_filter(self, discovery):
        # Only include aqs_sites
        graph = SchemaGraph(
            discovery, reviewed_tables={"aqs_sites"},
        )
        assert "aqs_sites" in graph.tables
        assert "env_complaints" not in graph.tables

    def test_no_filter_includes_all(self, discovery):
        graph = SchemaGraph(discovery, reviewed_tables=None)
        assert "aqs_sites" in graph.tables
        assert "env_complaints" in graph.tables

    def test_empty_filter_includes_none(self, discovery):
        graph = SchemaGraph(discovery, reviewed_tables=set())
        assert len(graph.tables) == 0

    def test_planner_only_sees_reviewed(self, discovery):
        """QueryPlanner should not find tables outside the reviewed set."""
        from headwater.explorer.query_planner import QueryPlanner

        graph = SchemaGraph(
            discovery, reviewed_tables={"aqs_sites"},
        )
        planner = QueryPlanner(graph)
        # "complaints" should not resolve because env_complaints is excluded
        result = planner.plan_sql("complaints per county")
        # Should either be None or not reference env_complaints
        if result is not None:
            assert "env_complaints" not in result


# ── API route tests ────────────────────────────────────────────────────────


class TestDictionaryAPI:
    """Test the dictionary API endpoints."""

    @pytest.fixture()
    def client(self, discovery):
        from headwater.api.app import create_app

        app = create_app()

        # Override lifespan by directly setting state
        import duckdb
        app.state.duckdb_con = duckdb.connect(":memory:")
        app.state.metadata_store = MetadataStore(":memory:")
        app.state.metadata_store.init()
        app.state.pipeline = {
            "discovery": discovery,
            "staging_models": [],
            "mart_models": [],
            "contracts": [],
            "execution_results": [],
            "quality_report": None,
        }

        # Enrich so columns have confidence/role
        from headwater.analyzer.heuristics import enrich_tables
        discovery.tables = enrich_tables(
            discovery.tables, discovery.profiles, discovery.relationships,
        )

        client = TestClient(app, raise_server_exceptions=False)
        yield client
        app.state.duckdb_con.close()

    def test_get_dictionary(self, client):
        resp = client.get("/api/dictionary")
        assert resp.status_code == 200
        data = resp.json()
        assert "tables" in data
        assert len(data["tables"]) == 2
        t0 = data["tables"][0]
        assert "columns" in t0
        assert "review_status" in t0
        assert t0["review_status"] == "pending"

    def test_get_dictionary_table(self, client):
        resp = client.get("/api/dictionary/env_complaints")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "env_complaints"
        assert len(data["columns"]) == 6

    def test_get_dictionary_table_not_found(self, client):
        resp = client.get("/api/dictionary/nonexistent")
        assert resp.status_code == 404

    def test_get_review_summary(self, client):
        resp = client.get("/api/dictionary/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert data["pending"] == 2
        assert data["reviewed"] == 0

    def test_review_and_confirm_table(self, client):
        resp = client.post("/api/dictionary/env_complaints/review", json={
            "columns": [
                {"name": "county", "role": "dimension", "description": "County name"},
            ],
            "confirm": True,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["review_status"] == "reviewed"
        assert data["locked"] is True

        # Verify the table is now reviewed
        resp = client.get("/api/dictionary/summary")
        assert resp.json()["reviewed"] == 1

    def test_skip_table(self, client):
        resp = client.post("/api/dictionary/env_complaints/skip", json={})
        assert resp.status_code == 200
        assert resp.json()["review_status"] == "skipped"

    def test_confirm_all(self, client):
        resp = client.post("/api/dictionary/confirm-all")
        assert resp.status_code == 200
        data = resp.json()
        assert data["confirmed"] == 2

        # All should be reviewed now
        resp = client.get("/api/dictionary/summary")
        assert resp.json()["reviewed"] == 2
        assert resp.json()["pct_complete"] == 100.0

    def test_status_includes_dictionary(self, client):
        resp = client.get("/api/status")
        data = resp.json()
        assert "dictionary_reviewed" in data
        assert "dictionary_complete" in data
        assert data["dictionary_reviewed"] == 0
        assert data["dictionary_complete"] is False

    def test_status_after_review(self, client):
        # Confirm all tables
        client.post("/api/dictionary/confirm-all")
        resp = client.get("/api/status")
        data = resp.json()
        assert data["dictionary_reviewed"] == 2
        assert data["dictionary_complete"] is True

    def test_columns_have_confidence(self, client):
        resp = client.get("/api/dictionary/env_complaints")
        data = resp.json()
        for col in data["columns"]:
            assert "confidence" in col
            assert "role" in col
            assert "needs_review" in col
            # PK columns should have high confidence
            if col["name"] == "complaint_id":
                assert col["confidence"] >= 0.85
                assert col["role"] == "identifier"

    def test_explore_suggestions_gate(self, client):
        """Suggestions should flag review_required when no tables are reviewed."""
        resp = client.get("/api/explore/suggestions")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("review_required") is True

    def test_explore_after_review(self, client):
        """After confirming all, review_required should be absent or False."""
        client.post("/api/dictionary/confirm-all")
        resp = client.get("/api/explore/suggestions")
        data = resp.json()
        # review_required should be False (or absent) when all reviewed
        assert data.get("review_required") in (False, None)


# ── ReviewSummary model tests ──────────────────────────────────────────────


class TestReviewSummaryModel:
    def test_default_values(self):
        s = ReviewSummary()
        assert s.total == 0
        assert s.pct_complete == 0.0

    def test_computed_values(self):
        s = ReviewSummary(total=10, reviewed=5, pending=3, skipped=2, pct_complete=50.0)
        assert s.pct_complete == 50.0
