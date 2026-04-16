"""Tests for the LanceDB vector store."""

import pytest

from headwater.core.vector_store import VectorStore, encode_text, encode_texts


@pytest.fixture
def vector_store(tmp_path):
    store = VectorStore(tmp_path / "test_vector_store")
    yield store
    store.close()


class TestEncoding:
    def test_encode_text_returns_384_dim(self):
        vec = encode_text("hello world")
        assert len(vec) == 384
        assert all(isinstance(v, float) for v in vec)

    def test_encode_texts_batch(self):
        vecs = encode_texts(["hello", "world", "test"])
        assert len(vecs) == 3
        assert all(len(v) == 384 for v in vecs)

    def test_encode_texts_empty(self):
        assert encode_texts([]) == []

    def test_similar_texts_closer_than_dissimilar(self):
        import numpy as np

        v1 = np.array(encode_text("environmental health complaints"))
        v2 = np.array(encode_text("health complaints from residents"))
        v3 = np.array(encode_text("quantum physics equations"))

        sim_12 = np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))
        sim_13 = np.dot(v1, v3) / (np.linalg.norm(v1) * np.linalg.norm(v3))
        assert sim_12 > sim_13


class TestVectorStore:
    def test_index_and_search(self, vector_store):
        entries = [
            {
                "id": "dim_zone_geography",
                "entry_type": "dimension",
                "name": "zone_geography",
                "display_name": "Zone / Geographic Area",
                "text": (
                    "Zone Geography. Administrative zones for monitoring. "
                    "Synonyms: county, borough, district, zone, area, neighborhood. "
                    "Values: Downtown Core, Industrial Park, Riverside"
                ),
            },
            {
                "id": "metric_complaint_count",
                "entry_type": "metric",
                "name": "complaint_count",
                "display_name": "Total Complaints",
                "text": (
                    "Complaint Count. Total number of complaint records. "
                    "Expression: COUNT(*). Table: complaints."
                ),
            },
        ]
        count = vector_store.index_entries("proj1", entries)
        assert count == 2

        # Search for "county" should match zone_geography
        results = vector_store.search("county", project_id="proj1")
        assert len(results) > 0
        assert results[0]["name"] == "zone_geography"

    def test_search_with_type_filter(self, vector_store):
        entries = [
            {
                "id": "dim_zone",
                "entry_type": "dimension",
                "name": "zone_geography",
                "display_name": "Zone",
                "text": "Zone Geography. Synonyms: county, district.",
            },
            {
                "id": "met_count",
                "entry_type": "metric",
                "name": "complaint_count",
                "display_name": "Count",
                "text": "Complaint Count. COUNT(*) from complaints.",
            },
        ]
        vector_store.index_entries("proj1", entries)

        results = vector_store.search("zone", project_id="proj1", entry_type="dimension")
        assert all(r["entry_type"] == "dimension" for r in results)

    def test_clear_project(self, vector_store):
        entries = [
            {
                "id": "dim1",
                "entry_type": "dimension",
                "name": "test",
                "display_name": "Test",
                "text": "Test dimension for clearing.",
            },
        ]
        vector_store.index_entries("proj_to_clear", entries)
        results = vector_store.search("test", project_id="proj_to_clear")
        assert len(results) > 0

        vector_store.clear_project("proj_to_clear")
        results = vector_store.search("test", project_id="proj_to_clear")
        assert len(results) == 0

    def test_search_empty_store(self, vector_store):
        results = vector_store.search("anything")
        assert results == []

    def test_reindex_replaces_entries(self, vector_store):
        entries_v1 = [
            {
                "id": "dim1",
                "entry_type": "dimension",
                "name": "old_dim",
                "display_name": "Old",
                "text": "Old dimension that should be replaced.",
            },
        ]
        vector_store.index_entries("proj1", entries_v1)

        entries_v2 = [
            {
                "id": "dim2",
                "entry_type": "dimension",
                "name": "new_dim",
                "display_name": "New",
                "text": "New dimension after re-index.",
            },
        ]
        vector_store.index_entries("proj1", entries_v2)

        results = vector_store.search("dimension", project_id="proj1")
        names = [r["name"] for r in results]
        assert "new_dim" in names
        assert "old_dim" not in names
