"""Tests for the Kuzu graph store."""

import pytest

from headwater.core.graph_store import GraphStore


@pytest.fixture
def graph_store(tmp_path):
    store = GraphStore(tmp_path / "test_graph_store")
    store.init_schema()
    yield store
    store.close()


@pytest.fixture
def loaded_graph(graph_store):
    """Graph store loaded with the Riverton sample data structure."""
    tables = [
        {"name": "complaints", "row_count": 3000, "domain": "Environmental"},
        {"name": "zones", "row_count": 25, "domain": "Environmental"},
        {"name": "sites", "row_count": 500, "domain": "Monitoring"},
        {"name": "sensors", "row_count": 832, "domain": "Monitoring"},
        {"name": "readings", "row_count": 49302, "domain": "Monitoring"},
        {"name": "inspections", "row_count": 1243, "domain": "Environmental"},
        {"name": "incidents", "row_count": 5000, "domain": "Environmental"},
        {"name": "programs", "row_count": 10, "domain": "Environmental"},
    ]
    graph_store.load_tables(tables)

    relationships = [
        {
            "from_table": "complaints",
            "from_column": "zone_id",
            "to_table": "zones",
            "to_column": "zone_id",
            "rel_type": "many_to_one",
            "confidence": 0.95,
            "ref_integrity": 1.0,
        },
        {
            "from_table": "complaints",
            "from_column": "related_site_id",
            "to_table": "sites",
            "to_column": "site_id",
            "rel_type": "many_to_one",
            "confidence": 0.80,
            "ref_integrity": 0.28,
        },
        {
            "from_table": "sites",
            "from_column": "zone_id",
            "to_table": "zones",
            "to_column": "zone_id",
            "rel_type": "many_to_one",
            "confidence": 0.95,
            "ref_integrity": 1.0,
        },
        {
            "from_table": "sensors",
            "from_column": "site_id",
            "to_table": "sites",
            "to_column": "site_id",
            "rel_type": "many_to_one",
            "confidence": 0.95,
            "ref_integrity": 1.0,
        },
        {
            "from_table": "readings",
            "from_column": "sensor_id",
            "to_table": "sensors",
            "to_column": "sensor_id",
            "rel_type": "many_to_one",
            "confidence": 0.95,
            "ref_integrity": 1.0,
        },
        {
            "from_table": "inspections",
            "from_column": "site_id",
            "to_table": "sites",
            "to_column": "site_id",
            "rel_type": "many_to_one",
            "confidence": 0.95,
            "ref_integrity": 1.0,
        },
        {
            "from_table": "incidents",
            "from_column": "zone_id",
            "to_table": "zones",
            "to_column": "zone_id",
            "rel_type": "many_to_one",
            "confidence": 0.90,
            "ref_integrity": 0.95,
        },
        {
            "from_table": "incidents",
            "from_column": "reporting_facility_id",
            "to_table": "sites",
            "to_column": "site_id",
            "rel_type": "many_to_one",
            "confidence": 0.85,
            "ref_integrity": 0.90,
        },
    ]
    graph_store.load_relationships(relationships)
    return graph_store


class TestGraphStoreInit:
    def test_init_schema(self, graph_store):
        # Should not raise
        graph_store.init_schema()

    def test_load_tables(self, graph_store):
        count = graph_store.load_tables(
            [
                {"name": "test_table", "row_count": 100},
            ]
        )
        assert count == 1

    def test_load_relationships(self, graph_store):
        graph_store.load_tables(
            [
                {"name": "orders", "row_count": 1000},
                {"name": "customers", "row_count": 50},
            ]
        )
        count = graph_store.load_relationships(
            [
                {
                    "from_table": "orders",
                    "from_column": "customer_id",
                    "to_table": "customers",
                    "to_column": "customer_id",
                    "rel_type": "many_to_one",
                    "confidence": 0.9,
                    "ref_integrity": 1.0,
                },
            ]
        )
        assert count == 1


class TestConformedDimensions:
    def test_zones_is_conformed_dimension(self, loaded_graph):
        conformed = loaded_graph.find_conformed_dimensions(min_connections=3)
        names = [c["name"] for c in conformed]
        assert "zones" in names

    def test_sites_is_conformed_dimension(self, loaded_graph):
        conformed = loaded_graph.find_conformed_dimensions(min_connections=3)
        names = [c["name"] for c in conformed]
        assert "sites" in names


class TestStarSchemas:
    def test_finds_hub_tables(self, loaded_graph):
        stars = loaded_graph.find_star_schemas()
        hubs = [s["hub"] for s in stars]
        # zones and sites should be hubs (3+ incoming FKs)
        assert "zones" in hubs or "sites" in hubs


class TestChains:
    def test_finds_multi_hop_chains(self, loaded_graph):
        chains = loaded_graph.find_chains()
        # readings -> sensors -> sites is a 2-hop chain
        assert len(chains) > 0
        has_2_plus_hops = any(c["hop_count"] >= 2 for c in chains)
        assert has_2_plus_hops


class TestNullableFKWarnings:
    def test_finds_low_integrity_fk(self, loaded_graph):
        warnings = loaded_graph.find_nullable_fk_warnings(threshold=0.5)
        # complaints.related_site_id has 28% integrity
        assert len(warnings) > 0
        from_cols = [w["from_column"] for w in warnings]
        assert "related_site_id" in from_cols


class TestGraphData:
    def test_get_graph_data(self, loaded_graph):
        data = loaded_graph.get_graph_data()
        assert "nodes" in data
        assert "edges" in data
        assert len(data["nodes"]) == 8
        assert len(data["edges"]) == 8

    def test_nodes_have_required_fields(self, loaded_graph):
        data = loaded_graph.get_graph_data()
        for node in data["nodes"]:
            assert "id" in node
            assert "row_count" in node

    def test_edges_have_required_fields(self, loaded_graph):
        data = loaded_graph.get_graph_data()
        for edge in data["edges"]:
            assert "source" in edge
            assert "target" in edge
            assert "from_column" in edge
            assert "to_column" in edge


class TestClear:
    def test_clear_and_reinit(self, loaded_graph):
        loaded_graph.clear()
        data = loaded_graph.get_graph_data()
        assert len(data["nodes"]) == 0
        assert len(data["edges"]) == 0
