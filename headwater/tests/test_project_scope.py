"""Tests for project-to-source scoping helpers."""

from __future__ import annotations

from headwater.api.project_scope import (
    project_for_source,
    project_sources,
    resolve_project,
    scoped_pipeline,
    visible_projects,
)
from headwater.core.models import SourceConfig, TableInfo


class _Store:
    def get_source(self, source_name):
        return next((row for row in self.list_sources() if row["name"] == source_name), None)

    def list_sources(self):
        return [
            {"name": "ny_taxi_postgres", "display_name": "NY Taxi"},
            {"name": "orders_csv", "display_name": "Orders"},
            {"name": "sample", "display_name": "Sample"},
        ]

    def list_projects(self):
        return [
            {
                "id": "ny-taxi",
                "slug": "ny-taxi",
                "display_name": "NY Taxi",
                "sources": [],
            },
            {
                "id": "ny_taxi_postgres",
                "slug": "ny-taxi-postgres",
                "display_name": "ny_taxi_postgres",
                "sources": [],
            },
            {
                "id": "orders",
                "slug": "orders",
                "display_name": "Orders",
                "sources": ["orders_csv"],
            },
        ]

    def list_project_context_items(self, project_id, *, item_type=None, status=None):
        items = [
            {
                "id": "source_alias:ny-taxi:ny_taxi_postgres",
                "project_id": "ny-taxi",
                "source_name": "ny_taxi_postgres",
                "item_type": "source_alias",
                "scope": "source",
                "name": "ny_taxi_postgres",
                "status": "approved",
                "value": {
                    "source_names": ["ny_taxi_postgres"],
                    "aliases": ["NY Taxi warehouse"],
                },
            },
            {
                "id": "table_alias:ny-taxi:yellow",
                "project_id": "ny-taxi",
                "source_name": "ny_taxi_postgres",
                "item_type": "table_alias",
                "scope": "table",
                "name": "yellow trips",
                "table_name": "tlc_raw_yellow_tripdata_2026_01",
                "status": "approved",
                "value": {
                    "table_names": ["tlc_raw_yellow_tripdata_2026_01"],
                    "aliases": ["yellow trips"],
                },
            },
            {
                "id": "table_alias:ny-taxi:green",
                "project_id": "ny-taxi",
                "source_name": "ny_taxi_postgres",
                "item_type": "table_alias",
                "scope": "table",
                "name": "green trips",
                "table_name": "tlc_raw_green_tripdata_2026_02",
                "status": "approved",
                "value": {
                    "table_names": ["tlc_raw_green_tripdata_2026_02"],
                    "aliases": ["green trips"],
                },
            },
        ]
        return [
            item
            for item in items
            if item["project_id"] == project_id
            and (item_type is None or item["item_type"] == item_type)
            and (status is None or item["status"] == status)
        ]


class _ScopedStore(_Store):
    def get_project(self, project_id):
        return {
            "id": project_id,
            "slug": "ny-taxi",
            "display_name": "NY Taxi",
            "description": "NY Taxi for Jan, Feb 2026",
            "sources": [],
        }

    def rebuild_discovery(self, source_name):
        from headwater.core.models import DiscoveryResult

        return DiscoveryResult(
            source=SourceConfig(name=source_name, type="postgres", uri="postgresql://db"),
            tables=[
                TableInfo(name="complaints", columns=[], row_count=10),
                TableInfo(name="tlc_raw_yellow_tripdata_2026_01", columns=[], row_count=10),
                TableInfo(name="tlc_raw_green_tripdata_2026_02", columns=[], row_count=10),
            ],
            profiles=[],
            relationships=[],
        )

    def get_models(self, source_name):
        return []

    def get_execution_results(self):
        return []

    def get_latest_quality_report(self, source_name):
        return None


def test_project_sources_uses_context_source_alias():
    project = {
        "id": "ny-taxi",
        "slug": "ny-taxi",
        "display_name": "NY Taxi",
        "sources": [],
    }

    assert project_sources(project, _Store()) == ["ny_taxi_postgres"]


def test_project_for_source_prefers_real_project_over_shadow_project():
    project = project_for_source(_Store(), "ny_taxi_postgres")

    assert project is not None
    assert project["id"] == "ny-taxi"


def test_visible_projects_hides_source_name_shadow_projects():
    projects = visible_projects(_Store())

    assert [project["id"] for project in projects] == ["ny-taxi", "orders", "sample"]


def test_resolve_project_falls_back_to_source_backed_project():
    project = resolve_project(_Store(), "sample")

    assert project is not None
    assert project["id"] == "sample"
    assert project["sources"] == ["sample"]


def test_scoped_pipeline_filters_mixed_source_tables_for_taxi_project():
    from types import SimpleNamespace

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                metadata_store=_ScopedStore(),
                pipeline={},
            )
        )
    )

    pipeline = scoped_pipeline(request, "ny-taxi")

    assert [table.name for table in pipeline["discovery"].tables] == [
        "tlc_raw_yellow_tripdata_2026_01",
        "tlc_raw_green_tripdata_2026_02",
    ]


class _NoAliasStore(_Store):
    def list_project_context_items(self, project_id, *, item_type=None, status=None):
        return []


def test_project_sources_do_not_use_slug_bias_without_context_alias():
    project = {
        "id": "ny-taxi",
        "slug": "ny-taxi",
        "display_name": "NY Taxi",
        "sources": [],
    }

    assert project_sources(project, _NoAliasStore()) == ["ny-taxi"]


def test_project_scope_contains_no_hard_coded_taxi_aliases():
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "headwater/api/project_scope.py"
    ).read_text(encoding="utf-8").lower()

    assert "yellow" not in source
    assert "green" not in source
    assert "fhv" not in source
    assert "tlc" not in source


class _SourceOnlyScopedStore(_ScopedStore):
    def get_project(self, project_id):
        if project_id == "sample":
            return None
        return super().get_project(project_id)

    def get_source(self, source_name):
        if source_name == "sample":
            return {"name": "sample", "display_name": "Sample"}
        return None


def test_scoped_pipeline_accepts_source_name_without_project_row():
    from types import SimpleNamespace

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                metadata_store=_SourceOnlyScopedStore(),
                pipeline={},
            )
        )
    )

    pipeline = scoped_pipeline(request, "sample")

    assert pipeline["project"]["id"] == "sample"
    assert pipeline["source_names"] == ["sample"]
    assert pipeline["discovery"].source.name == "sample"
