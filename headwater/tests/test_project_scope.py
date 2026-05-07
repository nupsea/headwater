"""Tests for project-to-source scoping helpers."""

from __future__ import annotations

from headwater.api.project_scope import (
    project_for_source,
    project_sources,
    scoped_pipeline,
    visible_projects,
)
from headwater.core.models import SourceConfig, TableInfo


class _Store:
    def list_sources(self):
        return [
            {"name": "ny_taxi_postgres", "display_name": "NY Taxi"},
            {"name": "orders_csv", "display_name": "Orders"},
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


def test_project_sources_infers_underscore_source_from_dash_project_slug():
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

    assert [project["id"] for project in projects] == ["ny-taxi", "orders"]


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
