"""Architecture boundary tests for service and route layering."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text()


def test_pipeline_route_does_not_import_private_discovery_helpers():
    content = _read("headwater/api/routes/pipeline.py")
    assert "from headwater.api.routes.discovery import (" not in content
    assert "_persist_discovery_data" not in content
    assert "_persist_catalog_data" not in content
    assert "_persist_semantic_data" not in content
    assert "_build_graph_and_index" not in content


def test_source_sync_does_not_import_private_route_pipeline_runner():
    content = _read("headwater/services/source_sync.py")
    assert "from headwater.api.routes.pipeline import _run_pipeline_inner" not in content


def test_service_layer_owns_pipeline_orchestration():
    content = _read("headwater/services/pipeline_runner.py")
    assert "def run_pipeline(" in content
