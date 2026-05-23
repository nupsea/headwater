"""Architecture boundary tests for service and route layering."""

from __future__ import annotations

from pathlib import Path
import re

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


def test_generic_runtime_modules_do_not_reintroduce_domain_lens_vocabulary():
    targets = [
        "headwater/api/routes/insights.py",
        "headwater/api/routes/explore.py",
        "headwater/explorer/nl_to_sql.py",
        "headwater/explorer/statistical.py",
        "headwater/explorer/suggestions.py",
    ]
    forbidden_terms = [
        "pricing",
        "sales",
        "fare",
        "tip",
        "dispatch",
        "compliance",
    ]

    leaks: list[str] = []
    for relative_path in targets:
        content = _read(relative_path).lower()
        for term in forbidden_terms:
            if re.search(rf"(?<![a-z0-9_]){re.escape(term)}(?![a-z0-9_])", content):
                leaks.append(f"{relative_path}:{term}")

    assert leaks == []
