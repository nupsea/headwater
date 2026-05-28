"""Architecture boundary tests for service and route layering."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGE_ROOT = ROOT / "headwater"


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


def test_production_package_does_not_contain_fixture_specific_dataset_terms():
    forbidden_terms = [
        "radiology",
        "movielens",
        "nytaxi",
        "taxi",
        "patient_type h",
        "ny taxi",
        "tlc",
    ]

    leaks: list[str] = []
    for path in PACKAGE_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        content = path.read_text().lower()
        for term in forbidden_terms:
            if re.search(rf"(?<![a-z0-9_]){re.escape(term)}(?![a-z0-9_])", content):
                leaks.append(f"{path.relative_to(ROOT)}:{term}")

    assert leaks == []


def test_h2_service_modules_do_not_contain_domain_specific_terms():
    """H2 service modules must never reference specific domain entities.

    Domain context belongs in user-provided project resources (data/<domain>/) —
    not hardcoded in generic engine logic.
    """
    h2_service_paths = list((PACKAGE_ROOT / "services").glob("h2_*.py"))
    forbidden_terms = [
        "patient",
        "hospital",
        "radiology",
        "modality",
        "inspection",
        "movielens",
        "movie",
        "rating",
        "pickup",
        "dropoff",
        "taxi",
        "fare",
    ]
    leaks: list[str] = []
    for path in h2_service_paths:
        content = path.read_text().lower()
        for term in forbidden_terms:
            # Allow occurrences only inside comments or docstrings that serve as
            # illustrative examples — flag actual string literals and logic
            for lineno, line in enumerate(content.splitlines(), 1):
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue  # comment lines are fine
                if re.search(rf"(?<![a-z0-9_]){re.escape(term)}(?![a-z0-9_])", line):
                    leaks.append(f"{path.relative_to(ROOT)}:{lineno}:{term}")

    assert leaks == [], (
        "H2 service modules contain domain-specific terms. "
        "Move domain context to project resource files under data/<domain>/."
    )
