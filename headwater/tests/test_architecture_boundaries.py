"""Architecture boundary tests — H2 package cleanliness.

Guards that the production package stays free of dataset-specific and
domain-specific terms (domain context belongs in user-provided project
resources under ``data/<domain>/``, never in generic engine logic).
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGE_ROOT = ROOT / "headwater"


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
            # Allow occurrences only inside comments that serve as illustrative
            # examples — flag actual string literals and logic.
            for lineno, line in enumerate(content.splitlines(), 1):
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue
                if re.search(rf"(?<![a-z0-9_]){re.escape(term)}(?![a-z0-9_])", line):
                    leaks.append(f"{path.relative_to(ROOT)}:{lineno}:{term}")

    assert leaks == [], (
        "H2 service modules contain domain-specific terms. "
        "Move domain context to project resource files under data/<domain>/."
    )
