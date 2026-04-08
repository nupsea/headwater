"""Quality report generation from contract check results."""

from __future__ import annotations

from headwater.core.models import ContractCheckResult, QualityReport


def build_report(results: list[ContractCheckResult]) -> QualityReport:
    """Aggregate contract check results into a quality report."""
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    return QualityReport(
        total_contracts=len(results),
        passed=passed,
        failed=failed,
        skipped=0,
        results=results,
    )
