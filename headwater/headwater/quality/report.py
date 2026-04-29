"""Quality report generation from contract check results."""

from __future__ import annotations

from headwater.core.models import ContractCheckResult, QualityReport


def build_report(results: list[ContractCheckResult]) -> QualityReport:
    """Aggregate contract check results into a quality report."""
    passed = sum(1 for r in results if r.passed and not r.skipped)
    failed = sum(1 for r in results if not r.passed and not r.skipped)
    skipped = sum(1 for r in results if r.skipped)
    return QualityReport(
        total_contracts=len(results),
        passed=passed,
        failed=failed,
        skipped=skipped,
        results=results,
    )
