"""Runtime lifecycle transitions for quality contracts."""

from __future__ import annotations

from typing import Any

ACTIVE_CONTRACT_STATUSES = {"observing", "enforced", "failing", "recovered"}


def apply_contract_statuses(contracts: list[Any], results: list[Any]) -> dict:
    """Update in-memory contract statuses from quality check results."""
    by_id = {getattr(contract, "id", None): contract for contract in contracts}
    transitions = {"failing": [], "recovered": [], "observing": []}
    for result in results:
        rule_id = getattr(result, "rule_id", "") or ""
        contract = by_id.get(rule_id)
        if contract is None:
            continue
        current = getattr(contract, "status", "proposed")
        if current == "disabled":
            continue

        next_status = current
        if not bool(getattr(result, "passed", False)):
            next_status = "failing"
        elif current == "failing":
            next_status = "recovered"
        elif current == "proposed":
            next_status = "observing"

        if next_status != current:
            contract.status = next_status
            transitions.setdefault(next_status, []).append(rule_id)
    return transitions
