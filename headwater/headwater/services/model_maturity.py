"""Model impact and maturity analysis."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def build_model_impact_report(
    *,
    models: list[Any],
    contracts: list[Any],
    execution_results: list[Any],
    latest_quality: dict | None,
    sources: list[dict],
    table_sources: dict[str, str],
) -> dict:
    """Return a model impact graph and maturity summary from current system state."""
    downstream = _downstream_index(models)
    contracts_by_model = _contracts_by_model(contracts)
    execution_by_model = {r.model_name: r for r in execution_results}
    failed_quality = _failed_quality_by_model(latest_quality)
    source_state = {s["name"]: s for s in sources}

    impacted_models = []
    model_rows = []
    for model in models:
        source_names = sorted(
            {
                table_sources.get(table)
                for table in getattr(model, "source_tables", [])
                if table_sources.get(table)
            }
        )
        source_drift = sum(
            int((source_state.get(source_name) or {}).get("drift_count") or 0)
            for source_name in source_names
        )
        quality_failures = failed_quality.get(model.name, 0)
        executed = bool(
            execution_by_model.get(model.name)
            and getattr(execution_by_model[model.name], "success", False)
        )
        model_contracts = contracts_by_model.get(model.name, [])
        state = _maturity_state(model, executed, model_contracts, source_drift, quality_failures)
        blockers = _blockers(model, executed, model_contracts, source_drift, quality_failures)
        row = {
            "name": model.name,
            "model_type": model.model_type,
            "status": model.status,
            "maturity_state": state,
            "source_tables": model.source_tables,
            "source_names": source_names,
            "depends_on": model.depends_on,
            "downstream_models": downstream.get(model.name, []),
            "contracts": len(model_contracts),
            "quality_failures": quality_failures,
            "source_drift_count": source_drift,
            "blockers": blockers,
        }
        if blockers:
            impacted_models.append(row)
        model_rows.append(row)

    summary = _summary(model_rows, models, contracts, execution_results, latest_quality)
    summary["top_blockers"] = _top_blockers(model_rows)
    summary["impacted_models"] = len(impacted_models)
    return {"summary": summary, "models": model_rows}


def _downstream_index(models: list[Any]) -> dict[str, list[str]]:
    downstream: dict[str, list[str]] = defaultdict(list)
    model_names = {m.name for m in models}
    source_to_model: dict[str, list[str]] = defaultdict(list)
    for model in models:
        for table in getattr(model, "source_tables", []):
            source_to_model[table].append(model.name)
    for model in models:
        for dependency in getattr(model, "depends_on", []):
            if dependency in model_names:
                downstream[dependency].append(model.name)
            for upstream in source_to_model.get(dependency, []):
                if upstream != model.name:
                    downstream[upstream].append(model.name)
    return {name: sorted(set(values)) for name, values in downstream.items()}


def _contracts_by_model(contracts: list[Any]) -> dict[str, list[Any]]:
    by_model: dict[str, list[Any]] = defaultdict(list)
    for contract in contracts:
        name = _normalise_model_name(contract.model_name)
        by_model[name].append(contract)
    return by_model


def _failed_quality_by_model(latest_quality: dict | None) -> dict[str, int]:
    failures: dict[str, int] = defaultdict(int)
    if not latest_quality:
        return failures
    for result in latest_quality.get("results", []) or []:
        if not bool(result.get("passed")):
            failures[_normalise_model_name(result.get("model_name", ""))] += 1
    return failures


def _normalise_model_name(model_name: str) -> str:
    return model_name.split(".")[-1] if "." in model_name else model_name


def _maturity_state(
    model: Any,
    executed: bool,
    contracts: list[Any],
    source_drift: int,
    quality_failures: int,
) -> str:
    if model.status == "rejected":
        return "deprecated"
    if source_drift > 0 or quality_failures > 0:
        return "invalidated"
    if executed and contracts:
        return "monitored"
    if executed:
        return "materialized"
    if model.status == "approved":
        return "approved"
    if model.status == "proposed":
        return "review_pending"
    return "drafted"


def _blockers(
    model: Any,
    executed: bool,
    contracts: list[Any],
    source_drift: int,
    quality_failures: int,
) -> list[str]:
    blockers = []
    if model.model_type == "mart" and model.status == "proposed":
        blockers.append("Needs human review")
    if model.status == "rejected":
        blockers.append("Rejected; revise or regenerate")
    if model.status == "approved" and not executed:
        blockers.append("Approved but not materialized")
    if executed and not contracts:
        blockers.append("Materialized without quality contracts")
    if source_drift > 0:
        blockers.append(f"{source_drift} upstream drift event(s)")
    if quality_failures > 0:
        blockers.append(f"{quality_failures} failing quality check(s)")
    return blockers


def _summary(
    model_rows: list[dict],
    models: list[Any],
    contracts: list[Any],
    execution_results: list[Any],
    latest_quality: dict | None,
) -> dict:
    total = len(models)
    marts = [m for m in models if m.model_type == "mart"]
    reviewed_marts = [m for m in marts if m.status in {"approved", "rejected", "executed"}]
    executed = [r for r in execution_results if getattr(r, "success", False)]
    monitored = [row for row in model_rows if row["maturity_state"] == "monitored"]
    invalidated = [row for row in model_rows if row["maturity_state"] == "invalidated"]

    review_pct = _pct(len(reviewed_marts), len(marts))
    materialized_pct = _pct(len(executed), total)
    monitored_pct = _pct(len(monitored), total)
    quality_score = float(latest_quality.get("score", 100.0)) if latest_quality else 100.0
    contract_coverage_pct = _pct(
        len({row["name"] for row in model_rows if row["contracts"] > 0}),
        total,
    )
    score = round(
        (review_pct * 0.25)
        + (materialized_pct * 0.25)
        + (monitored_pct * 0.2)
        + (contract_coverage_pct * 0.15)
        + (quality_score * 0.15)
        - min(len(invalidated) * 5, 25),
        2,
    )
    return {
        "total_models": total,
        "mart_models": len(marts),
        "reviewed_marts": len(reviewed_marts),
        "materialized_models": len(executed),
        "monitored_models": len(monitored),
        "invalidated_models": len(invalidated),
        "contracts": len(contracts),
        "review_pct": review_pct,
        "materialized_pct": materialized_pct,
        "monitored_pct": monitored_pct,
        "contract_coverage_pct": contract_coverage_pct,
        "quality_score": quality_score,
        "maturity_score": max(0.0, min(score, 100.0)),
    }


def _top_blockers(model_rows: list[dict]) -> list[dict]:
    counts: dict[str, int] = defaultdict(int)
    for row in model_rows:
        for blocker in row["blockers"]:
            counts[blocker] += 1
    return [
        {"title": title, "count": count}
        for title, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 100.0
    return round((numerator / denominator) * 100, 2)
