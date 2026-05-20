"""Evaluation helpers for project context bootstrap output."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    Relationship,
    SourceConfig,
    TableInfo,
)
from headwater.services.context_bootstrap import bootstrap_project_context


def load_context_gold(path: str | Path) -> dict:
    """Load a YAML/JSON context-evaluation gold file."""
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    data = (
        json.loads(text)
        if source.suffix.lower() == ".json"
        else yaml.safe_load(text) or {}
    )
    return data if isinstance(data, dict) else {}


def load_context_eval_cases(paths: list[str | Path]) -> list[dict]:
    """Load gold files and build deterministic context-evaluation cases."""
    cases = []
    for path in paths:
        source = Path(path)
        gold = load_context_gold(source)
        fixture_name = str(gold.get("fixture") or source.stem)
        discovery = _fixture_discovery(fixture_name)
        cases.append(
            {
                "name": str(gold.get("name") or fixture_name),
                "bundle": bootstrap_project_context(
                    discovery,
                    project_id=str(gold.get("project_id") or discovery.source.name),
                ),
                "gold": gold,
                "gold_path": str(source),
            }
        )
    return cases


def evaluate_context_bundle(bundle: Any, gold: dict) -> dict:
    """Grade context proposals against explicit gold expectations.

    Expected gold keys are optional and may include row_grain, row_entity,
    time_anchor, pk_candidates, fk_candidates, top_dimensions, top_measures,
    fallback_questions, question_substrings, min_fallback_questions, and
    forbidden_terms.
    """
    items = _bundle_items(bundle)
    checks: list[dict] = []
    checks.extend(_table_value_checks(items, gold, "row_grain", "columns"))
    checks.extend(_table_value_checks(items, gold, "row_entity", "entity"))
    checks.extend(_table_value_checks(items, gold, "time_anchor", "column"))
    checks.extend(_pk_checks(items, gold.get("pk_candidates") or {}))
    checks.extend(_fk_checks(items, gold.get("fk_candidates") or []))
    checks.extend(_cold_start_checks(items, gold))
    checks.append(_forbidden_term_check(items, gold.get("forbidden_terms") or []))

    scored = [check for check in checks if check["name"] != "forbidden_terms" or check["expected"]]
    passed = sum(1 for check in scored if check["passed"])
    failed = len(scored) - passed
    category_metrics = _category_metrics(scored)
    return {
        "passed": failed == 0,
        "score": round(passed / len(scored), 4) if scored else 1.0,
        "metrics": {
            "total_checks": len(scored),
            "passed_checks": passed,
            "failed_checks": failed,
        },
        "category_metrics": category_metrics,
        "checks": checks,
    }


def evaluate_context_suite(
    cases: list[dict],
    *,
    min_score: float = 1.0,
    min_category_score: float | None = None,
) -> dict:
    """Evaluate named context fixtures and aggregate CI-friendly metrics.

    Each case must provide name, bundle, and gold. The suite passes only when
    every fixture reaches its configured thresholds.
    """
    fixture_results = []
    for case in cases:
        result = evaluate_context_bundle(case["bundle"], case["gold"])
        thresholds = _fixture_thresholds(
            case["gold"],
            default_min_score=min_score,
            default_min_category_score=min_category_score,
        )
        threshold_failures = _threshold_failures(result, thresholds)
        fixture_results.append(
            {
                "name": case["name"],
                "passed": not threshold_failures,
                "score": result["score"],
                "thresholds": thresholds,
                "metrics": result["metrics"],
                "category_metrics": result["category_metrics"],
                "threshold_failures": threshold_failures,
                "failures": [
                    check for check in result["checks"] if not check["passed"]
                ],
            }
        )

    total_checks = sum(item["metrics"]["total_checks"] for item in fixture_results)
    passed_checks = sum(item["metrics"]["passed_checks"] for item in fixture_results)
    failed_checks = sum(item["metrics"]["failed_checks"] for item in fixture_results)
    failed_fixtures = [item for item in fixture_results if not item["passed"]]
    category_metrics = _aggregate_category_metrics(fixture_results)
    return {
        "passed": not failed_fixtures,
        "min_score": min_score,
        "min_category_score": min_category_score,
        "score": round(passed_checks / total_checks, 4) if total_checks else 1.0,
        "metrics": {
            "fixture_count": len(fixture_results),
            "failed_fixture_count": len(failed_fixtures),
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
        },
        "category_metrics": category_metrics,
        "fixtures": fixture_results,
    }


def build_context_eval_metrics(result: dict) -> dict:
    """Build a compact, stable metrics artifact for regression tracking."""
    return {
        "schema_version": 1,
        "passed": result["passed"],
        "score": result["score"],
        "thresholds": {
            "min_score": result["min_score"],
            "min_category_score": result["min_category_score"],
        },
        "metrics": result["metrics"],
        "categories": _metrics_artifact_categories(result["category_metrics"]),
        "fixtures": [
            {
                "name": fixture["name"],
                "passed": fixture["passed"],
                "score": fixture["score"],
                "thresholds": fixture["thresholds"],
                "metrics": fixture["metrics"],
                "categories": _metrics_artifact_categories(fixture["category_metrics"]),
                "threshold_failures": fixture["threshold_failures"],
                "failure_names": [failure["name"] for failure in fixture["failures"]],
            }
            for fixture in result["fixtures"]
        ],
    }


def _metrics_artifact_categories(category_metrics: dict) -> dict:
    return {
        category: {
            "total_checks": metrics["total_checks"],
            "passed_checks": metrics["passed_checks"],
            "failed_checks": metrics["failed_checks"],
            "exact_match_score": metrics["score"],
        }
        for category, metrics in category_metrics.items()
    }


def _fixture_thresholds(
    gold: dict,
    *,
    default_min_score: float,
    default_min_category_score: float | None,
) -> dict:
    configured = gold.get("thresholds") or {}
    if not isinstance(configured, dict):
        configured = {}
    category_thresholds = (
        configured.get("category_min_scores")
        or configured.get("categories")
        or gold.get("category_min_scores")
        or {}
    )
    if not isinstance(category_thresholds, dict):
        category_thresholds = {}
    return {
        "min_score": float(configured.get("min_score", default_min_score)),
        "min_category_score": (
            float(configured["min_category_score"])
            if "min_category_score" in configured
            else default_min_category_score
        ),
        "category_min_scores": {
            str(category): float(score)
            for category, score in category_thresholds.items()
        },
    }


def _threshold_failures(result: dict, thresholds: dict) -> list[dict]:
    failures = []
    min_score = thresholds["min_score"]
    if result["score"] < min_score:
        failures.append(
            {
                "scope": "fixture",
                "name": "score",
                "expected": min_score,
                "actual": result["score"],
            }
        )
    category_min = thresholds["min_category_score"]
    if category_min is not None:
        for category, metrics in result["category_metrics"].items():
            if metrics["score"] < category_min:
                failures.append(
                    {
                        "scope": "category",
                        "name": category,
                        "expected": category_min,
                        "actual": metrics["score"],
                    }
                )
    for category, min_category_score in thresholds["category_min_scores"].items():
        metrics = result["category_metrics"].get(category)
        actual_score = metrics["score"] if metrics else 0.0
        if actual_score < min_category_score:
            failures.append(
                {
                    "scope": "category",
                    "name": category,
                    "expected": min_category_score,
                    "actual": actual_score,
                }
            )
    return failures


def _bundle_items(bundle: Any) -> list[dict]:
    raw_items = (
        bundle.get("items", [])
        if isinstance(bundle, dict)
        else getattr(bundle, "items", [])
    )
    items: list[dict] = []
    for item in raw_items:
        if isinstance(item, dict):
            items.append(item)
        elif hasattr(item, "model_dump"):
            items.append(item.model_dump(mode="json"))
    return items


def _table_value_checks(
    items: list[dict],
    gold: dict,
    item_type: str,
    value_key: str,
) -> list[dict]:
    expected = gold.get(item_type) or {}
    if not isinstance(expected, dict):
        return []
    actual_by_table = {
        item.get("table_name") or (item.get("value") or {}).get("table"): item.get("value") or {}
        for item in items
        if item.get("item_type") == item_type
    }
    checks = []
    for table_name, expected_value in expected.items():
        actual_value = (actual_by_table.get(table_name) or {}).get(value_key)
        checks.append(
            _check(
                f"{item_type}:{table_name}",
                _normalize_expected(expected_value),
                _normalize_expected(actual_value),
            )
        )
    return checks


def _pk_checks(items: list[dict], expected: dict) -> list[dict]:
    if not isinstance(expected, dict):
        return []
    actual_by_table: dict[str, set[tuple[str, ...]]] = {}
    for item in items:
        if item.get("item_type") != "pk_candidate":
            continue
        value = item.get("value") or {}
        table = item.get("table_name") or value.get("table")
        if not table:
            continue
        actual_by_table.setdefault(table, set()).add(tuple(value.get("columns") or []))
    checks = []
    for table, columns in expected.items():
        expected_columns = tuple(_as_list(columns))
        actual = sorted(actual_by_table.get(table, set()))
        checks.append(
            _check(
                f"pk_candidate:{table}",
                expected_columns,
                expected_columns if expected_columns in actual else None,
            )
        )
    return checks


def _fk_checks(items: list[dict], expected: list[dict]) -> list[dict]:
    checks = []
    actual = {
        (
            value.get("from_table"),
            value.get("from_column"),
            value.get("to_table"),
            value.get("to_column"),
        )
        for item in items
        if item.get("item_type") == "fk_candidate"
        for value in [item.get("value") or {}]
    }
    for relation in expected:
        relation_key = (
            relation.get("from_table"),
            relation.get("from_column"),
            relation.get("to_table"),
            relation.get("to_column"),
        )
        checks.append(
            _check(
                "fk_candidate:"
                f"{relation_key[0]}.{relation_key[1]}->{relation_key[2]}.{relation_key[3]}",
                relation_key,
                relation_key if relation_key in actual else None,
            )
        )
    return checks


def _cold_start_checks(items: list[dict], gold: dict) -> list[dict]:
    cold_start = next(
        (
            item.get("value") or {}
            for item in items
            if item.get("item_type") == "cold_start_summary"
        ),
        {},
    )
    checks: list[dict] = []
    checks.extend(
        _top_column_checks(
            "top_dimensions",
            cold_start.get("top_dimensions") or [],
            gold.get("top_dimensions") or [],
        )
    )
    checks.extend(
        _top_column_checks(
            "top_measures",
            cold_start.get("top_measures") or [],
            gold.get("top_measures") or [],
        )
    )
    min_questions = gold.get("min_fallback_questions")
    if min_questions is not None:
        questions = cold_start.get("fallback_questions") or []
        checks.append(
            {
                "name": "fallback_question_count",
                "category": "fallback_questions",
                "passed": len(questions) >= int(min_questions),
                "expected": int(min_questions),
                "actual": len(questions),
            }
        )
    for expected_question in gold.get("fallback_questions") or []:
        questions = cold_start.get("fallback_questions") or []
        checks.append(
            {
                "name": f"fallback_question:{expected_question}",
                "category": "fallback_questions",
                "passed": expected_question in questions,
                "expected": expected_question,
                "actual": questions,
            }
        )
    for substring in gold.get("question_substrings") or []:
        questions = [
            str(question).lower()
            for question in cold_start.get("fallback_questions") or []
        ]
        expected = str(substring).lower()
        checks.append(
            {
                "name": f"fallback_question_contains:{substring}",
                "category": "fallback_questions",
                "passed": any(expected in question for question in questions),
                "expected": expected,
                "actual": cold_start.get("fallback_questions") or [],
            }
        )
    return checks


def _top_column_checks(
    name: str,
    actual_items: list[dict],
    expected_items: list[dict],
) -> list[dict]:
    actual = [(item.get("table_name"), item.get("column_name")) for item in actual_items]
    checks = []
    for expected in expected_items:
        expected_key = (expected.get("table_name"), expected.get("column_name"))
        checks.append(
            _check(
                f"{name}:{expected_key[0]}.{expected_key[1]}",
                expected_key,
                expected_key if expected_key in actual else None,
            )
        )
    return checks


def _forbidden_term_check(items: list[dict], forbidden_terms: list[str]) -> dict:
    serialized = json.dumps(items, sort_keys=True).lower()
    leaks = [term for term in forbidden_terms if str(term).lower() in serialized]
    return {
        "name": "forbidden_terms",
        "category": "forbidden_terms",
        "passed": not leaks,
        "expected": forbidden_terms,
        "actual": leaks,
    }


def _check(name: str, expected: Any, actual: Any) -> dict:
    return {
        "name": name,
        "category": _check_category(name),
        "passed": actual == expected,
        "expected": expected,
        "actual": actual,
    }


def _category_metrics(checks: list[dict]) -> dict:
    grouped: dict[str, dict] = {}
    for check in checks:
        category = check.get("category") or _check_category(check["name"])
        metrics = grouped.setdefault(
            category,
            {"total_checks": 0, "passed_checks": 0, "failed_checks": 0},
        )
        metrics["total_checks"] += 1
        if check["passed"]:
            metrics["passed_checks"] += 1
        else:
            metrics["failed_checks"] += 1
    for metrics in grouped.values():
        total = metrics["total_checks"]
        metrics["score"] = round(metrics["passed_checks"] / total, 4) if total else 1.0
    return dict(sorted(grouped.items()))


def _aggregate_category_metrics(fixture_results: list[dict]) -> dict:
    grouped: dict[str, dict] = {}
    for fixture in fixture_results:
        for category, source_metrics in fixture["category_metrics"].items():
            metrics = grouped.setdefault(
                category,
                {"total_checks": 0, "passed_checks": 0, "failed_checks": 0},
            )
            metrics["total_checks"] += source_metrics["total_checks"]
            metrics["passed_checks"] += source_metrics["passed_checks"]
            metrics["failed_checks"] += source_metrics["failed_checks"]
    for metrics in grouped.values():
        total = metrics["total_checks"]
        metrics["score"] = round(metrics["passed_checks"] / total, 4) if total else 1.0
    return dict(sorted(grouped.items()))


def _check_category(name: str) -> str:
    if name.startswith("fallback_question"):
        return "fallback_questions"
    if ":" in name:
        return name.split(":", 1)[0]
    return name


def _as_list(value: Any) -> list:
    return value if isinstance(value, list) else [value]


def _normalize_expected(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(value)
    return value


def _fixture_discovery(name: str) -> DiscoveryResult:
    fixtures = {
        "finance_transactions": _finance_transactions_fixture_discovery,
        "manufacturing_runs": _manufacturing_runs_fixture_discovery,
        "operations_events": _operations_events_fixture_discovery,
        "orders": _orders_fixture_discovery,
        "patient_encounters": _patient_encounters_fixture_discovery,
        "random_schema": _random_schema_fixture_discovery,
    }
    if name in fixtures:
        return fixtures[name]()
    raise ValueError(f"Unknown context evaluation fixture: {name}")


def _orders_fixture_discovery() -> DiscoveryResult:
    return DiscoveryResult(
        source=SourceConfig(name="src", type="json", path="/data/orders"),
        tables=[
            TableInfo(
                name="orders",
                row_count=100,
                columns=[
                    ColumnInfo(name="order_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="customer_id", dtype="int64", semantic_type="foreign_key"),
                    ColumnInfo(name="created_at", dtype="timestamp"),
                    ColumnInfo(name="status", dtype="varchar"),
                    ColumnInfo(name="amount", dtype="double"),
                ],
            ),
            TableInfo(
                name="customers",
                row_count=10,
                columns=[
                    ColumnInfo(name="customer_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="segment", dtype="varchar"),
                ],
            ),
        ],
        profiles=[
            ColumnProfile(
                table_name="orders",
                column_name="order_id",
                dtype="int64",
                distinct_count=100,
                uniqueness_ratio=1.0,
            ),
            ColumnProfile(
                table_name="orders",
                column_name="status",
                dtype="varchar",
                distinct_count=3,
                top_values=[("new", 40), ("paid", 35), ("shipped", 25)],
            ),
            ColumnProfile(
                table_name="orders",
                column_name="amount",
                dtype="double",
                distinct_count=90,
                min_value=1.0,
                max_value=500.0,
            ),
            ColumnProfile(
                table_name="customers",
                column_name="customer_id",
                dtype="int64",
                distinct_count=10,
                uniqueness_ratio=1.0,
            ),
        ],
        relationships=[
            Relationship(
                from_table="orders",
                from_column="customer_id",
                to_table="customers",
                to_column="customer_id",
                type="many_to_one",
                confidence=0.92,
                referential_integrity=1.0,
                source="inferred_name",
            )
        ],
    )


def _finance_transactions_fixture_discovery() -> DiscoveryResult:
    return DiscoveryResult(
        source=SourceConfig(name="finance_transactions", type="csv", path="/data/finance"),
        tables=[
            TableInfo(
                name="transactions",
                row_count=1_000,
                columns=[
                    ColumnInfo(name="transaction_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="account_id", dtype="int64", semantic_type="foreign_key"),
                    ColumnInfo(name="posted_at", dtype="timestamp", role="temporal"),
                    ColumnInfo(
                        name="merchant_category",
                        dtype="varchar",
                        semantic_type="dimension",
                    ),
                    ColumnInfo(name="amount_usd", dtype="double", semantic_type="metric"),
                    ColumnInfo(
                        name="transaction_status",
                        dtype="varchar",
                        semantic_type="dimension",
                    ),
                ],
            ),
            TableInfo(
                name="accounts",
                row_count=100,
                columns=[
                    ColumnInfo(name="account_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="account_segment", dtype="varchar", semantic_type="dimension"),
                ],
            ),
        ],
        profiles=[
            _profile("transactions", "transaction_id", "int64", 1_000, uniqueness_ratio=1.0),
            _profile("accounts", "account_id", "int64", 100, uniqueness_ratio=1.0),
            _profile(
                "accounts",
                "account_segment",
                "varchar",
                3,
                top_values=[("consumer", 60), ("business", 30), ("enterprise", 10)],
            ),
            _profile(
                "transactions",
                "merchant_category",
                "varchar",
                12,
                top_values=[("grocery", 220), ("fuel", 150), ("travel", 80)],
            ),
            _profile(
                "transactions",
                "transaction_status",
                "varchar",
                4,
                top_values=[("posted", 850), ("pending", 100), ("reversed", 50)],
            ),
            _profile("transactions", "amount_usd", "double", 940, min_value=1.25, max_value=2500.0),
        ],
        relationships=[
            _relationship("transactions", "account_id", "accounts", "account_id"),
        ],
    )


def _manufacturing_runs_fixture_discovery() -> DiscoveryResult:
    return DiscoveryResult(
        source=SourceConfig(name="manufacturing_runs", type="csv", path="/data/manufacturing"),
        tables=[
            TableInfo(
                name="production_runs",
                row_count=500,
                columns=[
                    ColumnInfo(name="run_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="machine_id", dtype="int64", semantic_type="foreign_key"),
                    ColumnInfo(name="started_at", dtype="timestamp", role="temporal"),
                    ColumnInfo(name="shift_code", dtype="varchar", semantic_type="dimension"),
                    ColumnInfo(name="units_produced", dtype="int64", semantic_type="metric"),
                    ColumnInfo(name="defect_count", dtype="int64", semantic_type="metric"),
                ],
            ),
            TableInfo(
                name="machines",
                row_count=20,
                columns=[
                    ColumnInfo(name="machine_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="plant_code", dtype="varchar", semantic_type="dimension"),
                ],
            ),
        ],
        profiles=[
            _profile("production_runs", "run_id", "int64", 500, uniqueness_ratio=1.0),
            _profile("machines", "machine_id", "int64", 20, uniqueness_ratio=1.0),
            _profile(
                "machines",
                "plant_code",
                "varchar",
                3,
                top_values=[("P1", 8), ("P2", 7), ("P3", 5)],
            ),
            _profile(
                "production_runs",
                "shift_code",
                "varchar",
                3,
                top_values=[("A", 180), ("B", 170), ("C", 150)],
            ),
            _profile(
                "production_runs",
                "units_produced",
                "int64",
                220,
                min_value=50,
                max_value=1200,
            ),
            _profile("production_runs", "defect_count", "int64", 30, min_value=0, max_value=45),
        ],
        relationships=[
            _relationship("production_runs", "machine_id", "machines", "machine_id"),
        ],
    )


def _operations_events_fixture_discovery() -> DiscoveryResult:
    return DiscoveryResult(
        source=SourceConfig(name="operations_events", type="csv", path="/data/operations"),
        tables=[
            TableInfo(
                name="events",
                row_count=2_500,
                columns=[
                    ColumnInfo(name="event_uuid", dtype="varchar", is_primary_key=True),
                    ColumnInfo(name="site_id", dtype="int64", semantic_type="foreign_key"),
                    ColumnInfo(name="event_time", dtype="timestamp", role="temporal"),
                    ColumnInfo(name="event_type", dtype="varchar", semantic_type="dimension"),
                    ColumnInfo(name="severity", dtype="varchar", semantic_type="dimension"),
                    ColumnInfo(name="duration_minutes", dtype="double", semantic_type="metric"),
                ],
            ),
            TableInfo(
                name="sites",
                row_count=30,
                columns=[
                    ColumnInfo(name="site_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="region", dtype="varchar", semantic_type="dimension"),
                ],
            ),
        ],
        profiles=[
            _profile("events", "event_uuid", "varchar", 2_500, uniqueness_ratio=1.0),
            _profile("sites", "site_id", "int64", 30, uniqueness_ratio=1.0),
            _profile(
                "events",
                "event_type",
                "varchar",
                8,
                top_values=[("alarm", 900), ("inspection", 700), ("maintenance", 500)],
            ),
            _profile(
                "events",
                "severity",
                "varchar",
                4,
                top_values=[("low", 1200), ("medium", 900), ("high", 400)],
            ),
            _profile(
                "sites",
                "region",
                "varchar",
                4,
                top_values=[("north", 10), ("south", 8), ("west", 7)],
            ),
            _profile("events", "duration_minutes", "double", 300, min_value=0.5, max_value=480.0),
        ],
        relationships=[
            _relationship("events", "site_id", "sites", "site_id"),
        ],
    )


def _patient_encounters_fixture_discovery() -> DiscoveryResult:
    return DiscoveryResult(
        source=SourceConfig(name="patient_encounters", type="csv", path="/data/health"),
        tables=[
            TableInfo(
                name="encounters",
                row_count=1_200,
                columns=[
                    ColumnInfo(name="encounter_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="patient_id", dtype="int64", semantic_type="foreign_key"),
                    ColumnInfo(name="admitted_at", dtype="timestamp", role="temporal"),
                    ColumnInfo(name="department", dtype="varchar", semantic_type="dimension"),
                    ColumnInfo(
                        name="discharge_disposition",
                        dtype="varchar",
                        semantic_type="dimension",
                    ),
                    ColumnInfo(name="length_of_stay_days", dtype="double", semantic_type="metric"),
                ],
            ),
            TableInfo(
                name="patients",
                row_count=700,
                columns=[
                    ColumnInfo(name="patient_id", dtype="int64", is_primary_key=True),
                    ColumnInfo(name="age_band", dtype="varchar", semantic_type="dimension"),
                ],
            ),
        ],
        profiles=[
            _profile("encounters", "encounter_id", "int64", 1_200, uniqueness_ratio=1.0),
            _profile("patients", "patient_id", "int64", 700, uniqueness_ratio=1.0),
            _profile(
                "encounters",
                "department",
                "varchar",
                9,
                top_values=[("emergency", 400), ("cardiology", 180), ("orthopedics", 150)],
            ),
            _profile(
                "encounters",
                "discharge_disposition",
                "varchar",
                5,
                top_values=[("home", 800), ("transfer", 210), ("rehab", 120)],
            ),
            _profile(
                "patients",
                "age_band",
                "varchar",
                6,
                top_values=[("65-74", 220), ("55-64", 180), ("75-84", 140)],
            ),
            _profile(
                "encounters",
                "length_of_stay_days",
                "double",
                40,
                min_value=0.0,
                max_value=35.0,
            ),
        ],
        relationships=[
            _relationship("encounters", "patient_id", "patients", "patient_id"),
        ],
    )


def _random_schema_fixture_discovery() -> DiscoveryResult:
    return DiscoveryResult(
        source=SourceConfig(name="random_schema", type="csv", path="/data/random"),
        tables=[
            TableInfo(
                name="wide_records",
                row_count=300,
                columns=[
                    ColumnInfo(name="record_key", dtype="varchar"),
                    ColumnInfo(name="batch_code", dtype="varchar", semantic_type="dimension"),
                    ColumnInfo(name="event_ts", dtype="timestamp", role="temporal"),
                    ColumnInfo(name="value_num", dtype="double", semantic_type="metric"),
                ],
            )
        ],
        profiles=[
            _profile("wide_records", "record_key", "varchar", 300, uniqueness_ratio=1.0),
            _profile(
                "wide_records",
                "batch_code",
                "varchar",
                5,
                top_values=[("B1", 90), ("B2", 80), ("B3", 70)],
            ),
            _profile("wide_records", "value_num", "double", 250, min_value=-10.0, max_value=99.5),
        ],
    )


def _profile(
    table_name: str,
    column_name: str,
    dtype: str,
    distinct_count: int,
    *,
    uniqueness_ratio: float = 0.0,
    min_value: float | None = None,
    max_value: float | None = None,
    top_values: list[tuple[str, int]] | None = None,
) -> ColumnProfile:
    return ColumnProfile(
        table_name=table_name,
        column_name=column_name,
        dtype=dtype,
        distinct_count=distinct_count,
        uniqueness_ratio=uniqueness_ratio,
        min_value=min_value,
        max_value=max_value,
        top_values=top_values,
    )


def _relationship(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
) -> Relationship:
    return Relationship(
        from_table=from_table,
        from_column=from_column,
        to_table=to_table,
        to_column=to_column,
        type="many_to_one",
        confidence=0.92,
        referential_integrity=1.0,
        source="inferred_name",
    )
