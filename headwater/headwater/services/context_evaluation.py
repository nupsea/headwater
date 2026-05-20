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
    return {
        "passed": failed == 0,
        "score": round(passed / len(scored), 4) if scored else 1.0,
        "metrics": {
            "total_checks": len(scored),
            "passed_checks": passed,
            "failed_checks": failed,
        },
        "checks": checks,
    }


def evaluate_context_suite(cases: list[dict], *, min_score: float = 1.0) -> dict:
    """Evaluate named context fixtures and aggregate CI-friendly metrics.

    Each case must provide name, bundle, and gold. The suite passes only when
    every fixture reaches min_score.
    """
    fixture_results = []
    for case in cases:
        result = evaluate_context_bundle(case["bundle"], case["gold"])
        fixture_results.append(
            {
                "name": case["name"],
                "passed": result["score"] >= min_score,
                "score": result["score"],
                "metrics": result["metrics"],
                "failures": [
                    check for check in result["checks"] if not check["passed"]
                ],
            }
        )

    total_checks = sum(item["metrics"]["total_checks"] for item in fixture_results)
    passed_checks = sum(item["metrics"]["passed_checks"] for item in fixture_results)
    failed_checks = sum(item["metrics"]["failed_checks"] for item in fixture_results)
    failed_fixtures = [item for item in fixture_results if not item["passed"]]
    return {
        "passed": not failed_fixtures,
        "min_score": min_score,
        "score": round(passed_checks / total_checks, 4) if total_checks else 1.0,
        "metrics": {
            "fixture_count": len(fixture_results),
            "failed_fixture_count": len(failed_fixtures),
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
        },
        "fixtures": fixture_results,
    }


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
        "passed": not leaks,
        "expected": forbidden_terms,
        "actual": leaks,
    }


def _check(name: str, expected: Any, actual: Any) -> dict:
    return {
        "name": name,
        "passed": actual == expected,
        "expected": expected,
        "actual": actual,
    }


def _as_list(value: Any) -> list:
    return value if isinstance(value, list) else [value]


def _normalize_expected(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(value)
    return value


def _fixture_discovery(name: str) -> DiscoveryResult:
    if name == "orders":
        return _orders_fixture_discovery()
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
