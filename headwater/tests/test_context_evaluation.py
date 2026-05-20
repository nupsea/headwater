"""Tests for context bootstrap evaluation against gold expectations."""

from __future__ import annotations

from pathlib import Path

from headwater.core.models import (
    ColumnInfo,
    ColumnProfile,
    DiscoveryResult,
    Relationship,
    SourceConfig,
    TableInfo,
)
from headwater.services.context_bootstrap import bootstrap_project_context
from headwater.services.context_evaluation import (
    evaluate_context_bundle,
    load_context_gold,
)

GOLD = Path(__file__).resolve().parent / "golden" / "context_bootstrap_orders.yaml"


def test_context_bundle_evaluation_passes_gold_fixture():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")
    gold = load_context_gold(GOLD)

    result = evaluate_context_bundle(bundle, gold)

    assert result["passed"] is True
    assert result["score"] == 1.0
    assert result["metrics"]["failed_checks"] == 0
    assert result["metrics"]["total_checks"] >= 9
    check_names = {check["name"] for check in result["checks"]}
    assert "row_grain:orders" in check_names
    assert "pk_candidate:orders" in check_names
    assert "top_measures:orders.amount" in check_names
    assert "forbidden_terms" in check_names


def test_context_bundle_evaluation_reports_failed_gold_expectation():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")
    gold = {
        "row_grain": {"orders": ["not_the_key"]},
        "forbidden_terms": ["orders"],
    }

    result = evaluate_context_bundle(bundle, gold)

    assert result["passed"] is False
    assert result["metrics"]["failed_checks"] == 2
    failures = {check["name"]: check for check in result["checks"] if not check["passed"]}
    assert failures["row_grain:orders"]["actual"] == ("order_id",)
    assert failures["forbidden_terms"]["actual"] == ["orders"]


def _orders_discovery() -> DiscoveryResult:
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
