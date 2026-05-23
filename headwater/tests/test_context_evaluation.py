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
from headwater.services.context_drift import reconcile_project_context_drift
from headwater.services.context_evaluation import (
    build_context_eval_metrics,
    context_gold_coverage,
    evaluate_context_bundle,
    evaluate_context_suite,
    load_context_eval_cases,
    load_context_gold,
)

GOLD = Path(__file__).resolve().parent / "golden" / "context_bootstrap_orders.yaml"
GOLD_DIR = Path(__file__).resolve().parent / "golden"


def test_context_bundle_evaluation_passes_gold_fixture():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")
    gold = load_context_gold(GOLD)

    result = evaluate_context_bundle(bundle, gold)

    assert result["passed"] is True
    assert result["score"] == 1.0
    assert result["metrics"]["failed_checks"] == 0
    assert result["metrics"]["total_checks"] >= 9
    assert result["category_metrics"]["row_grain"]["score"] == 1.0
    assert result["category_metrics"]["fallback_questions"]["total_checks"] >= 4
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


def test_forbidden_terms_use_word_boundaries():
    result = evaluate_context_bundle(
        {
            "project_id": "src",
            "source_names": ["src"],
            "dataset_contexts": [],
            "items": [
                {
                    "id": "open_question:1",
                    "project_id": "src",
                    "item_type": "open_question",
                    "scope": "project",
                    "name": "follow_up",
                    "value": {"question": "Should we review order_status next?"},
                }
            ],
            "resources": [],
        },
        {"forbidden_terms": ["order"]},
    )

    check = next(item for item in result["checks"] if item["name"] == "forbidden_terms")
    assert check["passed"] is True
    assert check["actual"] == []


def test_context_evaluation_suite_aggregates_fixture_metrics():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")
    gold = load_context_gold(GOLD)

    result = evaluate_context_suite(
        [{"name": "orders", "bundle": bundle, "gold": gold}],
        min_score=1.0,
    )

    assert result["passed"] is True
    assert result["score"] == 1.0
    assert result["metrics"]["fixture_count"] == 1
    assert result["metrics"]["failed_fixture_count"] == 0
    assert result["category_metrics"]["pk_candidate"]["passed_checks"] == 1
    assert result["fixtures"][0]["category_metrics"]["top_measures"]["score"] == 1.0
    assert result["fixtures"][0]["name"] == "orders"
    assert result["fixtures"][0]["failures"] == []


def test_context_evaluation_suite_fails_below_threshold():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")

    result = evaluate_context_suite(
        [
            {
                "name": "orders",
                "bundle": bundle,
                "gold": {
                    "row_grain": {"orders": ["not_the_key"]},
                    "time_anchor": {"orders": "created_at"},
                },
            }
        ],
        min_score=1.0,
    )

    assert result["passed"] is False
    assert result["metrics"]["fixture_count"] == 1
    assert result["metrics"]["failed_fixture_count"] == 1
    assert result["metrics"]["failed_checks"] == 1
    assert result["fixtures"][0]["score"] == 0.5
    assert result["fixtures"][0]["failures"][0]["name"] == "row_grain:orders"


def test_context_evaluation_accepts_documented_delta_without_hiding_it():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")
    result = evaluate_context_suite(
        [
            {
                "name": "orders",
                "bundle": bundle,
                "gold": {
                    "row_grain": {"orders": ["not_the_key"]},
                    "time_anchor": {"orders": "created_at"},
                    "accepted_deltas": [
                        {
                            "check": "row_grain:orders",
                            "reason": "temporary migration parity gap",
                            "until": "metadata migration complete",
                        }
                    ],
                },
            }
        ],
        min_score=1.0,
    )

    assert result["passed"] is True
    assert result["metrics"]["accepted_delta_checks"] == 1
    assert result["category_metrics"]["row_grain"]["accepted_delta_checks"] == 1
    assert result["fixtures"][0]["failures"] == []
    assert result["fixtures"][0]["accepted_deltas"][0]["raw_passed"] is False


def test_context_evaluation_suite_fails_configured_category_threshold():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")

    result = evaluate_context_suite(
        [
            {
                "name": "orders",
                "bundle": bundle,
                "gold": {
                    "row_grain": {"orders": ["not_the_key"]},
                    "time_anchor": {"orders": "created_at"},
                    "thresholds": {
                        "min_score": 0.0,
                        "category_min_scores": {"row_grain": 1.0},
                    },
                },
            }
        ],
        min_score=1.0,
    )

    assert result["passed"] is False
    assert result["fixtures"][0]["score"] == 0.5
    assert result["fixtures"][0]["thresholds"]["min_score"] == 0.0
    assert result["fixtures"][0]["threshold_failures"] == [
        {
            "scope": "category",
            "name": "row_grain",
            "expected": 1.0,
            "actual": 0.0,
        }
    ]


def test_context_bundle_evaluation_supports_question_intents():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")
    gold = {
        "question_intents": [
            {
                "name": "row_grain_question",
                "all_of": ["row", "orders"],
            },
            {
                "name": "analysis_question",
                "any_of": ["measures", "dimensions"],
            },
        ]
    }

    result = evaluate_context_bundle(bundle, gold)

    assert result["passed"] is True
    check_names = {check["name"] for check in result["checks"]}
    assert "fallback_question_intent:row_grain_question" in check_names
    assert "fallback_question_intent:analysis_question" in check_names


def test_context_bundle_evaluation_reports_question_intent_failure():
    bundle = bootstrap_project_context(_orders_discovery(), project_id="src")
    gold = {
        "question_intents": [
            {
                "name": "missing_business_label",
                "all_of": ["profit"],
            }
        ]
    }

    result = evaluate_context_bundle(bundle, gold)

    assert result["passed"] is False
    failures = {check["name"]: check for check in result["checks"] if not check["passed"]}
    assert failures["fallback_question_intent:missing_business_label"]["expected"] == {
        "all_of": ["profit"],
        "any_of": [],
    }


def test_load_context_eval_cases_builds_named_fixture_from_gold():
    cases = load_context_eval_cases([GOLD])

    assert len(cases) == 1
    assert cases[0]["name"] == "orders"
    assert cases[0]["gold"]["fixture"] == "orders"
    assert cases[0]["gold_coverage"]["complete"] is True
    assert cases[0]["gold_path"].endswith("context_bootstrap_orders.yaml")

    result = evaluate_context_suite(cases, min_score=1.0)
    assert result["passed"] is True


def test_context_gold_coverage_reports_missing_categories():
    coverage = context_gold_coverage(
        {
            "row_grain": {"orders": ["order_id"]},
            "min_fallback_questions": 3,
        }
    )

    assert coverage["complete"] is False
    assert "row_grain" in coverage["present"]
    assert "question_gold" in coverage["present"]
    assert "row_entity" in coverage["missing"]
    assert "fk_candidates" in coverage["missing"]


def test_cross_domain_fixture_inputs_are_data_driven():
    cross_domain_gold = [
        path
        for path in sorted(GOLD_DIR.glob("context_bootstrap_*.yaml"))
        if path != GOLD
    ]

    cases = load_context_eval_cases(cross_domain_gold)
    service_source = (
        Path(__file__).resolve().parent.parent
        / "headwater"
        / "services"
        / "context_evaluation.py"
    ).read_text(encoding="utf-8")

    assert len(cases) == 7
    for case in cases:
        assert case["gold"]["discovery_path"].startswith("context_fixtures/")
        assert case["name"] not in service_source


def test_all_context_eval_gold_fixtures_pass():
    cases = load_context_eval_cases(sorted(GOLD_DIR.glob("context_bootstrap_*.yaml")))

    result = evaluate_context_suite(cases, min_score=1.0)

    assert result["passed"] is True
    assert result["gold_coverage"]["complete"] is True
    assert result["metrics"]["fixture_count"] == 8
    assert result["category_metrics"]["semantic_role"]["passed_checks"] == 6
    assert result["category_metrics"]["insight_family"]["passed_checks"] == 5
    assert result["category_metrics"]["absent_category"]["passed_checks"] == 2
    assert result["category_metrics"]["row_grain"]["failed_checks"] == 0
    assert result["category_metrics"]["top_dimensions"]["passed_checks"] >= 10


def test_nytaxi_fixture_scores_project_metadata_roles_and_families():
    cases = load_context_eval_cases([GOLD_DIR / "context_bootstrap_nytaxi.yaml"])

    result = evaluate_context_suite(cases, min_score=1.0)

    assert result["passed"] is True
    assert result["fixtures"][0]["gold_coverage"]["present"][-2:] == [
        "semantic_roles",
        "insight_families",
    ]
    assert result["category_metrics"]["semantic_role"]["passed_checks"] == 6
    assert result["category_metrics"]["insight_family"]["passed_checks"] == 5


def test_nytaxi_without_metadata_degrades_to_structural_checks():
    cases = load_context_eval_cases([GOLD_DIR / "context_bootstrap_nytaxi_structural.yaml"])

    result = evaluate_context_suite(cases, min_score=1.0)

    assert result["passed"] is True
    assert "semantic_role" not in result["category_metrics"]
    assert "insight_family" not in result["category_metrics"]
    assert result["category_metrics"]["absent_category"]["passed_checks"] == 2
    assert result["fixtures"][0]["gold_coverage"]["complete"] is True


def test_reviewed_context_drift_is_domain_neutral_across_fixtures(meta):
    fixture_paths = [
        GOLD_DIR / "context_bootstrap_finance_transactions.yaml",
        GOLD_DIR / "context_bootstrap_manufacturing_runs.yaml",
        GOLD_DIR / "context_bootstrap_nytaxi.yaml",
        GOLD_DIR / "context_bootstrap_patient_encounters.yaml",
    ]
    cases = load_context_eval_cases(fixture_paths)

    for case in cases:
        discovery = case["discovery"]
        meta.upsert_source(
            discovery.source.name,
            discovery.source.type,
            discovery.source.path,
            None,
        )
        item = next(
            bundle_item
            for bundle_item in case["bundle"].items
            if bundle_item.item_type == "column_semantics"
        )
        reviewed = item.model_dump(mode="json")
        reviewed["status"] = "approved"
        reviewed["source"] = "user"
        reviewed["confidence"] = 0.95
        meta.replace_project_context(
            reviewed["project_id"],
            source_name=discovery.source.name,
            items=[reviewed],
        )

        drifted_discovery = _discovery_without_column(
            discovery,
            item.table_name,
            item.column_name,
        )
        summary = reconcile_project_context_drift(
            meta,
            drifted_discovery,
            project_id=reviewed["project_id"],
            source_name=discovery.source.name,
            drift_report={"id": 42},
        )
        updated = meta.get_project_context_item(
            item.id,
            project_id=reviewed["project_id"],
        )

        assert summary["items_flagged"] == 1
        assert summary["items"][0]["code"] == "column_missing"
        assert summary["drift_type_counts"] == {"schema": 1}
        assert summary["severity_counts"] == {"critical": 1}
        assert updated["status"] == "needs_review"
        assert updated["source"] == "context_drift"
        assert updated["confidence"] == 0.25
        assert updated["value"]["drift_status"] == "invalidated"
        assert updated["value"]["drift_report_id"] == 42
        assert updated["evidence"][-1]["payload"]["code"] == "column_missing"


def test_context_evaluation_metrics_artifact_tracks_category_scores():
    cases = load_context_eval_cases([GOLD])
    result = evaluate_context_suite(cases, min_score=1.0, min_category_score=0.9)

    metrics = build_context_eval_metrics(result)

    assert metrics["schema_version"] == 1
    assert metrics["thresholds"] == {"min_score": 1.0, "min_category_score": 0.9}
    assert metrics["gold_coverage"]["complete"] is True
    assert metrics["metrics"]["accepted_delta_checks"] == 0
    assert metrics["categories"]["row_grain"]["exact_match_score"] == 1.0
    assert metrics["categories"]["row_grain"]["accepted_delta_checks"] == 0
    assert metrics["fixtures"][0]["name"] == "orders"
    assert metrics["fixtures"][0]["gold_coverage"]["complete"] is True
    assert metrics["fixtures"][0]["categories"]["top_measures"]["failed_checks"] == 0
    assert metrics["fixtures"][0]["failure_names"] == []
    assert metrics["fixtures"][0]["accepted_delta_names"] == []


def _discovery_without_column(
    discovery: DiscoveryResult,
    table_name: str,
    column_name: str,
) -> DiscoveryResult:
    tables = []
    for table in discovery.tables:
        if table.name != table_name:
            tables.append(table)
            continue
        tables.append(
            table.model_copy(
                update={
                    "columns": [
                        column for column in table.columns if column.name != column_name
                    ]
                }
            )
        )
    return discovery.model_copy(update={"tables": tables})


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
