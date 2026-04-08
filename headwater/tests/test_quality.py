"""Tests for the quality layer: contract checking and reporting."""

from __future__ import annotations

import duckdb

from headwater.core.models import ContractRule
from headwater.quality.checker import check_contract, check_contracts
from headwater.quality.report import build_report

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_con() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with a test table."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE stg_items ("
        "  item_id VARCHAR NOT NULL, "
        "  name VARCHAR, "
        "  price DOUBLE, "
        "  category VARCHAR"
        ")"
    )
    con.execute(
        "INSERT INTO stg_items VALUES "
        "('i1', 'Widget', 10.0, 'hardware'), "
        "('i2', 'Gadget', 25.0, 'electronics'), "
        "('i3', 'Doohickey', 5.0, 'hardware'), "
        "('i4', NULL, 15.0, 'electronics'), "
        "('i5', 'Thingamajig', 100.0, 'toys')"
    )
    return con


# ---------------------------------------------------------------------------
# Not-null check tests
# ---------------------------------------------------------------------------


class TestNotNullCheck:
    def test_passes_for_non_null_column(self):
        con = _make_con()
        rule = ContractRule(
            id="nn1",
            model_name="stg_items",
            column_name="item_id",
            rule_type="not_null",
            expression='"item_id" IS NOT NULL',
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is True
        assert result.observed_value == 0

    def test_fails_for_nullable_column(self):
        con = _make_con()
        rule = ContractRule(
            id="nn2",
            model_name="stg_items",
            column_name="name",
            rule_type="not_null",
            expression='"name" IS NOT NULL',
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is False
        assert result.observed_value == 1


# ---------------------------------------------------------------------------
# Uniqueness check tests
# ---------------------------------------------------------------------------


class TestUniqueCheck:
    def test_passes_for_unique_column(self):
        con = _make_con()
        rule = ContractRule(
            id="uq1",
            model_name="stg_items",
            column_name="item_id",
            rule_type="unique",
            expression='COUNT(*) = COUNT(DISTINCT "item_id")',
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is True

    def test_fails_for_non_unique_column(self):
        con = _make_con()
        # Add a duplicate
        con.execute("INSERT INTO stg_items VALUES ('i1', 'Dup', 1.0, 'other')")
        rule = ContractRule(
            id="uq2",
            model_name="stg_items",
            column_name="item_id",
            rule_type="unique",
            expression='COUNT(*) = COUNT(DISTINCT "item_id")',
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is False
        assert result.observed_value > 0


# ---------------------------------------------------------------------------
# Range check tests
# ---------------------------------------------------------------------------


class TestRangeCheck:
    def test_passes_when_in_range(self):
        con = _make_con()
        rule = ContractRule(
            id="rg1",
            model_name="stg_items",
            column_name="price",
            rule_type="range",
            expression='"price" BETWEEN 0.00 AND 200.00',
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is True

    def test_fails_when_out_of_range(self):
        con = _make_con()
        rule = ContractRule(
            id="rg2",
            model_name="stg_items",
            column_name="price",
            rule_type="range",
            expression='"price" BETWEEN 0.00 AND 20.00',
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is False
        assert result.observed_value == 2  # 25.0 and 100.0 out of range


# ---------------------------------------------------------------------------
# Cardinality check tests
# ---------------------------------------------------------------------------


class TestCardinalityCheck:
    def test_passes_when_all_known(self):
        con = _make_con()
        rule = ContractRule(
            id="cd1",
            model_name="stg_items",
            column_name="category",
            rule_type="cardinality",
            expression="\"category\" IN ('hardware', 'electronics', 'toys')",
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is True

    def test_fails_with_unknown_values(self):
        con = _make_con()
        rule = ContractRule(
            id="cd2",
            model_name="stg_items",
            column_name="category",
            rule_type="cardinality",
            expression="\"category\" IN ('hardware', 'electronics')",
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is False
        assert result.observed_value == 1  # 'toys' not in allowed set


# ---------------------------------------------------------------------------
# Batch checking tests
# ---------------------------------------------------------------------------


class TestBatchChecking:
    def test_only_active_rules(self):
        con = _make_con()
        rules = [
            ContractRule(
                id="a1", model_name="stg_items", column_name="item_id",
                rule_type="not_null", expression='"item_id" IS NOT NULL',
                status="observing",
            ),
            ContractRule(
                id="a2", model_name="stg_items", column_name="name",
                rule_type="not_null", expression='"name" IS NOT NULL',
                status="proposed",  # Should be skipped
            ),
        ]
        results = check_contracts(con, rules, only_active=True)
        assert len(results) == 1
        assert results[0].rule_id == "a1"

    def test_all_rules_when_not_filtered(self):
        con = _make_con()
        rules = [
            ContractRule(
                id="b1", model_name="stg_items", column_name="item_id",
                rule_type="not_null", expression='"item_id" IS NOT NULL',
                status="observing",
            ),
            ContractRule(
                id="b2", model_name="stg_items", column_name="name",
                rule_type="not_null", expression='"name" IS NOT NULL',
                status="proposed",
            ),
        ]
        results = check_contracts(con, rules, only_active=False)
        assert len(results) == 2

    def test_error_handling(self):
        con = _make_con()
        rule = ContractRule(
            id="err1", model_name="nonexistent_table", column_name="x",
            rule_type="not_null", expression='"x" IS NOT NULL',
            status="observing",
        )
        result = check_contract(con, rule)
        assert result.passed is False
        assert "error" in result.message.lower()


# ---------------------------------------------------------------------------
# Report tests
# ---------------------------------------------------------------------------


class TestQualityReport:
    def test_report_aggregation(self):
        con = _make_con()
        rules = [
            ContractRule(
                id="r1", model_name="stg_items", column_name="item_id",
                rule_type="not_null", expression='"item_id" IS NOT NULL',
                status="observing",
            ),
            ContractRule(
                id="r2", model_name="stg_items", column_name="name",
                rule_type="not_null", expression='"name" IS NOT NULL',
                status="observing",
            ),
        ]
        results = check_contracts(con, rules, only_active=True)
        report = build_report(results)
        assert report.total_contracts == 2
        assert report.passed == 1  # item_id passes
        assert report.failed == 1  # name fails
        assert len(report.results) == 2

    def test_empty_report(self):
        report = build_report([])
        assert report.total_contracts == 0
        assert report.passed == 0
        assert report.failed == 0
