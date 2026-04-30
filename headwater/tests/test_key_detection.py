"""Tests for PK/FK auto-detection module."""

from __future__ import annotations

from headwater.core.models import ColumnInfo
from headwater.profiler.key_detection import (
    detect_composite_keys,
    suggest_foreign_keys,
    suggest_primary_keys,
)


def _profile(col_name: str, dtype: str, row_count: int, distinct: int, nulls: int) -> dict:
    """Helper to build a column profile dict."""
    return {
        "column_name": col_name,
        "dtype": dtype,
        "stats": {
            "row_count": row_count,
            "distinct_count": distinct,
            "null_count": nulls,
            "min": 1,
            "max": distinct,
        },
    }


class TestSuggestPrimaryKeys:
    def test_unique_id_column(self):
        profiles = [_profile("zone_id", "INTEGER", 100, 100, 0)]
        result = suggest_primary_keys("zones", profiles)
        assert len(result) == 1
        assert result[0].column == "zone_id"
        assert result[0].confidence >= 0.8
        assert "100% unique" in result[0].reasons[0]

    def test_column_named_id(self):
        profiles = [_profile("id", "INTEGER", 500, 500, 0)]
        result = suggest_primary_keys("users", profiles)
        assert len(result) == 1
        assert result[0].column == "id"
        assert result[0].confidence >= 0.9

    def test_low_uniqueness_excluded(self):
        profiles = [_profile("status", "TEXT", 1000, 5, 0)]
        result = suggest_primary_keys("orders", profiles)
        assert len(result) == 0

    def test_high_null_rate_excluded(self):
        profiles = [_profile("zone_id", "INTEGER", 100, 100, 10)]
        result = suggest_primary_keys("zones", profiles)
        assert len(result) == 0

    def test_empty_table(self):
        profiles = [_profile("id", "INTEGER", 0, 0, 0)]
        result = suggest_primary_keys("empty", profiles)
        assert len(result) == 0

    def test_multiple_candidates_sorted(self):
        profiles = [
            _profile("id", "INTEGER", 100, 100, 0),
            _profile("code", "TEXT", 100, 100, 0),
        ]
        result = suggest_primary_keys("items", profiles)
        assert len(result) == 2
        # 'id' should rank higher due to name pattern
        assert result[0].column == "id"
        assert result[0].confidence >= result[1].confidence

    def test_near_unique_column(self):
        # 96% unique, 0% null -> still a candidate
        profiles = [_profile("email", "TEXT", 1000, 960, 0)]
        result = suggest_primary_keys("users", profiles)
        assert len(result) == 1
        assert result[0].uniqueness_ratio == 0.96

    def test_empty_profiles(self):
        assert suggest_primary_keys("t", []) == []


class TestSuggestForeignKeys:
    def test_name_matching_zone_id(self):
        tables = {
            "inspections": [_profile("zone_id", "INTEGER", 1000, 50, 0)],
            "zones": [_profile("zone_id", "INTEGER", 50, 50, 0)],
        }
        result = suggest_foreign_keys(tables)
        assert len(result) >= 1
        fk = result[0]
        assert fk.from_table == "inspections"
        assert fk.from_column == "zone_id"
        assert fk.to_table == "zones"

    def test_with_known_pks(self):
        tables = {
            "inspections": [_profile("zone_id", "INTEGER", 1000, 50, 0)],
            "zones": [
                _profile("id", "INTEGER", 50, 50, 0),
                _profile("zone_id", "INTEGER", 50, 50, 0),
            ],
        }
        pk_cols = {"zones": ["id"]}
        result = suggest_foreign_keys(tables, pk_cols)
        assert len(result) >= 1
        # Should target the known PK
        fk = result[0]
        assert fk.to_column == "id"

    def test_no_match_for_missing_table(self):
        tables = {
            "inspections": [_profile("widget_id", "INTEGER", 100, 20, 0)],
        }
        result = suggest_foreign_keys(tables)
        assert len(result) == 0

    def test_plural_table_matching(self):
        tables = {
            "orders": [_profile("customer_id", "INTEGER", 500, 100, 0)],
            "customers": [_profile("customer_id", "INTEGER", 100, 100, 0)],
        }
        result = suggest_foreign_keys(tables)
        assert len(result) >= 1
        assert result[0].to_table == "customers"

    def test_key_suffix_matching(self):
        tables = {
            "transactions": [_profile("account_key", "TEXT", 500, 100, 0)],
            "accounts": [_profile("account_key", "TEXT", 100, 100, 0)],
        }
        result = suggest_foreign_keys(tables, {"accounts": ["account_key"]})
        assert len(result) >= 1
        assert result[0].from_table == "transactions"
        assert result[0].from_column == "account_key"
        assert result[0].to_table == "accounts"
        assert result[0].to_column == "account_key"

    def test_skips_own_pk(self):
        tables = {
            "zones": [_profile("zone_id", "INTEGER", 50, 50, 0)],
        }
        pk_cols = {"zones": ["zone_id"]}
        result = suggest_foreign_keys(tables, pk_cols)
        assert len(result) == 0

    def test_empty_input(self):
        assert suggest_foreign_keys({}) == []


class TestCompositeKeys:
    def test_two_id_columns(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS main")
        # 500 students x 200 courses = 100k possible combos, but only 10k rows
        # so (student_id, course_id) should be unique
        con.execute("""
            CREATE TABLE main.enrollments AS
            SELECT
                (i % 500) + 1 AS student_id,
                (i // 500) + 1 AS course_id
            FROM generate_series(0, 9999) t(i)
        """)
        profiles = [
            _profile("student_id", "INTEGER", 10000, 500, 0),
            _profile("course_id", "INTEGER", 10000, 200, 0),
        ]
        result = detect_composite_keys(con, "enrollments", "main", profiles)
        assert len(result) >= 1
        assert "student_id" in result[0].columns
        assert "course_id" in result[0].columns
        con.close()

    def test_no_composite_for_single_id(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS main")
        con.execute("""
            CREATE TABLE main.zones AS
            SELECT i AS zone_id, 'zone_' || i AS name
            FROM generate_series(1, 100) t(i)
        """)
        profiles = [
            _profile("zone_id", "INTEGER", 100, 100, 0),
            _profile("name", "TEXT", 100, 80, 0),
        ]
        result = detect_composite_keys(con, "zones", "main", profiles)
        # zone_id is fully unique (95%+), so it's excluded as single-column PK candidate
        # With only 2 columns and zone_id excluded, no composite key possible
        assert len(result) == 0
        con.close()

    def test_prefers_state_county_over_metric_measure(self):
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS main")
        con.execute("""
            CREATE TABLE main.aqi_by_county (
                state VARCHAR,
                county VARCHAR,
                good_days INTEGER
            )
        """)
        con.execute("""
            INSERT INTO main.aqi_by_county VALUES
                ('CA', 'Albany', 10),
                ('CA', 'Erie', 20),
                ('NY', 'Albany', 10),
                ('NY', 'Erie', 20)
        """)
        profiles = [
            _profile("state", "TEXT", 4, 2, 0),
            _profile("county", "TEXT", 4, 2, 0),
            _profile("good_days", "INTEGER", 4, 2, 0),
        ]
        columns = [
            ColumnInfo(name="state", dtype="varchar", role="dimension"),
            ColumnInfo(name="county", dtype="varchar", role="dimension"),
            ColumnInfo(name="good_days", dtype="int64", role="metric"),
        ]

        result = detect_composite_keys(
            con, "aqi_by_county", "main", profiles, columns=columns
        )

        assert result
        assert result[0].columns == ["state", "county"]
        assert "good_days" not in result[0].columns
        con.close()

    def test_empty_profiles(self):
        import duckdb

        con = duckdb.connect(":memory:")
        assert detect_composite_keys(con, "t", "main", []) == []
        con.close()
