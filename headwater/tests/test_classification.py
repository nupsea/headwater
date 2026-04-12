"""Tests for shared column classification helpers."""

from __future__ import annotations

from headwater.core.classification import is_dimension_column, is_metric_column
from headwater.core.models import ColumnInfo, ColumnProfile

# ---------------------------------------------------------------------------
# is_metric_column tests
# ---------------------------------------------------------------------------


class TestIsMetricColumn:
    def test_latitude_not_metric(self):
        col = ColumnInfo(name="latitude", dtype="float64", semantic_type="geographic")
        assert is_metric_column(col) is False

    def test_longitude_not_metric(self):
        col = ColumnInfo(name="longitude", dtype="float64", semantic_type="geographic")
        assert is_metric_column(col) is False

    def test_lat_shorthand_not_metric(self):
        col = ColumnInfo(name="lat", dtype="float64")
        assert is_metric_column(col) is False

    def test_explicit_metric_is_metric(self):
        col = ColumnInfo(name="severity_score", dtype="float64", semantic_type="metric")
        assert is_metric_column(col) is True

    def test_numeric_without_semantic_type_is_metric(self):
        col = ColumnInfo(name="response_days", dtype="int64")
        assert is_metric_column(col) is True

    def test_id_column_not_metric(self):
        col = ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id")
        assert is_metric_column(col) is False

    def test_primary_key_not_metric(self):
        col = ColumnInfo(
            name="row_num", dtype="int64", is_primary_key=True,
        )
        assert is_metric_column(col) is False

    def test_dimension_not_metric(self):
        col = ColumnInfo(name="county", dtype="varchar", semantic_type="dimension")
        assert is_metric_column(col) is False

    def test_pii_not_metric(self):
        col = ColumnInfo(name="email", dtype="varchar", semantic_type="pii")
        assert is_metric_column(col) is False

    def test_code_suffix_not_metric(self):
        col = ColumnInfo(name="zip_code", dtype="int64")
        assert is_metric_column(col) is False

    def test_number_suffix_not_metric(self):
        col = ColumnInfo(name="complaint_number", dtype="int64")
        assert is_metric_column(col) is False

    def test_text_not_metric(self):
        col = ColumnInfo(name="description", dtype="varchar", semantic_type="text")
        assert is_metric_column(col) is False

    def test_varchar_not_metric_even_without_type(self):
        col = ColumnInfo(name="borough", dtype="varchar")
        assert is_metric_column(col) is False

    def test_with_profile_respects_dtype(self):
        col = ColumnInfo(name="value", dtype="float64")
        profile = ColumnProfile(
            table_name="t", column_name="value", dtype="float64",
        )
        assert is_metric_column(col, profile) is True

    def test_varchar_with_metric_semantic_type_not_metric(self):
        """A VARCHAR column tagged 'metric' by name pattern is NOT a metric."""
        col = ColumnInfo(
            name="units_of_measure", dtype="varchar", semantic_type="metric",
        )
        assert is_metric_column(col) is False

    def test_varchar_measure_column_not_metric(self):
        """units_of_measure is varchar -- can't AVG it."""
        col = ColumnInfo(name="units_of_measure", dtype="varchar")
        assert is_metric_column(col) is False


# ---------------------------------------------------------------------------
# is_dimension_column tests
# ---------------------------------------------------------------------------


class TestIsDimensionColumn:
    def test_varchar_dimension(self):
        col = ColumnInfo(name="county", dtype="varchar", semantic_type="dimension")
        assert is_dimension_column(col) is True

    def test_varchar_without_semantic_type(self):
        col = ColumnInfo(name="borough", dtype="varchar")
        assert is_dimension_column(col) is True

    def test_numeric_dimension_with_semantic_type(self):
        col = ColumnInfo(
            name="community_board", dtype="int64", semantic_type="dimension",
        )
        assert is_dimension_column(col) is True

    def test_numeric_with_low_cardinality_profile(self):
        col = ColumnInfo(name="community_board", dtype="int64")
        profile = ColumnProfile(
            table_name="t", column_name="community_board",
            dtype="int64", distinct_count=35,
        )
        assert is_dimension_column(col, profile) is True

    def test_numeric_with_high_cardinality_profile(self):
        col = ColumnInfo(name="score", dtype="float64")
        profile = ColumnProfile(
            table_name="t", column_name="score",
            dtype="float64", distinct_count=9000,
        )
        assert is_dimension_column(col, profile) is False

    def test_high_cardinality_varchar_excluded(self):
        col = ColumnInfo(name="address", dtype="varchar")
        profile = ColumnProfile(
            table_name="t", column_name="address",
            dtype="varchar", distinct_count=9000,
        )
        assert is_dimension_column(col, profile) is False

    def test_id_column_not_dimension(self):
        col = ColumnInfo(name="complaint_id", dtype="int64", semantic_type="id")
        assert is_dimension_column(col) is False

    def test_foreign_key_not_dimension(self):
        col = ColumnInfo(name="zone_id", dtype="varchar", semantic_type="foreign_key")
        assert is_dimension_column(col) is False

    def test_geographic_not_dimension(self):
        col = ColumnInfo(name="latitude", dtype="float64", semantic_type="geographic")
        assert is_dimension_column(col) is False

    def test_explicit_metric_stays_metric(self):
        """Numeric low-cardinality column with metric semantic type stays metric."""
        col = ColumnInfo(name="rating", dtype="int64", semantic_type="metric")
        profile = ColumnProfile(
            table_name="t", column_name="rating",
            dtype="int64", distinct_count=5,
        )
        assert is_dimension_column(col, profile) is False

    def test_primary_key_not_dimension(self):
        col = ColumnInfo(name="id", dtype="int64", is_primary_key=True)
        assert is_dimension_column(col) is False
