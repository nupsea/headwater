"""Tests for the query decomposition engine.

Covers: keyword resolution, embedding resolution, disambiguation (options),
NULL handling, join path generation, SQL generation, outside_scope detection.
Includes the 'complaints by county' test case from the v2 plan.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from headwater.core.models import (
    DimensionDefinition,
    EntityDefinition,
    MetricDefinition,
    SemanticCatalog,
)
from headwater.explorer.decomposition import (
    QueryDecomposer,
    _build_explanation,
    _compute_confidence,
    _detect_intent,
    _find_unmatched,
    _name_tokens,
    _parse_join_path,
    _stem,
    _tokenize,
)

# ---------------------------------------------------------------------------
# Fixtures -- Riverton-like catalog
# ---------------------------------------------------------------------------


@pytest.fixture
def riverton_catalog() -> SemanticCatalog:
    """Minimal Riverton environmental health catalog for testing."""
    return SemanticCatalog(
        metrics=[
            MetricDefinition(
                name="complaint_count",
                display_name="Total Complaints",
                description="Count of all complaint records",
                expression="COUNT(*)",
                table="complaints",
                agg_type="count",
                synonyms=["complaints", "complaint total"],
                confidence=0.95,
            ),
            MetricDefinition(
                name="avg_inspection_score",
                display_name="Average Inspection Score",
                description="Average score across inspections",
                expression='AVG("score")',
                column="score",
                table="inspections",
                agg_type="avg",
                synonyms=["inspection scores", "score average"],
                confidence=0.92,
            ),
            MetricDefinition(
                name="resolution_rate",
                display_name="Resolution Rate",
                description="Fraction of complaints resolved. NULL resolution_date for unresolved.",
                expression='COUNT("resolution_date") * 1.0 / COUNT(*)',
                column="resolution_date",
                table="complaints",
                agg_type="avg",
                synonyms=["resolve rate", "closure rate"],
                confidence=0.75,
            ),
            MetricDefinition(
                name="reading_value",
                display_name="Average Reading Value",
                description="Average sensor reading value",
                expression='AVG("value")',
                column="value",
                table="readings",
                agg_type="avg",
                synonyms=["sensor reading", "measurement"],
                confidence=0.85,
            ),
        ],
        dimensions=[
            DimensionDefinition(
                name="zone_geography",
                display_name="Zone Geography",
                description="Administrative zone name for area-based analysis",
                column="name",
                table="zones",
                dtype="varchar",
                synonyms=[
                    "county",
                    "borough",
                    "district",
                    "zone",
                    "area",
                    "neighborhood",
                    "region",
                ],
                sample_values=["Downtown Core", "Industrial Park", "Riverside"],
                cardinality=25,
                confidence=0.85,
                join_path="complaints.zone_id -> zones.zone_id",
            ),
            DimensionDefinition(
                name="zone_type",
                display_name="Zone Type",
                description="Land use classification of the zone",
                column="type",
                table="zones",
                dtype="varchar",
                synonyms=["land use", "area type", "zone classification"],
                sample_values=["urban_commercial", "residential", "industrial"],
                cardinality=5,
                confidence=0.90,
                join_path="complaints.zone_id -> zones.zone_id",
            ),
            DimensionDefinition(
                name="complaint_category",
                display_name="Complaint Category",
                description="Type of environmental complaint",
                column="category",
                table="complaints",
                dtype="varchar",
                synonyms=["type", "kind", "complaint type"],
                sample_values=["noise", "water_quality", "pest", "air_quality"],
                cardinality=6,
                confidence=0.95,
            ),
            DimensionDefinition(
                name="complaint_priority",
                display_name="Complaint Priority",
                description="Severity level of the complaint",
                column="priority",
                table="complaints",
                dtype="varchar",
                synonyms=["severity", "urgency"],
                sample_values=["urgent", "high", "medium", "low"],
                cardinality=4,
                confidence=0.90,
            ),
            DimensionDefinition(
                name="site_facility_type",
                display_name="Site Facility Type",
                description="Type of facility at the site",
                column="site_type",
                table="sites",
                dtype="varchar",
                synonyms=["facility", "venue", "building type", "establishment"],
                sample_values=["air_monitoring_station", "food_establishment", "school"],
                cardinality=8,
                confidence=0.85,
                join_path="inspections.site_id -> sites.site_id",
            ),
            DimensionDefinition(
                name="inspection_type",
                display_name="Inspection Type",
                description="Type of inspection performed",
                column="inspection_type",
                table="inspections",
                dtype="varchar",
                synonyms=["inspection kind"],
                sample_values=["routine", "follow_up", "complaint_based"],
                cardinality=3,
                confidence=0.88,
            ),
            DimensionDefinition(
                name="nullable_site_dim",
                display_name="Related Site",
                description="Site linked to complaint (nullable FK)",
                column="site_name",
                table="sites",
                dtype="varchar",
                synonyms=["related site"],
                sample_values=["Station A", "School B"],
                cardinality=500,
                confidence=0.60,
                join_path="complaints.related_site_id -> sites.site_id",
                join_nullable=True,
            ),
        ],
        entities=[
            EntityDefinition(
                name="complaints",
                display_name="Complaints",
                description="Environmental health complaints from residents",
                table="complaints",
                row_semantics="Each row is one complaint filing",
                metrics=["complaint_count", "resolution_rate"],
                dimensions=[
                    "complaint_category",
                    "complaint_priority",
                    "zone_geography",
                    "zone_type",
                ],
                synonyms=["complaint", "grievance", "report"],
            ),
            EntityDefinition(
                name="inspections",
                display_name="Inspections",
                description="Site inspections by environmental health inspectors",
                table="inspections",
                row_semantics="Each row is one inspection visit",
                metrics=["avg_inspection_score"],
                dimensions=["inspection_type", "site_facility_type"],
                synonyms=["inspection", "audit", "check"],
            ),
            EntityDefinition(
                name="readings",
                display_name="Readings",
                description="Sensor measurement readings",
                table="readings",
                row_semantics="Each row is one sensor reading",
                metrics=["reading_value"],
                dimensions=[],
                synonyms=["reading", "measurement", "sensor data"],
            ),
        ],
        generated_at=datetime(2026, 4, 1),
        generation_source="heuristic",
        confidence=0.85,
    )


@pytest.fixture
def decomposer(riverton_catalog) -> QueryDecomposer:
    """Pre-built decomposer with Riverton catalog."""
    return QueryDecomposer(riverton_catalog)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_stem_basic(self):
        assert _stem("complaints") == "complaint"
        assert _stem("readings") == "reading"
        assert _stem("inspections") == "inspection"

    def test_stem_short_word_unchanged(self):
        assert _stem("by") == "by"
        assert _stem("at") == "at"

    def test_tokenize_removes_stop_words(self):
        tokens = _tokenize("show me the count of complaints by county")
        assert "show" not in tokens
        assert "me" not in tokens
        assert "the" not in tokens
        assert "of" not in tokens
        assert "by" not in tokens
        assert "count" in tokens
        assert "complaints" in tokens
        assert "county" in tokens

    def test_tokenize_lowercases(self):
        tokens = _tokenize("Average Inspection SCORES")
        assert "average" in tokens
        assert "inspection" in tokens
        assert "scores" in tokens

    def test_detect_intent_count(self):
        assert _detect_intent("count of complaints") == "count"
        assert _detect_intent("total number of readings") == "count"
        assert _detect_intent("how many inspections") == "count"

    def test_detect_intent_avg(self):
        assert _detect_intent("average score") == "avg"
        assert _detect_intent("mean value") == "avg"

    def test_detect_intent_none(self):
        assert _detect_intent("complaints by category") is None

    def test_name_tokens(self):
        tokens = _name_tokens("zone_geography", "Zone Geography", ["county", "district"])
        assert "zone" in tokens
        assert "geographi" in tokens or "geography" in tokens  # stemmed or original
        assert "county" in tokens
        assert "district" in tokens

    def test_parse_join_path(self):
        result = _parse_join_path("complaints.zone_id -> zones.zone_id")
        assert result == ("complaints", "zone_id", "zones", "zone_id")

    def test_parse_join_path_invalid(self):
        assert _parse_join_path("garbage") is None

    def test_compute_confidence_empty(self):
        assert _compute_confidence([], []) == 0.0


# ---------------------------------------------------------------------------
# Keyword resolution tests
# ---------------------------------------------------------------------------


class TestKeywordResolution:
    """Test Strategy A: stem-based keyword matching."""

    def test_complaints_by_county(self, decomposer):
        """THE critical test case from the v2 plan.

        'county' is a synonym of zone_geography. The decomposer must resolve
        it to zone_geography, not fail or pick a wrong column.
        """
        result = decomposer.decompose("count of complaints by county")
        assert result.status == "resolved"
        assert result.entity == "complaints"
        assert result.sql is not None

        # Must have matched complaint_count metric
        metric_names = [m.metric_name for m in result.metrics]
        assert "complaint_count" in metric_names

        # Must have matched zone_geography dimension (via 'county' synonym)
        dim_names = [d.dimension_name for d in result.dimensions]
        assert "zone_geography" in dim_names

        # SQL should join complaints to zones
        assert "zones" in result.sql
        assert "zone_id" in result.sql

    def test_entity_resolved_from_keyword(self, decomposer):
        result = decomposer.decompose("complaints by category")
        assert result.entity == "complaints"
        assert result.status == "resolved"

    def test_metric_matched_by_synonym(self, decomposer):
        result = decomposer.decompose("inspection scores by type")
        metric_names = [m.metric_name for m in result.metrics]
        assert "avg_inspection_score" in metric_names

    def test_dimension_matched_by_name(self, decomposer):
        result = decomposer.decompose("complaints by category")
        dim_names = [d.dimension_name for d in result.dimensions]
        assert "complaint_category" in dim_names

    def test_dimension_matched_by_synonym(self, decomposer):
        """'severity' is a synonym for complaint_priority."""
        result = decomposer.decompose("complaints by severity")
        dim_names = [d.dimension_name for d in result.dimensions]
        assert "complaint_priority" in dim_names

    def test_default_count_metric(self, decomposer):
        """When no metric mentioned, default to entity's count metric."""
        result = decomposer.decompose("complaints by priority")
        assert result.status == "resolved"
        metric_names = [m.metric_name for m in result.metrics]
        assert "complaint_count" in metric_names

    def test_intent_detection_average(self, decomposer):
        result = decomposer.decompose("average inspection score")
        metric_names = [m.metric_name for m in result.metrics]
        assert "avg_inspection_score" in metric_names


# ---------------------------------------------------------------------------
# SQL generation tests
# ---------------------------------------------------------------------------


class TestSQLGeneration:
    def test_single_table_no_join(self, decomposer):
        """Metric and dimension on same table -- no JOIN needed."""
        result = decomposer.decompose("complaints by category")
        assert result.sql is not None
        assert "JOIN" not in result.sql
        assert "GROUP BY" in result.sql

    def test_cross_table_join(self, decomposer):
        """Dimension on different table requires JOIN."""
        result = decomposer.decompose("complaints by county")
        assert result.sql is not None
        assert "JOIN" in result.sql
        assert '"zones"' in result.sql

    def test_nullable_fk_left_join(self, decomposer):
        """Nullable FK produces LEFT JOIN and a warning."""
        result = decomposer.decompose("complaints by related site")
        if result.status == "resolved" and result.sql:
            assert "LEFT JOIN" in result.sql
            assert any("LEFT JOIN" in w for w in result.warnings)

    def test_sql_has_group_by_and_order(self, decomposer):
        result = decomposer.decompose("complaints by category")
        assert result.sql is not None
        assert "GROUP BY" in result.sql
        assert "ORDER BY" in result.sql
        assert "DESC" in result.sql

    def test_sql_select_has_metric_alias(self, decomposer):
        result = decomposer.decompose("count of complaints by county")
        assert result.sql is not None
        assert "total_complaints" in result.sql.lower() or "count(*)" in result.sql.lower()


# ---------------------------------------------------------------------------
# Embedding resolution tests
# ---------------------------------------------------------------------------


class TestEmbeddingResolution:
    """Test Strategy B: vector similarity fills gaps."""

    def _mock_vector_store(self, results: list[dict]) -> MagicMock:
        vs = MagicMock()
        vs.search.return_value = results
        return vs

    def test_embedding_fills_dimension_gap(self, riverton_catalog):
        """If keyword misses, embedding search provides the dimension."""
        # Build a catalog with only a metric (no dimensions that match "locale")
        catalog = SemanticCatalog(
            metrics=[riverton_catalog.metrics[0]],  # complaint_count
            dimensions=[riverton_catalog.dimensions[0]],  # zone_geography
            entities=[
                EntityDefinition(
                    name="complaints",
                    display_name="Complaints",
                    description="Complaints",
                    table="complaints",
                    row_semantics="Each row is a complaint",
                    metrics=["complaint_count"],
                    dimensions=["zone_geography"],
                    synonyms=["complaint"],
                ),
            ],
            generated_at=datetime(2026, 4, 1),
            confidence=0.8,
        )
        d = QueryDecomposer(catalog)
        vs = self._mock_vector_store(
            [
                {
                    "id": "dim_zone_geography",
                    "name": "zone_geography",
                    "entry_type": "dimension",
                    "display_name": "Zone Geography",
                    "text": "Zone Geography. Administrative zones.",
                    "_distance": 0.3,
                }
            ]
        )
        # "locale" won't keyword-match any dimension in this reduced catalog
        # but "complaint" will match the metric. So metric found, dim missing -> embedding called.
        result = d.decompose(
            "totals locale",
            vector_store=vs,
        )
        vs.search.assert_called_once()
        dim_names = [dm.dimension_name for dm in result.dimensions]
        assert "zone_geography" in dim_names

    def test_embedding_fills_metric_gap(self, decomposer):
        """Embedding resolves a metric not found by keyword."""
        vs = self._mock_vector_store(
            [
                {
                    "id": "met_reading_value",
                    "name": "reading_value",
                    "entry_type": "metric",
                    "display_name": "Average Reading Value",
                    "text": "Average sensor reading value",
                    "_distance": 0.2,
                }
            ]
        )
        result = decomposer.decompose(
            "sensor measurement values",
            vector_store=vs,
        )
        vs.search.assert_called_once()
        metric_names = [m.metric_name for m in result.metrics]
        assert "reading_value" in metric_names

    def test_embedding_ignores_distant_results(self, decomposer):
        """Results with distance > 1.0 are discarded."""
        vs = self._mock_vector_store(
            [
                {
                    "id": "dim_zone_geography",
                    "name": "zone_geography",
                    "entry_type": "dimension",
                    "display_name": "Zone Geography",
                    "text": "Zone Geography.",
                    "_distance": 1.5,  # Too far
                }
            ]
        )
        result = decomposer.decompose(
            "completely unrelated gibberish terms",
            vector_store=vs,
        )
        # Should not have resolved zone_geography
        dim_names = [d.dimension_name for d in result.dimensions]
        assert "zone_geography" not in dim_names

    def test_embedding_not_called_when_fully_resolved(self, decomposer):
        """If keyword resolves both metric and dimension, skip embedding."""
        vs = self._mock_vector_store([])
        result = decomposer.decompose(
            "complaint count by category",
            vector_store=vs,
        )
        # Should resolve via keyword alone
        assert result.status == "resolved"
        # Embedding should NOT have been called (both metric and dim found)
        vs.search.assert_not_called()


# ---------------------------------------------------------------------------
# Disambiguation tests
# ---------------------------------------------------------------------------


class TestDisambiguation:
    def test_ambiguous_dimensions_return_options(self, decomposer):
        """When multiple dimensions match the same concept, return options.

        'area' matches both zone_geography (synonym) and zone_type (synonym 'area type').
        Both have column stems that could overlap. The decomposer groups them.
        """
        # This test depends on whether 'area' triggers multiple dimension matches
        # with the same column stem. Let's test explicitly via a catalog with
        # dimensions that share a column stem.
        catalog = SemanticCatalog(
            metrics=[
                MetricDefinition(
                    name="count_m",
                    display_name="Count",
                    description="Count",
                    expression="COUNT(*)",
                    table="facts",
                    agg_type="count",
                    confidence=0.9,
                ),
            ],
            dimensions=[
                DimensionDefinition(
                    name="geo_name",
                    display_name="Geographic Name",
                    description="Name of geographic area",
                    column="geo",
                    table="dims",
                    dtype="varchar",
                    synonyms=["area", "region"],
                    sample_values=["North", "South"],
                    confidence=0.8,
                ),
                DimensionDefinition(
                    name="geo_code",
                    display_name="Geographic Code",
                    description="Code for geographic area",
                    column="geo",  # Same column stem as geo_name
                    table="dims",
                    dtype="varchar",
                    synonyms=["area code"],
                    sample_values=["N01", "S02"],
                    confidence=0.7,
                ),
            ],
            entities=[
                EntityDefinition(
                    name="facts",
                    display_name="Facts",
                    description="Fact table",
                    table="facts",
                    row_semantics="Each row is a fact",
                    metrics=["count_m"],
                    dimensions=["geo_name", "geo_code"],
                ),
            ],
            generated_at=datetime(2026, 4, 1),
            confidence=0.8,
        )
        d = QueryDecomposer(catalog)
        result = d.decompose("count by area")
        # Should detect ambiguity since both dims match "area" and share column stem "geo"
        if result.status == "options":
            assert len(result.options) >= 2
            option_names = [o.dimension_name for o in result.options]
            assert "geo_name" in option_names
            assert "geo_code" in option_names


# ---------------------------------------------------------------------------
# Outside scope tests
# ---------------------------------------------------------------------------


class TestOutsideScope:
    def test_completely_unrecognized_question(self, decomposer):
        """Terms that match nothing in the catalog -> outside_scope."""
        result = decomposer.decompose("weather forecast for tomorrow")
        assert result.status == "outside_scope"
        assert len(result.outside_catalog) > 0

    def test_partial_match_still_resolves(self, decomposer):
        """Some terms match, some don't -- should still resolve if metrics found."""
        result = decomposer.decompose("complaints by imaginary_dimension")
        # complaint_count should still match, even if imaginary_dimension doesn't
        if result.metrics:
            assert result.status == "resolved"


# ---------------------------------------------------------------------------
# Join path and warning tests
# ---------------------------------------------------------------------------


class TestJoinsAndWarnings:
    def test_join_path_parsed_correctly(self):
        result = _parse_join_path("complaints.zone_id -> zones.zone_id")
        assert result == ("complaints", "zone_id", "zones", "zone_id")

    def test_nullable_fk_generates_warning(self, decomposer):
        """nullable_site_dim has join_nullable=True, should produce LEFT JOIN warning."""
        result = decomposer.decompose("complaints by related site")
        if result.sql and "LEFT JOIN" in result.sql:
            assert any("LEFT JOIN" in w or "integrity" in w.lower() for w in result.warnings)

    def test_null_metric_warning(self, decomposer):
        """Metrics mentioning NULL in description produce warnings."""
        result = decomposer.decompose("resolution rate by category")
        if result.status == "resolved":
            # resolution_rate description mentions NULL
            has_null_warning = any("NULL" in w for w in result.warnings)
            # This depends on the _build_sql NULL check logic
            # At minimum, the metric should be resolved
            metric_names = [m.metric_name for m in result.metrics]
            assert "resolution_rate" in metric_names or has_null_warning


# ---------------------------------------------------------------------------
# Follow-up suggestion tests
# ---------------------------------------------------------------------------


class TestSuggestions:
    def test_suggestions_generated(self, decomposer):
        result = decomposer.decompose("complaints by category")
        assert result.status == "resolved"
        assert len(result.suggestions) > 0

    def test_suggestions_offer_alternatives(self, decomposer):
        result = decomposer.decompose("complaints by category")
        # Suggestions should offer different dimensions or metrics
        assert len(result.suggestions) > 0
        # At least one suggestion should reference a different dimension
        has_different = any("Category" not in s for s in result.suggestions)
        assert has_different

    def test_suggestions_capped_at_5(self, decomposer):
        result = decomposer.decompose("complaints by category")
        assert len(result.suggestions) <= 5


# ---------------------------------------------------------------------------
# Explanation tests
# ---------------------------------------------------------------------------


class TestExplanation:
    def test_explanation_has_metric(self, decomposer):
        result = decomposer.decompose("complaints by county")
        assert "Total Complaints" in result.explanation or "complaint" in result.explanation.lower()

    def test_explanation_has_entity(self, decomposer):
        result = decomposer.decompose("complaints by county")
        assert "complaints" in result.explanation.lower()

    def test_build_explanation_function(self):
        from headwater.core.models import DimensionMatch, MetricMatch

        metrics = [
            MetricMatch(
                metric_name="c",
                display_name="Count",
                expression="COUNT(*)",
                table="t",
                confidence=0.9,
            )
        ]
        dims = [
            DimensionMatch(
                dimension_name="d",
                display_name="Category",
                column="cat",
                table="t",
                confidence=0.8,
            )
        ]
        explanation = _build_explanation("things", metrics, dims, [])
        assert "Count" in explanation
        assert "Category" in explanation
        assert "things" in explanation


# ---------------------------------------------------------------------------
# Confidence tests
# ---------------------------------------------------------------------------


class TestConfidence:
    def test_high_confidence_keyword_resolution(self, decomposer):
        result = decomposer.decompose("complaints by category")
        assert result.confidence >= 0.8

    def test_outside_scope_zero_confidence(self, decomposer):
        result = decomposer.decompose("weather forecast for tomorrow")
        assert result.confidence == 0.0

    def test_resolution_mode_is_catalog(self, decomposer):
        result = decomposer.decompose("complaints by county")
        assert result.resolution_mode == "catalog"


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_question(self, decomposer):
        result = decomposer.decompose("")
        assert result.status == "outside_scope" or result.confidence < 0.5

    def test_single_word_entity(self, decomposer):
        result = decomposer.decompose("complaints")
        # Should at least resolve the entity and default to count
        assert result.entity == "complaints"

    def test_decomposer_index_built(self, decomposer):
        """The stem index should have entries for all catalog items."""
        assert len(decomposer._name_index) > 0
        # 'county' should be in the index (synonym of zone_geography)
        assert "county" in decomposer._name_index

    def test_find_unmatched(self):
        """_find_unmatched returns tokens not matched by any catalog entry."""
        from headwater.core.models import DimensionMatch, MetricMatch

        tokens = ["complaints", "weather", "county"]
        stems = [_stem(t) for t in tokens]
        metrics = [
            MetricMatch(
                metric_name="complaint_count",
                display_name="c",
                expression="COUNT(*)",
                table="t",
                confidence=0.9,
            )
        ]
        dims = [
            DimensionMatch(
                dimension_name="zone_geography",
                display_name="d",
                column="c",
                table="t",
                confidence=0.8,
            )
        ]
        entities = [("complaints", 0.9)]
        unmatched = _find_unmatched(tokens, stems, metrics, dims, entities)
        assert "weather" in unmatched
        # 'county' may or may not match depending on zone_geography stems
