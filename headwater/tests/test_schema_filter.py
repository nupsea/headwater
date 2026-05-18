"""Tests for the schema_filter module."""

from headwater.connectors.schema_filter import SchemaTableFilter


class TestSchemaTableFilter:
    """Tests for the standalone SchemaTableFilter component."""

    def test_empty_filter_accepts_everything(self):
        sf = SchemaTableFilter()
        assert sf.is_empty
        assert sf.accepts_schema("anything")
        assert sf.accepts_table("anything")

    def test_include_schemas(self):
        sf = SchemaTableFilter(include_schemas=["analytics", "reporting"])
        assert sf.accepts_schema("analytics")
        assert sf.accepts_schema("reporting")
        assert not sf.accepts_schema("staging")
        assert not sf.accepts_schema("scratch")

    def test_exclude_schemas(self):
        sf = SchemaTableFilter(exclude_schemas=["staging", "scratch"])
        assert sf.accepts_schema("analytics")
        assert sf.accepts_schema("reporting")
        assert not sf.accepts_schema("staging")
        assert not sf.accepts_schema("scratch")

    def test_schema_patterns_support_globs(self):
        sf = SchemaTableFilter(include_schemas=["data_*"], exclude_schemas=["data_tmp*"])
        assert sf.accepts_schema("data_core")
        assert not sf.accepts_schema("data_tmp")
        assert not sf.accepts_schema("reporting")

    def test_exclude_takes_precedence_over_include(self):
        sf = SchemaTableFilter(
            include_schemas=["analytics", "staging"],
            exclude_schemas=["staging"],
        )
        assert sf.accepts_schema("analytics")
        assert not sf.accepts_schema("staging")

    def test_case_insensitive_by_default(self):
        sf = SchemaTableFilter(include_schemas=["Analytics"])
        assert sf.accepts_schema("analytics")
        assert sf.accepts_schema("ANALYTICS")
        assert sf.accepts_schema("Analytics")

    def test_case_sensitive(self):
        sf = SchemaTableFilter(include_schemas=["Analytics"], case_sensitive=True)
        assert sf.accepts_schema("Analytics")
        assert not sf.accepts_schema("analytics")
        assert not sf.accepts_schema("ANALYTICS")

    def test_include_tables_glob(self):
        sf = SchemaTableFilter(include_tables=["dim_*", "fact_*"])
        assert sf.accepts_table("dim_customers")
        assert sf.accepts_table("fact_orders")
        assert not sf.accepts_table("stg_orders")
        assert not sf.accepts_table("raw_events")

    def test_include_tables_supports_qualified_patterns(self):
        sf = SchemaTableFilter(include_tables=["analytics.dim_*", "reporting.fact_*"])
        assert sf.accepts_table("dim_customers", "analytics")
        assert sf.accepts_table("fact_orders", "reporting")
        assert not sf.accepts_table("dim_customers", "public")
        assert not sf.accepts_table("fact_orders", "analytics")

    def test_exclude_tables_glob(self):
        sf = SchemaTableFilter(exclude_tables=["*_tmp", "*_backup"])
        assert sf.accepts_table("dim_customers")
        assert not sf.accepts_table("orders_tmp")
        assert not sf.accepts_table("orders_backup")

    def test_exclude_tables_supports_qualified_patterns(self):
        sf = SchemaTableFilter(exclude_tables=["staging.*", "analytics.orders"])
        assert not sf.accepts_table("events", "staging")
        assert not sf.accepts_table("orders", "analytics")
        assert sf.accepts_table("orders", "reporting")

    def test_include_and_exclude_tables(self):
        sf = SchemaTableFilter(
            include_tables=["dim_*"],
            exclude_tables=["dim_deprecated_*"],
        )
        assert sf.accepts_table("dim_customers")
        assert not sf.accepts_table("dim_deprecated_zones")
        assert not sf.accepts_table("fact_orders")

    def test_filter_schemas_bulk(self):
        sf = SchemaTableFilter(
            include_schemas=["analytics"],
            exclude_schemas=["pg_catalog"],
        )
        result = sf.filter_schemas(["analytics", "staging", "pg_catalog", "public"])
        assert result == ["analytics"]

    def test_filter_tables_with_schema_prefix(self):
        sf = SchemaTableFilter(
            include_schemas=["analytics"],
            exclude_tables=["*_tmp"],
        )
        tables = [
            "analytics.dim_customers",
            "analytics.orders_tmp",
            "staging.dim_products",
            "analytics.fact_orders",
        ]
        result = sf.filter_tables(tables)
        assert result == ["analytics.dim_customers", "analytics.fact_orders"]

    def test_filter_tables_with_qualified_schema_patterns(self):
        sf = SchemaTableFilter(include_schemas=["data.dim*", "prst.*", "view.*"])
        tables = [
            "data.dim_customer",
            "data.fact_orders",
            "prst.snapshot_orders",
            "view.daily_sales",
            "other.random_table",
        ]
        result = sf.filter_tables(tables)
        assert result == [
            "data.dim_customer",
            "prst.snapshot_orders",
            "view.daily_sales",
        ]

    def test_filter_tables_bare_names(self):
        sf = SchemaTableFilter(include_tables=["dim_*"])
        tables = ["dim_customers", "fact_orders", "dim_products"]
        result = sf.filter_tables(tables)
        assert result == ["dim_customers", "dim_products"]

    def test_filter_tables_with_schema_extractor(self):
        sf = SchemaTableFilter(include_schemas=["analytics"])
        tables = ["customers", "orders"]
        result = sf.filter_tables(
            tables,
            schema_extractor=lambda t: "analytics" if t == "customers" else "staging",
        )
        assert result == ["customers"]

    def test_from_config(self):
        sf = SchemaTableFilter.from_config({
            "include_schemas": ["analytics", "reporting"],
            "exclude_tables": ["*_tmp"],
            "max_tables": 50,  # ignored -- not a filter key
        })
        assert sf.include_schemas == ["analytics", "reporting"]
        assert sf.exclude_tables == ["*_tmp"]
        assert sf.include_tables == []

    def test_from_config_none(self):
        sf = SchemaTableFilter.from_config(None)
        assert sf.is_empty

    def test_from_config_csv_string(self):
        sf = SchemaTableFilter.from_config({
            "include_schemas": "analytics, reporting",
        })
        assert sf.include_schemas == ["analytics", "reporting"]

    def test_from_query_params(self):
        sf = SchemaTableFilter.from_query_params({
            "include_schemas": "analytics,reporting",
            "exclude_tables": "*_tmp,*_backup",
        })
        assert sf.include_schemas == ["analytics", "reporting"]
        assert sf.exclude_tables == ["*_tmp", "*_backup"]

    def test_describe(self):
        sf = SchemaTableFilter(include_schemas=["a"], exclude_tables=["*_tmp"])
        desc = sf.describe()
        assert desc == {"include_schemas": ["a"], "exclude_tables": ["*_tmp"]}

    def test_describe_empty(self):
        sf = SchemaTableFilter()
        assert sf.describe() == {}
