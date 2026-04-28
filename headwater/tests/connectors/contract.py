"""Reusable assertions for connector implementations."""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from headwater.core.models import SourceConfig


def assert_connector_contract(
    connector,
    config: SourceConfig,
    *,
    expected_tables: set[str],
    expected_columns: Mapping[str, set[str]],
) -> None:
    """Run the common connector behavior contract.

    The checks are intentionally capability-aware: a connector is only required
    to implement behavior it declares as supported.
    """
    capabilities = connector.capabilities()
    assert capabilities.test is True
    assert capabilities.modes

    connector.connect(config)

    if capabilities.list_tables:
        tables = set(connector.list_tables())
        assert expected_tables <= tables
    else:
        tables = expected_tables

    if capabilities.list_columns:
        for table_name, columns in expected_columns.items():
            column_rows = connector.list_columns(table_name)
            names = {row["name"] for row in column_rows}
            assert columns <= names
            assert all(row.get("ordinal_position") for row in column_rows)

    if capabilities.profile_table:
        for table_name, columns in expected_columns.items():
            stats = connector.profile(table_name)
            assert columns <= set(stats)
            assert all("null_count" in stats[column] for column in columns)

    if capabilities.sample_arrow:
        table_name = next(iter(expected_tables))
        sample = connector.sample(table_name, n=2)
        assert isinstance(sample, pa.Table)
        assert sample.num_rows <= 2

    if hasattr(connector, "close"):
        connector.close()
        connector.close()
