"""Connector capability models.

These models describe what a connector can safely do before the pipeline tries
to use it. They are intentionally transport-neutral so file, database,
warehouse, and catalog connectors can share the same contract.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ConnectorCapabilities(BaseModel):
    """Feature flags exposed by a connector implementation."""

    test: bool = True
    list_schemas: bool = False
    list_tables: bool = False
    list_columns: bool = False
    list_constraints: bool = False
    estimate_row_count: bool = False
    profile_table: bool = False
    sample_arrow: bool = False
    execute_readonly: bool = False
    load_to_duckdb: bool = False
    modes: list[str] = Field(default_factory=lambda: ["generate"])


class TableRef(BaseModel):
    """Connector-native table reference."""

    name: str
    schema_name: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.name}" if self.schema_name else self.name


class ColumnRef(BaseModel):
    """Connector-native column reference."""

    table: TableRef
    name: str
    data_type: str
    nullable: bool = True
    ordinal: int | None = None


class TableProfile(BaseModel):
    """Stable profile envelope for connector-pushdown profiling."""

    table: TableRef
    row_count: int | None = None
    column_stats: dict[str, dict] = Field(default_factory=dict)


class SourceProfile(BaseModel):
    """Stable source profile envelope for connector summaries."""

    source_name: str
    tables: list[TableProfile] = Field(default_factory=list)


UNSUPPORTED_CAPABILITIES = ConnectorCapabilities(
    test=False,
    modes=[],
)

FILE_GENERATE_CAPABILITIES = ConnectorCapabilities(
    list_schemas=True,
    list_tables=True,
    list_columns=True,
    estimate_row_count=True,
    profile_table=True,
    sample_arrow=True,
    load_to_duckdb=True,
    modes=["generate"],
)

POSTGRES_GENERATE_CAPABILITIES = ConnectorCapabilities(
    list_schemas=True,
    list_tables=True,
    list_columns=True,
    list_constraints=False,
    estimate_row_count=True,
    profile_table=True,
    sample_arrow=True,
    execute_readonly=True,
    load_to_duckdb=False,
    modes=["generate", "observe"],
)

DUCKDB_GENERATE_CAPABILITIES = ConnectorCapabilities(
    list_schemas=True,
    list_tables=True,
    list_columns=True,
    list_constraints=False,
    estimate_row_count=True,
    profile_table=True,
    sample_arrow=True,
    execute_readonly=True,
    load_to_duckdb=True,
    modes=["generate", "observe"],
)
