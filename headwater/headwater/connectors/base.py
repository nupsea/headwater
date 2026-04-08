"""Base connector protocol -- interface for all source connectors."""

from __future__ import annotations

from typing import Protocol

import duckdb

from headwater.core.models import SourceConfig


class BaseConnector(Protocol):
    """Interface that all source connectors must implement."""

    def connect(self, config: SourceConfig) -> None:
        """Validate and prepare the connection to the data source."""
        ...

    def load_to_duckdb(self, con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
        """Load all tables into DuckDB under the given schema. Returns table names."""
        ...
