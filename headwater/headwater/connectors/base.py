"""Base connector protocol -- interface for all source connectors."""

from __future__ import annotations

from typing import Protocol

import duckdb
import pyarrow as pa

from headwater.core.models import SourceConfig


class BaseConnector(Protocol):
    """Interface that all source connectors must implement.

    Two methods define the connector contract:
    - profile(): runs aggregate queries in-place, returns a stats dict per column.
      Available in both 'generate' and 'observe' modes.
    - sample(): returns an Arrow table of N rows for local DuckDB validation.
      Available in 'generate' mode only. Observe-mode connectors raise NotImplementedError.

    The legacy load_to_duckdb() method is also part of the interface for backwards
    compatibility with the existing pipeline (generate-mode file connectors).
    """

    def connect(self, config: SourceConfig) -> None:
        """Validate and prepare the connection to the data source."""
        ...

    def load_to_duckdb(self, con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
        """Load all tables into DuckDB under the given schema. Returns table names."""
        ...

    def profile(self, table_name: str) -> dict:
        """Run aggregate queries and return column-level stats dict.

        The returned dict has column names as keys. Each value is a dict with keys:
        min, max, count, distinct_count, null_count (all optional depending on type).

        Available in both generate and observe modes.
        """
        ...

    def sample(self, table_name: str, n: int = 10_000) -> pa.Table:
        """Return an Arrow table of up to N rows from the given table.

        Used only in generate mode for local DuckDB validation of generated SQL.
        Observe-mode connectors raise NotImplementedError with a clear message.
        """
        ...
