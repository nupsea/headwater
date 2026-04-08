"""JSON/NDJSON connector -- loads JSON files from a directory into DuckDB via Polars."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig


class JsonLoader:
    """Loads NDJSON files from a directory into DuckDB tables.

    Each .json file becomes a table named after the file stem.
    Nested objects/arrays are stored as DuckDB JSON/STRUCT/LIST types.
    """

    def __init__(self) -> None:
        self._path: Path | None = None
        self._files: list[Path] = []

    def connect(self, config: SourceConfig) -> None:
        if config.path is None:
            raise ConnectorError("JsonLoader requires a path")
        self._path = Path(config.path)
        if not self._path.is_dir():
            raise ConnectorError(f"Path is not a directory: {self._path}")
        self._files = sorted(self._path.glob("*.json"))
        if not self._files:
            raise ConnectorError(f"No .json files found in {self._path}")

    def load_to_duckdb(self, con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
        if not self._files:
            raise ConnectorError("Not connected -- call connect() first")

        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        loaded: list[str] = []

        for fp in self._files:
            table_name = fp.stem
            # Skip the generator script if it somehow has .json extension
            if table_name == "generate_sample":
                continue

            df = pl.read_ndjson(fp)
            # Register the Polars dataframe as a DuckDB view, then materialize
            con.register(f"_tmp_{table_name}", df)
            con.execute(
                f"CREATE OR REPLACE TABLE {schema}.{table_name} AS "
                f"SELECT * FROM _tmp_{table_name}"
            )
            con.unregister(f"_tmp_{table_name}")
            loaded.append(table_name)

        return loaded
