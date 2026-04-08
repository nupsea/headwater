"""CSV connector -- loads CSV files from a directory into DuckDB via Polars."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig


class CsvLoader:
    """Loads CSV files from a directory into DuckDB tables.

    Each .csv file becomes a table named after the file stem.
    """

    def __init__(self) -> None:
        self._path: Path | None = None
        self._files: list[Path] = []

    def connect(self, config: SourceConfig) -> None:
        if config.path is None:
            raise ConnectorError("CsvLoader requires a path")
        self._path = Path(config.path)
        if not self._path.is_dir():
            raise ConnectorError(f"Path is not a directory: {self._path}")
        self._files = sorted(self._path.glob("*.csv"))
        if not self._files:
            raise ConnectorError(f"No .csv files found in {self._path}")

    def load_to_duckdb(self, con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
        if not self._files:
            raise ConnectorError("Not connected -- call connect() first")

        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        loaded: list[str] = []

        for fp in self._files:
            table_name = fp.stem
            df = pl.read_csv(fp, infer_schema_length=10000, try_parse_dates=True)
            con.register(f"_tmp_{table_name}", df)
            con.execute(
                f"CREATE OR REPLACE TABLE {schema}.{table_name} AS "
                f"SELECT * FROM _tmp_{table_name}"
            )
            con.unregister(f"_tmp_{table_name}")
            loaded.append(table_name)

        return loaded
