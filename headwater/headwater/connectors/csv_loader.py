"""CSV connector -- loads CSV files from a directory into DuckDB via Polars."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa

from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig


class CsvLoader:
    """Loads CSV files from a directory into DuckDB tables.

    Each .csv file becomes a table named after the file stem.

    Implements BaseConnector: profile() and sample() operate on in-memory Polars
    DataFrames loaded from disk.
    """

    def __init__(self) -> None:
        self._path: Path | None = None
        self._files: list[Path] = []
        self._frames: dict[str, pl.DataFrame] = {}

    def connect(self, config: SourceConfig) -> None:
        if config.path is None:
            raise ConnectorError("CsvLoader requires a path")
        self._path = Path(config.path)
        if not self._path.is_dir():
            raise ConnectorError(f"Path is not a directory: {self._path}")
        self._files = sorted(self._path.glob("*.csv"))
        if not self._files:
            raise ConnectorError(f"No .csv files found in {self._path}")

    def _get_frame(self, table_name: str) -> pl.DataFrame:
        """Load or return cached DataFrame for the given table name."""
        if table_name not in self._frames:
            if self._path is None:
                raise ConnectorError("Not connected -- call connect() first")
            fp = self._path / f"{table_name}.csv"
            if not fp.exists():
                raise ConnectorError(f"No CSV file for table '{table_name}': {fp}")
            self._frames[table_name] = pl.read_csv(
                fp, infer_schema_length=10000, try_parse_dates=True
            )
        return self._frames[table_name]

    def profile(self, table_name: str) -> dict:
        """Run Polars aggregate expressions and return column-level stats."""
        df = self._get_frame(table_name)
        stats: dict[str, dict] = {}
        for col_name in df.columns:
            series = df[col_name]
            col_stats: dict = {
                "count": len(series),
                "null_count": series.null_count(),
                "distinct_count": series.n_unique(),
            }
            if series.dtype in (
                pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                pl.Float32, pl.Float64,
            ):
                non_null = series.drop_nulls()
                if len(non_null) > 0:
                    col_stats["min"] = non_null.min()
                    col_stats["max"] = non_null.max()
            stats[col_name] = col_stats
        return stats

    def sample(self, table_name: str, n: int = 10_000) -> pa.Table:
        """Return up to N rows as an Arrow table for local DuckDB validation."""
        df = self._get_frame(table_name)
        if len(df) > n:
            df = df.sample(n=n, seed=42)
        return df.to_arrow()

    def load_to_duckdb(self, con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
        if not self._files:
            raise ConnectorError("Not connected -- call connect() first")

        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        loaded: list[str] = []

        for fp in self._files:
            table_name = fp.stem
            df = pl.read_csv(fp, infer_schema_length=10000, try_parse_dates=True)
            self._frames[table_name] = df
            con.register(f"_tmp_{table_name}", df)
            con.execute(
                f"CREATE OR REPLACE TABLE {schema}.{table_name} AS "
                f"SELECT * FROM _tmp_{table_name}"
            )
            con.unregister(f"_tmp_{table_name}")
            loaded.append(table_name)

        return loaded
