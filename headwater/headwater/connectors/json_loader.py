"""JSON/NDJSON connector -- loads JSON files from a directory into DuckDB via Polars."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa

from headwater.core.exceptions import ConnectorError
from headwater.core.models import SourceConfig


class JsonLoader:
    """Loads NDJSON files from a directory into DuckDB tables.

    Each .json file becomes a table named after the file stem.
    Nested objects/arrays are stored as DuckDB JSON/STRUCT/LIST types.

    Implements BaseConnector: profile() and sample() operate on in-memory Polars
    DataFrames loaded from disk -- no bulk copy to a remote system.
    """

    def __init__(self) -> None:
        self._path: Path | None = None
        self._files: list[Path] = []
        # Cache loaded DataFrames to avoid re-reading for profile/sample
        self._frames: dict[str, pl.DataFrame] = {}

    def connect(self, config: SourceConfig) -> None:
        if config.path is None:
            raise ConnectorError("JsonLoader requires a path")
        self._path = Path(config.path)
        if not self._path.is_dir():
            raise ConnectorError(f"Path is not a directory: {self._path}")
        self._files = sorted(self._path.glob("*.json"))
        if not self._files:
            raise ConnectorError(f"No .json files found in {self._path}")

    def _get_frame(self, table_name: str) -> pl.DataFrame:
        """Load or return cached DataFrame for the given table name."""
        if table_name not in self._frames:
            if self._path is None:
                raise ConnectorError("Not connected -- call connect() first")
            fp = self._path / f"{table_name}.json"
            if not fp.exists():
                raise ConnectorError(f"No JSON file for table '{table_name}': {fp}")
            self._frames[table_name] = pl.read_ndjson(fp)
        return self._frames[table_name]

    def profile(self, table_name: str) -> dict:
        """Run Polars aggregate expressions and return column-level stats.

        Returns a dict keyed by column name. Each value contains:
        count, null_count, distinct_count, and (for numeric columns) min/max.
        """
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
            # Skip the generator script if it somehow has .json extension
            if table_name == "generate_sample":
                continue

            df = pl.read_ndjson(fp)
            # Cache the frame for profile/sample calls
            self._frames[table_name] = df
            # Register the Polars dataframe as a DuckDB view, then materialize
            con.register(f"_tmp_{table_name}", df)
            con.execute(
                f"CREATE OR REPLACE TABLE {schema}.{table_name} AS "
                f"SELECT * FROM _tmp_{table_name}"
            )
            con.unregister(f"_tmp_{table_name}")
            loaded.append(table_name)

        return loaded
