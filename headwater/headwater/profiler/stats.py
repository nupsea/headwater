"""Polars-based statistical profiler for DuckDB tables."""

from __future__ import annotations

import re

import duckdb
import polars as pl

from headwater.core.models import ColumnProfile, TableInfo

# Regex patterns for column value detection
_PATTERNS = {
    "email": re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]{2,}$"),
    "uuid": re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I),
    "phone": re.compile(r"^\+?[\d\s\-().]{7,20}$"),
    "iso_date": re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?"),
    "url": re.compile(r"^https?://", re.I),
    "hex_id": re.compile(r"^[0-9a-f]{8,}$", re.I),
}

_NUMERIC_DTYPES = {"int64", "float64"}
_STRING_DTYPES = {"varchar"}
_TEMPORAL_DTYPES = {"date", "timestamp"}


def profile_table(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: TableInfo,
    sample_size: int = 50_000,
) -> list[ColumnProfile]:
    """Profile all columns in a table. Returns a list of ColumnProfile."""
    row_count = table.row_count

    # Sample if table is large
    if row_count > sample_size:
        query = f'SELECT * FROM "{schema}"."{table.name}" USING SAMPLE {sample_size} ROWS'
    else:
        query = f'SELECT * FROM "{schema}"."{table.name}"'

    arrow_table = con.execute(query).to_arrow_table()
    df = pl.from_arrow(arrow_table)

    profiles: list[ColumnProfile] = []
    for col_info in table.columns:
        col_name = col_info.name
        dtype = col_info.dtype

        # Skip complex types (json, list, struct) for now
        if dtype in ("json", "list"):
            profiles.append(
                ColumnProfile(
                    table_name=table.name,
                    column_name=col_name,
                    dtype=dtype,
                )
            )
            continue

        if col_name not in df.columns:
            continue

        # Also skip if the Polars dtype is a complex type (List, Struct, Object)
        series = df[col_name]
        pl_dtype = series.dtype
        if isinstance(pl_dtype, (pl.List, pl.Array, pl.Struct, pl.Object)):
            profiles.append(
                ColumnProfile(
                    table_name=table.name,
                    column_name=col_name,
                    dtype=dtype,
                )
            )
            continue

        profile = _profile_column(table.name, col_name, dtype, series)
        profiles.append(profile)

    return profiles


def _profile_column(table_name: str, col_name: str, dtype: str, series: pl.Series) -> ColumnProfile:
    """Compute stats for a single column."""
    total = len(series)
    null_count = series.null_count()
    null_rate = null_count / total if total > 0 else 0.0

    # Drop nulls for distinct/value analysis
    non_null = series.drop_nulls()
    distinct_count = non_null.n_unique() if len(non_null) > 0 else 0
    non_null_count = len(non_null)
    uniqueness_ratio = distinct_count / non_null_count if non_null_count > 0 else 0.0

    profile = ColumnProfile(
        table_name=table_name,
        column_name=col_name,
        dtype=dtype,
        null_count=null_count,
        null_rate=round(null_rate, 4),
        distinct_count=distinct_count,
        uniqueness_ratio=round(uniqueness_ratio, 4),
    )

    if dtype in _NUMERIC_DTYPES and non_null_count > 0:
        _add_numeric_stats(profile, non_null)
    elif dtype in _STRING_DTYPES and non_null_count > 0:
        _add_string_stats(profile, non_null)
    elif dtype in _TEMPORAL_DTYPES and non_null_count > 0:
        _add_temporal_stats(profile, non_null)

    return profile


def _add_numeric_stats(profile: ColumnProfile, series: pl.Series) -> None:
    """Add numeric statistics to profile."""
    try:
        cast = series.cast(pl.Float64, strict=False).drop_nulls()
        if len(cast) == 0:
            return
        profile.min_value = float(cast.min())  # type: ignore[arg-type]
        profile.max_value = float(cast.max())  # type: ignore[arg-type]
        profile.mean = round(float(cast.mean()), 4)  # type: ignore[arg-type]
        profile.median = round(float(cast.median()), 4)  # type: ignore[arg-type]
        profile.stddev = round(float(cast.std()), 4) if len(cast) > 1 else 0.0  # type: ignore[arg-type]
        profile.p25 = float(cast.quantile(0.25))  # type: ignore[arg-type]
        profile.p75 = float(cast.quantile(0.75))  # type: ignore[arg-type]
        profile.p95 = float(cast.quantile(0.95))  # type: ignore[arg-type]
    except Exception:
        pass  # Some numeric columns may have non-castable values


def _add_string_stats(profile: ColumnProfile, series: pl.Series) -> None:
    """Add string statistics and pattern detection to profile."""
    cast = series.cast(pl.String, strict=False).drop_nulls()
    if len(cast) == 0:
        return

    lengths = cast.str.len_chars()
    profile.min_length = int(lengths.min())  # type: ignore[arg-type]
    profile.max_length = int(lengths.max())  # type: ignore[arg-type]
    profile.avg_length = round(float(lengths.mean()), 1)  # type: ignore[arg-type]

    # Top values for low-cardinality columns
    if profile.distinct_count <= 100:
        vc = cast.value_counts().sort("count", descending=True).head(20)
        profile.top_values = [(str(row[0]), int(row[1])) for row in vc.iter_rows()]

    # Pattern detection on a sample
    sample = cast.head(200).to_list()
    profile.detected_pattern = _detect_pattern(sample)


def _add_temporal_stats(profile: ColumnProfile, series: pl.Series) -> None:
    """Add temporal statistics to profile."""
    try:
        profile.min_date = str(series.min())
        profile.max_date = str(series.max())
    except Exception:
        pass


def _detect_pattern(values: list[str]) -> str | None:
    """Detect a common pattern in string values. Returns pattern name or None."""
    if not values:
        return None

    # Test each pattern against a sample
    for name, regex in _PATTERNS.items():
        matches = sum(1 for v in values[:100] if regex.match(str(v)))
        if matches / min(len(values), 100) > 0.7:
            return name

    return None


def profile_all(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    tables: list[TableInfo],
    sample_size: int = 50_000,
) -> list[ColumnProfile]:
    """Profile all columns across all tables."""
    all_profiles: list[ColumnProfile] = []
    for table in tables:
        profiles = profile_table(con, schema, table, sample_size)
        all_profiles.extend(profiles)
    return all_profiles
