"""pg_ingest.py -- Load real-world CSV data into a local Postgres database.

Uses Polars to read each CSV (lazy scan -> collect), infers schema, snake_cases
column names, and uses psycopg2 copy_expert for fast bulk load.

Usage:
    python tools/pg_ingest.py [--dsn DSN] [--schema SCHEMA] [--no-drop]
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import time
from pathlib import Path

import polars as pl
import psycopg2
import psycopg2.extensions

# ---------------------------------------------------------------------------
# File-to-table mapping
# ---------------------------------------------------------------------------

_REAL_WORLD_DIR = Path(__file__).resolve().parent.parent / "data" / "real_world"

_FILES: list[tuple[Path, str]] = [
    (_REAL_WORLD_DIR / "air_quality" / "annual_aqi_by_county_2023.csv", "aqi_by_county"),
    (_REAL_WORLD_DIR / "air_quality" / "aqs_monitors_sampled.csv", "aqs_monitors"),
    (_REAL_WORLD_DIR / "air_quality" / "aqs_sites_clean.csv", "aqs_sites"),
    (_REAL_WORLD_DIR / "air_quality" / "daily_pm25_2023_sampled.csv", "daily_pm25"),
    (_REAL_WORLD_DIR / "complaints" / "nyc_env_complaints_sampled.csv", "env_complaints"),
    (_REAL_WORLD_DIR / "programs" / "epa_ej_grants.csv", "ej_grants"),
    (_REAL_WORLD_DIR / "water_quality" / "water_stations_md.csv", "water_stations"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SNAKE_RE = re.compile(r"[^a-z0-9_]")


def _snake(name: str) -> str:
    """Convert column name to snake_case: lowercase, replace non-alphanum with _, strip leading/trailing _."""
    s = name.strip().lower()
    s = s.replace(" ", "_")
    s = _SNAKE_RE.sub("_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "col"


def _polars_to_pg_type(dtype: pl.DataType) -> str:
    """Map a Polars dtype to a Postgres column type."""
    if dtype == pl.Int8 or dtype == pl.Int16 or dtype == pl.Int32:
        return "INTEGER"
    if dtype == pl.Int64:
        return "BIGINT"
    if dtype == pl.UInt8 or dtype == pl.UInt16 or dtype == pl.UInt32:
        return "INTEGER"
    if dtype == pl.UInt64:
        return "BIGINT"
    if dtype == pl.Float32 or dtype == pl.Float64:
        return "DOUBLE PRECISION"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.Date:
        return "DATE"
    if isinstance(dtype, pl.Datetime):
        return "TIMESTAMP"
    # String, Utf8, Null, Unknown, Categorical, everything else
    return "TEXT"


def _build_create_table(table: str, schema: str, df: pl.DataFrame) -> str:
    """Build a CREATE TABLE statement for the given DataFrame."""
    col_defs = []
    for name, dtype in zip(df.columns, df.dtypes):
        safe_name = _snake(name)
        pg_type = _polars_to_pg_type(dtype)
        col_defs.append(f'    "{safe_name}" {pg_type}')
    cols = ",\n".join(col_defs)
    return f'CREATE TABLE "{schema}"."{table}" (\n{cols}\n)'


def _df_to_copy_buffer(df: pl.DataFrame) -> io.StringIO:
    """Serialize a Polars DataFrame to a tab-separated StringIO buffer for COPY."""
    buf = io.StringIO()
    # Write rows as tab-separated values; handle None as \N (Postgres NULL marker)
    for row in df.iter_rows():
        parts = []
        for val in row:
            if val is None:
                parts.append("\\N")
            else:
                # Escape tab, newline, backslash
                s = str(val)
                s = s.replace("\\", "\\\\")
                s = s.replace("\t", "\\t")
                s = s.replace("\n", "\\n")
                s = s.replace("\r", "\\r")
                parts.append(s)
        buf.write("\t".join(parts) + "\n")
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# Core ingest logic
# ---------------------------------------------------------------------------


def _read_csv(csv_path: Path) -> pl.DataFrame:
    """Read a CSV with progressive fallback on schema inference failures.

    Attempt order:
    1. infer_schema_length=10_000 (fast, works for most files)
    2. infer_schema_length=None   (full-file scan, catches mixed-type columns)
    3. all columns as String      (always works, Postgres receives TEXT)
    """
    for kwargs in [
        {"infer_schema_length": 10_000, "try_parse_dates": True},
        {"infer_schema_length": None, "try_parse_dates": True},
        {"infer_schema_length": None, "try_parse_dates": False, "schema_overrides": {"*": pl.String}},
    ]:
        try:
            schema_overrides = kwargs.pop("schema_overrides", None)
            if schema_overrides:
                # Force all columns to String by reading without type inference
                return pl.read_csv(str(csv_path), infer_schema_length=0)
            return pl.scan_csv(str(csv_path), **kwargs).collect()
        except Exception:
            continue
    # Should never reach here; read_csv with infer_schema_length=0 always succeeds
    return pl.read_csv(str(csv_path), infer_schema_length=0)


def ingest_file(
    conn: psycopg2.extensions.connection,
    csv_path: Path,
    table_name: str,
    schema: str,
    drop: bool,
) -> dict:
    """Read one CSV and load it into Postgres. Returns stats dict."""
    t0 = time.monotonic()

    df = _read_csv(csv_path)

    # Rename columns to snake_case
    rename_map = {col: _snake(col) for col in df.columns}
    df = df.rename(rename_map)

    # Deduplicate column names (can happen after snake_casing)
    seen: dict[str, int] = {}
    deduped_cols = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            deduped_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            deduped_cols.append(col)
    if deduped_cols != df.columns:
        df = df.rename(dict(zip(df.columns, deduped_cols)))

    row_count = len(df)
    col_count = len(df.columns)

    with conn.cursor() as cur:
        if drop:
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"')

        ddl = _build_create_table(table_name, schema, df)
        cur.execute(ddl)

        if row_count > 0:
            col_list = ", ".join(f'"{c}"' for c in df.columns)
            copy_sql = f'COPY "{schema}"."{table_name}" ({col_list}) FROM STDIN WITH (FORMAT text, NULL \'\\N\')'
            buf = _df_to_copy_buffer(df)
            cur.copy_expert(copy_sql, buf)

    conn.commit()
    elapsed = time.monotonic() - t0
    return {"rows": row_count, "cols": col_count, "seconds": elapsed}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load real-world CSV data into a local Postgres database."
    )
    parser.add_argument(
        "--dsn",
        default="postgresql://headwater:headwater@localhost:5434/headwater_dev",
        help="Postgres DSN (default: postgresql://headwater:headwater@localhost:5434/headwater_dev)",
    )
    parser.add_argument(
        "--schema",
        default="public",
        help="Postgres schema to load tables into (default: public)",
    )
    parser.add_argument(
        "--no-drop",
        action="store_true",
        dest="no_drop",
        help="Do not drop and recreate tables (default: drop and recreate)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    drop = not args.no_drop

    print(f"Connecting to {args.dsn} ...")
    try:
        conn = psycopg2.connect(args.dsn)
    except psycopg2.OperationalError as exc:
        print(f"ERROR: Cannot connect to Postgres: {exc}", file=sys.stderr)
        sys.exit(1)

    # Ensure schema exists
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{args.schema}"')
    conn.commit()

    total_rows = 0
    t_all = time.monotonic()

    print(f"\nLoading {len(_FILES)} tables into schema '{args.schema}':\n")

    for csv_path, table_name in _FILES:
        if not csv_path.exists():
            print(f"  SKIP  {table_name:30s}  (file not found: {csv_path})")
            continue
        try:
            stats = ingest_file(conn, csv_path, table_name, args.schema, drop)
            total_rows += stats["rows"]
            print(
                f"  OK    {table_name:30s}  "
                f"{stats['rows']:>8,} rows  "
                f"{stats['cols']:>3} cols  "
                f"{stats['seconds']:.2f}s"
            )
        except Exception as exc:
            conn.rollback()
            print(f"  ERROR {table_name:30s}  {exc}", file=sys.stderr)

    conn.close()

    total_time = time.monotonic() - t_all
    print(f"\nSummary: {total_rows:,} total rows loaded in {total_time:.2f}s")


if __name__ == "__main__":
    main()
