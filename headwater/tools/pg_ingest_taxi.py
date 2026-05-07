#!/usr/bin/env python3
"""Load NYC TLC Parquet trip files into Postgres."""

from __future__ import annotations

import argparse
import io
import re
import time
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


DEFAULT_DSN = "postgresql://headwater_taxi:headwater_taxi@localhost:5435/ny_taxi"
DEFAULT_DATA_DIR = "/Users/sethurama/DEV/LM/OPEN_DATA/NY_TAXI"


def snake_case(name: str) -> str:
    name = name.replace("ID", "Id")
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = name.strip("_").lower()
    name = name.replace("pu_location_id", "pickup_location_id")
    name = name.replace("do_location_id", "dropoff_location_id")
    return name


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def postgres_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_int32(arrow_type):
        return "integer"
    if pa.types.is_int64(arrow_type):
        return "bigint"
    if pa.types.is_floating(arrow_type):
        return "double precision"
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "text"
    return "text"


def connect_with_retry(dsn: str, attempts: int = 30):
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return psycopg2.connect(dsn)
        except psycopg2.OperationalError as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"Postgres was not ready after {attempts} attempts") from last_error


def create_table(cur, schema: str, table: str, arrow_schema: pa.Schema) -> list[str]:
    column_names = [snake_case(name) for name in arrow_schema.names]
    columns_sql = [
        f"{quote_ident(name)} {postgres_type(field.type)}"
        for name, field in zip(column_names, arrow_schema)
    ]
    cur.execute(f"DROP TABLE IF EXISTS {quote_ident(schema)}.{quote_ident(table)}")
    cur.execute(f"CREATE TABLE {quote_ident(schema)}.{quote_ident(table)} ({', '.join(columns_sql)})")
    return column_names


def copy_batch(cur, schema: str, table: str, batch: pa.RecordBatch, column_names: list[str]) -> int:
    table_batch = pa.Table.from_batches([batch]).rename_columns(column_names)
    buf = io.BytesIO()
    pacsv.write_csv(table_batch, buf)
    buf.seek(0)
    columns_sql = ", ".join(quote_ident(name) for name in column_names)
    copy_sql = (
        f"COPY {quote_ident(schema)}.{quote_ident(table)} ({columns_sql}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )
    cur.copy_expert(copy_sql, io.TextIOWrapper(buf, encoding="utf-8"))
    return batch.num_rows


def create_indexes(cur, schema: str, table: str, columns: list[str]) -> None:
    index_candidates = [
        "pickup_datetime",
        "dropoff_datetime",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "request_datetime",
        "pickup_location_id",
        "dropoff_location_id",
    ]
    available = set(columns)
    for column in index_candidates:
        if column not in available:
            continue
        index_name = f"idx_{table}_{column}"[:63]
        cur.execute(
            f"CREATE INDEX {quote_ident(index_name)} "
            f"ON {quote_ident(schema)}.{quote_ident(table)} ({quote_ident(column)})"
        )


def ingest_file(conn, parquet_path: Path, schema: str, batch_size: int) -> int:
    table = parquet_path.stem.replace("-", "_")
    parquet_file = pq.ParquetFile(parquet_path)
    with conn.cursor() as cur:
        columns = create_table(cur, schema, table, parquet_file.schema_arrow)
    conn.commit()

    total_rows = 0
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        with conn.cursor() as cur:
            total_rows += copy_batch(cur, schema, table, batch, columns)
        conn.commit()

    with conn.cursor() as cur:
        create_indexes(cur, schema, table, columns)
        cur.execute(f"ANALYZE {quote_ident(schema)}.{quote_ident(table)}")
    conn.commit()
    return total_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--schema", default="tlc_raw")
    parser.add_argument("--batch-size", type=int, default=100_000)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    files = sorted(data_dir.glob("*.parquet"))
    if not files:
        raise SystemExit(f"No parquet files found under {data_dir}")

    conn = connect_with_retry(args.dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {quote_ident(args.schema)} CASCADE")
            cur.execute(f"CREATE SCHEMA {quote_ident(args.schema)}")
        conn.commit()

        for parquet_path in files:
            rows = ingest_file(conn, parquet_path, args.schema, args.batch_size)
            print(f"loaded {args.schema}.{parquet_path.stem.replace('-', '_')}: {rows:,} rows", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
