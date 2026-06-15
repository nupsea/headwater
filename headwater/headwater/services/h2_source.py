"""Headwater 2 source discovery and persistence helpers."""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import duckdb

from headwater.connectors.registry import get_connector
from headwater.core.models import DiscoveryResult, SourceConfig
from headwater.core.store import HeadwaterStore
from headwater.profiler.engine import discover
from headwater.profiler.schema import extract_schema
from headwater.profiler.stats import profile_all

logger = logging.getLogger(__name__)

# Source types whose data can be materialized + statistically profiled locally.
# Warehouses (postgres/mysql/snowflake/redshift) list + register schema only
# (statistical profiling via pushdown is a later phase).
_MATERIALIZABLE = frozenset({"json", "csv", "duckdb", "sqlite", "parquet"})

SourceType = Literal[
    "json",
    "csv",
    "duckdb",
    "sqlite",
    "parquet",
    "postgres",
    "mysql",
    "snowflake",
    "redshift",
]
_SOURCE_TYPES = {
    "json",
    "csv",
    "duckdb",
    "sqlite",
    "parquet",
    "postgres",
    "mysql",
    "snowflake",
    "redshift",
}

# Source types addressed by a connection URI/DSN rather than a local file path.
# These are connected (and later browsed/ingested) by URI; nothing is copied
# to a local path at discovery time.
_URI_TYPES = frozenset({"postgres", "mysql", "snowflake", "redshift"})

# URI scheme prefix -> source type, for auto-detection when no type is given.
_URI_SCHEMES: dict[str, SourceType] = {
    "postgres://": "postgres",
    "postgresql://": "postgres",
    "mysql://": "mysql",
    "snowflake://": "snowflake",
    "redshift://": "redshift",
    "redshift+iam://": "redshift",
}


@dataclass(slots=True)
class H2DiscoveryOutcome:
    """Discovery result plus persisted source snapshot metadata."""

    discovery: DiscoveryResult
    snapshot_id: str


def resolve_source_config(
    source: str,
    *,
    source_type: SourceType | None = None,
    name: str | None = None,
) -> SourceConfig:
    """Resolve a source path or DSN into a source config."""
    detected_type = source_type
    source_uri: str | None = None
    source_path: str | None = None

    if detected_type is None:
        for prefix, uri_type in _URI_SCHEMES.items():
            if source.startswith(prefix):
                detected_type = uri_type
                break
        else:
            data_path = Path(source).resolve()
            if data_path.is_dir():
                json_files = list(data_path.glob("*.json"))
                csv_files = list(data_path.glob("*.csv"))
                if json_files:
                    detected_type = "json"
                elif csv_files:
                    detected_type = "csv"
                else:
                    detected_type = "json"
            else:
                detected_type = "csv" if source.endswith(".csv") else "json"

    if name is None:
        if detected_type in _URI_TYPES:
            from urllib.parse import urlparse

            parsed = urlparse(source)
            name = parsed.hostname or str(detected_type)
        else:
            name = Path(source).resolve().name

    if detected_type in _URI_TYPES:
        source_uri = source
    else:
        source_path = str(Path(source).resolve())

    return SourceConfig(
        name=name,
        type=detected_type or "json",
        path=source_path,
        uri=source_uri,
        mode="generate",
    )


def discover_and_persist(
    source: str,
    *,
    store: HeadwaterStore,
    source_type: str | None = None,
    name: str | None = None,
    sample_size: int = 50_000,
) -> H2DiscoveryOutcome:
    """Run discovery and persist the source-owned H2 metadata."""
    config = resolve_source_config(
        source,
        source_type=_coerce_source_type(source_type),
        name=name,
    )
    connector = get_connector(config.type)
    connector.connect(config)

    con = duckdb.connect(":memory:")
    schema_name = config.name.replace("-", "_").replace(".", "_")
    if config.type in ("json", "csv", "duckdb", "sqlite"):
        connector.load_to_duckdb(con, schema_name)

    discovery = discover(con, schema_name, config, sample_size=sample_size)
    snapshot_id = f"{config.name}:{discovery.discovered_at.strftime('%Y%m%dT%H%M%S')}"

    store.upsert_source(config.name, config.type, config.path, config.uri)
    store.record_source_snapshot(
        config.name,
        snapshot_id,
        fingerprint=snapshot_id,
        payload={
            "table_count": len(discovery.tables),
            "profile_count": len(discovery.profiles),
            "relationship_count": len(discovery.relationships),
        },
    )

    for table in discovery.tables:
        store.upsert_table(
            config.name,
            table.name,
            schema_name=table.schema_name,
            row_count=table.row_count,
            description=table.description,
            domain=table.domain,
            selected=True,
        )
        for ordinal, column in enumerate(table.columns):
            store.upsert_column(
                config.name,
                table.name,
                column.name,
                column.dtype,
                nullable=column.nullable,
                is_primary_key=column.is_primary_key,
                description=column.description,
                semantic_type=column.semantic_type,
                ordinal=ordinal,
                locked=column.locked,
            )

    for profile in discovery.profiles:
        store.upsert_profile(
            config.name,
            profile.table_name,
            profile.column_name,
            profile.dtype,
            profile.model_dump(),
            snapshot_id=snapshot_id,
        )

    for relationship in discovery.relationships:
        store.insert_relationship(
            config.name,
            relationship.from_table,
            relationship.from_column,
            relationship.to_table,
            relationship.to_column,
            relationship.type,
            relationship.confidence,
            relationship.referential_integrity,
            snapshot_id=snapshot_id,
        )

    return H2DiscoveryOutcome(discovery=discovery, snapshot_id=snapshot_id)


def _config_from_source(src: dict[str, Any]) -> SourceConfig:
    return SourceConfig(
        name=src["name"],
        type=src["type"],
        path=src.get("path"),
        uri=src.get("uri"),
        mode="generate",
    )


def _schema_of(table_ref: str) -> str | None:
    # Warehouse list_tables yields schema-qualified names ("schema.table" or
    # "db.schema.table"); embedded sources yield bare names.
    return table_ref.rsplit(".", 1)[0] if "." in table_ref else None


def list_source_tables(store: HeadwaterStore, source_name: str) -> list[dict[str, Any]]:
    """List a source's tables WITHOUT ingesting or profiling them.

    Cheap catalog browse for large corpora (a warehouse with thousands of tables):
    uses the connector's ``list_tables`` + ``estimate_row_count`` (metadata
    pushdown) so the analyst can search and pick a project-relevant subset before
    anything is ingested.  ``ingested`` flags tables already pulled in.
    """
    src = store.get_source(source_name)
    if src is None:
        raise ValueError(f"Source '{source_name}' is not registered.")
    config = _config_from_source(src)
    connector = get_connector(config.type)
    connector.connect(config)
    try:
        names: list[str] = list(connector.list_tables())
        est: dict[str, int | None] = {}
        if hasattr(connector, "estimate_row_count"):
            for n in names:
                with contextlib.suppress(Exception):
                    est[n] = connector.estimate_row_count(n)
    finally:
        with contextlib.suppress(Exception):
            close = getattr(connector, "close", None)
            if callable(close):
                close()

    ingested = {t["name"] for t in store.get_tables(source_name)}
    return [
        {
            "table": n,
            "schema": _schema_of(n),
            "est_rows": est.get(n),
            "ingested": n in ingested,
        }
        for n in names
    ]


def ingest_tables(
    store: HeadwaterStore,
    source_name: str,
    table_names: list[str],
    *,
    sample_size: int = 50_000,
) -> dict[str, Any]:
    """Ingest ONLY the named subset of a source's tables.

    Embedded sources are materialized once and the selected tables are
    statistically profiled; warehouse sources register the selected tables +
    columns from metadata (``list_columns``) — schema only, no stats yet.  Lets a
    project pull just what it needs (e.g. the churn tables) from a big source.

    Returns ``ingested`` (table names that landed) and ``failed``
    (``[{"table", "error"}]``) — a table that cannot be registered is reported,
    never silently dropped.
    """
    from headwater.core.redaction import redact_secrets

    src = store.get_source(source_name)
    if src is None:
        raise ValueError(f"Source '{source_name}' is not registered.")
    wanted = list(dict.fromkeys(table_names))  # de-dupe, preserve order
    if not wanted:
        return {"ingested": [], "failed": [], "profiled": False, "snapshot_id": None}

    config = _config_from_source(src)
    connector = get_connector(config.type)
    connector.connect(config)
    schema_name = config.name.replace("-", "_").replace(".", "_")
    snapshot_id = f"{config.name}:{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
    profiled = config.type in _MATERIALIZABLE
    done: list[str] = []
    failed: list[dict[str, str]] = []
    unprofiled: list[dict[str, str]] = []  # registered fine, but stats failed

    try:
        if profiled:
            con = duckdb.connect(":memory:")
            try:
                connector.load_to_duckdb(con, schema_name)
                all_tables = extract_schema(con, schema_name)
                selected = [t for t in all_tables if t.name in set(wanted)]
                profiles = profile_all(con, schema_name, selected, sample_size)
            finally:
                con.close()
            found = {t.name for t in selected}
            failed.extend(
                {"table": n, "error": "Table not found in this source."}
                for n in wanted
                if n not in found
            )
            for table in selected:
                store.upsert_table(
                    config.name,
                    table.name,
                    schema_name=table.schema_name,
                    row_count=table.row_count,
                    description=table.description,
                    domain=table.domain,
                    selected=True,
                )
                for ordinal, column in enumerate(table.columns):
                    store.upsert_column(
                        config.name,
                        table.name,
                        column.name,
                        column.dtype,
                        nullable=column.nullable,
                        is_primary_key=column.is_primary_key,
                        description=column.description,
                        semantic_type=column.semantic_type,
                        ordinal=ordinal,
                        locked=column.locked,
                    )
                done.append(table.name)
            for profile in profiles:
                store.upsert_profile(
                    config.name,
                    profile.table_name,
                    profile.column_name,
                    profile.dtype,
                    profile.model_dump(),
                    snapshot_id=snapshot_id,
                )
        else:
            # Warehouse: register schema (tables + columns) from metadata only.
            # One table failing (permissions, drop race) must not lose the rest.
            for name in wanted:
                try:
                    cols = connector.list_columns(name)
                    if not cols:
                        failed.append(
                            {
                                "table": name,
                                "error": "No columns visible — check schema name and permissions.",
                            }
                        )
                        continue
                    est = None
                    if hasattr(connector, "estimate_row_count"):
                        try:
                            est = connector.estimate_row_count(name)
                        except Exception as exc:
                            logger.warning(
                                "Row estimate failed for %s.%s: %s",
                                config.name,
                                name,
                                redact_secrets(str(exc)),
                            )
                    # Pushdown stats (one bounded aggregate query, no rows
                    # transferred) so readiness/relevance/descriptions reason
                    # over real data, not just names. Best-effort: a slow or
                    # denied profile never blocks registration — but the failure
                    # is reported, never hidden (it is the difference between
                    # "0 rows" and "rows unknown").
                    stats: dict[str, dict[str, Any]] = {}
                    if hasattr(connector, "profile"):
                        try:
                            if hasattr(connector, "set_statement_timeout"):
                                with contextlib.suppress(Exception):
                                    connector.set_statement_timeout(60)
                            stats = connector.profile(name) or {}
                        except Exception as exc:
                            reason = redact_secrets(str(exc)) or exc.__class__.__name__
                            logger.warning(
                                "Profile failed for %s.%s: %s", config.name, name, reason
                            )
                            unprofiled.append({"table": name, "error": reason})
                    row_count = next(
                        (
                            int(s["row_count"])
                            for s in stats.values()
                            if s.get("row_count") is not None
                        ),
                        int(est) if est is not None else 0,
                    )
                    store.upsert_table(
                        config.name,
                        name,
                        schema_name=_schema_of(name),
                        row_count=row_count,
                        description=None,
                        domain=None,
                        selected=True,
                    )
                    for ordinal, c in enumerate(cols):
                        dtype = c.get("data_type") or "varchar"
                        store.upsert_column(
                            config.name,
                            name,
                            c["name"],
                            dtype,
                            nullable=bool(c.get("is_nullable", True)),
                            is_primary_key=False,
                            description=None,
                            semantic_type=None,
                            ordinal=int(c.get("ordinal_position", ordinal)),
                            locked=False,
                        )
                        col_stats = stats.get(c["name"])
                        if col_stats:
                            store.upsert_profile(
                                config.name,
                                name,
                                c["name"],
                                dtype,
                                _profile_from_pushdown_stats(
                                    name, c["name"], dtype, col_stats
                                ),
                                snapshot_id=snapshot_id,
                            )
                    done.append(name)
                except Exception as exc:
                    failed.append(
                        {
                            "table": name,
                            "error": redact_secrets(str(exc)) or exc.__class__.__name__,
                        }
                    )
    finally:
        with contextlib.suppress(Exception):
            close = getattr(connector, "close", None)
            if callable(close):
                close()

    store.upsert_source(config.name, config.type, config.path, config.uri)
    store.record_source_snapshot(
        config.name,
        snapshot_id,
        fingerprint=snapshot_id,
        payload={
            "ingested_tables": done,
            "failed_tables": [f["table"] for f in failed],
            "unprofiled_tables": [u["table"] for u in unprofiled],
            "profiled": profiled,
        },
    )
    return {
        "ingested": done,
        "failed": failed,
        "unprofiled": unprofiled,
        "profiled": profiled,
        "snapshot_id": snapshot_id,
    }


# "number" covers Snowflake's NUMBER family.
_NUMERIC_DTYPES = (
    "int", "numeric", "decimal", "float", "double", "real", "bigint", "number",
)
_TEMPORAL_DTYPES = ("date", "time")


def _profile_from_pushdown_stats(
    table_name: str, column_name: str, dtype: str, stats: dict[str, Any]
) -> dict[str, Any]:
    """Map a connector's pushdown aggregates onto the ColumnProfile shape.

    Warehouse profile() returns row_count/null_count/distinct_count plus min/max
    cast to varchar; numeric/temporal min-max are recovered by dtype so the
    catalog, readiness checks, and key suggestion read warehouse profiles the
    same way as locally profiled ones.
    """
    row_count = int(stats.get("row_count") or 0)
    null_count = int(stats.get("null_count") or 0)
    distinct = int(stats.get("distinct_count") or 0)
    profile: dict[str, Any] = {
        "table_name": table_name,
        "column_name": column_name,
        "dtype": dtype,
        "null_count": null_count,
        "null_rate": (null_count / row_count) if row_count else 0.0,
        "distinct_count": distinct,
        "uniqueness_ratio": (distinct / row_count) if row_count else 0.0,
    }
    lo, hi = stats.get("min"), stats.get("max")
    dtype_l = dtype.lower()
    if lo is not None and hi is not None:
        if any(k in dtype_l for k in _NUMERIC_DTYPES):
            with contextlib.suppress(ValueError, TypeError):
                profile["min_value"] = float(lo)
                profile["max_value"] = float(hi)
        elif any(k in dtype_l for k in _TEMPORAL_DTYPES):
            profile["min_date"] = str(lo)
            profile["max_date"] = str(hi)
    return profile


def _coerce_source_type(source_type: str | None) -> SourceType | None:
    if source_type is None:
        return None
    if source_type not in _SOURCE_TYPES:
        raise ValueError(f"Unsupported source type for H2 discovery: {source_type}")
    return cast(SourceType, source_type)
