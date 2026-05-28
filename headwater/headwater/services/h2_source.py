"""Headwater 2 source discovery and persistence helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

import duckdb

from headwater.connectors.registry import get_connector
from headwater.core.models import DiscoveryResult, SourceConfig
from headwater.core.store import HeadwaterStore
from headwater.profiler.engine import discover

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
        if source.startswith("postgres://") or source.startswith("postgresql://"):
            detected_type = "postgres"
        elif source.startswith("mysql://"):
            detected_type = "mysql"
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
        if detected_type == "postgres":
            from urllib.parse import urlparse

            parsed = urlparse(source)
            name = parsed.hostname or "postgres"
        else:
            name = Path(source).resolve().name

    if detected_type == "postgres":
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


def _coerce_source_type(source_type: str | None) -> SourceType | None:
    if source_type is None:
        return None
    if source_type not in _SOURCE_TYPES:
        raise ValueError(f"Unsupported source type for H2 discovery: {source_type}")
    return cast(SourceType, source_type)
