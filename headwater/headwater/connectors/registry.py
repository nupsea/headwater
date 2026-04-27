"""Connector registry -- maps source type strings to connector classes."""

from __future__ import annotations

from headwater.connectors.csv_loader import CsvLoader
from headwater.connectors.json_loader import JsonLoader
from headwater.connectors.postgres_loader import PostgresConnector
from headwater.core.exceptions import ConnectorError

_REGISTRY: dict[str, type] = {
    "json": JsonLoader,
    "csv": CsvLoader,
    "postgres": PostgresConnector,
}

# Catalog shown in the UI connector picker. `supported: True` means we can
# actually connect; `False` means the connector wizard accepts the choice but
# the backend will return a "not yet implemented" error.
CONNECTOR_CATALOG: list[dict] = [
    {
        "id": "postgres",
        "name": "PostgreSQL",
        "category": "OLTP",
        "color": "#336791",
        "glyph": "P",
        "supported": True,
    },
    {
        "id": "mysql",
        "name": "MySQL",
        "category": "OLTP",
        "color": "#4479a1",
        "glyph": "M",
        "supported": False,
    },
    {
        "id": "snowflake",
        "name": "Snowflake",
        "category": "Warehouse",
        "color": "#29b5e8",
        "glyph": "S",
        "supported": False,
    },
    {
        "id": "bigquery",
        "name": "BigQuery",
        "category": "Warehouse",
        "color": "#4285f4",
        "glyph": "BQ",
        "supported": False,
    },
    {
        "id": "redshift",
        "name": "Redshift",
        "category": "Warehouse",
        "color": "#cc2b5e",
        "glyph": "R",
        "supported": False,
    },
    {
        "id": "databricks",
        "name": "Databricks",
        "category": "Lakehouse",
        "color": "#ff3621",
        "glyph": "DB",
        "supported": False,
    },
    {
        "id": "sqlserver",
        "name": "SQL Server",
        "category": "OLTP",
        "color": "#a91d22",
        "glyph": "SQ",
        "supported": False,
    },
    {
        "id": "duckdb",
        "name": "DuckDB",
        "category": "Embedded",
        "color": "#fff100",
        "glyph": "D",
        "supported": False,
        "lightGlyph": True,
    },
    {
        "id": "oracle",
        "name": "Oracle",
        "category": "OLTP",
        "color": "#f80000",
        "glyph": "O",
        "supported": False,
    },
    {
        "id": "clickhouse",
        "name": "ClickHouse",
        "category": "OLAP",
        "color": "#ffcc00",
        "glyph": "CH",
        "supported": False,
        "lightGlyph": True,
    },
    {
        "id": "sqlite",
        "name": "SQLite",
        "category": "Embedded",
        "color": "#003b57",
        "glyph": "SL",
        "supported": False,
    },
    {
        "id": "trino",
        "name": "Trino",
        "category": "Federated",
        "color": "#dd00a1",
        "glyph": "T",
        "supported": False,
    },
    {
        "id": "json",
        "name": "JSON files",
        "category": "Files",
        "color": "#64748b",
        "glyph": "JS",
        "supported": True,
    },
    {
        "id": "csv",
        "name": "CSV files",
        "category": "Files",
        "color": "#64748b",
        "glyph": "CS",
        "supported": True,
    },
]


def get_connector(source_type: str):
    """Return a connector instance for the given source type."""
    cls = _REGISTRY.get(source_type)
    if cls is None:
        # Distinguish "known but not implemented" from "totally unknown" --
        # both raise the same exception class, but with different detail.
        catalogued = next((c for c in CONNECTOR_CATALOG if c["id"] == source_type), None)
        if catalogued is not None:
            raise ConnectorError(
                f"Connector '{catalogued['name']}' is in the picker but not yet implemented. "
                "Pick a supported source type for now."
            )
        raise ConnectorError(
            f"Unknown source type: {source_type}. Available: {list(_REGISTRY.keys())}"
        )
    return cls()


def register_connector(source_type: str, cls: type) -> None:
    """Register a custom connector class."""
    _REGISTRY[source_type] = cls


def list_connector_catalog() -> list[dict]:
    """Return the full connector picker catalog used by the UI."""
    return [dict(c) for c in CONNECTOR_CATALOG]
