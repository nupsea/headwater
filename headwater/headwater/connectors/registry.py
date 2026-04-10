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


def get_connector(source_type: str):
    """Return a connector instance for the given source type."""
    cls = _REGISTRY.get(source_type)
    if cls is None:
        raise ConnectorError(
            f"Unknown source type: {source_type}. Available: {list(_REGISTRY.keys())}"
        )
    return cls()


def register_connector(source_type: str, cls: type) -> None:
    """Register a custom connector class."""
    _REGISTRY[source_type] = cls
