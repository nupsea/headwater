"""Connector implementations must satisfy the shared capability contract."""

from __future__ import annotations

from pathlib import Path

from headwater.connectors.csv_loader import CsvLoader
from headwater.connectors.json_loader import JsonLoader
from headwater.core.models import SourceConfig
from tests.connectors.contract import assert_connector_contract

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sample"


def test_json_connector_contract():
    assert_connector_contract(
        JsonLoader(),
        SourceConfig(name="sample", type="json", path=str(SAMPLE_DIR)),
        expected_tables={"zones", "sites"},
        expected_columns={
            "zones": {"zone_id", "population"},
            "sites": {"site_id", "zone_id"},
        },
    )


def test_csv_connector_contract(tmp_path: Path):
    (tmp_path / "users.csv").write_text(
        "user_id,email,age\n1,a@example.com,30\n2,b@example.com,41\n",
        encoding="utf-8",
    )
    (tmp_path / "orders.csv").write_text(
        "order_id,user_id,amount\n10,1,20.5\n11,2,31.0\n",
        encoding="utf-8",
    )

    assert_connector_contract(
        CsvLoader(),
        SourceConfig(name="csv_sample", type="csv", path=str(tmp_path)),
        expected_tables={"users", "orders"},
        expected_columns={
            "users": {"user_id", "email"},
            "orders": {"order_id", "amount"},
        },
    )
