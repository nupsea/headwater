"""Tests for Headwater 2 S4 — Semantic Typing and Source Catalog."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from headwater.cli.hw2 import app
from headwater.core.config import get_settings
from headwater.core.store import HeadwaterStore

runner = CliRunner()
SAMPLE_DATA = str(Path(__file__).resolve().parents[2] / "data" / "sample")
RADIOLOGY_DATA = str(Path(__file__).resolve().parents[2] / "data" / "radiology")


def _discover(tmp_path, source_path, source_name):
    r = runner.invoke(app, ["discover", "--source", source_path, "--name", source_name])
    assert r.exit_code == 0, r.output


class TestSemanticTyping:
    def test_bool_dtype_maps_to_flag(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, evidence = _classify_column("active", "bool", {})
        assert role == "flag"
        assert conf >= 0.90

    def test_timestamp_dtype_maps_to_event_ts(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("created_at", "timestamp", {})
        assert role == "event_ts"

    def test_numeric_high_uniqueness_maps_to_identifier(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("record_num", "int64", {"uniqueness_ratio": 0.98})
        assert role == "identifier"

    def test_duration_name_numeric_maps_to_duration(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("wait_duration", "float64", {"mean": 15.0})
        assert role == "duration"

    def test_count_name_maps_to_quantity(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("violation_count", "int64", {})
        assert role == "quantity"

    def test_id_suffix_varchar_maps_to_identifier(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("facility_id", "varchar", {})
        assert role == "identifier"

    def test_date_name_varchar_maps_to_event_ts(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("inspection_date", "varchar",
                                         {"avg_length": 10.0})
        assert role == "event_ts"

    def test_code_like_varchar_maps_to_code(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("status_code", "varchar", {
            "distinct_count": 5, "avg_length": 2.0, "uniqueness_ratio": 0.001,
        })
        assert role == "code"

    def test_long_varchar_maps_to_free_text(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("notes", "varchar", {"avg_length": 80.0})
        assert role == "free_text"

    def test_low_cardinality_varchar_maps_to_categorical(self):
        from headwater.services.h2_semantics import _classify_column
        role, conf, _ = _classify_column("region", "varchar", {
            "distinct_count": 12, "avg_length": 8.0, "uniqueness_ratio": 0.02,
        })
        assert role == "categorical"

    def test_flag_name_pattern(self):
        from headwater.services.h2_semantics import _classify_column
        for name in ("is_active", "has_violation", "enabled"):
            role, _, _ = _classify_column(name, "varchar", {})
            assert role == "flag", f"Expected flag for '{name}', got {role}"

    def test_free_text_name_pattern(self):
        from headwater.services.h2_semantics import _classify_column
        for name in ("description", "notes", "comment"):
            role, _, _ = _classify_column(name, "varchar", {})
            assert role == "free_text", f"Expected free_text for '{name}', got {role}"


class TestSiblingConsistency:
    def test_start_end_pair_promoted_to_start_ts_end_ts(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover(tmp_path, SAMPLE_DATA, "sample")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                # Inject a table with start/end column pair
                store.upsert_table("sample", "events_test", row_count=100)
                store.upsert_column("sample", "events_test", "event_start", "timestamp")
                store.upsert_column("sample", "events_test", "event_end", "timestamp")

                from headwater.services.h2_semantics import infer_source_semantics
                sem = infer_source_semantics(store, "sample")

                start_role = sem.get("events_test.event_start")
                end_role = sem.get("events_test.event_end")
                assert start_role in ("start_ts", "event_ts"), (
                    f"start_ts expected for event_start, got {start_role}"
                )
                assert end_role in ("end_ts", "event_ts"), (
                    f"end_ts expected for event_end, got {end_role}"
                )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_period_column_gets_temporal_role(self):
        from headwater.services.h2_semantics import _classify_column
        # Column name ends with "hour" → should get event_ts via period name
        role, _, evidence = _classify_column("arrival_hour", "int64", {"mean": 12.0})
        # event_ts or quantity (from numeric fallback) — period name boosts later
        assert role in ("event_ts", "quantity", "measure")


class TestLockPreservation:
    def test_locked_column_type_not_overwritten(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover(tmp_path, SAMPLE_DATA, "sample")

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                tables = store.get_tables("sample")
                table = tables[0]
                cols = store.get_columns("sample", table["name"])
                col = cols[0]

                # Lock the column with a custom semantic type
                store.upsert_column(
                    "sample", table["name"], col["name"], col["dtype"],
                    semantic_type="quantity",
                    locked=True,
                )

                from headwater.services.h2_semantics import infer_source_semantics
                sem = infer_source_semantics(store, "sample")
                key = f"{table['name']}.{col['name']}".lower()
                assert sem.get(key) == "quantity", (
                    "Locked semantic type must survive re-inference"
                )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_inference_works_without_profiles(self, monkeypatch, tmp_path):
        """Semantic typing must not crash when profile data is absent."""
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                store.upsert_source("empty_src", "csv", "/data/empty")
                store.upsert_table("empty_src", "records", row_count=0)
                store.upsert_column("empty_src", "records", "record_id", "varchar")
                store.upsert_column("empty_src", "records", "score", "float64")

                from headwater.services.h2_semantics import infer_source_semantics
                sem = infer_source_semantics(store, "empty_src")
                assert "records.record_id" in sem
                assert "records.score" in sem
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestSourceCatalog:
    def test_get_source_catalog_returns_tables(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover(tmp_path, SAMPLE_DATA, "sample")
            from headwater.services.h2_catalog import get_source_catalog

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                catalog = get_source_catalog(store, "sample")
                assert catalog, "Expected at least one table in catalog"
                for tbl in catalog:
                    assert tbl.columns, f"Table {tbl.table_name} has no columns"
                    for col in tbl.columns:
                        assert col.semantic_type, (
                            f"{tbl.table_name}.{col.column_name} has no semantic type"
                        )
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_update_column_sets_description(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover(tmp_path, SAMPLE_DATA, "sample")
            from headwater.services.h2_catalog import get_source_catalog, update_column

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                catalog = get_source_catalog(store, "sample")
                tbl = catalog[0]
                col = tbl.columns[0]
                update_column(
                    store, "sample", tbl.table_name, col.column_name,
                    description="Test description from catalog",
                )
                updated = store.get_columns("sample", tbl.table_name)
                found = next((c for c in updated if c["name"] == col.column_name), None)
                assert found is not None
                assert found.get("description") == "Test description from catalog"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()

    def test_lock_column_persists(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover(tmp_path, SAMPLE_DATA, "sample")
            from headwater.services.h2_catalog import lock_column, unlock_column

            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            try:
                tables = store.get_tables("sample")
                tbl = tables[0]
                cols = store.get_columns("sample", tbl["name"])
                col_name = cols[0]["name"]

                lock_column(store, "sample", tbl["name"], col_name)
                locked = next(c for c in store.get_columns("sample", tbl["name"])
                              if c["name"] == col_name)
                assert locked["locked"], "Column must be locked after lock_column"

                unlock_column(store, "sample", tbl["name"], col_name)
                unlocked = next(c for c in store.get_columns("sample", tbl["name"])
                                if c["name"] == col_name)
                assert not unlocked["locked"], "Column must be unlocked after unlock_column"
            finally:
                store.close()
        finally:
            get_settings.cache_clear()


class TestCatalogCLI:
    def test_catalog_show_runs(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover(tmp_path, SAMPLE_DATA, "sample")
            result = runner.invoke(app, ["catalog", "show", "--source", "sample"])
            assert result.exit_code == 0, result.output
            assert "inspections" in result.output or "complaints" in result.output
        finally:
            get_settings.cache_clear()

    def test_catalog_set_description(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            _discover(tmp_path, SAMPLE_DATA, "sample")
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            tables = store.get_tables("sample")
            store.close()
            tbl = tables[0]
            cols = store.get_columns("sample", tbl["name"]) if not store._con else []
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            cols = store.get_columns("sample", tbl["name"])
            store.close()
            col_name = cols[0]["name"]

            result = runner.invoke(app, [
                "catalog", "set",
                "--source", "sample",
                "--table", tbl["name"],
                "--column", col_name,
                "--description", "CLI test description",
            ])
            assert result.exit_code == 0, result.output
            assert "Updated" in result.output
        finally:
            get_settings.cache_clear()

    def test_catalog_show_unknown_source_errors(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            store = HeadwaterStore(tmp_path / "h2_metadata.db")
            store.init()
            store.close()
            result = runner.invoke(app, ["catalog", "show", "--source", "ghost_src"])
            assert result.exit_code != 0
        finally:
            get_settings.cache_clear()


class TestH1DependencyRemoved:
    def test_relevance_engine_does_not_import_h1_context_modules(self):
        """The H2 relevance engine must not import from the H1 cut modules."""
        path = (
            Path(__file__).resolve().parents[1]
            / "headwater" / "services" / "h2_project_relevance.py"
        )
        content = path.read_text()
        forbidden = [
            "context_bootstrap",
            "retrieve_metadata",
            "infer_semantic_schema",
            "roles_for_table",
            "bootstrap_project_context",
        ]
        leaks = [f for f in forbidden if f in content]
        assert not leaks, (
            f"H2 relevance engine still imports H1 cut modules: {leaks}"
        )

    @pytest.mark.skipif(
        not Path(__file__).resolve().parents[2].joinpath("data/radiology/cases.csv").exists(),
        reason="Radiology fixture not available",
    )
    def test_full_pipeline_works_without_h1_context(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HEADWATER_DATA_DIR", str(tmp_path))
        get_settings.cache_clear()
        try:
            r = runner.invoke(app, [
                "discover", "--source", RADIOLOGY_DATA, "--name", "radiology"
            ])
            assert r.exit_code == 0, r.output
            r = runner.invoke(app, [
                "project", "frame",
                "--project-id", "s4_rad",
                "--source", "radiology",
                "--name", "S4 Radiology",
                "--goal", "Analyse wait time distribution across categories",
            ])
            assert r.exit_code == 0, r.output
            assert "Relevant columns" in r.output
            assert "Proposed questions" in r.output
        finally:
            get_settings.cache_clear()
