"""Tests for the duration-derivation engine (detection + parse-to-minutes SQL)."""

from __future__ import annotations

import duckdb
import pytest

from headwater.services.h2_duration import (
    FORMATS,
    detect_duration,
    to_minutes_sql,
)


def test_detect_two_part_defaults_to_hh_mm_with_mm_ss_alternative():
    proposal = detect_duration(["00:22", "01:05", "00:48"])
    assert proposal is not None
    assert proposal.detected.id == "hh_mm"
    assert [a.id for a in proposal.alternatives] == ["mm_ss"]
    assert proposal.unit == "minutes"


def test_detect_three_part_is_hh_mm_ss():
    proposal = detect_duration(["00:22:00", "01:05:30"])
    assert proposal is not None
    assert proposal.detected.id == "hh_mm_ss"
    assert proposal.alternatives == []


def test_detect_pandas_timedelta_string():
    proposal = detect_duration(["0 days 00:22:00", "1 days 03:00:00"])
    assert proposal is not None
    assert proposal.detected.id == "days_hh_mm_ss"


def test_non_duration_text_yields_no_proposal():
    assert detect_duration(["Adult", "Home", "Outpatient"]) is None
    assert detect_duration([]) is None
    assert detect_duration([None, "", "  "]) is None


def test_majority_threshold_ignores_a_stray_value():
    # Mostly HH:MM with one junk value still proposes hh_mm.
    proposal = detect_duration(["00:10", "00:20", "00:30", "n/a"])
    assert proposal is not None and proposal.detected.id == "hh_mm"


def test_temporal_dtype_proposes_epoch_minutes():
    # A TIME/INTERVAL column is already a duration → epoch conversion, no samples.
    assert detect_duration([], dtype="time").detected.id == "epoch_minutes"
    assert detect_duration(["x"], dtype="INTERVAL").detected.id == "epoch_minutes"
    # A real timestamp/date is not treated as a duration measure.
    assert detect_duration([], dtype="timestamp") is None


@pytest.mark.parametrize(
    ("value", "expected_minutes"),
    [("00:01:00", 1.0), ("01:30:00", 90.0), ("00:00:30", 0.5)],
)
def test_epoch_minutes_sql_on_time_column(value, expected_minutes):
    con = duckdb.connect(":memory:")
    try:
        con.execute('CREATE TABLE t ("d" TIME)')
        con.execute("INSERT INTO t VALUES (?)", [value])
        expr = to_minutes_sql('"d"', "epoch_minutes")
        result = con.execute(f"SELECT {expr} FROM t").fetchone()
        assert result is not None
        assert result[0] == pytest.approx(expected_minutes)
    finally:
        con.close()


def test_to_minutes_sql_rejects_unknown_format():
    with pytest.raises(ValueError, match="Unknown duration format"):
        to_minutes_sql('"c"', "nope")


@pytest.mark.parametrize(
    ("format_id", "value", "expected_minutes"),
    [
        ("hh_mm", "01:30", 90.0),
        ("hh_mm", "00:10", 10.0),
        ("mm_ss", "01:30", 1.5),
        ("mm_ss", "00:30", 0.5),
        ("hh_mm_ss", "01:30:00", 90.0),
        ("hh_mm_ss", "00:22:30", 22.5),
        ("days_hh_mm_ss", "0 days 00:22:00", 22.0),
        ("days_hh_mm_ss", "1 days 00:00:00", 1440.0),
    ],
)
def test_generated_sql_computes_correct_minutes(format_id, value, expected_minutes):
    """The parse-to-minutes SQL must actually evaluate correctly in DuckDB."""
    con = duckdb.connect(":memory:")
    try:
        con.execute('CREATE TABLE t ("d" VARCHAR)')
        con.execute('INSERT INTO t VALUES (?)', [value])
        expr = to_minutes_sql('"d"', format_id)
        result = con.execute(f"SELECT {expr} AS minutes FROM t").fetchone()
        assert result is not None
        assert result[0] == pytest.approx(expected_minutes)
    finally:
        con.close()


def test_all_formats_have_distinct_ids_and_labels():
    ids = list(FORMATS)
    assert len(ids) == len(set(ids))
    labels = [f.label for f in FORMATS.values()]
    assert len(labels) == len(set(labels))
