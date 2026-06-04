"""Tests for the stated-insight engine (deterministic findings from result rows)."""

from __future__ import annotations

from headwater.services.h2_insight import infer_chart_spec, summarize_answer


def test_segment_finding_states_top_and_ratio():
    f = summarize_answer(
        chart_spec={"type": "bar", "x": "seg", "y": "avg_wait"},
        columns=["seg", "avg_wait"],
        rows=[{"seg": "A", "avg_wait": 20}, {"seg": "B", "avg_wait": 48}],
    )
    assert f is not None
    assert "B has the highest wait" in f.headline
    assert "48" in f.headline
    assert "lowest" in f.support and "A" in f.support


def test_segment_finding_respects_lowest_intent():
    # The question asks for the lowest — the finding must lead with the lowest,
    # not the highest bar.
    f = summarize_answer(
        title="Patient type with the lowest duration",
        chart_spec={"type": "bar", "x": "patient_type", "y": "total_duration"},
        columns=["patient_type", "total_duration"],
        rows=[
            {"patient_type": "Stable", "total_duration": 3.9},
            {"patient_type": "Admitted", "total_duration": 8.8},
        ],
    )
    assert f is not None
    assert "Stable has the lowest" in f.headline and "3.9" in f.headline
    assert "Highest is Admitted" in f.support


def test_segment_finding_default_is_highest():
    f = summarize_answer(
        title="Which patient type has the highest duration?",
        chart_spec={"type": "bar", "x": "patient_type", "y": "total_duration"},
        columns=["patient_type", "total_duration"],
        rows=[
            {"patient_type": "Stable", "total_duration": 3.9},
            {"patient_type": "Admitted", "total_duration": 8.8},
        ],
    )
    assert f is not None and "Admitted has the highest" in f.headline


def test_mixed_intent_falls_back_to_highest():
    # "highest" and "lowest" both present → don't guess; keep default.
    f = summarize_answer(
        title="Spread between highest and lowest duration",
        chart_spec={"type": "bar", "x": "seg", "y": "dur"},
        columns=["seg", "dur"],
        rows=[{"seg": "A", "dur": 1}, {"seg": "B", "dur": 9}],
    )
    assert f is not None and "B has the highest" in f.headline


def test_segment_finding_uses_value_labels():
    f = summarize_answer(
        chart_spec={"type": "bar", "x": "patient_type", "y": "avg_dur"},
        columns=["patient_type", "avg_dur"],
        rows=[{"patient_type": "A", "avg_dur": 10}, {"patient_type": "H", "avg_dur": 30}],
        value_labels={"patient_type": {"A": "Adult", "H": "Home"}},
    )
    assert f is not None and "Home has the highest" in f.headline


def test_temporal_finding_direction_and_peak():
    rising = summarize_answer(
        chart_spec={"type": "line", "x": "period", "y": "avg_x"},
        columns=["period", "avg_x"],
        rows=[{"period": "d1", "avg_x": 10}, {"period": "d2", "avg_x": 15}],
    )
    assert rising is not None and "rose" in rising.headline
    falling = summarize_answer(
        chart_spec={"type": "line", "x": "period", "y": "avg_x"},
        columns=["period", "avg_x"],
        rows=[{"period": "d1", "avg_x": 20}, {"period": "d2", "avg_x": 10}],
    )
    assert "fell" in falling.headline
    flat = summarize_answer(
        chart_spec={"type": "line", "x": "period", "y": "avg_x"},
        columns=["period", "avg_x"],
        rows=[{"period": "d1", "avg_x": 100}, {"period": "d2", "avg_x": 101}],
    )
    assert "steady" in flat.headline


def test_unit_suffix_is_appended():
    f = summarize_answer(
        chart_spec={"type": "bar", "x": "seg", "y": "avg_dur"},
        columns=["seg", "avg_dur"],
        rows=[{"seg": "A", "avg_dur": 5}, {"seg": "B", "avg_dur": 9}],
        unit="min",
    )
    assert "min" in f.headline


def test_coverage_finding_reports_record_count():
    f = summarize_answer(
        chart_spec={"type": "table"},
        columns=["total_records", "earliest", "latest"],
        rows=[{"total_records": 3193, "earliest": "2023-01-01", "latest": "2023-01-08"}],
    )
    assert f is not None and "3,193 records" in f.headline
    assert "2023-01-01" in f.support


def test_infer_chart_spec_bar_for_category_measure():
    spec = infer_chart_spec(
        ["modality", "exam_count"],
        [{"modality": "CT", "exam_count": 90}, {"modality": "MR", "exam_count": 40}],
    )
    assert spec == {"type": "bar", "x": "modality", "y": "exam_count"}


def test_infer_chart_spec_line_for_temporal_axis():
    spec = infer_chart_spec(
        ["exam_month", "exam_count"],
        [{"exam_month": "2023-01", "exam_count": 12}],
    )
    assert spec["type"] == "line" and spec["x"] == "exam_month"


def test_infer_chart_spec_table_when_unplottable():
    assert infer_chart_spec(["only_text"], [{"only_text": "x"}]) == {"type": "table"}
    assert infer_chart_spec([], []) == {"type": "table"}
    assert infer_chart_spec(["a", "b"], [{"a": 1, "b": 2}]) == {"type": "table"}


def test_no_finding_without_numeric_data():
    assert (
        summarize_answer(
            chart_spec={"type": "bar", "x": "seg", "y": "avg_x"},
            columns=["seg", "avg_x"],
            rows=[{"seg": "A", "avg_x": None}, {"seg": "B", "avg_x": "n/a"}],
        )
        is None
    )
    assert (
        summarize_answer(chart_spec={"type": "line", "x": "p", "y": "v"}, columns=[], rows=[])
        is None
    )
