"""Duration derivation end-to-end: confirm a parse, and the answer uses minutes."""

from __future__ import annotations

import pytest

from headwater.core.store import HeadwaterStore
from headwater.services.h2_answer import _measure_agg_expr, _resolve_col_info
from headwater.services.h2_readiness import _columns_with_satisfying_claim
from headwater.services.h2_resolve import confirm_duration_derivation


@pytest.fixture()
def store(tmp_path) -> HeadwaterStore:
    s = HeadwaterStore(tmp_path / "h2_metadata.db")
    s.init()
    s.upsert_project("p1", slug="p1", display_name="Project One")
    try:
        yield s
    finally:
        s.close()


def _seed_measure_card(store, card_id="p1:measure:cases.throughput_time"):
    store.upsert_resolve_item(
        card_id,
        project_id="p1",
        issue_kind="unusable_measure",
        title='Make "throughput time" measurable',
        body="Stored as text...",
        priority="high",
        status="open",
        payload={
            "table": "cases",
            "column": "throughput_time",
            "category": "input",
            "derivation": {"detected": {"id": "days_hh_mm_ss"}},
        },
    )
    return card_id


def test_confirm_writes_locked_derivation_claim_and_resolves_card(store):
    card_id = _seed_measure_card(store)
    result = confirm_duration_derivation(store, "p1", card_id, "days_hh_mm_ss")
    assert result == {"applied": True, "format": "days_hh_mm_ss", "unit": "minutes"}

    claims = store.list_semantic_claims("p1")
    derivation = next(c for c in claims if c["claim_type"] == "derivation")
    assert derivation["table_name"] == "cases"
    assert derivation["column_name"] == "throughput_time"
    assert derivation["claim"]["format"] == "days_hh_mm_ss"
    assert derivation["locked"]

    # The column now counts as satisfied, so the card clears on rebuild.
    assert "cases.throughput_time" in _columns_with_satisfying_claim(claims)

    card = next(r for r in store.list_resolve_items("p1") if r["id"] == card_id)
    assert card["status"] == "resolved"


def test_confirm_rejects_unknown_format(store):
    card_id = _seed_measure_card(store)
    with pytest.raises(ValueError, match="Unknown duration format"):
        confirm_duration_derivation(store, "p1", card_id, "bogus")


def test_confirm_without_column_is_a_noop(store):
    store.upsert_resolve_item(
        "p1:measure:none",
        project_id="p1",
        issue_kind="unusable_measure",
        title="x",
        priority="high",
        status="open",
        payload={"category": "input"},
    )
    result = confirm_duration_derivation(store, "p1", "p1:measure:none", "hh_mm")
    assert result["applied"] is False
    assert store.list_semantic_claims("p1") == []


def test_answer_measure_uses_derivation_sql_not_text_cast():
    # A column carrying a derivation_format aggregates the parsed minutes and adds
    # no "text" caveat.
    col = {"column": "throughput_time", "dtype": "varchar", "derivation_format": "days_hh_mm_ss"}
    caveats: list[str] = []
    expr = _measure_agg_expr(col, caveats)
    assert expr.startswith("AVG(")
    assert "split_part" in expr  # the parse-to-minutes expression
    assert "TRY_CAST" in expr
    assert caveats == []  # no "is text" complaint once a derivation exists


def test_text_measure_without_derivation_still_flags():
    col = {"column": "throughput_time", "dtype": "varchar"}
    caveats: list[str] = []
    _measure_agg_expr(col, caveats)
    assert caveats and "no numeric derivation" in caveats[0]


def test_resolve_col_info_marks_derived_text_column_as_measure():
    role_map = {"cases.throughput_time": {"dtype": "varchar", "derivation_format": "days_hh_mm_ss"}}
    info = _resolve_col_info("cases.throughput_time", role_map)
    assert info["role_class"] == "measure"
    assert info["derivation_format"] == "days_hh_mm_ss"
