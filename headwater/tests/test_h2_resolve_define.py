"""S-BIND: a Resolve card's definition binds to its column.

``_parse_enum_table`` turns the analyst's free text into either a code->meaning
map or a plain definition, and ``define_card`` writes that as a locked,
column-scoped semantic claim so the blocking gap clears on the next recompute
without the Schema & meaning detour.
"""

from __future__ import annotations

import pytest

from headwater.core.store import HeadwaterStore
from headwater.services.h2_readiness import _columns_with_satisfying_claim
from headwater.services.h2_resolve import _parse_enum_table, define_card

# ── Parser ──────────────────────────────────────────────────────────────────


def test_parses_a_markdown_code_table():
    md = "| code | meaning |\n| --- | --- |\n| A | Adult |\n| H | Household |"
    assert _parse_enum_table(md) == {"A": "Adult", "H": "Household"}


def test_skips_header_and_separator_rows():
    md = "| value | description |\n|:---|:---:|\n| X | thing |"
    assert _parse_enum_table(md) == {"X": "thing"}


def test_plain_text_is_not_a_code_table():
    assert _parse_enum_table("total_wait_time is service_ts minus arrival_time") == {}


def test_ignores_blank_cells():
    md = "| A | Adult |\n| H |  |"
    assert _parse_enum_table(md) == {"A": "Adult"}


# ── Binding against a store ───────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path) -> HeadwaterStore:
    s = HeadwaterStore(tmp_path / "h2_metadata.db")
    s.init()
    s.upsert_project("p1", slug="p1", display_name="Project One")
    try:
        yield s
    finally:
        s.close()


def _seed_enum_card(store, card_id="p1:enum:events.patient_type"):
    store.upsert_resolve_item(
        card_id,
        project_id="p1",
        issue_kind="enum_mapping_needed",
        title='What do the "events.patient_type" codes mean?',
        priority="high",
        status="open",
        payload={"table": "events", "column": "patient_type", "top_values": ["A", "H"]},
    )
    return card_id


def test_define_card_writes_a_locked_column_claim(store):
    card_id = _seed_enum_card(store)
    result = define_card(
        store,
        "p1",
        card_id,
        "| code | meaning |\n| --- | --- |\n| A | Adult |\n| H | Household |",
    )
    assert result["bound"] is True
    assert result["claim_type"] == "enum_mapping"
    assert (result["table"], result["column"]) == ("events", "patient_type")

    claims = store.list_semantic_claims("p1")
    assert _columns_with_satisfying_claim(claims) == {"events.patient_type"}
    bound = next(c for c in claims if c["column_name"] == "patient_type")
    assert bound["locked"]
    assert bound["claim"]["value"] == {"A": "Adult", "H": "Household"}
    # Raw markdown is kept so the card rehydrates exactly on a return visit.
    assert "| A | Adult |" in bound["claim"]["text"]

    # Card is marked resolved so it stops surfacing as work.
    item = next(r for r in store.list_resolve_items("p1") if r["id"] == card_id)
    assert item["status"] == "resolved"


def test_define_card_stores_free_text_as_a_definition(store):
    card_id = "p1:def:events.note"
    store.upsert_resolve_item(
        card_id,
        project_id="p1",
        issue_kind="missing_definition",
        title="define note",
        priority="low",
        status="open",
        payload={"table": "events", "column": "note"},
    )
    result = define_card(store, "p1", card_id, "Free-text operator note field.")
    assert result["bound"] is True
    assert result["claim_type"] == "definition"
    claim = next(
        c for c in store.list_semantic_claims("p1") if c["column_name"] == "note"
    )
    assert claim["claim"]["value"] == "Free-text operator note field."


def test_claim_display_round_trips_text_and_enum():
    from headwater.api.routes.h2 import _claim_display

    assert _claim_display(None) == ""
    assert _claim_display({"claim": {"text": "service_ts minus arrival"}}) == (
        "service_ts minus arrival"
    )
    rendered = _claim_display({"claim": {"value": {"A": "Adult", "H": "Household"}}})
    assert "| A | Adult |" in rendered
    assert "| H | Household |" in rendered


def test_define_then_get_resolve_rehydrates_definition(store):
    # The whole point of the bug fix: after defining, the card reports its saved
    # text so the UI no longer shows an empty box.
    card_id = _seed_enum_card(store)
    define_card(store, "p1", card_id, "| code | meaning |\n| --- | --- |\n| A | Adult |")

    from headwater.api.routes.h2 import _claim_display
    from headwater.services.h2_readiness import _columns_with_satisfying_claim

    claims = store.list_semantic_claims("p1")
    assert "events.patient_type" in _columns_with_satisfying_claim(claims)
    claim = next(c for c in claims if c["column_name"] == "patient_type")
    assert "| A | Adult |" in _claim_display(claim)


def test_define_card_without_a_column_is_not_bound(store):
    store.upsert_resolve_item(
        "p1:gap:q1",
        project_id="p1",
        issue_kind="cannot_answer_gap",
        title="Gap",
        priority="medium",
        status="open",
        payload={"question_id": "q1", "reason": "no time coverage"},
    )
    result = define_card(store, "p1", "p1:gap:q1", "some context")
    assert result["bound"] is False
    assert store.list_semantic_claims("p1") == []


def test_list_definitions_returns_saved_editable_context(store):
    card_id = _seed_enum_card(store)
    define_card(
        store, "p1", card_id,
        "| code | meaning |\n| --- | --- |\n| A | Adult |\n| H | Home |",
    )

    from headwater.services.h2_resolve import list_definitions

    defs = list_definitions(store, "p1")
    assert len(defs) == 1
    d = defs[0]
    assert (d["table"], d["column"]) == ("events", "patient_type")
    assert d["claim_type"] == "enum_mapping"
    assert d["values"] == ["A", "H"]
    assert "| A | Adult |" in d["text"]  # original markdown, for editing


def test_bind_definition_edits_in_place_without_a_live_card(store):
    """Saved context is editable later even after the card is gone."""
    from headwater.services.h2_resolve import bind_definition, list_definitions

    # First save (no resolve card needs to exist).
    bind_definition(store, "p1", "events", "patient_type",
                    "| code | meaning |\n| --- | --- |\n| A | Adult |")
    # Edit: revise the meaning.
    bind_definition(store, "p1", "events", "patient_type",
                    "| code | meaning |\n| --- | --- |\n| A | Ambulatory |")

    defs = list_definitions(store, "p1")
    assert len(defs) == 1, "editing must not create a duplicate"
    claim = next(c for c in store.list_semantic_claims("p1") if c["column_name"] == "patient_type")
    assert claim["claim"]["value"] == {"A": "Ambulatory"}
    assert claim["locked"]
