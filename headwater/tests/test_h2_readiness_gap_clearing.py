"""Evidence-derived gap clearing: a defined column stops being a blocking gap.

Regression guard for the user-found bug where ``no_blocking_gaps`` never cleared
after a column's meaning was supplied.  ``_columns_with_satisfying_claim`` is the
pure function the readiness evaluator subtracts from its high-priority-open set.
"""

from __future__ import annotations

from headwater.services.h2_readiness import _columns_with_satisfying_claim


def _claim(table, column, value, locked=False):
    return {
        "table_name": table,
        "column_name": column,
        "claim": {"value": value},
        "locked": locked,
    }


def test_empty_bootstrap_enum_is_not_satisfied():
    # Codes detected but no meanings filled in yet -> still a gap.
    claims = [_claim("cases", "patient_type", {"A": "", "H": ""})]
    assert _columns_with_satisfying_claim(claims) == set()


def test_filled_enum_mapping_is_satisfied():
    claims = [_claim("cases", "patient_type", {"A": "Adult", "H": "Household"})]
    assert _columns_with_satisfying_claim(claims) == {"cases.patient_type"}


def test_nonempty_definition_is_satisfied():
    claims = [_claim("cases", "patient_type", "Patient classification code")]
    assert _columns_with_satisfying_claim(claims) == {"cases.patient_type"}


def test_locked_claim_is_satisfied_even_if_empty():
    claims = [_claim("cases", "patient_type", {}, locked=True)]
    assert _columns_with_satisfying_claim(claims) == {"cases.patient_type"}


def test_distinct_tables_are_independent():
    # Defining cases.patient_type must NOT satisfy events.patient_type.
    claims = [
        _claim("cases", "patient_type", {"A": "Adult"}),
        _claim("events", "patient_type", {"A": ""}),
    ]
    assert _columns_with_satisfying_claim(claims) == {"cases.patient_type"}


def test_claim_without_column_is_ignored():
    claims = [_claim("", "", {"A": "Adult"})]
    assert _columns_with_satisfying_claim(claims) == set()
