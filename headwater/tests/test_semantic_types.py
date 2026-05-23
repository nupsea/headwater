"""Tests for generic semantic type detectors."""

from __future__ import annotations

from headwater.analyzer.semantic_types import detect_semantic_types, primary_semantic_type
from headwater.core.models import ColumnProfile


def _profile(
    column_name: str,
    values: list[tuple[str, int]],
    *,
    dtype: str = "varchar",
) -> ColumnProfile:
    return ColumnProfile(
        table_name="records",
        column_name=column_name,
        dtype=dtype,
        top_values=values,
        distinct_count=len(values),
    )


def test_detects_email_as_sensitive_format() -> None:
    profile = _profile(
        "contact_email",
        [("a@example.com", 5), ("b@example.org", 3), ("not-email", 1)],
    )

    evidence = primary_semantic_type("contact_email", "varchar", profile)

    assert evidence is not None
    assert evidence.semantic_type == "email"
    assert evidence.sensitive is True
    assert evidence.support_count == 8
    assert evidence.sample_size == 9
    assert evidence.confidence > 0.9


def test_detects_currency_code_without_business_labels() -> None:
    profile = _profile("currency", [("USD", 10), ("AUD", 8), ("EUR", 2)])

    evidence = primary_semantic_type("currency", "varchar", profile)

    assert evidence is not None
    assert evidence.semantic_type == "currency_code"
    assert evidence.sensitive is False


def test_mixed_format_below_threshold_does_not_detect() -> None:
    profile = _profile(
        "external_ref",
        [("550e8400-e29b-41d4-a716-446655440000", 2), ("plain-ref", 8)],
    )

    detections = detect_semantic_types("external_ref", "varchar", profile)

    assert all(item.semantic_type != "uuid" for item in detections)


def test_detects_latitude_from_name_and_range() -> None:
    profile = ColumnProfile(
        table_name="records",
        column_name="latitude",
        dtype="double",
        min_value=-33.9,
        max_value=-27.4,
        distinct_count=30,
    )

    evidence = primary_semantic_type("latitude", "double", profile)

    assert evidence is not None
    assert evidence.semantic_type == "latitude"
    assert evidence.confidence == 0.94


def test_iban_uses_checksum_and_marks_sensitive() -> None:
    profile = _profile("account_number", [("GB82WEST12345698765432", 4)])

    evidence = primary_semantic_type("account_number", "varchar", profile)

    assert evidence is not None
    assert evidence.semantic_type == "iban"
    assert evidence.sensitive is True
