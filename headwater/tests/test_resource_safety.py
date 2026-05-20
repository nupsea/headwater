"""Tests for project context resource safety classification."""

from __future__ import annotations

from headwater.services.resource_safety import classified_resource_metadata


def test_resource_classification_defaults_unknown_and_blocks_external_llm() -> None:
    metadata = classified_resource_metadata({})

    assert metadata["classification"] == "unknown"
    assert metadata["external_llm_allowed"] is False
    assert metadata["requires_redaction"] is True


def test_public_resource_requires_explicit_external_llm_allowance() -> None:
    metadata = classified_resource_metadata(
        {"classification": "public", "allow_external_llm": True}
    )

    assert metadata["classification"] == "public"
    assert metadata["external_llm_allowed"] is True
    assert metadata["requires_redaction"] is False


def test_sensitive_content_overrides_public_classification() -> None:
    metadata = classified_resource_metadata(
        {"classification": "public", "allow_external_llm": True},
        content="api_key=secret-value",
    )

    assert metadata["classification"] == "sensitive"
    assert metadata["external_llm_allowed"] is False
    assert metadata["requires_redaction"] is True
