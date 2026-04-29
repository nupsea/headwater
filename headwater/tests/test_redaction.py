"""Tests for credential redaction helpers."""

from __future__ import annotations

from headwater.core.redaction import redact_secrets


def test_redacts_uri_password():
    value = "postgresql://user:secret@localhost:5432/db"

    redacted = redact_secrets(value)

    assert redacted == "postgresql://user:***@localhost:5432/db"
    assert "secret" not in redacted


def test_redacts_sensitive_query_params():
    value = "mysql://user@localhost/db?password=secret&ssl=true&api_key=abc"

    redacted = redact_secrets(value)

    assert "password=%2A%2A%2A" in redacted
    assert "api_key=%2A%2A%2A" in redacted
    assert "secret" not in redacted
    assert "abc" not in redacted


def test_redacts_nested_payloads():
    payload = {
        "uri": "postgresql://user:secret@localhost/db",
        "config": {"password": "secret", "safe": "value"},
    }

    redacted = redact_secrets(payload)

    assert redacted["uri"] == "postgresql://user:***@localhost/db"
    assert redacted["config"]["password"] == "***"
    assert redacted["config"]["safe"] == "value"
