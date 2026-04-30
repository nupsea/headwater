"""Utilities for redacting credentials from logs, events, and API payloads."""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_SENSITIVE_QUERY_KEYS = ("password", "passwd", "pwd", "token", "secret", "key")
_URI_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"<>]+")


def redact_secrets(value):
    """Recursively redact credentials from strings, dicts, lists, and tuples."""
    if isinstance(value, str):
        return _redact_string(value)
    if isinstance(value, dict):
        return {
            key: "***" if _is_sensitive_key(str(key)) else redact_secrets(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_secrets(item) for item in value)
    return value


def _redact_string(value: str) -> str:
    if "://" not in value:
        return value
    return _URI_RE.sub(lambda match: _redact_uri(match.group(0)), value)


def _redact_uri(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    if not parsed.scheme or not parsed.netloc:
        return value

    username = parsed.username
    password = parsed.password
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""

    if username is not None and password is not None:
        auth = f"{username}:***@"
    elif username is not None:
        auth = f"{username}@"
    else:
        auth = ""

    query = urlencode(
        [
            (key, "***" if _is_sensitive_key(key) else val)
            for key, val in parse_qsl(parsed.query, keep_blank_values=True)
        ]
    )
    return urlunsplit((parsed.scheme, f"{auth}{host}{port}", parsed.path, query, parsed.fragment))


def _is_sensitive_key(key: str) -> bool:
    lower = key.lower()
    return any(part in lower for part in _SENSITIVE_QUERY_KEYS)
