"""Resource classification and external-use gates."""

from __future__ import annotations

import re

_CLASSIFICATIONS = {"public", "internal", "sensitive", "unknown"}
_SECRET_RE = re.compile(
    r"(api[_-]?key|secret|password|token|private[_-]?key)\s*[:=]",
    re.IGNORECASE,
)
_PII_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+|\+?[0-9][0-9().\-\s]{7,}[0-9]")


def classified_resource_metadata(
    metadata: dict | None,
    *,
    content: str | None = None,
) -> dict:
    """Return resource metadata with conservative safety classification fields."""
    result = dict(metadata or {})
    classification = _normalize_classification(
        result.get("classification")
        or result.get("resource_classification")
        or result.get("sensitivity")
    )
    if _looks_sensitive(content):
        classification = "sensitive"
    result["classification"] = classification
    result["external_llm_allowed"] = _external_llm_allowed(result, classification)
    result["requires_redaction"] = classification in {"internal", "sensitive", "unknown"}
    return result


def _normalize_classification(value: object) -> str:
    if isinstance(value, str) and value.strip().lower() in _CLASSIFICATIONS:
        return value.strip().lower()
    return "unknown"


def _external_llm_allowed(metadata: dict, classification: str) -> bool:
    return classification == "public" and bool(metadata.get("allow_external_llm"))


def _looks_sensitive(content: str | None) -> bool:
    if not content:
        return False
    return bool(_SECRET_RE.search(content) or _PII_RE.search(content))
