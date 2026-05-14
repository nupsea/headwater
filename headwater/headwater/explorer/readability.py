"""Shared helpers for making Explore dimensions business-readable."""

from __future__ import annotations

import re
from typing import Any

from headwater.analyzer.metadata_retrieval import RetrievedMetadata, lookup_for_column

_READABLE_LABEL_TOKENS = (
    "name",
    "label",
    "description",
    "title",
    "zone",
    "borough",
    "region",
)

LOW_SIGNAL_DIMENSION_TOKENS = (
    "flag",
    "indicator",
    "store_and_fwd",
    "source_file",
    "source_system",
    "load_batch",
    "ingest",
    "extract",
    "audit",
    "deleted",
)

BUSINESS_DIMENSION_TOKENS = (
    "type",
    "category",
    "status",
    "reason",
    "channel",
    "segment",
    "service",
    "region",
    "zone",
    "site",
    "payment",
)

BUILTIN_ENUM_LABEL_REGISTRY: dict[str, dict[object, str]] = {
    "payment_type": {
        0: "Flex fare",
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    },
}

BUILTIN_ENUM_DIMENSION_LABELS = {
    "payment_type": "payment method",
}

_OPAQUE_ALPHANUMERIC_RE = re.compile(r"^[A-Z]{1,4}\d{2,}$")
_OPAQUE_BOOLEANISH_VALUES = {"y", "n", "yes", "no", "true", "false", "0", "1"}


def is_label_like_column(column_name: str) -> bool:
    lower = column_name.lower()
    return any(token in lower for token in _READABLE_LABEL_TOKENS) and "id" not in lower


def enum_mapping_for_column(
    column_name: str,
    metadata: RetrievedMetadata | None = None,
) -> dict[object, str] | None:
    column_key = column_name.lower()
    metadata_mapping = metadata.enum_mappings.get(column_key) if metadata else None
    if metadata_mapping:
        return metadata_mapping
    return BUILTIN_ENUM_LABEL_REGISTRY.get(column_key)


def enum_dimension_label(column_name: str, fallback: str) -> str:
    return BUILTIN_ENUM_DIMENSION_LABELS.get(column_name.lower(), fallback)


def enum_case_expression(
    column_name: str,
    raw_expr: str,
    metadata: RetrievedMetadata | None = None,
) -> str | None:
    mapping = enum_mapping_for_column(column_name, metadata)
    if not mapping:
        return None

    cases = []
    for value, label in mapping.items():
        cases.append(
            f"WHEN CAST({raw_expr} AS VARCHAR) = {_sql_string_literal(value)} "
            f"THEN {_sql_string_literal(label)}"
        )
    return (
        "CASE "
        + " ".join(cases)
        + f" ELSE CONCAT('Unknown (', CAST({raw_expr} AS VARCHAR), ')') END"
    )


def is_readable_dimension(
    table_name: str,
    column_name: str,
    lookup_index: dict[str, dict[str, str]],
    metadata: RetrievedMetadata | None = None,
) -> bool:
    return (
        is_label_like_column(column_name)
        or enum_mapping_for_column(column_name, metadata) is not None
        or lookup_for_column(table_name, column_name, lookup_index) is not None
    )


def is_opaque_business_value(value: Any) -> bool:
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return float(value).is_integer()

    text = str(value).strip()
    if not text:
        return False
    if " -> " in text:
        parts = [part.strip() for part in text.split("->") if part.strip()]
        return len(parts) >= 2 and all(_is_opaque_business_atom(part) for part in parts)
    return _is_opaque_business_atom(text)


def is_low_signal_dimension(column_name: str, profile: Any | None) -> bool:
    lower = column_name.lower()
    distinct = getattr(profile, "distinct_count", 0) if profile is not None else 0
    business_named = any(token in lower for token in BUSINESS_DIMENSION_TOKENS)
    technical_named = any(token in lower for token in LOW_SIGNAL_DIMENSION_TOKENS)

    if technical_named and distinct <= 3:
        return True

    top_values = getattr(profile, "top_values", None) if profile is not None else None
    if profile is None or not top_values or business_named:
        return False

    values = {str(value).strip().lower() for value, _count in top_values[:4]}
    return distinct <= 3 and values <= _OPAQUE_BOOLEANISH_VALUES


def _sql_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _is_opaque_business_atom(text: str) -> bool:
    normalized = text.strip()
    if not normalized:
        return False
    lower = normalized.lower()
    if lower in _OPAQUE_BOOLEANISH_VALUES:
        return True
    if normalized.isdigit():
        return True
    if _OPAQUE_ALPHANUMERIC_RE.match(normalized):
        return True
    if len(normalized) == 1 and normalized.isalpha():
        return True
    if " " in normalized:
        return False
    return False
