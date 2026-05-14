"""Shared helpers for making Explore dimensions business-readable."""

from __future__ import annotations

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


def _sql_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"
