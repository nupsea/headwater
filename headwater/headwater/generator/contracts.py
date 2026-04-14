"""Quality contract generator -- auto-generates rules from column profiles."""

from __future__ import annotations

import uuid

from headwater.core.models import ColumnProfile, ContractRule


def generate_contracts(
    profiles: list[ColumnProfile],
    model_prefix: str = "stg_",
    target_schema: str = "staging",
) -> list[ContractRule]:
    """Generate quality contract rules from column profiles.

    All contracts start in 'proposed' status (then -> observing -> enforced).
    """
    rules: list[ContractRule] = []

    for p in profiles:
        model_name = f"{target_schema}.{model_prefix}{p.table_name}"

        # Not-null contract
        if p.null_rate == 0.0 and p.distinct_count > 0:
            rules.append(
                ContractRule(
                    id=_make_id(),
                    model_name=model_name,
                    column_name=p.column_name,
                    rule_type="not_null",
                    expression=f'"{p.column_name}" IS NOT NULL',
                    severity="error",
                    description=f"{p.column_name} must not be null (observed 0% nulls)",
                    confidence=0.95,
                    status="proposed",
                )
            )

        # Uniqueness contract
        if p.uniqueness_ratio == 1.0 and p.distinct_count > 1:
            rules.append(
                ContractRule(
                    id=_make_id(),
                    model_name=model_name,
                    column_name=p.column_name,
                    rule_type="unique",
                    expression=(f'COUNT(*) = COUNT(DISTINCT "{p.column_name}")'),
                    severity="error",
                    description=f"{p.column_name} must be unique (observed 100% uniqueness)",
                    confidence=0.9,
                    status="proposed",
                )
            )

        # Range contract for numeric columns
        if p.min_value is not None and p.max_value is not None:
            headroom = abs(p.max_value - p.min_value) * 0.5
            lower = p.min_value - headroom if p.min_value >= 0 else p.min_value * 1.5
            upper = p.max_value + headroom
            # For non-negative columns, don't allow negatives
            if p.min_value >= 0:
                lower = max(0, lower)
            rules.append(
                ContractRule(
                    id=_make_id(),
                    model_name=model_name,
                    column_name=p.column_name,
                    rule_type="range",
                    expression=(f'"{p.column_name}" BETWEEN {lower:.2f} AND {upper:.2f}'),
                    severity="warning",
                    description=(
                        f"{p.column_name} expected in range "
                        f"[{lower:.2f}, {upper:.2f}] "
                        f"(observed [{p.min_value}, {p.max_value}])"
                    ),
                    confidence=0.8,
                    status="proposed",
                )
            )

        # Low-cardinality allowed values
        if p.top_values and p.distinct_count <= 30 and p.dtype == "varchar":
            allowed = [v for v, _ in p.top_values]
            values_str = ", ".join(f"'{v.replace(chr(39), chr(39) + chr(39))}'" for v in allowed)
            rules.append(
                ContractRule(
                    id=_make_id(),
                    model_name=model_name,
                    column_name=p.column_name,
                    rule_type="cardinality",
                    expression=f'"{p.column_name}" IN ({values_str})',
                    severity="warning",
                    description=(
                        f"{p.column_name} expected to be one of {len(allowed)} known values"
                    ),
                    confidence=0.75,
                    status="proposed",
                )
            )

    # Table-level row count contracts
    table_row_counts: dict[str, int] = {}
    for p in profiles:
        if p.table_name not in table_row_counts:
            # Use distinct_count of the first column as a proxy for row count
            table_row_counts[p.table_name] = 0

    return rules


def _make_id() -> str:
    return str(uuid.uuid4())[:8]
