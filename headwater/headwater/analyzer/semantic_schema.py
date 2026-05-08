"""Semantic role inference for insight generation.

The inference layer is intentionally generic: it maps physical columns onto
canonical lifecycle, metric, location, and quality roles that insight families
can use without embedding a domain-specific code path.
"""

from __future__ import annotations

import re
from collections import defaultdict

from headwater.core.models import (
    ColumnProfile,
    DatasetContext,
    DiscoveryResult,
    SemanticColumnRole,
    SemanticDerivedField,
    SemanticSchema,
)

_TEMPORAL_DTYPE = re.compile(r"(date|time|timestamp|datetime)", re.I)
_NUMERIC_DTYPE = re.compile(r"(int|float|double|decimal|numeric|real|bigint|hugeint)", re.I)


def _role_pattern(
    role: str,
    pattern: str,
    confidence: float,
    reason: str,
) -> tuple[str, re.Pattern[str], float, str]:
    return role, re.compile(pattern, re.I), confidence, reason


_ROLE_PATTERNS: list[tuple[str, re.Pattern[str], float, str]] = [
    _role_pattern(
        "lifecycle_start_ts",
        r"(pickup|start|begin|opened|created|order(ed)?|event).*"
        r"(_?date|_?time|_?ts|datetime)|(^|_)pickup_datetime$|^started_at$",
        0.92,
        "lifecycle start timestamp",
    ),
    _role_pattern(
        "lifecycle_end_ts",
        r"(dropoff|end|finish|closed|resolved|delivered|completed).*"
        r"(_?date|_?time|_?ts|datetime)|(^|_)dropoff_datetime$|^ended_at$",
        0.92,
        "lifecycle end timestamp",
    ),
    _role_pattern(
        "request_ts",
        r"(request|dispatch|book|created).*(_?date|_?time|_?ts|datetime)",
        0.86,
        "request or dispatch timestamp",
    ),
    _role_pattern(
        "event_ts",
        r"(^|_)(date|datetime|timestamp|time|ts|period|event_date|created_at|updated_at)$",
        0.72,
        "event timestamp",
    ),
    _role_pattern(
        "origin_id",
        r"(^|_)(origin|source|from|pickup|pu|start|departure).*"
        r"(_?id|location|zone|station|site|place)|^pulocationid$",
        0.88,
        "origin/location identifier",
    ),
    _role_pattern(
        "destination_id",
        r"(^|_)(destination|dest|target|to|dropoff|do|end|arrival).*"
        r"(_?id|location|zone|station|site|place)|^dolocationid$",
        0.88,
        "destination/location identifier",
    ),
    _role_pattern(
        "location_id",
        r"(^|_)(location|zone|site|place|station|region).*(_?id|code)?$",
        0.68,
        "location identifier",
    ),
    _role_pattern("distance", r"(distance|miles|kilometers|km|mi)$", 0.9, "distance measure"),
    _role_pattern(
        "duration",
        r"(duration|elapsed|trip_time|travel_time|time_seconds|time_min)",
        0.88,
        "duration measure",
    ),
    _role_pattern(
        "amount",
        r"(amount|fare|price|cost|charge|revenue|sales|total|payment|pay)$",
        0.78,
        "monetary amount",
    ),
    _role_pattern("tip_amount", r"(tip|gratuity)", 0.9, "tip amount"),
    _role_pattern(
        "count",
        r"(^|_)(count|qty|quantity|units|passenger_count|num_)",
        0.72,
        "count measure",
    ),
    _role_pattern(
        "service_type",
        r"(service|provider|vendor|type|category|class|mode)$",
        0.72,
        "service or category",
    ),
]


def infer_semantic_schema(
    discovery: DiscoveryResult,
    context: DatasetContext | None = None,
) -> SemanticSchema:
    """Infer canonical roles and derived fields from schema/profile metadata."""
    profile_index = {(p.table_name, p.column_name): p for p in discovery.profiles}
    roles: list[SemanticColumnRole] = []

    for table in discovery.tables:
        for col in table.columns:
            profile = profile_index.get((table.name, col.name))
            role, confidence, reason = _infer_column_role(col.name, col.dtype, profile)

            if col.locked and col.role:
                role = _role_from_locked_column(col.role, col.semantic_type) or role
                confidence = max(confidence, 0.96)
                source = "human_lock"
                reason = "Confirmed in the data dictionary"
            else:
                source = "name_registry" if confidence >= 0.7 else "profile_stats"

            if context and context.lifecycle and role in {"lifecycle_start_ts", "lifecycle_end_ts"}:
                confidence = min(0.98, confidence + 0.03)
                source = "context" if source != "human_lock" else source

            if role:
                roles.append(
                    SemanticColumnRole(
                        table_name=table.name,
                        column_name=col.name,
                        canonical_role=role,
                        confidence=round(confidence, 3),
                        source=source,
                        locked=col.locked,
                        reason=reason,
                    )
                )

    return SemanticSchema(
        source_name=discovery.source.name,
        columns=roles,
        derived_fields=_derive_fields(roles),
    )


def roles_by_table(schema: SemanticSchema) -> dict[str, dict[str, SemanticColumnRole]]:
    grouped: dict[str, dict[str, SemanticColumnRole]] = defaultdict(dict)
    for role in schema.columns:
        current = grouped[role.table_name].get(role.canonical_role)
        if current is None or role.confidence > current.confidence:
            grouped[role.table_name][role.canonical_role] = role
    return dict(grouped)


def roles_for_table(schema: SemanticSchema, table_name: str) -> dict[str, SemanticColumnRole]:
    return roles_by_table(schema).get(table_name, {})


def ambiguous_roles(schema: SemanticSchema, threshold: float = 0.8) -> list[SemanticColumnRole]:
    return [role for role in schema.columns if not role.locked and role.confidence < threshold]


def _infer_column_role(
    name: str,
    dtype: str,
    profile: ColumnProfile | None,
) -> tuple[str | None, float, str | None]:
    for role, pattern, confidence, reason in _ROLE_PATTERNS:
        if pattern.search(name):
            if role.endswith("_ts") and not _TEMPORAL_DTYPE.search(dtype):
                confidence -= 0.18
            if role in {"distance", "duration", "amount", "tip_amount", "count"} and (
                not _NUMERIC_DTYPE.search(dtype)
            ):
                confidence -= 0.2
            return role, max(0.5, confidence), reason

    if _TEMPORAL_DTYPE.search(dtype):
        return "event_ts", 0.66, "temporal dtype"
    if (
        profile
        and _NUMERIC_DTYPE.search(dtype)
        and profile.distinct_count > 10
        and not re.search(r"(_id|^id$|code$|key$)", name, re.I)
    ):
        return "measure", 0.58, "numeric column with variation"
    return None, 0.0, None


def _role_from_locked_column(role: str | None, semantic_type: str | None) -> str | None:
    value = " ".join(v for v in (role, semantic_type) if v).lower()
    if "temporal" in value or "date" in value or "time" in value:
        return "event_ts"
    if "geo" in value or "location" in value:
        return "location_id"
    if "metric" in value:
        return "measure"
    if "dimension" in value:
        return "service_type"
    return None


def _derive_fields(roles: list[SemanticColumnRole]) -> list[SemanticDerivedField]:
    by_table: dict[str, dict[str, SemanticColumnRole]] = defaultdict(dict)
    for role in roles:
        current = by_table[role.table_name].get(role.canonical_role)
        if current is None or role.confidence > current.confidence:
            by_table[role.table_name][role.canonical_role] = role

    derived: list[SemanticDerivedField] = []
    for table_name, table_roles in by_table.items():
        start = table_roles.get("lifecycle_start_ts") or table_roles.get("event_ts")
        end = table_roles.get("lifecycle_end_ts")
        request = table_roles.get("request_ts")
        origin = table_roles.get("origin_id") or table_roles.get("location_id")
        dest = table_roles.get("destination_id")
        distance = table_roles.get("distance")

        if start:
            derived.extend(
                [
                    SemanticDerivedField(
                        table_name=table_name,
                        name="event_date",
                        expression=f"CAST({quote_ident(start.column_name)} AS DATE)",
                        role="date_bucket",
                        required_roles=[start.canonical_role],
                        confidence=start.confidence,
                    ),
                    SemanticDerivedField(
                        table_name=table_name,
                        name="event_hour",
                        expression=f"EXTRACT(hour FROM {quote_ident(start.column_name)})",
                        role="hour_bucket",
                        required_roles=[start.canonical_role],
                        confidence=start.confidence,
                    ),
                    SemanticDerivedField(
                        table_name=table_name,
                        name="day_of_week",
                        expression=f"EXTRACT(dow FROM {quote_ident(start.column_name)})",
                        role="weekday_bucket",
                        required_roles=[start.canonical_role],
                        confidence=start.confidence,
                    ),
                ]
            )
        if start and end:
            derived.append(
                SemanticDerivedField(
                    table_name=table_name,
                    name="duration_min",
                    expression=(
                        f"date_diff('second', {quote_ident(start.column_name)}, "
                        f"{quote_ident(end.column_name)}) / 60.0"
                    ),
                    role="duration_minutes",
                    required_roles=[start.canonical_role, end.canonical_role],
                    confidence=min(start.confidence, end.confidence),
                )
            )
        if request and start:
            derived.append(
                SemanticDerivedField(
                    table_name=table_name,
                    name="wait_min",
                    expression=(
                        f"date_diff('second', {quote_ident(request.column_name)}, "
                        f"{quote_ident(start.column_name)}) / 60.0"
                    ),
                    role="wait_minutes",
                    required_roles=[request.canonical_role, start.canonical_role],
                    confidence=min(request.confidence, start.confidence),
                )
            )
        if start and end and distance:
            duration_expr = (
                f"date_diff('second', {quote_ident(start.column_name)}, "
                f"{quote_ident(end.column_name)}) / 3600.0"
            )
            derived.append(
                SemanticDerivedField(
                    table_name=table_name,
                    name="speed_per_hour",
                    expression=(
                        f"CASE WHEN {duration_expr} > 0 THEN "
                        f"{quote_ident(distance.column_name)} / ({duration_expr}) END"
                    ),
                    role="speed",
                    required_roles=[
                        start.canonical_role,
                        end.canonical_role,
                        distance.canonical_role,
                    ],
                    confidence=min(start.confidence, end.confidence, distance.confidence),
                )
            )
        if origin and dest:
            derived.append(
                SemanticDerivedField(
                    table_name=table_name,
                    name="route_pair",
                    expression=(
                        f"CAST({quote_ident(origin.column_name)} AS VARCHAR) || ' -> ' || "
                        f"CAST({quote_ident(dest.column_name)} AS VARCHAR)"
                    ),
                    role="route_pair",
                    required_roles=[origin.canonical_role, dest.canonical_role],
                    confidence=min(origin.confidence, dest.confidence),
                )
            )
    return derived


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
