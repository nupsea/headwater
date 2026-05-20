"""Semantic role inference for insight generation.

Core inference stays domain-neutral. Project or source vocabularies are loaded
from ``metadata/<project>/semantic_schema.yaml``.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

import yaml

from headwater.analyzer.metadata_retrieval import RetrievedMetadata
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


_DEFAULT_ROLE_SPEC: list[dict[str, str | float]] = []


def _metadata_root() -> Path:
    return Path(__file__).resolve().parents[2] / "metadata"


def _slugify_metadata_key(value: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized.strip("-")


def _compact_metadata_key(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def _candidate_role_spec_paths(source_name: str | None, project_id: str | None) -> list[Path]:
    candidates: list[Path] = []
    metadata_root = _metadata_root()
    for name in [project_id, source_name]:
        if not name:
            continue
        variants = [
            str(name),
            _slugify_metadata_key(str(name)),
            _compact_metadata_key(str(name)),
        ]
        for variant in variants:
            if not variant:
                continue
            path = metadata_root / variant / "semantic_schema.yaml"
            if path not in candidates:
                candidates.append(path)
    return candidates


def _parse_role_spec(path: Path) -> dict | None:
    try:
        parsed = yaml.safe_load(path.read_text()) or {}
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    try:
        parsed = json.loads(path.read_text())
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return None
    return None


def _merge_role_specs(
    base: list[dict[str, str | float]],
    override: dict,
) -> list[dict[str, str | float]]:
    roles = override.get("roles")
    if not isinstance(roles, list):
        return list(base)
    normalized = [
        role
        for role in roles
        if isinstance(role, dict)
        and isinstance(role.get("role"), str)
        and isinstance(role.get("pattern"), str)
    ]
    if override.get("replace_defaults"):
        return normalized
    return normalized + list(base)


def _role_patterns(
    source_name: str | None = None,
    project_id: str | None = None,
) -> list[tuple[str, re.Pattern[str], float, str]]:
    spec = list(_DEFAULT_ROLE_SPEC)
    for path in _candidate_role_spec_paths(source_name, project_id):
        if not path.exists():
            continue
        parsed = _parse_role_spec(path)
        if parsed:
            spec = _merge_role_specs(spec, parsed)
            break
    patterns: list[tuple[str, re.Pattern[str], float, str]] = []
    for entry in spec:
        role = str(entry["role"])
        pattern = str(entry["pattern"])
        confidence = float(entry.get("confidence", 0.7))
        reason = str(entry.get("reason") or role.replace("_", " "))
        patterns.append(_role_pattern(role, pattern, confidence, reason))
    return patterns


def infer_semantic_schema(
    discovery: DiscoveryResult,
    context: DatasetContext | None = None,
    project_id: str | None = None,
    metadata: RetrievedMetadata | None = None,
) -> SemanticSchema:
    """Infer canonical roles and derived fields from schema/profile metadata."""
    profile_index = {(p.table_name, p.column_name): p for p in discovery.profiles}
    role_patterns = _role_patterns(discovery.source.name, project_id)
    roles: list[SemanticColumnRole] = []

    for table in discovery.tables:
        for col in table.columns:
            profile = profile_index.get((table.name, col.name))
            role, confidence, reason = _infer_column_role(
                col.name,
                col.dtype,
                profile,
                role_patterns,
            )
            context_role = None
            if metadata is not None:
                context_role = metadata.locked_roles.get((table.name, col.name))

            if col.locked and col.role:
                role = _role_from_locked_column(col.role, col.semantic_type) or role
                confidence = max(confidence, 0.96)
                source = "human_lock"
                reason = "Confirmed in the data dictionary"
            elif context_role:
                role = _role_from_locked_column(context_role, context_role) or role
                confidence = max(confidence, 0.94)
                source = "context"
                reason = "Confirmed in the project context layer"
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
        derived_fields=_derive_fields(roles, metadata),
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
    role_patterns: list[tuple[str, re.Pattern[str], float, str]],
) -> tuple[str | None, float, str | None]:
    for role, pattern, confidence, reason in role_patterns:
        if pattern.search(name):
            if role.endswith("_ts") and not _TEMPORAL_DTYPE.search(dtype):
                confidence -= 0.18
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
    if role:
        normalized_role = _normalize_role_name(role)
        role_aliases = {
            "temporal": "event_ts",
            "date": "event_ts",
            "datetime": "event_ts",
            "timestamp": "event_ts",
            "time": "event_ts",
            "metric": "measure",
        }
        return role_aliases.get(normalized_role, normalized_role)
    value = (semantic_type or "").lower()
    if "temporal" in value or "date" in value or "time" in value:
        return "event_ts"
    if "id" in value or "key" in value:
        return "identifier"
    if "metric" in value:
        return "measure"
    if "dimension" in value:
        return "dimension"
    if "text" in value or "string" in value:
        return "text"
    return None


def _derive_fields(
    roles: list[SemanticColumnRole],
    metadata: RetrievedMetadata | None = None,
) -> list[SemanticDerivedField]:
    by_table: dict[str, dict[str, SemanticColumnRole]] = defaultdict(dict)
    for role in roles:
        current = by_table[role.table_name].get(role.canonical_role)
        if current is None or role.confidence > current.confidence:
            by_table[role.table_name][role.canonical_role] = role

    derived: list[SemanticDerivedField] = []
    for table_name, table_roles in by_table.items():
        start = _preferred_time_role(table_roles)

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
        derived.extend(_context_derived_fields(table_name, table_roles, metadata))
    return derived


def _preferred_time_role(
    table_roles: dict[str, SemanticColumnRole],
) -> SemanticColumnRole | None:
    for role_name, role in table_roles.items():
        if role_name == "event_ts" or role_name.endswith("_ts"):
            return role
    return None


def _context_derived_fields(
    table_name: str,
    table_roles: dict[str, SemanticColumnRole],
    metadata: RetrievedMetadata | None,
) -> list[SemanticDerivedField]:
    if metadata is None:
        return []
    derived: list[SemanticDerivedField] = []
    for item in metadata.context_items:
        if item.item_type != "derived_field" or item.status not in {"approved", "locked"}:
            continue
        if item.table_name and item.table_name != table_name:
            continue
        value = item.value or {}
        required_roles = [str(role) for role in value.get("required_roles") or []]
        if not required_roles or not all(role in table_roles for role in required_roles):
            continue
        expression = str(value.get("expression") or "").strip()
        if not expression:
            continue
        derived.append(
            SemanticDerivedField(
                table_name=table_name,
                name=str(value.get("name") or item.name),
                expression=_fill_role_placeholders(expression, table_roles),
                role=str(value.get("role") or value.get("semantic_type") or item.name),
                required_roles=required_roles,
                confidence=item.confidence,
            )
        )
    return derived


def _fill_role_placeholders(
    expression: str,
    table_roles: dict[str, SemanticColumnRole],
) -> str:
    rendered = expression
    for role_name, role in table_roles.items():
        rendered = rendered.replace("{" + role_name + "}", quote_ident(role.column_name))
    return rendered


def _normalize_role_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return normalized.strip("_")


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
