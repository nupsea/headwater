"""Headwater 2 S4 — Generic Semantic Typing.

Infers canonical semantic roles for every column in a source from four generic
signals applied in priority order:

  1. User-locked semantic_type in the H2 store (non-overridable)
  2. Resource-backed locked claims (from hw2 resource add --lock)
  3. dtype-based rules (bool → flag, timestamp → event_ts, numeric → measure)
  4. Name-pattern rules (_id/_key → identifier, date/time hints → event_ts)
  5. Profile statistics (uniqueness, cardinality, avg_length, top_values)
  6. Sibling consistency (start_X + end_X in same table → both event_ts)

No domain knowledge is encoded.  The roles produced match the vocabulary used
by the relevance engine (_TEMPORAL_ROLE_PREFIXES, _MEASURE_ROLES, etc.) so this
module is a drop-in replacement for H1's infer_semantic_schema.

Canonical roles:
  event_ts    timestamp or date-as-string column
  start_ts    start of a duration pair (sibling of end_ts)
  end_ts      end of a duration pair (sibling of start_ts)
  measure     generic numeric measurement
  duration    time duration (numeric or HH:MM varchar)
  quantity    count/amount/total (numeric, semantically summable)
  categorical low-cardinality varchar (< 100 distinct values)
  code        very short varchar with few distinct values (enum-like)
  identifier  unique or near-unique ID column
  flag        boolean or binary column
  free_text   long varchar (descriptions, notes, titles)
  location    geographic reference column
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from headwater.core.store import HeadwaterStore

# ── Constants ─────────────────────────────────────────────────────────────────

_TIMESTAMP_DTYPES = frozenset({
    "timestamp", "date", "datetime", "timestamptz", "timestamp with time zone",
    "timestamp without time zone",
})
_NUMERIC_DTYPES = frozenset({
    "int", "int8", "int16", "int32", "int64", "integer", "bigint", "smallint",
    "float", "float32", "float64", "double", "decimal", "numeric", "real",
})
_BOOL_DTYPES = frozenset({"bool", "boolean"})

_ID_SUFFIXES = re.compile(r"(^id$|_id$|_key$|_uuid$|_pk$|_fk$|_ref$)", re.I)
_DATE_NAME = re.compile(
    r"(^date$|_date$|_at$|^at$|_on$|^on$|_time$|created_|updated_|modified_|since_|timestamp$)",
    re.I,
)
_DURATION_NAME = re.compile(
    r"(duration|elapsed|wait|scan_time|throughput_time|cycle_time|"
    r"processing_time|lead_time|turnaround|spent|_ttl$)", re.I,
)
_QUANTITY_NAME = re.compile(
    r"(^count$|_count$|^num_|^number_|^total_|^sum_|^qty|quantity|^amount$)",
    re.I,
)
_FLAG_NAME = re.compile(
    r"(^is_|^has_|^was_|^can_|^should_|^flag$|_flag$|_yn$|_bool$|^active$|^enabled$)",
    re.I,
)
_LOCATION_NAME = re.compile(
    r"(^lat$|latitude|^lon$|^lng$|longitude|centroid|_geo$|^geom$|location_id|_zone$)",
    re.I,
)
_FREE_TEXT_NAME = re.compile(r"(description|note|comment|remark|narrative|text|body|content)", re.I)
_START_NAME = re.compile(r"(^start|_start$|^begin|_begin$|_from$|^from_)", re.I)
_END_NAME = re.compile(r"(^end_|^end$|_end$|_to$|^to_|_finish$|_complete)", re.I)
_PERIOD_NAME = re.compile(r"(period|season|quarter|year|month|week|hour|day$)", re.I)

_CODE_MAX_DISTINCT = 30
_CODE_MAX_AVG_LEN = 4.0
_CODE_MAX_UNIQUENESS = 0.05
_IDENTIFIER_MIN_UNIQUENESS = 0.95
_FREE_TEXT_MIN_AVG_LEN = 25.0
_CATEGORICAL_MAX_DISTINCT = 100


@dataclass(slots=True)
class ColumnSemantic:
    table_name: str
    column_name: str
    canonical_role: str
    confidence: float
    locked: bool = False
    evidence: list[str] = field(default_factory=list)


SemanticMap = dict[str, str]  # "table.column" → canonical_role


def infer_source_semantics(
    store: HeadwaterStore,
    source_name: str,
    *,
    project_id: str | None = None,
) -> SemanticMap:
    """Return a {table.column → canonical_role} mapping for all source columns.

    Locks in the H2 store and resource-backed claims take precedence over
    automatically inferred roles.
    """
    profiles = store.get_profiles(source_name)
    tables = store.get_tables(source_name)

    locked_roles: dict[str, str] = {}
    if project_id:
        for claim in store.list_semantic_claims(project_id):
            if claim.get("locked") and claim.get("claim_type") in ("definition", "semantic_type"):
                table = claim.get("table_name") or ""
                col = claim.get("column_name") or ""
                if table and col:
                    role = claim.get("claim", {}).get("semantic_type") or ""
                    if role:
                        locked_roles[f"{table}.{col}".lower()] = role

    profile_map: dict[str, dict[str, Any]] = {
        f"{p['table_name']}.{p['column_name']}".lower(): p["profile"]
        for p in profiles
    }

    col_meta: dict[str, dict[str, Any]] = {}
    for table in tables:
        for col in store.get_columns(source_name, table["name"]):
            key = f"{table['name']}.{col['name']}".lower()
            col_meta[key] = {
                "dtype": (col.get("dtype") or "varchar").lower(),
                "locked": bool(col.get("locked")),
                "semantic_type": col.get("semantic_type") or "",
                "table_name": table["name"],
                "column_name": col["name"],
            }

    # First pass: classify each column individually
    semantics: dict[str, ColumnSemantic] = {}
    for key, meta in col_meta.items():
        table_name = meta["table_name"]
        col_name = meta["column_name"]
        dtype = meta["dtype"]
        profile = profile_map.get(key, {})

        # Locked store value (highest priority)
        if meta["locked"] and meta["semantic_type"]:
            semantics[key] = ColumnSemantic(
                table_name=table_name,
                column_name=col_name,
                canonical_role=meta["semantic_type"],
                confidence=1.0,
                locked=True,
                evidence=["store_lock"],
            )
            continue

        # Resource-backed locked claim
        if key in locked_roles:
            semantics[key] = ColumnSemantic(
                table_name=table_name,
                column_name=col_name,
                canonical_role=locked_roles[key],
                confidence=0.95,
                locked=True,
                evidence=["resource_lock"],
            )
            continue

        role, confidence, evidence = _classify_column(col_name, dtype, profile)
        semantics[key] = ColumnSemantic(
            table_name=table_name,
            column_name=col_name,
            canonical_role=role,
            confidence=confidence,
            locked=False,
            evidence=evidence,
        )

    # Second pass: sibling consistency within each table
    _apply_sibling_consistency(semantics, col_meta)

    return {key: sem.canonical_role for key, sem in semantics.items()}


def _classify_column(
    col_name: str,
    dtype: str,
    profile: dict[str, Any],
) -> tuple[str, float, list[str]]:
    """Return (canonical_role, confidence, evidence) for one column."""
    # ── dtype-first rules ──────────────────────────────────────────────────
    if dtype in _BOOL_DTYPES:
        return "flag", 0.97, ["dtype=bool"]

    if dtype in _TIMESTAMP_DTYPES:
        if _START_NAME.search(col_name):
            return "start_ts", 0.90, ["dtype=timestamp", "name=start_"]
        if _END_NAME.search(col_name):
            return "end_ts", 0.90, ["dtype=timestamp", "name=end_"]
        return "event_ts", 0.92, ["dtype=timestamp"]

    uniqueness = float(profile.get("uniqueness_ratio") or 0.0)
    distinct = int(profile.get("distinct_count") or 0)
    avg_len = float(profile.get("avg_length") or 0.0)
    top_values = profile.get("top_values") or []

    if dtype in _NUMERIC_DTYPES:
        # Location-like numerics
        if _LOCATION_NAME.search(col_name):
            return "location", 0.80, ["dtype=numeric", "name=geo"]

        # Duration by name
        if _DURATION_NAME.search(col_name):
            return "duration", 0.85, ["dtype=numeric", "name=duration"]

        # Identifier by uniqueness or name
        if uniqueness >= _IDENTIFIER_MIN_UNIQUENESS:
            return "identifier", 0.88, ["dtype=numeric", f"uniqueness={uniqueness:.2f}"]
        if _ID_SUFFIXES.search(col_name):
            return "identifier", 0.85, ["dtype=numeric", "name=_id"]

        # Flag: 2 distinct values that look binary
        if distinct <= 2 and top_values:
            vals = {str(v[0]).strip().lower() for v in top_values if v}
            if vals.issubset({"0", "1", "true", "false", "y", "n", "yes", "no"}):
                return "flag", 0.88, ["dtype=numeric", "binary_values"]

        # Quantity by name
        if _QUANTITY_NAME.search(col_name):
            return "quantity", 0.82, ["dtype=numeric", "name=count/total"]

        return "measure", 0.78, ["dtype=numeric"]

    # ── varchar rules (in priority order) ─────────────────────────────────
    # Identifier by name
    if _ID_SUFFIXES.search(col_name):
        return "identifier", 0.85, ["dtype=varchar", "name=_id/_key"]

    # Date string by name
    if _DATE_NAME.search(col_name):
        return "event_ts", 0.80, ["dtype=varchar", "name=date/time"]

    # Duration string (varchar HH:MM or similar)
    if _DURATION_NAME.search(col_name):
        return "duration", 0.78, ["dtype=varchar", "name=duration"]

    # Location
    if _LOCATION_NAME.search(col_name):
        return "location", 0.80, ["dtype=varchar", "name=geo"]

    # Free text by name
    if _FREE_TEXT_NAME.search(col_name):
        return "free_text", 0.82, ["name=description/note"]

    # Flag by name
    if _FLAG_NAME.search(col_name):
        return "flag", 0.82, ["name=is_/has_"]

    # Profile-based varchar classification
    if uniqueness >= _IDENTIFIER_MIN_UNIQUENESS and distinct > 10:
        return "identifier", 0.82, [f"uniqueness={uniqueness:.2f}"]

    if (
        distinct >= 2
        and distinct <= _CODE_MAX_DISTINCT
        and avg_len <= _CODE_MAX_AVG_LEN
        and uniqueness <= _CODE_MAX_UNIQUENESS
    ):
        return "code", 0.78, [f"distinct={distinct}", f"avg_len={avg_len:.1f}"]

    if avg_len >= _FREE_TEXT_MIN_AVG_LEN:
        return "free_text", 0.72, [f"avg_len={avg_len:.1f}"]

    if distinct <= _CATEGORICAL_MAX_DISTINCT:
        return "categorical", 0.68, [f"distinct={distinct}"]

    return "categorical", 0.55, ["fallback"]


def _apply_sibling_consistency(
    semantics: dict[str, ColumnSemantic],
    col_meta: dict[str, dict[str, Any]],
) -> None:
    """Boost confidence for paired start/end temporal columns in the same table."""
    by_table: dict[str, list[str]] = {}
    for key, meta in col_meta.items():
        by_table.setdefault(meta["table_name"], []).append(key)

    for table_keys in by_table.values():
        start_cols = [k for k in table_keys if _START_NAME.search(k.split(".")[-1])]
        end_cols = [k for k in table_keys if _END_NAME.search(k.split(".")[-1])]

        # Promote start/end pairs to start_ts/end_ts
        for s_key in start_cols:
            sem = semantics.get(s_key)
            if (
                sem and not sem.locked
                and sem.canonical_role in ("event_ts", "measure")
                and end_cols
            ):
                sem.canonical_role = "start_ts"
                sem.confidence = min(1.0, sem.confidence + 0.10)
                sem.evidence.append("sibling_pair")

        for e_key in end_cols:
            sem = semantics.get(e_key)
            if (
                sem and not sem.locked
                and sem.canonical_role in ("event_ts", "measure")
                and start_cols
            ):
                sem.canonical_role = "end_ts"
                sem.confidence = min(1.0, sem.confidence + 0.10)
                sem.evidence.append("sibling_pair")

        # Period columns (year, month, hour, day) are temporal but not start/end
        for key in table_keys:
            col_name = key.split(".")[-1]
            sem = semantics.get(key)
            if (
                sem and not sem.locked
                and _PERIOD_NAME.search(col_name)
                and sem.canonical_role not in ("event_ts", "start_ts", "end_ts")
            ):
                sem.canonical_role = "event_ts"
                sem.evidence.append("period_name")
