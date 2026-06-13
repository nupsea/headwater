"""A compact, domain-agnostic upper ontology — the missing 'meaning' layer.

Not OWL: a small fixed set of concept types every dataset maps onto. The *model*
is fixed; the *assignments* are inferred from dtype/cardinality/name-shape/stats
(never hardcoded domain values), then proposed by the LLM and verified here. This
is the deterministic verifier + heuristic fallback for the ``ontology.map`` node.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

from headwater.knowledge.projection import GraphNode

Concept = Literal[
    "Entity",
    "Event",
    "Measure",
    "Dimension",
    "Code",
    "Actor",
    "Location",
    "TimeAnchor",
    "Identifier",
    "Derivation",
]

Relation = Literal[
    "MEASURES",
    "OCCURS_AT",
    "LOCATED_IN",
    "PERFORMED_BY",
    "BELONGS_TO",
    "REFERENCES",
    "DERIVED_FROM",
    "MAPS_TO_CODE",
    "SEGMENTS",
]

# Generic (non-domain) name-shape cues. These are measurement/structure English,
# not domain entities — kept deliberately free of any dataset's vocabulary.
_TIME_RE = re.compile(r"(^|_)(date|time|datetime|timestamp|year|month|day|week|hour)s?($|_)", re.I)
_TIME_AT_RE = re.compile(r"_at$", re.I)
_ID_RE = re.compile(r"(^id$|_id$|_key$|uuid|guid)", re.I)
# Boolean-ish shape: a flag is a segmenting attribute, never an aggregatable
# quantity — "which X has the highest downgrade_flag" is not a question.
_FLAG_RE = re.compile(r"(^|_)(is|has|flag|bool|active|enabled|deleted)(_|$)", re.I)
_LOCATION_RE = re.compile(
    r"(location|site|zone|region|room|area|city|country|state|address|geo|lat|lon)", re.I
)
_STATUS_RE = re.compile(r"(status|state|flag|type|category|class|kind|group|segment)", re.I)
_STEP_RE = re.compile(r"(step|stage|phase|activity|action|process)", re.I)

# unit-of-measure cues for a numeric Measure (generic vocabulary only).
_UNIT_CUES: tuple[tuple[str, str], ...] = (
    ("duration", r"(duration|elapsed|minutes|seconds|hours|mins|secs|hrs|_ms$|latency)"),
    ("count", r"(count|num_|_num|qty|quantity|number)"),
    ("amount", r"(amount|total|sum|price|cost|revenue|value|balance)"),
    ("rate", r"(rate|ratio|pct|percent|share|fraction|avg_|mean_)"),
)

# dtype-family prefixes (handles int64/float64/bigint/timestamp_ns/... from any engine).
_NUMERIC_PREFIXES = (
    "int", "float", "double", "decimal", "numeric", "real", "number",
    "bigint", "smallint", "tinyint", "long",
)
_TEMPORAL_PREFIXES = ("date", "datetime", "timestamp", "time")


def _is_numeric(dtype: str) -> bool:
    d = dtype.lower()
    return d != "bool" and any(d.startswith(p) for p in _NUMERIC_PREFIXES)


def _is_temporal(dtype: str) -> bool:
    d = dtype.lower()
    return any(d.startswith(p) for p in _TEMPORAL_PREFIXES)


@dataclass(frozen=True, slots=True)
class ColumnStats:
    """The I-3-safe column summary the classifier reads (no raw rows)."""

    ref: str  # "table.column"
    dtype: str
    distinct: int
    total: int
    top_values: tuple[str, ...] = ()
    is_key: bool = False
    in_fk: bool = False

    @property
    def distinct_ratio(self) -> float:
        return self.distinct / self.total if self.total else 0.0


@dataclass(frozen=True, slots=True)
class ConceptAssignment:
    """A column's inferred meaning — advisory until confirmed + locked (I-6)."""

    col_ref: str
    concept: Concept
    props: dict[str, str] = field(default_factory=dict)
    confidence: float = 0.5
    locked: bool = False
    source: Literal["heuristic", "llm", "user"] = "heuristic"

    @property
    def table(self) -> str:
        return self.col_ref.rsplit(".", 1)[0] if "." in self.col_ref else ""

    def to_node(self) -> GraphNode:
        return GraphNode(
            id=f"col:{self.col_ref}",
            type=self.concept,
            props={
                **self.props,
                "ref": self.col_ref,
                "table": self.table,
                "confidence": f"{self.confidence:.2f}",
                "locked": "1" if self.locked else "0",
                "source": self.source,
            },
        )


def _is_binary_flag(stats: ColumnStats, name: str) -> bool:
    """True when the evidence says this numeric column is a two-valued flag.

    Precedence: observed statistics first — known distinct count <= 2 IS a
    flag, known distinct count > 2 is NOT one. The name-shape cue (is_/has_/
    _flag) decides only when no statistics exist; it never overrides evidence.
    """
    if not _is_numeric(stats.dtype):
        return False
    if stats.distinct > 2:
        return False  # evidence: many values -> a quantity, whatever the name
    if 0 < stats.distinct <= 2 and stats.total > 2:
        return True  # evidence: two-valued -> a flag, whatever the name
    return bool(_FLAG_RE.search(name))  # no stats -> fall back to name shape


def _unit_for(name: str) -> str | None:
    for unit, pat in _UNIT_CUES:
        if re.search(pat, name, re.I):
            return unit
    return None


def _dimension_kind(name: str) -> str:
    if _STEP_RE.search(name):
        return "step"
    if _LOCATION_RE.search(name):
        return "location"
    if _STATUS_RE.search(name):
        return "status"
    return "category"


def classify_column(stats: ColumnStats) -> ConceptAssignment:
    """Deterministically classify one column into the upper ontology.

    Order matters: identifiers and time anchors are recognised before the
    numeric/categorical split so a high-cardinality key is not mistaken for a
    Measure, nor a date for a Dimension.
    """
    name = stats.ref.rsplit(".", 1)[-1]
    dtype = stats.dtype.lower()

    # 1. TimeAnchor — temporal dtype or an unambiguous time-like name.
    if _is_temporal(dtype) or _TIME_RE.search(name) or _TIME_AT_RE.search(name):
        return ConceptAssignment(stats.ref, "TimeAnchor", {"grain": "row"}, 0.85)

    # 2. Identifier — key-shaped name, or a near-unique key/fk column.
    if _ID_RE.search(name) or stats.is_key or (stats.in_fk and stats.distinct_ratio > 0.9):
        return ConceptAssignment(stats.ref, "Identifier", {}, 0.8)

    # 3. Location — explicit geographic/place name shape.
    if _LOCATION_RE.search(name):
        return ConceptAssignment(stats.ref, "Location", {"kind": "location"}, 0.7)

    # 4. Flag / binary — a two-valued column is a segmenting attribute, not an
    # aggregatable quantity. Ranking by a flag is always nonsense ("highest
    # downgrade_flag: 0"); grouping BY it is fine. Evidence outranks the name:
    # the flag-shaped-name cue applies only when statistics are absent or agree
    # — "active_devices" with 400 distinct values is a count, whatever the
    # morpheme suggests.
    if dtype == "bool" or _is_binary_flag(stats, name):
        return ConceptAssignment(stats.ref, "Dimension", {"kind": "status"}, 0.8)

    # 5. Measure — numeric, not an identifier/flag, with a unit cue or spread.
    if _is_numeric(dtype):
        unit = _unit_for(name)
        conf = 0.85 if unit else 0.6
        return ConceptAssignment(stats.ref, "Measure", {"unit": unit or "quantity"}, conf)

    # 6. Code — low-cardinality, code-like short tokens.
    if stats.distinct and stats.distinct <= 50 and _looks_code_like(stats.top_values):
        return ConceptAssignment(stats.ref, "Code", {"needs_mapping": "1"}, 0.6)

    # 7. Dimension — the categorical default for everything else.
    return ConceptAssignment(stats.ref, "Dimension", {"kind": _dimension_kind(name)}, 0.55)


def compatible_concept(concept: str, stats: ColumnStats) -> bool:
    """Deterministic gate for an LLM-proposed concept (L proposes, D verifies).

    A proposal is accepted only when the column's observable shape can support
    it: a Measure must be numeric, a TimeAnchor temporal (by dtype or name).
    Categorical concepts are shape-permissive — the model's reading of the
    name/description is the added signal there.
    """
    name = stats.ref.rsplit(".", 1)[-1]
    if concept == "Measure":
        # Numeric, and not a flag/binary — a two-valued column can never be a
        # rankable quantity, whatever the model says.
        if not _is_numeric(stats.dtype) or stats.dtype.lower() == "bool":
            return False
        return not _is_binary_flag(stats, name)
    if concept == "TimeAnchor":
        return _is_temporal(stats.dtype) or bool(
            _TIME_RE.search(name) or _TIME_AT_RE.search(name)
        )
    return concept in {"Dimension", "Code", "Identifier", "Location"}


def _looks_code_like(values: tuple[str, ...]) -> bool:
    if not values:
        return False
    short = [v for v in values if v and len(str(v)) <= 6]
    coded = [v for v in short if re.fullmatch(r"[A-Za-z0-9._-]+", str(v))]
    return len(coded) >= max(1, len(values) // 2)
