"""Detect text-duration columns and propose a parse-to-minutes derivation.

Domain-agnostic and advisory.  Detection inspects sample values, picks a
best-guess duration shape, and offers alternative interpretations; nothing is
applied until the user confirms (the chosen derivation is stored as a locked
semantic claim by the resolve flow, and the answer generator then aggregates the
parsed minutes instead of failing to cast text).

Extensibility: a new duration shape is one entry in ``FORMATS`` plus one row in
``_SHAPES`` — no other module changes.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

# Cap how many sample values detection inspects.
_MAX_SAMPLES = 50
# Fraction of samples that must match a shape before we propose it.
_MATCH_THRESHOLD = 0.6


@dataclass(frozen=True)
class DurationFormat:
    """One recognized duration encoding and how to convert it to minutes.

    ``to_minutes_sql`` takes an already-quoted column reference and returns a
    DuckDB expression yielding a numeric value in minutes (NULL on bad input,
    via TRY_CAST — never raises).
    """

    id: str
    label: str
    to_minutes_sql: Callable[[str], str]


def _hh_mm(c: str) -> str:
    return (
        f"(TRY_CAST(split_part({c}, ':', 1) AS DOUBLE) * 60 "
        f"+ TRY_CAST(split_part({c}, ':', 2) AS DOUBLE))"
    )


def _mm_ss(c: str) -> str:
    return (
        f"(TRY_CAST(split_part({c}, ':', 1) AS DOUBLE) "
        f"+ TRY_CAST(split_part({c}, ':', 2) AS DOUBLE) / 60)"
    )


def _hh_mm_ss(c: str) -> str:
    return (
        f"(TRY_CAST(split_part({c}, ':', 1) AS DOUBLE) * 60 "
        f"+ TRY_CAST(split_part({c}, ':', 2) AS DOUBLE) "
        f"+ TRY_CAST(split_part({c}, ':', 3) AS DOUBLE) / 60)"
    )


def _epoch_minutes(c: str) -> str:
    # For a TIME / INTERVAL column the value is already a duration; epoch gives
    # seconds, so divide by 60 for minutes.
    return f"(EXTRACT(EPOCH FROM {c}) / 60.0)"


def _days_hh_mm_ss(c: str) -> str:
    # e.g. "0 days 00:22:00": days in field 1, "HH:MM:SS" in field 3.
    t = f"split_part({c}, ' ', 3)"
    return (
        f"(TRY_CAST(split_part({c}, ' ', 1) AS DOUBLE) * 1440 "
        f"+ TRY_CAST(split_part({t}, ':', 1) AS DOUBLE) * 60 "
        f"+ TRY_CAST(split_part({t}, ':', 2) AS DOUBLE) "
        f"+ TRY_CAST(split_part({t}, ':', 3) AS DOUBLE) / 60)"
    )


# Registry of supported formats, keyed by stable id.
FORMATS: dict[str, DurationFormat] = {
    "epoch_minutes": DurationFormat(
        "epoch_minutes", "time / duration value", _epoch_minutes
    ),
    "hh_mm": DurationFormat("hh_mm", "HH:MM (hours : minutes)", _hh_mm),
    "mm_ss": DurationFormat("mm_ss", "MM:SS (minutes : seconds)", _mm_ss),
    "hh_mm_ss": DurationFormat("hh_mm_ss", "HH:MM:SS", _hh_mm_ss),
    "days_hh_mm_ss": DurationFormat(
        "days_hh_mm_ss", "D days HH:MM:SS", _days_hh_mm_ss
    ),
}

# Column dtypes that are already a duration/time value — convert via epoch, no
# string parsing. (Real timestamps/dates are excluded: they aren't measures.)
_TEMPORAL_DURATION_DTYPES = frozenset({"time", "time_ns", "interval", "duration"})


def is_temporal_duration_dtype(dtype: str | None) -> bool:
    """True for a TIME/INTERVAL dtype that detects as a duration from its type alone."""
    return bool(dtype) and dtype.strip().lower() in _TEMPORAL_DURATION_DTYPES

# A shape is a value pattern mapped to a default interpretation + alternatives.
# Order matters: most specific first.
_SHAPES: list[tuple[re.Pattern[str], str, list[str]]] = [
    (re.compile(r"^\d+\s+days?\s+\d{1,2}:\d{2}:\d{2}$", re.IGNORECASE), "days_hh_mm_ss", []),
    (re.compile(r"^\d{1,3}:\d{2}:\d{2}$"), "hh_mm_ss", []),
    (re.compile(r"^\d{1,3}:\d{2}$"), "hh_mm", ["mm_ss"]),
]


@dataclass(frozen=True)
class DurationProposal:
    """A best-guess derivation plus user-selectable alternatives."""

    detected: DurationFormat
    alternatives: list[DurationFormat] = field(default_factory=list)
    samples: list[str] = field(default_factory=list)
    unit: str = "minutes"

    @property
    def all_formats(self) -> list[DurationFormat]:
        return [self.detected, *self.alternatives]


def _clean_samples(values: Iterable[object]) -> list[str]:
    out: list[str] = []
    for v in values:
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
        if len(out) >= _MAX_SAMPLES:
            break
    return out


def detect_duration(
    values: Iterable[object], dtype: str | None = None
) -> DurationProposal | None:
    """Propose a duration derivation, or None if the column isn't a duration.

    A TIME/INTERVAL ``dtype`` is already a duration value → propose an epoch
    conversion (no samples needed).  Otherwise the column is text: pick the first
    string shape that matches a majority of the samples, so arbitrary text yields
    no proposal.
    """
    samples = _clean_samples(values)
    if is_temporal_duration_dtype(dtype):
        return DurationProposal(detected=FORMATS["epoch_minutes"], samples=samples[:5])
    if not samples:
        return None
    for pattern, default_id, alt_ids in _SHAPES:
        matches = sum(1 for s in samples if pattern.match(s))
        if matches >= max(1, int(len(samples) * _MATCH_THRESHOLD)):
            return DurationProposal(
                detected=FORMATS[default_id],
                alternatives=[FORMATS[a] for a in alt_ids],
                samples=samples[:5],
            )
    return None


def to_minutes_sql(quoted_column: str, format_id: str) -> str:
    """DuckDB expression converting ``quoted_column`` (text) to minutes.

    ``quoted_column`` must already be a safe, quoted identifier.
    """
    fmt = FORMATS.get(format_id)
    if fmt is None:
        raise ValueError(f"Unknown duration format '{format_id}'.")
    return fmt.to_minutes_sql(quoted_column)
