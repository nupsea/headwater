"""Turn an executed answer into a plain-English finding.

Deterministic and domain-agnostic: given the chart spec + result rows, state the
single most useful takeaway ("X is highest at N, 2.3x the lowest", "Y fell 18%
over the period").  This is what makes the final page read as an *answer* rather
than a query result.  No LLM, no hardcoded domain terms — it reads the shape of
the data (ranking / trend / coverage) and the resolved value labels.

Extensibility: a new chart shape is one branch in ``summarize_answer`` + one
small helper.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Column-name hints that a category axis is really a time axis (→ line chart).
_TEMPORAL_HINT = re.compile(
    r"period|date|_at\b|time|month|year|week|day|quarter|hour", re.IGNORECASE
)

# Below this relative change a trend is "held steady" rather than rose/fell.
_FLAT_PCT = 0.05
# Only call out a top-vs-bottom ratio when it's material.
_MIN_RATIO = 1.2

# A question's wording tells us which end of the ranking is the answer. "Which
# segment has the lowest wait?" should surface the lowest, not the highest.
# Only unambiguous directional words — "worst"/"best" depend on whether high is
# good or bad for the measure, so we don't guess from them.
_LOW_INTENT = re.compile(
    r"\b(lowest|least|smallest|fewest|shortest|minimum|min|bottom)\b",
    re.IGNORECASE,
)
_HIGH_INTENT = re.compile(
    r"\b(highest|most|largest|greatest|longest|maximum|max|top)\b",
    re.IGNORECASE,
)


def _ranking_intent(title: str | None) -> str:
    """Return 'low' or 'high' (default) for the end of the ranking the question asks for."""
    if title and _LOW_INTENT.search(title) and not _HIGH_INTENT.search(title):
        return "low"
    return "high"


@dataclass(frozen=True)
class Finding:
    """One stated takeaway for an answer."""

    headline: str
    support: str = ""


def _num(v: Any) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    return None


def _fmt(v: float) -> str:
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    if abs(v - round(v)) < 1e-9:
        return f"{int(round(v))}"
    return f"{v:.1f}"


def _measure_name(y: str) -> str:
    name = y
    for prefix in ("avg_", "sum_", "total_", "mean_", "count_"):
        if name.startswith(prefix):
            name = name[len(prefix) :]
            break
    return name.replace("_", " ").strip() or y


def _label(col: str, value: Any, value_labels: dict[str, dict[str, str]]) -> str:
    mapping = value_labels.get(col) or {}
    return mapping.get(str(value)) or str(value)


def infer_chart_spec(
    columns: list[str], rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """Pick a chart for an arbitrary result (e.g. a promoted console query).

    First numeric column is the measure (y); first non-numeric column is the
    category/time axis (x).  A time-like x name → line, else bar.  Falls back to
    a plain table when there's nothing sensible to plot.
    """
    if not columns or not rows:
        return {"type": "table"}
    sample = rows[:20]
    numeric = [c for c in columns if any(_num(r.get(c)) is not None for r in sample)]
    non_numeric = [c for c in columns if c not in numeric]
    if not numeric or not non_numeric:
        return {"type": "table"}
    x, y = non_numeric[0], numeric[0]
    return {"type": "line" if _TEMPORAL_HINT.search(x) else "bar", "x": x, "y": y}


def summarize_answer(
    *,
    chart_spec: dict[str, Any] | None,
    columns: list[str],
    rows: list[dict[str, Any]],
    value_labels: dict[str, dict[str, str]] | None = None,
    unit: str | None = None,
    title: str | None = None,
) -> Finding | None:
    """Return a one-line finding for an executed answer, or None if not derivable.

    ``title`` orients ranking findings: a question that asks for the *lowest*
    surfaces the lowest segment, not the highest — the finding must answer the
    question that was actually asked.
    """
    if not rows:
        return None
    spec = chart_spec or {}
    ctype = spec.get("type")
    x, y = spec.get("x"), spec.get("y")
    labels = value_labels or {}
    suffix = f" {unit}" if unit else ""

    if ctype == "bar" and x and y:
        return _segment_finding(rows, x, y, labels, suffix, _ranking_intent(title))
    if ctype == "line" and x and y:
        return _temporal_finding(rows, x, y, suffix)
    return _coverage_finding(rows, columns)


def _segment_finding(
    rows: list[dict[str, Any]],
    x: str,
    y: str,
    labels: dict[str, dict[str, str]],
    suffix: str,
    intent: str = "high",
) -> Finding | None:
    pairs = [
        (_label(x, r.get(x), labels), _num(r.get(y)))
        for r in rows
        if _num(r.get(y)) is not None
    ]
    pairs = [(lbl, val) for lbl, val in pairs if val is not None]
    if not pairs:
        return None
    pairs.sort(key=lambda p: p[1], reverse=True)
    name = _measure_name(y)
    high_label, high_val = pairs[0]
    low_label, low_val = pairs[-1]

    if intent == "low":
        # The question asked for the lowest — lead with it.
        headline = f"{low_label} has the lowest {name}: {_fmt(low_val)}{suffix}."
        support = ""
        if len(pairs) > 1:
            if low_val > 0 and high_val / low_val >= _MIN_RATIO:
                support = (
                    f"Highest is {high_label} ({_fmt(high_val)}{suffix}), "
                    f"{high_val / low_val:.1f}x higher."
                )
            else:
                support = f"Highest is {high_label} ({_fmt(high_val)}{suffix})."
        return Finding(headline=headline, support=support)

    headline = f"{high_label} has the highest {name}: {_fmt(high_val)}{suffix}."
    support = ""
    if len(pairs) > 1:
        if low_val > 0 and high_val / low_val >= _MIN_RATIO:
            support = (
                f"{high_val / low_val:.1f}x the lowest, {low_label} "
                f"({_fmt(low_val)}{suffix})."
            )
        else:
            support = f"Lowest is {low_label} ({_fmt(low_val)}{suffix})."
    return Finding(headline=headline, support=support)


def _temporal_finding(
    rows: list[dict[str, Any]], x: str, y: str, suffix: str
) -> Finding | None:
    series = [(r.get(x), _num(r.get(y))) for r in rows if _num(r.get(y)) is not None]
    series = [(t, v) for t, v in series if v is not None]
    if not series:
        return None
    name = _measure_name(y)
    first_val, last_val = series[0][1], series[-1][1]
    peak_t, peak_v = max(series, key=lambda p: p[1])
    if first_val > 0:
        pct = (last_val - first_val) / first_val
        if abs(pct) < _FLAT_PCT:
            direction = "held roughly steady"
        else:
            direction = f"{'rose' if pct > 0 else 'fell'} {abs(pct) * 100:.0f}%"
        headline = f"{name.capitalize()} {direction} over the period."
    else:
        headline = f"{name.capitalize()} changes over the period."
    support = (
        f"Peak {_fmt(peak_v)}{suffix} at {peak_t}; "
        f"{len(series)} period{'s' if len(series) != 1 else ''}."
    )
    return Finding(headline=headline, support=support)


def _coverage_finding(
    rows: list[dict[str, Any]], columns: list[str]
) -> Finding | None:
    row = rows[0]
    total = None
    for c in columns:
        if "record" in c.lower() or "count" in c.lower() or "total" in c.lower():
            total = _num(row.get(c))
            if total is not None:
                break
    if total is None:
        return None
    earliest = next(
        (row.get(c) for c in columns if "earliest" in c.lower() or "min" in c.lower()), None
    )
    latest = next(
        (row.get(c) for c in columns if "latest" in c.lower() or "max" in c.lower()), None
    )
    headline = f"{_fmt(total)} records in scope."
    support = f"Spanning {earliest} to {latest}." if earliest and latest else ""
    return Finding(headline=headline, support=support)
