"""Headwater 2 S9 — Generic EDA and Insight Families.

Runs role-driven insight families against stored profile data and produces
ranked findings.  Families are generic: they trigger on semantic role classes
(timestamp, measure, categorical, identity) not on domain-specific column names.

Families:
  coverage      → null rates and row-count sufficiency
  distribution  → quantile spread, coefficient of variation, tail behaviour
  concentration → dominant-value share for categorical columns
  temporal      → coverage span, recency, date range
  uniqueness    → uniqueness ratio for identity/measure columns
  relationship  → FK coverage and referential integrity from stored relationships

Findings are persisted as claim_type='eda_finding' semantic claims and used by
the insight_confidence readiness contract.  Each finding has an effect_size and
confidence so callers can rank by impact without domain knowledge.

Raw data rows are never loaded — all computation runs on stored profile
statistics (null_rate, top_values, mean, stddev, p25/p75/p95, min/max dates).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from headwater.core.store import HeadwaterStore

_COVERAGE_POOR_THRESHOLD = 0.20   # null_rate ≥ 20 % is notable
_COVERAGE_CRITICAL_THRESHOLD = 0.50  # ≥ 50 % is a blocker
_CV_HIGH_THRESHOLD = 1.0           # coefficient of variation > 1 = highly variable
_CONCENTRATION_THRESHOLD = 0.60    # dominant value > 60 % = unbalanced
_MIN_COVERAGE_DAYS = 7             # fewer than 7 days of temporal data is thin


@dataclass(slots=True)
class EdaFinding:
    col_ref: str          # "table.column" or "table1→table2"
    family: str           # coverage|distribution|concentration|temporal|uniqueness|relationship
    title: str
    detail: str
    confidence: float     # 0.0–1.0 — how reliable is this finding
    effect_size: float    # 0.0–1.0 — how impactful / actionable
    flags: list[str] = field(default_factory=list)   # e.g. "high_null", "unbalanced"


@dataclass(slots=True)
class EDAReport:
    project_id: str
    source_name: str
    findings: list[EdaFinding] = field(default_factory=list)
    claims_created: int = 0

    @property
    def critical_findings(self) -> list[EdaFinding]:
        return [f for f in self.findings if "critical" in f.flags]

    @property
    def insight_confidence_score(self) -> float:
        """Aggregate confidence score used by the insight_confidence contract."""
        if not self.findings:
            return 0.0
        quality_penalties = sum(
            0.3 for f in self.findings if "high_null_critical" in f.flags
        )
        base = sum(f.confidence * f.effect_size for f in self.findings) / len(self.findings)
        return max(0.0, min(1.0, base - quality_penalties))


def run_eda(
    store: HeadwaterStore,
    project_id: str,
) -> EDAReport:
    """Run all EDA families against the project's source profiles and store findings."""
    project = store.get_project(project_id)
    if project is None:
        raise ValueError(f"Project '{project_id}' is not registered.")

    project_sources = store.get_project_sources(project_id)
    if not project_sources:
        raise ValueError(f"Project '{project_id}' has no linked source.")

    source_name = project_sources[0]["source_name"]
    profiles = store.get_profiles(source_name)
    relationships = store.get_relationships(source_name)
    claims = store.list_semantic_claims(project_id)
    questions = store.list_questions(project_id)

    # Build col_ref → semantic_role mapping from relevance claims
    role_map: dict[str, str] = {}
    for claim in claims:
        if claim.get("claim_type") == "relevance":
            table = claim.get("table_name") or ""
            col = claim.get("column_name") or ""
            if table and col:
                role = claim.get("claim", {}).get("semantic_role") or ""
                role_map[f"{table}.{col}".lower()] = role

    # Collect needed columns across all project questions
    needed_cols: set[str] = set()
    for q in questions:
        for c in q.get("question", {}).get("needed_columns") or []:
            needed_cols.add(c.lower())

    all_findings: list[EdaFinding] = []

    for p in profiles:
        col_ref = f"{p['table_name']}.{p['column_name']}"
        role = role_map.get(col_ref.lower(), "")
        prof = p.get("profile") or {}
        dtype = (p.get("dtype") or "varchar").lower()
        is_needed = col_ref.lower() in needed_cols

        all_findings.extend(_coverage_family(col_ref, prof, is_needed))
        all_findings.extend(_distribution_family(col_ref, prof, dtype, role))
        all_findings.extend(_concentration_family(col_ref, prof, dtype, role))
        all_findings.extend(_temporal_family(col_ref, prof, dtype, role))
        all_findings.extend(_uniqueness_family(col_ref, prof, role))

    all_findings.extend(_relationship_family(relationships))

    # Rank by effect_size × confidence DESC
    all_findings.sort(key=lambda f: -(f.effect_size * f.confidence))

    report = EDAReport(
        project_id=project_id,
        source_name=source_name,
        findings=all_findings,
    )

    report.claims_created = _persist_findings(store, project_id, source_name, all_findings)
    _update_insight_confidence(store, project_id, source_name, report, questions)

    return report


# ── Families ──────────────────────────────────────────────────────────────────

def _coverage_family(
    col_ref: str,
    profile: dict[str, Any],
    is_needed: bool,
) -> list[EdaFinding]:
    null_rate = float(profile.get("null_rate") or 0.0)
    if null_rate < _COVERAGE_POOR_THRESHOLD:
        return []

    pct = int(null_rate * 100)
    flags: list[str] = []
    if null_rate >= _COVERAGE_CRITICAL_THRESHOLD:
        flags = ["high_null_critical", "critical"]
        effect = 0.9
        confidence = 0.95
        detail = (
            f"{pct}% of values are null — this exceeds the critical threshold "
            f"and will block structural_integrity contracts for questions that need this column."
        )
    else:
        flags = ["high_null"]
        effect = 0.5 + (null_rate - _COVERAGE_POOR_THRESHOLD) * 0.5
        confidence = 0.90
        detail = (
            f"{pct}% of values are null. "
            f"Verify whether nulls are expected (not-applicable semantics) "
            f"or represent data collection gaps."
        )

    priority = "critical" if null_rate >= _COVERAGE_CRITICAL_THRESHOLD else "notable"
    return [EdaFinding(
        col_ref=col_ref,
        family="coverage",
        title=f"{priority.title()} null rate in {col_ref.split('.')[-1]} ({pct}%)",
        detail=detail,
        confidence=confidence,
        effect_size=min(1.0, effect),
        flags=flags,
    )]


def _distribution_family(
    col_ref: str,
    profile: dict[str, Any],
    dtype: str,
    role: str,
) -> list[EdaFinding]:
    _numeric = {
        "int", "int8", "int16", "int32", "int64", "integer", "bigint",
        "float", "float32", "float64", "double", "decimal", "numeric", "real",
    }
    if dtype not in _numeric and role not in {"measure", "duration", "quantity", "metric"}:
        return []

    mean = profile.get("mean")
    stddev = profile.get("stddev")
    p25 = profile.get("p25")
    p75 = profile.get("p75")
    p95 = profile.get("p95")
    median = profile.get("median")

    findings: list[EdaFinding] = []

    if mean and stddev and abs(float(mean)) > 1e-9:
        cv = abs(float(stddev)) / abs(float(mean))
        if cv > _CV_HIGH_THRESHOLD:
            col_name = col_ref.split(".")[-1]
            findings.append(EdaFinding(
                col_ref=col_ref,
                family="distribution",
                title=f"High variability in {col_name} (CV={cv:.1f})",
                detail=(
                    f"Mean={_fmt(mean)}, stddev={_fmt(stddev)}, "
                    f"coefficient of variation={cv:.2f}. "
                    "High variability suggests outliers or multiple sub-populations — "
                    "segment before averaging."
                ),
                confidence=0.80,
                effect_size=min(1.0, cv / 3.0),
                flags=["high_variability"],
            ))

    if p25 and p75 and p95 and median:
        iqr = float(p75) - float(p25)
        tail_ratio = (float(p95) - float(median)) / (iqr + 1e-9)
        if tail_ratio > 3.0:
            col_name = col_ref.split(".")[-1]
            findings.append(EdaFinding(
                col_ref=col_ref,
                family="distribution",
                title=f"Right-skewed distribution in {col_name}",
                detail=(
                    f"Median={_fmt(median)}, p75={_fmt(p75)}, p95={_fmt(p95)}. "
                    f"The p95 is {tail_ratio:.1f}× the IQR above the median — "
                    "a heavy right tail suggests extreme values dominate averages."
                ),
                confidence=0.75,
                effect_size=min(1.0, tail_ratio / 6.0),
                flags=["right_skewed"],
            ))

    return findings


def _concentration_family(
    col_ref: str,
    profile: dict[str, Any],
    dtype: str,
    role: str,
) -> list[EdaFinding]:
    _cat_roles = {"categorical", "code", "flag", "category"}
    if dtype not in ("varchar", "text", "string", "category") and role not in _cat_roles:
        return []

    top_values = profile.get("top_values") or []
    if not top_values:
        return []

    total = sum(int(v[1]) for v in top_values if len(v) >= 2)
    if total == 0:
        return []

    top_count = int(top_values[0][1]) if top_values else 0
    top_value = str(top_values[0][0]) if top_values else ""
    top_pct = top_count / total

    if top_pct < _CONCENTRATION_THRESHOLD:
        return []

    col_name = col_ref.split(".")[-1]
    return [EdaFinding(
        col_ref=col_ref,
        family="concentration",
        title=f"Dominant value in {col_name} ({int(top_pct * 100)}%)",
        detail=(
            f"The most frequent value '{top_value}' appears in "
            f"{int(top_pct * 100)}% of records ({top_count:,} of {total:,}). "
            "Segmentation by this column will be heavily skewed — "
            "consider whether under-represented values still merit analysis."
        ),
        confidence=0.88,
        effect_size=top_pct,
        flags=["unbalanced"],
    )]


def _temporal_family(
    col_ref: str,
    profile: dict[str, Any],
    dtype: str,
    role: str,
) -> list[EdaFinding]:
    _ts_roles = {"event_ts", "start_ts", "end_ts", "time_anchor", "temporal"}
    _ts_dtypes = {"timestamp", "date", "datetime", "timestamptz"}
    if dtype not in _ts_dtypes and role not in _ts_roles:
        return []

    min_date = profile.get("min_date") or profile.get("min_value")
    max_date = profile.get("max_date") or profile.get("max_value")
    if not (min_date and max_date):
        return []

    findings: list[EdaFinding] = []
    col_name = col_ref.split(".")[-1]

    # Temporal coverage note (always useful, even without issues)
    findings.append(EdaFinding(
        col_ref=col_ref,
        family="temporal",
        title=f"Temporal coverage in {col_name}",
        detail=f"Range: {str(min_date)[:10]} → {str(max_date)[:10]}.",
        confidence=0.95,
        effect_size=0.3,
        flags=["temporal_coverage"],
    ))

    # Try to compute days coverage
    try:
        from datetime import datetime

        def _parse(d: Any) -> datetime:
            s = str(d).replace("T", " ").replace("Z", "").strip()[:19]
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Cannot parse date: {d}")

        days = (_parse(max_date) - _parse(min_date)).days
        if days < _MIN_COVERAGE_DAYS:
            findings.append(EdaFinding(
                col_ref=col_ref,
                family="temporal",
                title=f"Thin temporal coverage ({days} day{'s' if days != 1 else ''})",
                detail=(
                    f"Only {days} day(s) of data in {col_name}. "
                    "Time-series and trend questions may not be answerable — "
                    "week-over-week or month-over-month comparisons require more coverage."
                ),
                confidence=0.90,
                effect_size=max(0.3, 1.0 - days / _MIN_COVERAGE_DAYS),
                flags=["thin_coverage"],
            ))
    except (ValueError, TypeError, AttributeError):
        pass

    return findings


def _uniqueness_family(
    col_ref: str,
    profile: dict[str, Any],
    role: str,
) -> list[EdaFinding]:
    uniqueness = profile.get("uniqueness_ratio")
    if uniqueness is None:
        return []

    u = float(uniqueness)
    col_name = col_ref.split(".")[-1]
    findings: list[EdaFinding] = []

    if u >= 0.99 and role not in {"identifier", "primary_key", "foreign_key", "entity"}:
        findings.append(EdaFinding(
            col_ref=col_ref,
            family="uniqueness",
            title=f"{col_name} appears to be a unique identifier (ratio={u:.2f})",
            detail=(
                f"Uniqueness ratio {u:.3f} — nearly every value is distinct. "
                "This column is likely a primary key or surrogate ID and should not "
                "be used as a segmentation or grouping axis."
            ),
            confidence=0.85,
            effect_size=0.4,
            flags=["likely_identifier"],
        ))
    elif u < 0.01 and role not in {"categorical", "code", "flag"}:
        findings.append(EdaFinding(
            col_ref=col_ref,
            family="uniqueness",
            title=f"{col_name} has very low uniqueness (ratio={u:.4f})",
            detail=(
                f"Uniqueness ratio {u:.4f} — most values repeat. "
                "This looks like a categorical or code column. "
                "Consider mapping its values before using in analysis."
            ),
            confidence=0.82,
            effect_size=0.35,
            flags=["low_uniqueness"],
        ))

    return findings


def _relationship_family(
    relationships: list[dict[str, Any]],
) -> list[EdaFinding]:
    findings: list[EdaFinding] = []
    for rel in relationships:
        conf = float(rel.get("confidence") or 0.0)
        ri = float(rel.get("referential_integrity") or 0.0)
        from_t = rel.get("from_table", "")
        to_t = rel.get("to_table", "")
        col_ref = f"{from_t}→{to_t}"

        if conf >= 0.80 and ri < 0.95:
            findings.append(EdaFinding(
                col_ref=col_ref,
                family="relationship",
                title=f"Referential integrity gap: {from_t}→{to_t} ({int(ri * 100)}%)",
                detail=(
                    f"Relationship {from_t}→{to_t} has {int(ri * 100)}% referential integrity. "
                    f"{int((1 - ri) * 100)}% of records in {from_t} have no match in {to_t}. "
                    "Joins on this path may drop records silently."
                ),
                confidence=min(0.90, conf),
                effect_size=1.0 - ri,
                flags=["low_ri"],
            ))
        elif conf >= 0.80 and ri >= 0.99:
            findings.append(EdaFinding(
                col_ref=col_ref,
                family="relationship",
                title=f"Strong relationship: {from_t}→{to_t} ({int(ri * 100)}% RI)",
                detail=(
                    f"{int(ri * 100)}% referential integrity confirmed. "
                    "This join is safe for analytical queries."
                ),
                confidence=min(0.90, conf),
                effect_size=0.2,
                flags=["strong_relationship"],
            ))

    return findings


# ── Persistence ───────────────────────────────────────────────────────────────

def _persist_findings(
    store: HeadwaterStore,
    project_id: str,
    source_name: str,
    findings: list[EdaFinding],
) -> int:
    created = 0
    for i, f in enumerate(findings):
        # Column is the last component; the table may be schema-qualified.
        # Relationship findings ("t1→t2") have no dot-split semantics.
        parts = f.col_ref.rsplit(".", 1)
        table_name = parts[0] if len(parts) == 2 else None
        col_name = parts[1] if len(parts) == 2 else None
        claim_id = f"{project_id}:eda:{f.family}:{f.col_ref}:{i}"
        store.upsert_semantic_claim(
            claim_id,
            project_id=project_id,
            source_name=source_name,
            scope_type="column" if col_name else "source",
            table_name=table_name,
            column_name=col_name,
            claim_type="eda_finding",
            claim={
                "family": f.family,
                "title": f.title,
                "detail": f.detail,
                "effect_size": f.effect_size,
                "flags": f.flags,
            },
            status="proposed",
            confidence=f.confidence,
            source="eda",
            locked=False,
        )
        created += 1
    return created


def _update_insight_confidence(
    store: HeadwaterStore,
    project_id: str,
    source_name: str,
    report: EDAReport,
    questions: list[dict[str, Any]],
) -> None:
    """Update the insight_confidence contract for each question from EDA findings.

    Per-question and evidence-scoped (plan §4.2/4.3): the contract passes when
    the battery has run AND none of the question's OWN needed columns carries a
    critical quality finding. A critical issue elsewhere in the source must not
    fail an unrelated question — and a global score blend must not certify past
    a critical issue. Calibrated confidence over the full evidence subgraph is
    the P3 follow-on; this contract is the fail-closed gate.
    """
    critical_col_refs = {f.col_ref.lower() for f in report.critical_findings}

    for question in questions:
        needed = [c.lower() for c in (question.get("question", {}).get("needed_columns") or [])]
        critical_hits = [c for c in needed if c in critical_col_refs]
        relevant = [f for f in report.findings if f.col_ref.lower() in needed]
        passed = not critical_hits

        store.upsert_readiness_contract(
            f"{question['id']}:contract:insight_confidence",
            question_id=question["id"],
            contract_type="insight_confidence",
            passed=passed,
            note=(
                f"EDA computed {len(report.findings)} findings "
                f"({len(relevant)} on this question's columns); no critical "
                "quality issues in needed columns."
                if passed
                else (
                    "EDA detected critical quality issues in needed columns: "
                    + ", ".join(critical_hits[:3])
                )
            ),
            evidence={
                "critical_cols": critical_hits,
                "relevant_findings": len(relevant),
                "total_findings": len(report.findings),
                "eda_score": report.insight_confidence_score,
            },
            snapshot_id=None,
        )


# ── Formatting ────────────────────────────────────────────────────────────────

def _fmt(value: Any) -> str:
    try:
        v = float(value)
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        if abs(v) >= 1:
            return f"{v:.2f}"
        return f"{v:.4f}"
    except (TypeError, ValueError):
        return str(value)
