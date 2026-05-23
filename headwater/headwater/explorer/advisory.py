"""Metadata-driven advisory helpers shared by Explore and Insights."""

from __future__ import annotations

import re

from headwater.analyzer.metadata_retrieval import RetrievedMetadata

LEGACY_PRIORITY_INSIGHTS_FIELD = "business_insights"
LEGACY_PRIORITY_SOURCE = "business"
DEFAULT_PRIORITY_CATEGORY = "Decision Signals"
DEFAULT_DECISION_LENS = "Decision Signals"
QUALITY_DECISION_LENS = "Data Quality"


def iter_context_lenses(metadata: RetrievedMetadata | None) -> list[dict]:
    if metadata is None:
        return []
    return list(metadata.business_lenses)


def context_lens_label(
    metadata: RetrievedMetadata | None,
    decisions: str | None = None,
) -> str | None:
    decision_text = (decisions or "").lower()
    for lens in iter_context_lenses(metadata):
        decision_terms = lens_terms(lens, "decision_terms", "decision_keywords", "terms")
        if decision_terms and decision_text and not any(term in decision_text for term in decision_terms):
            continue
        label = lens.get("label") or lens.get("title") or lens.get("name") or lens.get("key")
        if isinstance(label, str) and label.strip():
            return label.strip()
    return None


def context_lens_bonus(
    question: str,
    decisions: str | None,
    metadata: RetrievedMetadata | None,
) -> int:
    if metadata is None or not decisions:
        return 0
    question_text = question.lower()
    decision_text = decisions.lower()
    best = 0
    for lens in iter_context_lenses(metadata):
        decision_terms = lens_terms(lens, "decision_terms", "decision_keywords", "terms")
        question_terms = lens_terms(lens, "question_terms", "question_keywords", "signals")
        if decision_terms and not any(term in decision_text for term in decision_terms):
            continue
        if question_terms and not any(term in question_text for term in question_terms):
            continue
        try:
            priority = int(lens.get("priority", 3))
        except (TypeError, ValueError):
            priority = 3
        best = max(best, max(1, priority))
    return best


def lens_terms(lens: dict, *keys: str) -> list[str]:
    terms: list[str] = []
    for key in keys:
        value = lens.get(key)
        if isinstance(value, str):
            terms.extend(part.strip().lower() for part in re.split(r"[,;]", value))
        elif isinstance(value, list):
            terms.extend(str(part).strip().lower() for part in value)
    return [term for term in terms if term]
