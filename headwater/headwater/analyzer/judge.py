"""LLM-as-judge for Headwater 2 answer certification.

The judge is the *second* factor in two-factor certification.  Statistical
contracts (see ``services/h2_readiness``) establish that the data *can* support
an answer; the judge evaluates whether the generated SQL and its aggregated
result *actually* answer the question without misinterpretation.  Both must
agree before an answer is certified — neither replaces the other.

Invariant I-3: the judge receives only the question text, the SQL text,
column/semantic metadata, and an aggregated result-statistics summary.  It never
sees raw data rows.

Graceful degradation: when no LLM is available (NoLLMProvider, Ollama down,
offline mode, or token budget exhausted), ``analyze`` yields an empty dict and
the verdict is ``"unavailable"``.  An unavailable judge does NOT certify — the
answer holds at "doubtful".  Certification always requires an explicit judge
approval, so the system never falsely certifies when it cannot verify.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Literal

from headwater.analyzer.llm import LLMProvider, NoLLMProvider

# "pending"  — the judge has not been run for this answer yet.
# "stale"    — the judge ran, but an input changed since; its verdict no longer
#              applies and certification must be re-run.
JudgeVerdict = Literal[
    "certified", "doubtful", "reject", "unavailable", "pending", "stale"
]

_SYSTEM = (
    "You are a rigorous data-analysis reviewer. You judge whether a SQL query "
    "and its aggregated result correctly and unambiguously answer a business "
    "question, given only column metadata and result statistics — you never see "
    "raw rows. Be skeptical: if the mapping from the question to the query is "
    "ambiguous, if a column's meaning is unclear, or if the result is degenerate "
    "(empty, a single group, or an all-null measure), do NOT certify. "
    "Respond with a single JSON object and nothing else."
)

_SCHEMA_HINT = (
    '{"verdict": "certified" | "doubtful" | "reject", '
    '"confidence": <number 0..1>, '
    '"reasons": ["<short reason>", ...]}'
)

_VALID_VERDICTS = {"certified", "doubtful", "reject"}


@dataclass(slots=True)
class JudgeResult:
    """A judge verdict over one answer."""

    verdict: JudgeVerdict
    confidence: float
    reasons: list[str] = field(default_factory=list)
    available: bool = True

    @property
    def approves(self) -> bool:
        """True only when the judge explicitly certified the answer."""
        return self.verdict == "certified"


def build_judge_prompt(
    *,
    question_title: str,
    question_reason: str,
    sql_text: str | None,
    columns: list[dict[str, Any]],
    result_stats: dict[str, Any],
) -> str:
    """Assemble the I-3-safe judge prompt (metadata + stats only)."""
    col_lines = [
        f"- {c.get('ref', c.get('column', '?'))}: "
        f"dtype={c.get('dtype', '?')}, role={c.get('role') or c.get('semantic_role') or '?'}"
        + (f", desc={c['description']}" if c.get("description") else "")
        for c in columns
    ]
    columns_block = "\n".join(col_lines) if col_lines else "(none provided)"

    return (
        f"QUESTION:\n{question_title}\n"
        f"WHY THIS QUESTION:\n{question_reason or '(not provided)'}\n\n"
        f"COLUMNS AVAILABLE (metadata only):\n{columns_block}\n\n"
        f"GENERATED SQL:\n{sql_text or '(no SQL generated)'}\n\n"
        f"RESULT STATISTICS (aggregates only, no raw rows):\n"
        f"{json.dumps(result_stats, indent=2, default=str)}\n\n"
        "Judge whether the SQL and its result correctly and unambiguously answer "
        "the question. Certify only if you are confident there is no "
        "misinterpretation. Respond as JSON matching this schema:\n"
        f"{_SCHEMA_HINT}"
    )


def judge_answer(
    provider: LLMProvider,
    *,
    question_title: str,
    question_reason: str = "",
    sql_text: str | None,
    columns: list[dict[str, Any]] | None = None,
    result_stats: dict[str, Any] | None = None,
) -> JudgeResult:
    """Run the LLM judge over one answer. Never raises; degrades to unavailable."""
    if isinstance(provider, NoLLMProvider):
        return JudgeResult(
            verdict="unavailable",
            confidence=0.0,
            reasons=["No LLM provider configured for judging."],
            available=False,
        )

    # A result that never executed cannot be judged as correct.
    if not sql_text or result_stats is None:
        return JudgeResult(
            verdict="reject",
            confidence=0.0,
            reasons=["No executed result to evaluate."],
        )

    prompt = build_judge_prompt(
        question_title=question_title,
        question_reason=question_reason,
        sql_text=sql_text,
        columns=columns or [],
        result_stats=result_stats,
    )

    raw = _invoke(provider, prompt, _SYSTEM)
    if not raw:
        return JudgeResult(
            verdict="unavailable",
            confidence=0.0,
            reasons=["LLM judge did not return a verdict (provider unavailable)."],
            available=False,
        )
    return _parse_verdict(raw)


def _parse_verdict(raw: dict[str, Any]) -> JudgeResult:
    verdict = str(raw.get("verdict", "")).strip().lower()
    if verdict not in _VALID_VERDICTS:
        # Unparseable verdict is not an approval — hold at doubtful.
        return JudgeResult(
            verdict="doubtful",
            confidence=float(_coerce_float(raw.get("confidence"), 0.0)),
            reasons=_coerce_reasons(raw.get("reasons"))
            or ["Judge returned an unrecognized verdict."],
        )
    return JudgeResult(
        verdict=verdict,  # type: ignore[arg-type]
        confidence=max(0.0, min(1.0, _coerce_float(raw.get("confidence"), 0.0))),
        reasons=_coerce_reasons(raw.get("reasons")),
    )


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_reasons(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value][:5]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _invoke(provider: LLMProvider, prompt: str, system: str) -> dict[str, Any]:
    """Call the async provider from sync code, tolerating a running loop."""
    try:
        return asyncio.run(provider.analyze(prompt, system))
    except RuntimeError:
        # Already inside an event loop — run on a fresh one.
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(provider.analyze(prompt, system))
        finally:
            loop.close()
    except Exception:
        return {}
