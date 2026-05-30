"""Tests for the LLM-as-judge certification factor."""

from __future__ import annotations

from typing import Any

from headwater.analyzer.judge import build_judge_prompt, judge_answer
from headwater.analyzer.llm import NoLLMProvider


class _FakeProvider:
    """Minimal async LLM provider stub returning a canned verdict dict."""

    def __init__(self, response: dict[str, Any]):
        self._response = response

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        self.last_prompt = prompt
        return self._response


_STATS = {
    "row_count": 12,
    "column_count": 2,
    "columns": {
        "period": {"dtype": "Date", "null_count": 0, "distinct_count": 12},
        "avg_wait": {"dtype": "Float64", "null_count": 0, "min": 3.1, "max": 9.4},
    },
}
_COLS = [
    {"ref": "events.created_at", "dtype": "timestamp", "role": "event_ts"},
    {"ref": "events.wait_minutes", "dtype": "double", "role": "measure"},
]


def test_no_llm_provider_is_unavailable_not_certified():
    result = judge_answer(
        NoLLMProvider(),
        question_title="When are waits worst?",
        sql_text="SELECT 1",
        columns=_COLS,
        result_stats=_STATS,
    )
    assert result.verdict == "unavailable"
    assert result.available is False
    assert result.approves is False  # never certifies without a judge


def test_certified_verdict_approves():
    provider = _FakeProvider(
        {"verdict": "certified", "confidence": 0.91, "reasons": ["clear temporal mapping"]}
    )
    result = judge_answer(
        provider,
        question_title="When are waits worst?",
        question_reason="temporal trend of wait time",
        sql_text="SELECT date_trunc('day', created_at) AS period, avg(wait_minutes) ...",
        columns=_COLS,
        result_stats=_STATS,
    )
    assert result.verdict == "certified"
    assert result.approves is True
    assert 0.9 <= result.confidence <= 1.0
    # I-3: the prompt must not be able to carry raw rows — only stats/metadata.
    assert "RESULT STATISTICS" in provider.last_prompt
    assert "row_count" in provider.last_prompt


def test_reject_and_doubtful_do_not_approve():
    for verdict in ("reject", "doubtful"):
        provider = _FakeProvider({"verdict": verdict, "confidence": 0.4, "reasons": ["x"]})
        result = judge_answer(
            provider,
            question_title="q",
            sql_text="SELECT 1",
            columns=_COLS,
            result_stats=_STATS,
        )
        assert result.verdict == verdict
        assert result.approves is False


def test_unparseable_verdict_holds_at_doubtful():
    provider = _FakeProvider({"verdict": "definitely", "confidence": "high"})
    result = judge_answer(
        provider, question_title="q", sql_text="SELECT 1",
        columns=_COLS, result_stats=_STATS,
    )
    assert result.verdict == "doubtful"
    assert result.approves is False


def test_empty_provider_response_is_unavailable():
    provider = _FakeProvider({})
    result = judge_answer(
        provider, question_title="q", sql_text="SELECT 1",
        columns=_COLS, result_stats=_STATS,
    )
    assert result.verdict == "unavailable"
    assert result.available is False


def test_missing_result_is_rejected():
    provider = _FakeProvider({"verdict": "certified", "confidence": 1.0})
    result = judge_answer(
        provider, question_title="q", sql_text=None, columns=_COLS, result_stats=None,
    )
    assert result.verdict == "reject"
    assert result.approves is False


def test_prompt_contains_no_row_payload():
    prompt = build_judge_prompt(
        question_title="q",
        question_reason="r",
        sql_text="SELECT 1",
        columns=_COLS,
        result_stats=_STATS,
    )
    assert "GENERATED SQL" in prompt
    assert "RESULT STATISTICS" in prompt
    # Only aggregate keys appear; no per-row data payload (no "rows" JSON key).
    assert '"rows"' not in prompt
