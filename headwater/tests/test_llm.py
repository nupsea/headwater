"""Tests for deterministic LLM request controls."""

from __future__ import annotations

from headwater.analyzer.llm import (
    LLMTokenBudget,
    estimate_llm_tokens,
    make_llm_request_hash,
    redact_llm_payload,
)


def test_llm_request_hash_is_stable_for_sorted_payloads() -> None:
    first = make_llm_request_hash(
        prompt_template_version="semantic-v1",
        input_payload={"table": "orders", "columns": ["id", "amount"]},
        provider="anthropic",
        model="claude-sonnet",
        configuration={"temperature": 0, "max_tokens": 1000},
    )
    second = make_llm_request_hash(
        prompt_template_version="semantic-v1",
        input_payload={"columns": ["id", "amount"], "table": "orders"},
        provider="anthropic",
        model="claude-sonnet",
        configuration={"max_tokens": 1000, "temperature": 0},
    )

    assert first == second
    assert len(first) == 64


def test_llm_request_hash_changes_with_template_provider_model_or_config() -> None:
    base = {
        "prompt_template_version": "semantic-v1",
        "input_payload": {"table": "orders"},
        "provider": "anthropic",
        "model": "claude-sonnet",
        "configuration": {"temperature": 0},
    }

    baseline = make_llm_request_hash(**base)

    assert (
        make_llm_request_hash(**{**base, "prompt_template_version": "semantic-v2"})
        != baseline
    )
    assert make_llm_request_hash(**{**base, "provider": "ollama"}) != baseline
    assert make_llm_request_hash(**{**base, "model": "llama3.1:8b"}) != baseline
    assert (
        make_llm_request_hash(**{**base, "configuration": {"temperature": 0.2}})
        != baseline
    )


def test_llm_payload_redaction_is_recursive_and_deterministic() -> None:
    redacted = redact_llm_payload(
        {
            "nested": {
                "api_key": "secret",
                "authorization": "Bearer token",
                "safe": "value",
            },
            "items": [{"password": "secret"}, {"name": "ok"}],
        }
    )

    assert redacted == {
        "items": [{"password": "[REDACTED]"}, {"name": "ok"}],
        "nested": {
            "api_key": "[REDACTED]",
            "authorization": "[REDACTED]",
            "safe": "value",
        },
    }


def test_llm_token_budget_tracks_remaining_capacity() -> None:
    budget = LLMTokenBudget(max_tokens=10)

    assert budget.can_spend(6) is True
    budget.record(6)
    assert budget.remaining_tokens == 4
    assert budget.can_spend(5) is False
    assert budget.can_spend(4) is True


def test_unlimited_llm_token_budget_always_allows_spend() -> None:
    budget = LLMTokenBudget(max_tokens=0)

    assert budget.remaining_tokens is None
    assert budget.can_spend(1_000_000) is True


def test_llm_token_estimate_is_deterministic_and_nonzero() -> None:
    assert estimate_llm_tokens("abcd") == 1
    assert estimate_llm_tokens("abcde") == 2
    assert estimate_llm_tokens("") == 0
