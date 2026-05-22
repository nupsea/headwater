"""Tests for Ollama LLM provider."""

from __future__ import annotations

import json

import httpx
import pytest

from headwater.analyzer.llm import (
    LLM_REQUEST_TEMPLATE_VERSION,
    get_provider,
    make_llm_request_hash,
)
from headwater.analyzer.ollama import OllamaProvider
from headwater.core.config import HeadwaterSettings
from headwater.core.metadata import MetadataStore


def test_ollama_provider_init() -> None:
    """OllamaProvider should initialize with settings."""
    settings = HeadwaterSettings(llm_provider="ollama")
    provider = OllamaProvider(settings)
    assert provider._model == "llama3.1:8b"
    assert provider._base_url == "http://localhost:11434"
    assert provider._timeout == 120


def test_ollama_provider_custom_model() -> None:
    """Custom model name should be used when not a Claude model."""
    settings = HeadwaterSettings(llm_provider="ollama", llm_model="mistral:7b")
    provider = OllamaProvider(settings)
    assert provider._model == "mistral:7b"


def test_ollama_provider_claude_model_fallback() -> None:
    """Claude model names should fall back to ollama default."""
    settings = HeadwaterSettings(llm_provider="ollama", llm_model="claude-sonnet-4-20250514")
    provider = OllamaProvider(settings)
    assert provider._model == "llama3.1:8b"


def test_get_provider_ollama() -> None:
    """get_provider should return OllamaProvider for 'ollama' setting."""
    settings = HeadwaterSettings(llm_provider="ollama")
    provider = get_provider(settings)
    assert isinstance(provider, OllamaProvider)


def test_get_provider_none() -> None:
    """get_provider should return NoLLMProvider for 'none' setting."""
    from headwater.analyzer.llm import NoLLMProvider

    settings = HeadwaterSettings(llm_provider="none")
    provider = get_provider(settings)
    assert isinstance(provider, NoLLMProvider)


@pytest.mark.asyncio()
async def test_ollama_analyze_success() -> None:
    """OllamaProvider should parse a JSON response from Ollama API."""
    expected = {"description": "Test table", "domain": "Testing"}

    # Mock transport that returns a valid Ollama response
    class MockTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            body = {
                "message": {
                    "role": "assistant",
                    "content": json.dumps(expected),
                },
            }
            return httpx.Response(200, json=body)

    settings = HeadwaterSettings(llm_provider="ollama")
    provider = OllamaProvider(settings)

    # Patch the _call_ollama method to use our mock
    async def mock_call(payload: dict) -> dict:
        async with httpx.AsyncClient(transport=MockTransport()) as client:
            resp = await client.post(
                f"{provider._base_url}/api/chat",
                json=payload,
            )
            data = resp.json()
            content = data["message"]["content"]
            return json.loads(content)

    provider._call_ollama = mock_call  # type: ignore[method-assign]

    result = await provider.analyze("Test prompt")
    assert result == expected


@pytest.mark.asyncio()
async def test_ollama_analyze_failure_returns_empty() -> None:
    """OllamaProvider should return empty dict on failure."""
    settings = HeadwaterSettings(llm_provider="ollama")
    provider = OllamaProvider(settings)

    # Patch to raise an error
    async def mock_call(payload: dict) -> dict:
        msg = "Connection refused"
        raise httpx.ConnectError(msg)

    provider._call_ollama = mock_call  # type: ignore[method-assign]

    result = await provider.analyze("Test prompt")
    assert result == {}


@pytest.mark.asyncio()
async def test_ollama_audit_log() -> None:
    """OllamaProvider should write to LLM audit log."""
    store = MetadataStore(":memory:")
    store.init()

    settings = HeadwaterSettings(llm_provider="ollama")
    provider = OllamaProvider(settings, store=store)

    expected = {"description": "Audited"}

    async def mock_call(payload: dict) -> dict:
        return expected

    provider._call_ollama = mock_call  # type: ignore[method-assign]

    await provider.analyze("Audit test prompt")

    logs = store.get_llm_audit_log()
    assert len(logs) == 1
    assert logs[0]["provider"] == "ollama"
    assert logs[0]["model"] == "llama3.1:8b"
    assert "Audit test prompt" in logs[0]["prompt_text"]
    assert logs[0]["prompt_hash"] == make_llm_request_hash(
        prompt_template_version=LLM_REQUEST_TEMPLATE_VERSION,
        input_payload={
            "system": (
                "You are a data analysis assistant. "
                "Respond with valid JSON only. "
                "Return a single JSON object matching the schema described in the user prompt."
            ),
            "prompt": "Audit test prompt",
        },
        provider="ollama",
        model="llama3.1:8b",
        configuration={"format": "json", "stream": False, "timeout": 120},
    )

    store.close()


@pytest.mark.asyncio()
async def test_ollama_normalizes_audit_response_when_source_policy_blocks_raw() -> None:
    store = MetadataStore(":memory:")
    store.init()
    store.upsert_source("orders", "json", "/data/orders", None)
    store.upsert_source_meta("orders", config={"classification": "internal"})

    settings = HeadwaterSettings(llm_provider="ollama")
    provider = OllamaProvider(settings, store=store, source_name="orders")

    expected = {"description": "Sensitive generated summary"}

    async def mock_call(payload: dict) -> dict:
        return expected

    provider._call_ollama = mock_call  # type: ignore[method-assign]

    result = await provider.analyze("Audit test prompt")
    logs = store.get_llm_audit_log()

    assert result == expected
    assert logs[0]["response_storage_policy"] == "normalized"
    assert len(logs[0]["response_hash"]) == 64
    assert "Sensitive generated summary" not in logs[0]["response_text"]
    assert '"top_level_keys":["description"]' in logs[0]["response_text"]
    store.close()


@pytest.mark.asyncio()
async def test_ollama_uses_cached_audit_response_without_live_call() -> None:
    """OllamaProvider should replay cached responses for matching request hashes."""
    store = MetadataStore(":memory:")
    store.init()
    settings = HeadwaterSettings(llm_provider="ollama")
    provider = OllamaProvider(settings, store=store)
    prompt = "Cached prompt"
    prompt_hash = make_llm_request_hash(
        prompt_template_version=LLM_REQUEST_TEMPLATE_VERSION,
        input_payload={
            "system": (
                "You are a data analysis assistant. "
                "Respond with valid JSON only. "
                "Return a single JSON object matching the schema described in the user prompt."
            ),
            "prompt": prompt,
        },
        provider="ollama",
        model="llama3.1:8b",
        configuration={"format": "json", "stream": False, "timeout": 120},
    )
    store.insert_llm_audit(
        "ollama",
        "llama3.1:8b",
        prompt_text=prompt,
        response_text='{"description": "Cached"}',
        prompt_hash=prompt_hash,
        tokens_in=11,
        tokens_out=4,
    )

    async def fail_call(payload: dict) -> dict:
        raise AssertionError("live call should not run")

    provider._call_ollama = fail_call  # type: ignore[method-assign]

    result = await provider.analyze(prompt)
    logs = store.get_llm_audit_log()

    assert result == {"description": "Cached"}
    assert logs[0]["cached"] == 1
    assert logs[0]["tokens_in"] == 11
    store.close()


@pytest.mark.asyncio()
async def test_ollama_offline_mode_returns_empty_on_cache_miss() -> None:
    """Offline mode should not attempt live provider calls when cache is missing."""
    store = MetadataStore(":memory:")
    store.init()
    settings = HeadwaterSettings(llm_provider="ollama", llm_offline_mode=True)
    provider = OllamaProvider(settings, store=store)

    async def fail_call(payload: dict) -> dict:
        raise AssertionError("live call should not run")

    provider._call_ollama = fail_call  # type: ignore[method-assign]

    result = await provider.analyze("Uncached prompt")
    logs = store.get_llm_audit_log()

    assert result == {}
    assert len(logs) == 1
    assert logs[0]["cached"] == 1
    assert logs[0]["response_text"] == ""
    store.close()


@pytest.mark.asyncio()
async def test_ollama_token_budget_exhaustion_skips_live_call() -> None:
    """Budget exhaustion should produce an auditable partial-result state."""
    store = MetadataStore(":memory:")
    store.init()
    settings = HeadwaterSettings(llm_provider="ollama", llm_max_tokens_per_run=1)
    provider = OllamaProvider(settings, store=store)

    async def fail_call(payload: dict) -> dict:
        raise AssertionError("live call should not run")

    provider._call_ollama = fail_call  # type: ignore[method-assign]

    result = await provider.analyze("Prompt that is too large for the budget")
    logs = store.get_llm_audit_log()

    assert result == {}
    assert len(logs) == 1
    assert logs[0]["cached"] == 0
    assert "llm_token_budget_exhausted" in logs[0]["response_text"]
    store.close()


@pytest.mark.asyncio()
async def test_ollama_source_token_budget_exhaustion_skips_live_call() -> None:
    """Source-scoped budget exhaustion should be auditable."""
    store = MetadataStore(":memory:")
    store.init()
    store.insert_llm_audit(
        "ollama",
        "llama3.1:8b",
        prompt_text="prior",
        response_text="{}",
        source_name="orders",
        tokens_in=10,
        tokens_out=10,
    )
    settings = HeadwaterSettings(llm_provider="ollama", llm_max_tokens_per_source=21)
    provider = OllamaProvider(settings, store=store, source_name="orders")

    async def fail_call(payload: dict) -> dict:
        raise AssertionError("live call should not run")

    provider._call_ollama = fail_call  # type: ignore[method-assign]

    result = await provider.analyze("Prompt that exceeds remaining source budget")
    logs = store.get_llm_audit_log()

    assert result == {}
    assert logs[0]["source_name"] == "orders"
    assert "llm_source_token_budget_exhausted" in logs[0]["response_text"]
    store.close()
