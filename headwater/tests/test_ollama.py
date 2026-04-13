"""Tests for Ollama LLM provider."""

from __future__ import annotations

import json

import httpx
import pytest

from headwater.analyzer.llm import get_provider
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

    store.close()
