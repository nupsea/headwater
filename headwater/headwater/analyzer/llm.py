"""LLM provider protocol and implementations."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from headwater.core.config import HeadwaterSettings

logger = logging.getLogger(__name__)


class LLMProvider:
    """Base LLM provider interface."""

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        """Send a prompt to the LLM and return parsed JSON response."""
        raise NotImplementedError


class NoLLMProvider(LLMProvider):
    """No-op provider -- returns empty dict, triggers heuristic fallback."""

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        return {}


class AnthropicProvider(LLMProvider):
    """Claude API provider using the Anthropic SDK."""

    def __init__(self, settings: HeadwaterSettings) -> None:
        if not settings.llm_api_key:
            raise ValueError("HEADWATER_LLM_API_KEY is required for Anthropic provider")
        import anthropic

        self._client = anthropic.AsyncAnthropic(api_key=settings.llm_api_key)
        self._model = settings.llm_model

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        """Send prompt to Claude and return parsed JSON response."""
        import anthropic

        try:
            msg = await self._client.messages.create(
                model=self._model,
                max_tokens=4096,
                system=system or "You are a data analysis assistant. Respond with valid JSON only.",
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.content[0].text
            # Try to extract JSON from the response
            return _parse_json_response(text)
        except anthropic.APIError as e:
            logger.warning("Anthropic API error: %s", e)
            return {}
        except Exception as e:
            logger.warning("LLM analysis failed: %s", e)
            return {}


def get_provider(settings: HeadwaterSettings) -> LLMProvider:
    """Factory: return the appropriate LLM provider based on settings."""
    if settings.llm_provider == "anthropic":
        return AnthropicProvider(settings)
    return NoLLMProvider()


def make_cache_key(table_name: str, column_names: list[str]) -> str:
    """Generate a stable cache key for an LLM analysis request."""
    content = f"{table_name}:{','.join(sorted(column_names))}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def _parse_json_response(text: str) -> dict[str, Any]:
    """Extract JSON from an LLM response that may contain markdown fences."""
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [line for line in lines if not line.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}
