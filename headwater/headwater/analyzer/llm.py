"""LLM provider protocol and implementations."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import TYPE_CHECKING, Any

from headwater.core.config import HeadwaterSettings

if TYPE_CHECKING:
    from headwater.core.metadata import MetadataStore

logger = logging.getLogger(__name__)

LLM_REQUEST_TEMPLATE_VERSION = "llm-provider-analyze-v1"
_SENSITIVE_KEY_RE = re.compile(
    r"(api[_-]?key|authorization|bearer|credential|password|secret|token)",
    re.IGNORECASE,
)
_REDACTED_VALUE = "[REDACTED]"


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

    def __init__(
        self,
        settings: HeadwaterSettings,
        store: MetadataStore | None = None,
    ) -> None:
        if not settings.llm_api_key:
            raise ValueError("HEADWATER_LLM_API_KEY is required for Anthropic provider")
        import anthropic

        self._client = anthropic.AsyncAnthropic(api_key=settings.llm_api_key)
        self._model = settings.llm_model
        self._store = store

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        """Send prompt to Claude and return parsed JSON response.

        Always writes to llm_audit_log if a MetadataStore was provided at init.
        On failure, writes an empty response row with tokens=0.
        """
        import anthropic

        _system = system or (
            "You are a data analysis assistant. "
            "You MUST respond with valid JSON only — no prose, no markdown fences, no explanation. "
            "Return a single JSON object matching the schema described in the user prompt."
        )
        response_text = ""
        tokens_in = 0
        tokens_out = 0
        try:
            msg = await self._client.messages.create(
                model=self._model,
                max_tokens=4096,
                system=_system,
                messages=[{"role": "user", "content": prompt}],
            )
            response_text = msg.content[0].text
            tokens_in = msg.usage.input_tokens
            tokens_out = msg.usage.output_tokens
            result = _parse_json_response(response_text)
        except anthropic.APIError as e:
            logger.warning("Anthropic API error: %s", e)
            result = {}
        except Exception as e:
            logger.warning("LLM analysis failed: %s", e)
            result = {}
        finally:
            if self._store is not None:
                try:
                    prompt_hash = make_llm_request_hash(
                        prompt_template_version=LLM_REQUEST_TEMPLATE_VERSION,
                        input_payload={"system": _system, "prompt": prompt},
                        provider="anthropic",
                        model=self._model,
                        configuration={"max_tokens": 4096},
                    )
                    self._store.insert_llm_audit(
                        provider="anthropic",
                        model=self._model,
                        prompt_text=prompt,
                        response_text=response_text,
                        prompt_hash=prompt_hash,
                        tokens_in=tokens_in,
                        tokens_out=tokens_out,
                    )
                except Exception as audit_err:
                    logger.warning("Failed to write LLM audit log: %s", audit_err)
        return result


def get_provider(
    settings: HeadwaterSettings,
    store: MetadataStore | None = None,
) -> LLMProvider:
    """Factory: return the appropriate LLM provider based on settings."""
    if settings.llm_provider == "anthropic":
        return AnthropicProvider(settings, store=store)
    if settings.llm_provider == "ollama":
        from headwater.analyzer.ollama import OllamaProvider

        return OllamaProvider(settings, store=store)
    return NoLLMProvider()


def make_cache_key(table_name: str, column_names: list[str]) -> str:
    """Generate a stable cache key for an LLM analysis request."""
    content = f"{table_name}:{','.join(sorted(column_names))}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def make_cache_key_from_text(text: str) -> str:
    """Generate a SHA-256 hash of arbitrary text (for prompt deduplication)."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def make_llm_request_hash(
    *,
    prompt_template_version: str,
    input_payload: dict[str, Any],
    provider: str,
    model: str,
    configuration: dict[str, Any] | None = None,
) -> str:
    """Generate a content-addressed hash for an auditable LLM request."""
    envelope = {
        "prompt_template_version": prompt_template_version,
        "input_payload": redact_llm_payload(input_payload),
        "provider": provider,
        "model": model,
        "configuration": redact_llm_payload(configuration or {}),
    }
    encoded = _stable_json(envelope)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def redact_llm_payload(value: Any) -> Any:
    """Return a deterministic copy of payload data with sensitive keys redacted."""
    if isinstance(value, dict):
        redacted = {}
        for key in sorted(value):
            if _SENSITIVE_KEY_RE.search(str(key)):
                redacted[str(key)] = _REDACTED_VALUE
            else:
                redacted[str(key)] = redact_llm_payload(value[key])
        return redacted
    if isinstance(value, list | tuple):
        return [redact_llm_payload(item) for item in value]
    return value


def _stable_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )


def _parse_json_response(text: str) -> dict[str, Any]:
    """Extract JSON from an LLM response that may contain markdown fences or plain SQL.

    Recovery attempts (in order):
    1. Strip markdown fences and parse JSON.
    2. Extract content between triple backticks (any language label).
    3. If the response starts with SELECT/CREATE/WITH, wrap as {"sql": <text>}.
    4. Log a WARNING and return {} if all recovery fails.
    """
    raw = text
    text = text.strip()

    # Attempt 1: strip code fences and parse JSON
    cleaned = text
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [line for line in lines if not line.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Attempt 2: extract content between triple backticks
    fence_match = re.search(r"```(?:\w+)?\n(.*?)```", text, re.DOTALL)
    if fence_match:
        inner = fence_match.group(1).strip()
        try:
            return json.loads(inner)
        except json.JSONDecodeError:
            # Inner content might be SQL — fall through to attempt 3
            if re.match(r"^\s*(SELECT|CREATE|WITH)\b", inner, re.IGNORECASE):
                return {"sql": inner}

    # Attempt 3: plain SQL response
    if re.match(r"^\s*(SELECT|CREATE|WITH)\b", text, re.IGNORECASE):
        return {"sql": text}

    # All recovery failed
    logger.warning("LLM response could not be parsed as JSON: %s", raw[:200])
    return {}
