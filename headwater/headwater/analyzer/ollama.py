"""Ollama LLM provider -- local model inference via the Ollama API."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import httpx

from headwater.analyzer.llm import LLMProvider, make_cache_key_from_text
from headwater.core.config import HeadwaterSettings

if TYPE_CHECKING:
    from headwater.core.metadata import MetadataStore

logger = logging.getLogger(__name__)

_OLLAMA_DEFAULT_MODEL = "llama3.1:8b"


class OllamaProvider(LLMProvider):
    """Local Ollama LLM provider using the /api/chat endpoint."""

    def __init__(
        self,
        settings: HeadwaterSettings,
        store: MetadataStore | None = None,
    ) -> None:
        self._base_url = settings.ollama_base_url.rstrip("/")
        # Use the configured model, but fall back to a sensible ollama default
        # if the user hasn't overridden it from the anthropic default.
        self._model = (
            settings.llm_model
            if not settings.llm_model.startswith("claude")
            else _OLLAMA_DEFAULT_MODEL
        )
        self._store = store
        self._timeout = settings.ollama_timeout

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        """Send prompt to Ollama and return parsed JSON response.

        Uses format: "json" to ensure valid JSON output from the model.
        Retries once on timeout. Writes to llm_audit_log if a MetadataStore
        was provided at init.
        """
        _system = system or (
            "You are a data analysis assistant. "
            "Respond with valid JSON only. "
            "Return a single JSON object matching the schema described in the user prompt."
        )

        messages = [
            {"role": "system", "content": _system},
            {"role": "user", "content": prompt},
        ]

        payload = {
            "model": self._model,
            "messages": messages,
            "stream": False,
            "format": "json",
        }

        response_text = ""
        tokens_in = 0
        tokens_out = 0
        result: dict[str, Any] = {}

        try:
            result = await self._call_ollama(payload)
            response_text = json.dumps(result) if result else ""
        except httpx.TimeoutException:
            logger.warning("Ollama timeout, retrying once...")
            try:
                result = await self._call_ollama(payload)
                response_text = json.dumps(result) if result else ""
            except Exception as e:
                logger.warning("Ollama retry failed: %s", e)
                result = {}
        except Exception as e:
            logger.warning("Ollama analysis failed: %s", e)
            result = {}
        finally:
            if self._store is not None:
                try:
                    prompt_hash = make_cache_key_from_text(prompt)
                    self._store.insert_llm_audit(
                        provider="ollama",
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

    async def _call_ollama(self, payload: dict) -> dict[str, Any]:
        """Make a single HTTP call to the Ollama API."""
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                f"{self._base_url}/api/chat",
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        # Extract the assistant's message content
        content = data.get("message", {}).get("content", "")
        if not content:
            return {}

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            logger.warning("Ollama response not valid JSON: %s", content[:200])
            return {}
