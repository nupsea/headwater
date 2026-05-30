"""Ollama LLM provider -- local model inference via the Ollama API."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import httpx

from headwater.analyzer.llm import (
    _BUDGET_EXHAUSTED_RESPONSE,
    _SOURCE_BUDGET_EXHAUSTED_RESPONSE,
    LLM_REQUEST_TEMPLATE_VERSION,
    LLMProvider,
    LLMTokenBudget,
    _cached_response,
    _parse_json_response,
    _raw_response_allowed,
    estimate_llm_tokens,
    make_llm_request_hash,
)
from headwater.core.config import HeadwaterSettings

if TYPE_CHECKING:
    from headwater.core.metadata import MetadataStore

logger = logging.getLogger(__name__)

_OLLAMA_DEFAULT_MODEL = "qwen2.5:14b-instruct"


class OllamaProvider(LLMProvider):
    """Local Ollama LLM provider using the /api/chat endpoint."""

    def __init__(
        self,
        settings: HeadwaterSettings,
        store: MetadataStore | None = None,
        token_budget: LLMTokenBudget | None = None,
        source_name: str | None = None,
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
        self._offline_mode = settings.llm_offline_mode
        self._token_budget = token_budget or LLMTokenBudget(settings.llm_max_tokens_per_run)
        self._source_name = source_name
        self._source_token_budget = max(0, int(settings.llm_max_tokens_per_source or 0))

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
        prompt_hash = make_llm_request_hash(
            prompt_template_version=LLM_REQUEST_TEMPLATE_VERSION,
            input_payload={"system": _system, "prompt": prompt},
            provider="ollama",
            model=self._model,
            configuration={
                "format": "json",
                "stream": False,
                "timeout": self._timeout,
            },
        )
        cached = _cached_response(
            self._store,
            provider="ollama",
            model=self._model,
            prompt_hash=prompt_hash,
        )
        if cached is not None:
            self._insert_cached_audit(
                prompt,
                cached.get("response_text") or "",
                prompt_hash=prompt_hash,
                tokens_in=int(cached.get("tokens_in") or 0),
                tokens_out=int(cached.get("tokens_out") or 0),
            )
            return _parse_json_response(cached.get("response_text") or "")
        if self._offline_mode:
            self._insert_cached_audit(prompt, "", prompt_hash=prompt_hash)
            return {}
        estimated_tokens = estimate_llm_tokens(_system) + estimate_llm_tokens(prompt)
        if not self._token_budget.can_spend(estimated_tokens):
            self._insert_budget_audit(prompt, prompt_hash=prompt_hash)
            return {}
        if not self._source_budget_allows(estimated_tokens):
            self._insert_source_budget_audit(prompt, prompt_hash=prompt_hash)
            return {}

        try:
            result = await self._call_ollama(payload)
            response_text = json.dumps(result) if result else ""
            self._token_budget.record(estimated_tokens)
        except httpx.TimeoutException:
            logger.warning("Ollama timeout, retrying once...")
            try:
                result = await self._call_ollama(payload)
                response_text = json.dumps(result) if result else ""
                self._token_budget.record(estimated_tokens)
            except Exception as e:
                logger.warning("Ollama retry failed: %s", e)
                result = {}
        except Exception as e:
            logger.warning("Ollama analysis failed: %s", e)
            result = {}
        finally:
            if self._store is not None:
                try:
                    self._store.insert_llm_audit(
                        provider="ollama",
                        model=self._model,
                        prompt_text=prompt,
                        response_text=response_text,
                        source_name=self._source_name,
                        prompt_hash=prompt_hash,
                        tokens_in=tokens_in,
                        tokens_out=tokens_out,
                        raw_response_allowed=_raw_response_allowed(
                            self._store,
                            self._source_name,
                        ),
                    )
                except Exception as audit_err:
                    logger.warning("Failed to write LLM audit log: %s", audit_err)

        return result

    def _insert_cached_audit(
        self,
        prompt: str,
        response_text: str,
        *,
        prompt_hash: str,
        tokens_in: int = 0,
        tokens_out: int = 0,
    ) -> None:
        if self._store is None:
            return
        try:
            self._store.insert_llm_audit(
                provider="ollama",
                model=self._model,
                prompt_text=prompt,
                response_text=response_text,
                source_name=self._source_name,
                prompt_hash=prompt_hash,
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                cached=1,
                raw_response_allowed=_raw_response_allowed(
                    self._store,
                    self._source_name,
                ),
            )
        except Exception as audit_err:
            logger.warning("Failed to write cached LLM audit log: %s", audit_err)

    def _insert_budget_audit(self, prompt: str, *, prompt_hash: str) -> None:
        if self._store is None:
            return
        try:
            self._store.insert_llm_audit(
                provider="ollama",
                model=self._model,
                prompt_text=prompt,
                response_text=_BUDGET_EXHAUSTED_RESPONSE,
                source_name=self._source_name,
                prompt_hash=prompt_hash,
                cached=0,
                raw_response_allowed=True,
            )
        except Exception as audit_err:
            logger.warning("Failed to write budget LLM audit log: %s", audit_err)

    def _insert_source_budget_audit(self, prompt: str, *, prompt_hash: str) -> None:
        if self._store is None:
            return
        try:
            self._store.insert_llm_audit(
                provider="ollama",
                model=self._model,
                prompt_text=prompt,
                response_text=_SOURCE_BUDGET_EXHAUSTED_RESPONSE,
                source_name=self._source_name,
                prompt_hash=prompt_hash,
                cached=0,
                raw_response_allowed=True,
            )
        except Exception as audit_err:
            logger.warning("Failed to write source-budget LLM audit log: %s", audit_err)

    def _source_budget_allows(self, estimated_tokens: int) -> bool:
        if self._source_token_budget <= 0 or not self._source_name or self._store is None:
            return True
        usage = self._store.get_llm_token_usage(
            provider="ollama",
            model=self._model,
            source_name=self._source_name,
        )
        return usage + max(0, estimated_tokens) <= self._source_token_budget

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
