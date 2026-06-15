"""LLM provider protocol and implementations."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Protocol

from headwater.core.config import HeadwaterSettings

logger = logging.getLogger(__name__)


class LLMAuditStore(Protocol):
    """Minimal interface for optional LLM audit logging.

    Decouples the providers from any concrete metadata store: any object exposing
    these methods works, and ``None`` disables auditing entirely.
    """

    def insert_llm_audit(self, *args: Any, **kwargs: Any) -> Any: ...

    def get_llm_token_usage(self, *args: Any, **kwargs: Any) -> int: ...

LLM_REQUEST_TEMPLATE_VERSION = "llm-provider-analyze-v1"
_SENSITIVE_KEY_RE = re.compile(
    r"(api[_-]?key|authorization|bearer|credential|password|secret|token)",
    re.IGNORECASE,
)
_REDACTED_VALUE = "[REDACTED]"
_BUDGET_EXHAUSTED_RESPONSE = json.dumps(
    {"status": "partial", "reason": "llm_token_budget_exhausted"}
)
_SOURCE_BUDGET_EXHAUSTED_RESPONSE = json.dumps(
    {"status": "partial", "reason": "llm_source_token_budget_exhausted"}
)


class LLMProvider:
    """Base LLM provider interface."""

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        """Send a prompt to the LLM and return parsed JSON response."""
        raise NotImplementedError


class NoLLMProvider(LLMProvider):
    """No-op provider -- returns empty dict, triggers heuristic fallback."""

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        return {}


class LLMTokenBudget:
    """Run-scoped token budget for live LLM calls."""

    def __init__(self, max_tokens: int = 0) -> None:
        self.max_tokens = max(0, int(max_tokens or 0))
        self.used_tokens = 0

    @property
    def limited(self) -> bool:
        return self.max_tokens > 0

    @property
    def remaining_tokens(self) -> int | None:
        if not self.limited:
            return None
        return max(0, self.max_tokens - self.used_tokens)

    def can_spend(self, estimated_tokens: int) -> bool:
        if not self.limited:
            return True
        return self.used_tokens + max(0, estimated_tokens) <= self.max_tokens

    def record(self, tokens: int) -> None:
        self.used_tokens += max(0, int(tokens or 0))


class AnthropicProvider(LLMProvider):
    """Claude API provider using the Anthropic SDK."""

    def __init__(
        self,
        settings: HeadwaterSettings,
        store: LLMAuditStore | None = None,
        token_budget: LLMTokenBudget | None = None,
        source_name: str | None = None,
    ) -> None:
        if not settings.llm_api_key:
            raise ValueError("HEADWATER_LLM_API_KEY is required for Anthropic provider")
        import anthropic

        self._client = anthropic.AsyncAnthropic(api_key=settings.llm_api_key)
        self._model = settings.llm_model
        self._store = store
        self._offline_mode = settings.llm_offline_mode
        self._token_budget = token_budget or LLMTokenBudget(settings.llm_max_tokens_per_run)
        self._source_name = source_name
        self._source_token_budget = max(0, int(settings.llm_max_tokens_per_source or 0))

    async def analyze(self, prompt: str, system: str = "") -> dict[str, Any]:
        """Send prompt to Claude and return parsed JSON response.

        Always writes to the audit log if an audit store was provided at init.
        On failure, writes an empty response row with tokens=0.
        """
        import anthropic

        _system = system or (
            "You are a data analysis assistant. "
            "You MUST respond with valid JSON only — no prose, no markdown fences, no explanation. "
            "Return a single JSON object matching the schema described in the user prompt."
        )
        prompt_hash = make_llm_request_hash(
            prompt_template_version=LLM_REQUEST_TEMPLATE_VERSION,
            input_payload={"system": _system, "prompt": prompt},
            provider="anthropic",
            model=self._model,
            configuration={"max_tokens": 4096},
        )
        cached = _cached_response(
            self._store,
            provider="anthropic",
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
            # content[0] is a union of block types; only TextBlock has .text.
            response_text = getattr(msg.content[0], "text", "") if msg.content else ""
            tokens_in = msg.usage.input_tokens
            tokens_out = msg.usage.output_tokens
            self._token_budget.record(tokens_in + tokens_out)
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
                    self._store.insert_llm_audit(
                        provider="anthropic",
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
                provider="anthropic",
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
                provider="anthropic",
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
                provider="anthropic",
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
            provider="anthropic",
            model=self._model,
            source_name=self._source_name,
        )
        return usage + max(0, estimated_tokens) <= self._source_token_budget


def get_provider(
    settings: HeadwaterSettings,
    store: LLMAuditStore | None = None,
    source_name: str | None = None,
) -> LLMProvider:
    """Factory: return the appropriate LLM provider based on settings."""
    if settings.llm_provider == "anthropic":
        budget = LLMTokenBudget(settings.llm_max_tokens_per_run)
        return AnthropicProvider(
            settings,
            store=store,
            token_budget=budget,
            source_name=source_name,
        )
    if settings.llm_provider == "ollama":
        from headwater.analyzer.ollama import OllamaProvider

        budget = LLMTokenBudget(settings.llm_max_tokens_per_run)
        return OllamaProvider(
            settings,
            store=store,
            token_budget=budget,
            source_name=source_name,
        )
    return NoLLMProvider()


def check_llm_available(
    settings: HeadwaterSettings, *, model: str | None = None
) -> tuple[bool, str]:
    """Best-effort reachability check for AI features.

    Returns ``(ok, message)``. When ``ok`` is False the message is a concrete,
    user-facing reason (provider off, Ollama not running, model not installed,
    missing API key) — so features can tell the user what's wrong instead of
    failing silently. ``model`` overrides which model to verify (e.g. the
    reasoning model).
    """
    provider = settings.llm_provider
    if provider == "none":
        return False, "AI is off. Choose a provider (e.g. `hw2 engine on` with Ollama)."
    if provider == "anthropic":
        if not settings.llm_api_key:
            return False, "Anthropic is selected but HEADWATER_LLM_API_KEY is not set."
        return True, ""
    if provider == "openai_compat":
        if not settings.openai_compat_base_url:
            return False, "OpenAI-compatible provider has no endpoint configured."
        return True, ""
    if provider == "ollama":
        import httpx

        url = settings.ollama_base_url.rstrip("/")
        want = model or settings.llm_model
        try:
            resp = httpx.get(f"{url}/api/tags", timeout=4.0)
            resp.raise_for_status()
            installed = [m.get("name", "") for m in resp.json().get("models", [])]
        except Exception:
            return False, f"Can't reach Ollama at {url}. Start it with `ollama serve`."
        family = want.split(":", 1)[0]
        if want and not any(t == want or t.startswith(family) for t in installed):
            return False, f"Model '{want}' isn't installed. Run: `ollama pull {want}`."
        return True, ""
    return True, ""


def _cached_response(
    store: LLMAuditStore | None,
    *,
    provider: str,
    model: str,
    prompt_hash: str,
) -> dict | None:
    if store is None:
        return None
    getter = getattr(store, "get_cached_llm_response", None)
    if getter is None:
        return None
    return getter(provider=provider, model=model, prompt_hash=prompt_hash)


def _raw_response_allowed(
    store: LLMAuditStore | None,
    source_name: str | None,
) -> bool:
    if store is None:
        return True
    getter = getattr(store, "llm_raw_response_allowed", None)
    if getter is None:
        return True
    try:
        return bool(getter(source_name))
    except Exception as err:
        logger.warning("Failed to evaluate LLM audit storage policy: %s", err)
        return False


def make_cache_key(table_name: str, column_names: list[str]) -> str:
    """Generate a stable cache key for an LLM analysis request."""
    content = f"{table_name}:{','.join(sorted(column_names))}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def make_cache_key_from_text(text: str) -> str:
    """Generate a SHA-256 hash of arbitrary text (for prompt deduplication)."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def estimate_llm_tokens(text: str) -> int:
    """Conservative deterministic token estimate for pre-call budget gates."""
    normalized = str(text or "")
    if not normalized:
        return 0
    return max(1, (len(normalized) + 3) // 4)


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
