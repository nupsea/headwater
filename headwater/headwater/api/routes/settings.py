"""Settings API -- LLM provider configuration read/update with file persistence."""

from __future__ import annotations

import logging
import os
from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel

from headwater.core.config import get_settings, save_settings_to_file

router = APIRouter()
logger = logging.getLogger(__name__)


class LLMSettingsResponse(BaseModel):
    """Current LLM provider configuration (read-only view)."""

    provider: str
    model: str
    ollama_base_url: str
    openai_compat_base_url: str | None
    has_api_key: bool
    has_openai_compat_key: bool


class LLMSettingsUpdate(BaseModel):
    """Payload for updating LLM provider settings."""

    provider: Literal["none", "anthropic", "ollama", "openai_compat"] | None = None
    model: str | None = None
    api_key: str | None = None
    ollama_base_url: str | None = None
    openai_compat_base_url: str | None = None
    openai_compat_api_key: str | None = None


class LLMVerifyResponse(BaseModel):
    """Result of LLM connectivity verification."""

    status: Literal["ok", "error"]
    provider: str
    model: str
    detail: str | None = None
    latency_ms: int | None = None


@router.get("/settings/llm")
async def get_llm_settings(request: Request) -> LLMSettingsResponse:
    """Return current LLM provider configuration. Never returns actual keys."""
    settings = get_settings()
    return LLMSettingsResponse(
        provider=settings.llm_provider,
        model=settings.llm_model,
        ollama_base_url=settings.ollama_base_url,
        openai_compat_base_url=settings.openai_compat_base_url,
        has_api_key=bool(settings.llm_api_key),
        has_openai_compat_key=bool(settings.openai_compat_api_key),
    )


# Cloud-only model prefixes that cannot run in Ollama
_CLOUD_MODEL_PREFIXES = ("claude-", "gpt-", "gemini-", "o1-", "o3-")


@router.put("/settings/llm")
async def update_llm_settings(
    body: LLMSettingsUpdate,
    request: Request,
) -> LLMSettingsResponse:
    """Update LLM provider settings. Persists non-secret values to settings.json."""
    from fastapi import HTTPException

    # Validate provider + model compatibility
    if (
        body.provider == "ollama"
        and body.model
        and any(body.model.startswith(p) for p in _CLOUD_MODEL_PREFIXES)
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                f"Model '{body.model}' is a cloud-only model and cannot "
                f"run in Ollama. Use a local model (e.g. mistral:latest, "
                f"gemma4:latest) or switch the provider to 'anthropic'."
            ),
        )
    if (
        body.provider == "anthropic"
        and body.model
        and not body.model.startswith("claude")
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                f"Model '{body.model}' is not an Anthropic model. "
                f"Use a Claude model (e.g. claude-sonnet-4-20250514) "
                f"or switch the provider."
            ),
        )

    changes: list[str] = []

    if body.provider is not None:
        os.environ["HEADWATER_LLM_PROVIDER"] = body.provider
        changes.append(f"provider={body.provider}")

    if body.model is not None:
        os.environ["HEADWATER_LLM_MODEL"] = body.model
        changes.append(f"model={body.model}")

    if body.api_key is not None:
        os.environ["HEADWATER_LLM_API_KEY"] = body.api_key
        changes.append("api_key=***")

    if body.ollama_base_url is not None:
        os.environ["HEADWATER_OLLAMA_BASE_URL"] = body.ollama_base_url
        changes.append(f"ollama_base_url={body.ollama_base_url}")

    if body.openai_compat_base_url is not None:
        os.environ["HEADWATER_OPENAI_COMPAT_BASE_URL"] = body.openai_compat_base_url
        changes.append(f"openai_compat_base_url={body.openai_compat_base_url}")

    if body.openai_compat_api_key is not None:
        os.environ["HEADWATER_OPENAI_COMPAT_API_KEY"] = body.openai_compat_api_key
        changes.append("openai_compat_api_key=***")

    # Clear the cached settings so next call gets fresh values
    get_settings.cache_clear()

    if changes:
        logger.info("LLM settings updated: %s", ", ".join(changes))

    # Persist non-secret settings to disk
    settings = get_settings()
    try:
        path = save_settings_to_file(settings)
        logger.info("Settings persisted to %s", path)
    except OSError:
        logger.exception("Failed to persist settings to disk")

    # Log activity
    store = request.app.state.metadata_store
    store.log_activity(
        "settings_updated",
        f"LLM settings changed: {', '.join(changes)}",
        artifact_type="settings",
        artifact_id="llm",
    )

    return LLMSettingsResponse(
        provider=settings.llm_provider,
        model=settings.llm_model,
        ollama_base_url=settings.ollama_base_url,
        openai_compat_base_url=settings.openai_compat_base_url,
        has_api_key=bool(settings.llm_api_key),
        has_openai_compat_key=bool(settings.openai_compat_api_key),
    )


@router.get("/settings/ollama-models")
async def list_ollama_models(request: Request):
    """Probe the Ollama server and return available model names."""
    import httpx

    settings = get_settings()
    base_url = settings.ollama_base_url or "http://localhost:11434"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{base_url}/api/tags")
            resp.raise_for_status()
            tags = resp.json()
            models = [m["name"] for m in tags.get("models", [])]
            return {"models": models, "base_url": base_url}
    except Exception as exc:
        return {"models": [], "base_url": base_url, "error": str(exc)}


@router.get("/settings/verify-llm")
async def verify_llm(request: Request) -> LLMVerifyResponse:
    """Test LLM provider connectivity."""
    import time

    settings = get_settings()
    provider = settings.llm_provider
    model = settings.llm_model

    if provider == "none":
        return LLMVerifyResponse(
            status="ok",
            provider=provider,
            model=model,
            detail="No LLM provider configured",
        )

    start = time.monotonic()

    if provider == "ollama":
        import httpx

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{settings.ollama_base_url}/api/tags")
                resp.raise_for_status()
                tags = resp.json()
                models = [m["name"] for m in tags.get("models", [])]
                elapsed = int((time.monotonic() - start) * 1000)

                # Validate the configured model exists in Ollama
                # Ollama model names can include or omit the :tag suffix
                model_base = model.split(":")[0] if model else ""
                available_bases = [m.split(":")[0] for m in models]
                if model not in models and model_base not in available_bases:
                    return LLMVerifyResponse(
                        status="error",
                        provider=provider,
                        model=model,
                        detail=(
                            f"Model '{model}' is not available in Ollama. "
                            f"Available models: {', '.join(models[:10])}. "
                            f"Pull it with: ollama pull {model}"
                        ),
                        latency_ms=elapsed,
                    )

                return LLMVerifyResponse(
                    status="ok",
                    provider=provider,
                    model=model,
                    detail=f"Connected. Available models: {', '.join(models[:5])}",
                    latency_ms=elapsed,
                )
        except Exception as exc:
            return LLMVerifyResponse(
                status="error",
                provider=provider,
                model=model,
                detail=f"Connection failed: {exc}",
            )

    if provider == "anthropic":
        if not settings.llm_api_key:
            return LLMVerifyResponse(
                status="error",
                provider=provider,
                model=model,
                detail="No API key configured",
            )
        try:
            import anthropic

            client = anthropic.Anthropic(api_key=settings.llm_api_key)
            client.messages.create(
                model=model,
                max_tokens=1,
                messages=[{"role": "user", "content": "hi"}],
            )
            elapsed = int((time.monotonic() - start) * 1000)
            return LLMVerifyResponse(
                status="ok",
                provider=provider,
                model=model,
                detail=f"Connected to Anthropic ({model})",
                latency_ms=elapsed,
            )
        except Exception as exc:
            return LLMVerifyResponse(
                status="error",
                provider=provider,
                model=model,
                detail=f"API call failed: {exc}",
            )

    if provider == "openai_compat":
        base_url = settings.openai_compat_base_url
        if not base_url:
            return LLMVerifyResponse(
                status="error",
                provider=provider,
                model=model,
                detail="No base URL configured",
            )
        import httpx

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{base_url}/models")
                resp.raise_for_status()
                elapsed = int((time.monotonic() - start) * 1000)
                return LLMVerifyResponse(
                    status="ok",
                    provider=provider,
                    model=model,
                    detail=f"Connected to {base_url}",
                    latency_ms=elapsed,
                )
        except Exception as exc:
            return LLMVerifyResponse(
                status="error",
                provider=provider,
                model=model,
                detail=f"Connection failed: {exc}",
            )

    return LLMVerifyResponse(
        status="error",
        provider=provider,
        model=model,
        detail=f"Unknown provider: {provider}",
    )
