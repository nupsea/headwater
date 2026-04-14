"""Settings API -- LLM provider configuration read/update."""

from __future__ import annotations

import logging
import os
from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel

from headwater.core.config import get_settings

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


@router.put("/settings/llm")
async def update_llm_settings(
    body: LLMSettingsUpdate,
    request: Request,
) -> LLMSettingsResponse:
    """Update LLM provider settings for the current session.

    Updates are applied to environment variables so that get_settings()
    picks them up. The lru_cache is cleared to force re-read.
    """
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

    # Return the new state
    settings = get_settings()
    return LLMSettingsResponse(
        provider=settings.llm_provider,
        model=settings.llm_model,
        ollama_base_url=settings.ollama_base_url,
        openai_compat_base_url=settings.openai_compat_base_url,
        has_api_key=bool(settings.llm_api_key),
        has_openai_compat_key=bool(settings.openai_compat_api_key),
    )
