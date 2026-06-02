"""Health check endpoint -- liveness + provider status for monitoring."""

from __future__ import annotations

import time

from fastapi import APIRouter

router = APIRouter()

_STARTUP_TIME: float = time.monotonic()

VERSION = "0.3.0"


@router.get("/health")
async def health_check():
    """Return liveness plus a best-effort LLM-provider probe.

    H2 routes own their stores per-request, so health is stateless: it reports
    the configured LLM provider (no network call) and uptime.
    """
    components: dict[str, str] = {}

    try:
        from headwater.analyzer.llm import get_provider
        from headwater.core.config import get_settings

        provider = get_provider(get_settings(), store=None)
        components["llm_provider"] = type(provider).__name__
    except Exception as e:
        components["llm_provider"] = f"not_configured: {e}"

    return {
        "status": "healthy",
        "components": components,
        "version": VERSION,
        "uptime_seconds": round(time.monotonic() - _STARTUP_TIME, 1),
    }
