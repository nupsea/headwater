"""Headwater configuration via environment variables and settings.json."""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

# Keys persisted to settings.json (never persist secrets like api_key)
_PERSISTED_KEYS = frozenset(
    {
        "llm_provider",
        "llm_model",
        "llm_offline_mode",
        "llm_max_tokens_per_run",
        "llm_max_tokens_per_source",
        "ollama_base_url",
        "ollama_timeout",
        "openai_compat_base_url",
        "sample_size",
        "log_level",
        "mart_min_relationships",
        "mart_min_metric_columns",
        "mart_min_rows",
    }
)


class HeadwaterSettings(BaseSettings):
    """Global settings. Reads from env vars prefixed HEADWATER_ and headwater.yaml."""

    model_config = {"env_prefix": "HEADWATER_"}

    # Directories
    data_dir: Path = Path.home() / ".headwater"

    # LLM
    # Ollama (local) is the default provider — no 3P account or key required.
    # Connecting Anthropic/OpenAI-compatible vendors is opt-in via settings.
    llm_provider: Literal["none", "anthropic", "ollama", "openai_compat"] = "ollama"
    llm_api_key: str | None = None
    llm_model: str = "qwen2.5:14b-instruct"
    llm_offline_mode: bool = False
    llm_max_tokens_per_run: int = 0
    llm_max_tokens_per_source: int = 0

    # Ollama (local LLM)
    ollama_base_url: str = "http://localhost:11434"
    ollama_timeout: int = 120  # seconds

    # OpenAI-compatible endpoint (vLLM, Together, Groq, etc.)
    openai_compat_base_url: str | None = None
    openai_compat_api_key: str | None = None

    # Profiling
    sample_size: int = 50_000

    # Logging
    log_level: str = "INFO"

    # Mart quality gate thresholds (US-503)
    mart_min_relationships: int = 2
    mart_min_metric_columns: int = 1
    mart_min_rows: int = 100

    # Reasoning engine (capability-gated; default off keeps the legacy path)
    reasoning_engine: bool = False
    knowledge_backend: Literal["sqlite", "duckpgq", "kuzu"] = "sqlite"
    insight_battery: bool = False
    pii_detection: bool = False

    @property
    def metadata_db_path(self) -> Path:
        return self.data_dir / "metadata.db"

    @property
    def analytical_db_path(self) -> Path:
        return self.data_dir / "analytical.duckdb"

    @property
    def vector_store_path(self) -> Path:
        return self.data_dir / "vector_store"

    @property
    def embedding_cache_path(self) -> Path:
        return self.data_dir / "embedding_cache"

    @property
    def graph_store_path(self) -> Path:
        return self.data_dir / "graph_store"

    @property
    def settings_file_path(self) -> Path:
        return self.data_dir / "settings.json"

    @property
    def setup_drafts_path(self) -> Path:
        return self.data_dir / "setup_drafts"

    @property
    def setup_draft_key_path(self) -> Path:
        return self.data_dir / "setup_drafts.key"

    def ensure_dirs(self) -> None:
        """Create data directory if it doesn't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)


def _load_settings_from_file(data_dir: Path) -> dict:
    """Load persisted settings from settings.json if it exists."""
    settings_path = data_dir / "settings.json"
    if settings_path.exists():
        try:
            with open(settings_path) as f:
                data = json.load(f)
            logger.info("Loaded settings from %s", settings_path)
            return {k: v for k, v in data.items() if k in _PERSISTED_KEYS}
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load settings.json: %s", exc)
    return {}


def save_settings_to_file(settings: HeadwaterSettings) -> Path:
    """Persist non-secret settings to settings.json. Returns the file path."""
    settings.ensure_dirs()
    data = {}
    for key in _PERSISTED_KEYS:
        val = getattr(settings, key, None)
        if val is not None:
            data[key] = val if not isinstance(val, Path) else str(val)
    path = settings.settings_file_path
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info("Saved settings to %s", path)
    return path


@lru_cache(maxsize=1)
def get_settings() -> HeadwaterSettings:
    """Singleton settings accessor. Env vars > file values > defaults.

    Pydantic BaseSettings treats constructor kwargs as highest priority,
    so we only pass file values for keys that have no env var set.
    """
    import os

    base = HeadwaterSettings()
    file_overrides = _load_settings_from_file(base.data_dir)
    if file_overrides:
        # Only use file values where the env var is NOT set
        env_prefix = "HEADWATER_"
        filtered = {
            k: v
            for k, v in file_overrides.items()
            if f"{env_prefix}{k.upper()}" not in os.environ
        }
        if filtered:
            return HeadwaterSettings(**filtered)
    return base
