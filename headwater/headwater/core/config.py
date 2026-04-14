"""Headwater configuration via environment variables and headwater.yaml."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings


class HeadwaterSettings(BaseSettings):
    """Global settings. Reads from env vars prefixed HEADWATER_ and headwater.yaml."""

    model_config = {"env_prefix": "HEADWATER_"}

    # Directories
    data_dir: Path = Path.home() / ".headwater"

    # LLM
    llm_provider: Literal["none", "anthropic", "ollama", "openai_compat"] = "none"
    llm_api_key: str | None = None
    llm_model: str = "claude-sonnet-4-20250514"

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
    def graph_store_path(self) -> Path:
        return self.data_dir / "graph_store"

    def ensure_dirs(self) -> None:
        """Create data directory if it doesn't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> HeadwaterSettings:
    """Singleton settings accessor."""
    return HeadwaterSettings()
