"""Runtime settings. Defaults keep the system in shadow mode with no live LLM."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

LlmMode = Literal["replay", "live", "stub"]
ApplyMode = Literal["off", "hitl"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    warehouse_dsn: str = "postgresql+psycopg://dq:dq@localhost:5433/warehouse"
    read_dsn: str | None = None
    audit_dsn: str | None = None
    apply_dsn: str | None = None
    llm_mode: LlmMode = "stub"
    apply_mode: ApplyMode = "off"
    llm_model: str = "openai:gpt-4.1-mini"
    openai_api_key: str | None = None
    openai_base_url: str | None = None
    replay_trace_path: Path | None = None
    traces_dir: Path = Path("traces")
    trace_postgres: bool = False
    catalog_mcp_host: str = "127.0.0.1"
    catalog_mcp_port: int = 8000
    sample_row_limit: int = 20
    eval_citation_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
    eval_groundedness_threshold: float = Field(default=0.8, ge=0.0, le=1.0)

    @property
    def live_configured(self) -> bool:
        return bool(self.openai_api_key) or bool(self.openai_base_url)


def get_settings() -> Settings:
    return Settings()
