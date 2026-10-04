"""Settings defaults stay independent of developer dotenv files and the process environment."""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.settings_isolation import isolated_settings

from airflow_dq_agent.config import Settings, get_settings

_PLANTED = """\
LLM_MODE=live
APPLY_MODE=hitl
CATALOG_MCP_PORT=9
OPENAI_API_KEY=sentinel-not-a-secret
"""


def test_default_settings_ignore_planted_dotenv_and_process_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(_PLANTED, encoding="utf-8")
    # Production Settings reads ".env" from the working directory, not from tmp_path.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LLM_MODE", "live")
    monkeypatch.setenv("APPLY_MODE", "hitl")
    monkeypatch.setenv("CATALOG_MCP_PORT", "9")
    monkeypatch.setenv("OPENAI_API_KEY", "sentinel-not-a-secret")

    settings = isolated_settings()

    assert settings.llm_mode == "stub"
    assert settings.apply_mode == "off"
    assert settings.catalog_mcp_port == 8000
    assert settings.openai_api_key is None


def test_production_settings_read_dotenv_and_environment_overrides_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(_PLANTED, encoding="utf-8")
    for name in ("LLM_MODE", "APPLY_MODE", "CATALOG_MCP_PORT", "OPENAI_API_KEY"):
        monkeypatch.delenv(name, raising=False)

    from_file = Settings(_env_file=dotenv)
    assert from_file.llm_mode == "live"
    assert from_file.apply_mode == "hitl"
    assert from_file.catalog_mcp_port == 9
    assert from_file.openai_api_key == "sentinel-not-a-secret"

    monkeypatch.setenv("LLM_MODE", "replay")
    monkeypatch.setenv("CATALOG_MCP_PORT", "7")
    overridden = Settings(_env_file=dotenv)
    assert overridden.llm_mode == "replay"
    assert overridden.catalog_mcp_port == 7
    assert overridden.apply_mode == "hitl"
    assert overridden.openai_api_key == "sentinel-not-a-secret"

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LLM_MODE", raising=False)
    monkeypatch.delenv("CATALOG_MCP_PORT", raising=False)
    loaded = get_settings()
    assert loaded.llm_mode == "live"
    assert loaded.apply_mode == "hitl"
    assert loaded.catalog_mcp_port == 9
    assert loaded.openai_api_key == "sentinel-not-a-secret"
    monkeypatch.setenv("APPLY_MODE", "off")
    assert get_settings().apply_mode == "off"
    assert get_settings().llm_mode == "live"
