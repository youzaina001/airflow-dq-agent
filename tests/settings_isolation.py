"""Test-only settings construction. Production Settings still reads dotenv."""

from __future__ import annotations

from typing import Any

import pytest

from airflow_dq_agent.config import Settings

# Field lookup uses this prefix, so real process variables such as LLM_MODE do not match.
_ISOLATED_ENV_PREFIX = "DQ_TEST_ISOLATED_"


def isolated_settings(**overrides: Any) -> Settings:
    """Code defaults plus explicit kwargs. No dotenv file and no process environment."""
    return Settings(_env_file=None, _env_prefix=_ISOLATED_ENV_PREFIX, **overrides)


def use_isolated_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make Settings() in this test use isolated_settings, including inside production calls."""
    original = Settings.__init__

    def _init(self: Settings, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("_env_file", None)
        kwargs.setdefault("_env_prefix", _ISOLATED_ENV_PREFIX)
        original(self, *args, **kwargs)

    monkeypatch.setattr(Settings, "__init__", _init)


def use_settings_without_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep process environment, but do not let a working-directory .env fill unset names."""
    original = Settings.__init__

    def _init(self: Settings, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("_env_file", None)
        original(self, *args, **kwargs)

    monkeypatch.setattr(Settings, "__init__", _init)
