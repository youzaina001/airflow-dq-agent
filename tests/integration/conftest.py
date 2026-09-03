"""Shared integration fixtures: isolated traces and a throwaway Postgres warehouse."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolated_trace_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep host integration runs independent of Compose-mounted trace ownership."""
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))


@pytest.fixture(scope="module")
def warehouse_dsn() -> str:
    configured = os.getenv("TEST_WAREHOUSE_DSN")
    if configured:
        return configured
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:16")
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker/Postgres unavailable: {exc}")
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql+psycopg")
    finally:
        container.stop()
