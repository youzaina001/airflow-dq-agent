"""Bundled demo catalog registers the synthetic warehouse; the generic server does not."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def test_fresh_demo_catalog_prepare_fills_catalogs() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from airflow_dq_agent.demo import catalog as demo_catalog; "
                "demo_catalog.prepare_demo_catalog(); "
                "from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS; "
                "from airflow_dq_agent.quality.registry import CHECK_SPECS; "
                "from airflow_dq_agent.catalog.service import list_checks, list_tables; "
                "assert TABLE_CONTRACTS, TABLE_CONTRACTS; "
                "assert CHECK_SPECS, CHECK_SPECS; "
                "assert list_tables(); "
                "assert list_checks()"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_fresh_mcp_server_does_not_load_demo_and_leaves_catalogs_empty() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "from airflow_dq_agent.catalog.mcp_server import build_server; "
                "build_server(); "
                "assert 'airflow_dq_agent.demo' not in sys.modules; "
                "from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS; "
                "from airflow_dq_agent.quality.registry import CHECK_SPECS; "
                "assert TABLE_CONTRACTS == {}, TABLE_CONTRACTS; "
                "assert CHECK_SPECS == {}, CHECK_SPECS"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_makefile_and_compose_start_demo_catalog() -> None:
    makefile = (_ROOT / "Makefile").read_text()
    compose = (_ROOT / "docker-compose.yaml").read_text()
    assert "airflow_dq_agent.demo.catalog" in makefile
    assert "airflow_dq_agent.demo.catalog" in compose
    assert "airflow_dq_agent.catalog.mcp_server" not in makefile
    assert "airflow_dq_agent.catalog.mcp_server" not in compose
