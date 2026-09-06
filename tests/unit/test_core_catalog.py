"""Core imports must not load the synthetic demo warehouse."""

from __future__ import annotations

import subprocess
import sys


def test_fresh_core_import_has_empty_catalogs() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import airflow_dq_agent; "
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
