#!/usr/bin/env python3
"""Fail the image build if Airflow core lacks https://github.com/apache/airflow/pull/68741."""

from importlib.metadata import distribution
from pathlib import Path

UPSTREAM_GUARD = b"ti.state in (TaskInstanceState.SCHEDULED, TaskInstanceState.QUEUED)"


def main() -> None:
    core = distribution("apache-airflow-core")
    scheduler = Path(str(core.locate_file("airflow/jobs/scheduler_job_runner.py")))
    if scheduler.read_bytes().count(UPSTREAM_GUARD) != 1:
        raise RuntimeError(
            f"Airflow core {core.version} lacks the PR 68741 queued resume guard; "
            "a stale worker success could fail a queued HITL resume"
        )
    print(f"Airflow core {core.version} includes PR 68741")


if __name__ == "__main__":
    main()
