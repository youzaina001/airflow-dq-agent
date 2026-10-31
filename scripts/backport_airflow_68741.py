#!/usr/bin/env python3
"""Backport https://github.com/apache/airflow/pull/68741 to the pinned image."""

import hashlib
from importlib.metadata import distribution
from pathlib import Path

SOURCE_SHA256 = "f70cb02c32d618dcbc94a9898dd0c40fb68aca607020258404f6ca580bca1873"
BEFORE = (
    b"                    # Resume-after-defer: trigger moved TI to scheduled (next_method set) before we saw the\n"
    b"                    # executor success from the defer exit for the same try_number.\n"
    b"                    ti.state == TaskInstanceState.SCHEDULED\n"
)
AFTER = (
    b"                    # Resume-after-defer: trigger moved TI to scheduled or queued (next_method set)\n"
    b"                    # before we saw the executor success from the defer exit for the same try_number.\n"
    b"                    ti.state in (TaskInstanceState.SCHEDULED, TaskInstanceState.QUEUED)\n"
)


def main() -> None:
    core = distribution("apache-airflow-core")
    if core.version != "3.3.2":
        raise RuntimeError(f"Airflow PR 68741 backport requires core 3.3.2; found {core.version}")
    scheduler = Path(str(core.locate_file("airflow/jobs/scheduler_job_runner.py")))
    source = scheduler.read_bytes()
    if hashlib.sha256(source).hexdigest() != SOURCE_SHA256:
        raise RuntimeError("Airflow scheduler source hash mismatch; review the PR 68741 backport")
    if source.count(BEFORE) != 1:
        raise RuntimeError("Airflow PR 68741 backport expected exactly one original guard")
    scheduler.write_bytes(source.replace(BEFORE, AFTER, 1))
    print("Applied Apache Airflow PR 68741 to core 3.3.2")


if __name__ == "__main__":
    main()
