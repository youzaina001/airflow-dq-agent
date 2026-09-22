"""Kill the apply process once after a committed apply_plan result.

Compose-only reproduction helper. Not imported by product code.
"""

from __future__ import annotations

import os
import shutil
import signal
from collections.abc import Iterable
from pathlib import Path
from typing import Any

DEFAULT_MARKER = "/opt/airflow/traces/.apply-crash-once"


def fresh_run_id(occupied: Iterable[str], token: str) -> str:
    """A DagRun id that is not the fixed proof id and not already stored."""
    if not token or not token.isalnum():
        raise ValueError("token must be a non-empty slug")
    occupied_ids = set(occupied)
    candidate = f"crash-retry-proof-{token}"
    suffix = 2
    while candidate in occupied_ids or candidate == "crash-retry-proof":
        candidate = f"crash-retry-proof-{token}-{suffix}"
        suffix += 1
    return candidate


def _stage_flag(backup: Path) -> Path:
    return backup.with_name(backup.name + ".staged")


def stage_adopter_dag(destination: Path, example: Path, backup: Path) -> bool:
    """Copy the example DAG into place. Return whether destination already existed."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    existed = destination.is_file()
    if existed:
        shutil.copy2(destination, backup)
    shutil.copyfile(example, destination)
    _stage_flag(backup).write_text("1\n" if existed else "0\n", encoding="utf-8")
    return existed


def unstage_adopter_dag(destination: Path, backup: Path, *, existed: bool) -> None:
    """Put back a pre-existing adopter DAG, or remove the copy this proof created."""
    if existed:
        if destination.exists():
            destination.unlink()
        backup.replace(destination)
    else:
        if destination.exists():
            destination.unlink()
        if backup.exists():
            backup.unlink()
    flag = _stage_flag(backup)
    if flag.exists():
        flag.unlink()


def quality_run_id_from_triggered_xcom(value: str) -> str:
    """The quality run id published by the triggered DagRun, not a later report."""
    cleaned = value.strip()
    if not cleaned:
        raise ValueError("triggered dag run did not publish a quality run id")
    return cleaned


def crash_once_after(result: Any, *, marker_path: str) -> None:
    """Persist the committed identity, then SIGKILL this process the first time."""
    if os.path.exists(marker_path):
        return
    parent = os.path.dirname(marker_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    identity = getattr(result, "apply_result_id", "committed")
    with open(marker_path, "w", encoding="utf-8") as handle:
        handle.write(str(identity))
    os.kill(os.getpid(), signal.SIGKILL)


def install_apply_crash_once(*, marker_path: str | None = None) -> None:
    import airflow_dq_agent.apply as apply_module
    from airflow_dq_agent.apply import executor as apply_executor

    marker = marker_path or os.environ.get("DQ_COMPOSE_CRASH_MARKER", DEFAULT_MARKER)
    original = apply_executor.apply_plan

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        crash_once_after(result, marker_path=marker)
        return result

    apply_executor.apply_plan = wrapped  # type: ignore[assignment]
    apply_module.apply_plan = wrapped  # type: ignore[assignment]
