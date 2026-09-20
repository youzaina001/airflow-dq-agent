"""Kill the apply process once after a committed apply_plan result.

Compose-only reproduction helper. Not imported by product code.
"""

from __future__ import annotations

import os
import signal
from typing import Any

DEFAULT_MARKER = "/opt/airflow/traces/.apply-crash-once"


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
