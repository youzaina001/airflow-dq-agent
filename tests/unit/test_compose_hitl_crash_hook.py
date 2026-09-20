"""Compose crash-after-commit hook: first committed apply dies; retry does not."""

from __future__ import annotations

import importlib.util
import signal
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[2]
HOOK = REPO / "scripts" / "compose-hitl-crash" / "crash_once.py"


def _load_hook() -> ModuleType:
    spec = importlib.util.spec_from_file_location("compose_hitl_crash_once", HOOK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Result:
    apply_result_id = "committed-result-1"


def test_crash_once_kills_first_committed_apply_then_leaves_retry_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hook = _load_hook()
    marker = tmp_path / "traces" / ".apply-crash-once"
    killed: list[int] = []
    monkeypatch.setattr(hook.os, "kill", lambda _pid, sig: killed.append(sig))

    hook.crash_once_after(_Result(), marker_path=str(marker))
    assert marker.read_text(encoding="utf-8") == "committed-result-1"
    assert killed == [signal.SIGKILL]

    hook.crash_once_after(_Result(), marker_path=str(marker))
    assert killed == [signal.SIGKILL]
    assert marker.read_text(encoding="utf-8") == "committed-result-1"
