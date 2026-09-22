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


def test_fresh_run_id_skips_a_proof_id_that_is_still_in_airflow_metadata() -> None:
    hook = _load_hook()

    chosen = hook.fresh_run_id({"crash-retry-proof"}, token="abc123")

    assert chosen == "crash-retry-proof-abc123"
    assert chosen not in {"crash-retry-proof"}
    assert hook.fresh_run_id({chosen}, token="abc123") == "crash-retry-proof-abc123-2"


def test_preexisting_adopter_dag_is_restored_after_the_proof_copy(tmp_path: Path) -> None:
    hook = _load_hook()
    destination = tmp_path / "dags" / "dq_external_invoice.py"
    destination.parent.mkdir()
    destination.write_text("local adopter copy\n", encoding="utf-8")
    example = tmp_path / "example.py"
    example.write_text("proof copy\n", encoding="utf-8")
    backup = tmp_path / "invoice.bak"

    existed = hook.stage_adopter_dag(destination, example, backup)
    assert existed is True
    assert destination.read_text(encoding="utf-8") == "proof copy\n"

    hook.unstage_adopter_dag(destination, backup, existed=existed)

    assert destination.read_text(encoding="utf-8") == "local adopter copy\n"
    assert not backup.exists()


def test_proof_created_adopter_dag_is_removed_when_none_existed(tmp_path: Path) -> None:
    hook = _load_hook()
    destination = tmp_path / "dags" / "dq_external_invoice.py"
    example = tmp_path / "example.py"
    example.write_text("proof copy\n", encoding="utf-8")
    backup = tmp_path / "invoice.bak"

    existed = hook.stage_adopter_dag(destination, example, backup)

    assert existed is False
    hook.unstage_adopter_dag(destination, backup, existed=existed)
    assert not destination.exists()


def test_quality_run_id_is_the_triggered_dag_run_not_a_later_report() -> None:
    hook = _load_hook()

    chosen = hook.quality_run_id_from_triggered_xcom("run-from-crash-retry")

    assert chosen == "run-from-crash-retry"
    assert chosen != "run-from-scheduled"
    with pytest.raises(ValueError, match="quality run id"):
        hook.quality_run_id_from_triggered_xcom("  ")


def test_crash_retry_script_uses_the_proof_guards() -> None:
    script = (REPO / "scripts" / "compose-hitl-crash-retry.sh").read_text(encoding="utf-8")

    assert "fresh_run_id" in script
    assert 'crash_run="crash-retry-proof"' not in script
    assert "stage_adopter_dag" in script
    assert "unstage_adopter_dag" in script
    assert 'rm -f "$DAG_FILE"' not in script
    assignment = next(
        line.strip() for line in script.splitlines() if line.strip().startswith("crash_qid=")
    )
    assert assignment == 'crash_qid="$(xcom_field run_suite_task run_id)"'
    assert script.index("wait_for_hitl") < script.index(assignment)
    assert "quality_run_id_from_triggered_xcom" in script


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
