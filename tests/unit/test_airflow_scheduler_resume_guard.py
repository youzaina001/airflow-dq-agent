import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
# Short excerpts of the scheduler guard before and after Apache Airflow PR 68741.
# Core 3.3.2 shipped BEFORE; core 3.4.0 ships AFTER. The image build checks the real source.
BEFORE = b"""ti_requeued = (
                ti.queued_by_job_id != job_id
                or executor.has_task(ti)
                or (
                    # Resume-after-defer: trigger moved TI to scheduled (next_method set) before we saw the
                    # executor success from the defer exit for the same try_number.
                    ti.state == TaskInstanceState.SCHEDULED
                    and state == TaskInstanceState.SUCCESS
                    and ti.next_method is not None
                )
            )
"""
AFTER = b"""ti_requeued = (
                ti.queued_by_job_id != job_id
                or executor.has_task(ti)
                or (
                    # Resume-after-defer: trigger moved TI to scheduled or queued (next_method set)
                    # before we saw the executor success from the defer exit for the same try_number.
                    ti.state in (TaskInstanceState.SCHEDULED, TaskInstanceState.QUEUED)
                    and state == TaskInstanceState.SUCCESS
                    and ti.next_method is not None
                )
            )
"""


def check_module(monkeypatch: pytest.MonkeyPatch, scheduler: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "check_airflow_68741", ROOT / "scripts/check_airflow_68741.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(
        module,
        "distribution",
        lambda name: SimpleNamespace(version="3.4.0", locate_file=lambda path: scheduler),
    )
    return module


def test_core_with_the_upstream_fix_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = tmp_path / "scheduler_job_runner.py"
    scheduler.write_bytes(AFTER)
    check_module(monkeypatch, scheduler).main()


@pytest.mark.parametrize("source", [BEFORE, b"# guard missing\n"])
def test_core_without_the_upstream_fix_fails_the_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: bytes
) -> None:
    scheduler = tmp_path / "scheduler_job_runner.py"
    scheduler.write_bytes(source)
    with pytest.raises(RuntimeError, match="PR 68741"):
        check_module(monkeypatch, scheduler).main()


@pytest.mark.parametrize(
    ("ti_state", "executor_state", "next_method", "expected"),
    [
        ("queued", "success", "execute_complete", True),
        ("scheduled", "success", "execute_complete", True),
        ("queued", "success", None, False),
        ("scheduled", "success", None, False),
        ("queued", "failed", "execute_complete", False),
        ("running", "success", "execute_complete", False),
    ],
)
def test_upstream_guard_treats_a_queued_resume_as_requeued(
    ti_state: str, executor_state: str, next_method: str | None, expected: bool
) -> None:
    states = SimpleNamespace(SCHEDULED="scheduled", QUEUED="queued", SUCCESS="success")
    namespace = {
        "ti": SimpleNamespace(state=ti_state, next_method=next_method, queued_by_job_id=1),
        "state": executor_state,
        "job_id": 1,
        "executor": SimpleNamespace(has_task=lambda ti: False),
        "TaskInstanceState": states,
    }
    exec(AFTER.strip(), namespace)
    assert namespace["ti_requeued"] is expected
