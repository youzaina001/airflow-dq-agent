import hashlib
import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
# A short excerpt of the upstream 3.3.2 guard; the rebuilt image checks the full source.
ORIGINAL = b"""ti_requeued = (
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


def backport_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "backport_airflow_68741", ROOT / "scripts/backport_airflow_68741.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def installed_core(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[ModuleType, Path]:
    module = backport_module()
    scheduler = tmp_path / "scheduler_job_runner.py"
    scheduler.write_bytes(ORIGINAL)
    monkeypatch.setattr(
        module,
        "distribution",
        lambda name: SimpleNamespace(version="3.3.2", locate_file=lambda path: scheduler),
    )
    monkeypatch.setattr(module, "SOURCE_SHA256", hashlib.sha256(ORIGINAL).hexdigest())
    return module, scheduler


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
def test_backport_preserves_resume_success_guards(
    installed_core: tuple[ModuleType, Path],
    ti_state: str,
    executor_state: str,
    next_method: str | None,
    expected: bool,
) -> None:
    module, scheduler = installed_core
    module.main()

    states = SimpleNamespace(SCHEDULED="scheduled", QUEUED="queued", SUCCESS="success")
    namespace = {
        "ti": SimpleNamespace(state=ti_state, next_method=next_method, queued_by_job_id=1),
        "state": executor_state,
        "job_id": 1,
        "executor": SimpleNamespace(has_task=lambda ti: False),
        "TaskInstanceState": states,
    }
    exec(scheduler.read_bytes(), namespace)
    assert namespace["ti_requeued"] is expected


def test_other_core_version_is_refused_without_writing(
    installed_core: tuple[ModuleType, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    module, scheduler = installed_core
    monkeypatch.setattr(
        module,
        "distribution",
        lambda name: SimpleNamespace(version="3.3.3", locate_file=lambda path: scheduler),
    )
    with pytest.raises(RuntimeError, match=r"requires core 3\.3\.2"):
        module.main()
    assert scheduler.read_bytes() == ORIGINAL


def test_changed_source_is_refused_without_writing(installed_core: tuple[ModuleType, Path]) -> None:
    module, scheduler = installed_core
    changed = ORIGINAL + b"# unexpected source change\n"
    scheduler.write_bytes(changed)
    with pytest.raises(RuntimeError, match="source hash mismatch"):
        module.main()
    assert scheduler.read_bytes() == changed


@pytest.mark.parametrize("source", [b"# guard missing\n", ORIGINAL + ORIGINAL])
def test_missing_or_duplicate_guard_is_refused_without_writing(
    installed_core: tuple[ModuleType, Path], monkeypatch: pytest.MonkeyPatch, source: bytes
) -> None:
    module, scheduler = installed_core
    scheduler.write_bytes(source)
    monkeypatch.setattr(module, "SOURCE_SHA256", hashlib.sha256(source).hexdigest())
    with pytest.raises(RuntimeError, match="exactly one original guard"):
        module.main()
    assert scheduler.read_bytes() == source


def test_already_patched_source_is_refused_without_writing(
    installed_core: tuple[ModuleType, Path],
) -> None:
    module, scheduler = installed_core
    module.main()
    patched = scheduler.read_bytes()
    with pytest.raises(RuntimeError, match="source hash mismatch"):
        module.main()
    assert scheduler.read_bytes() == patched
