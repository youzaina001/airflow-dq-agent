"""The pinned provider's wait path must keep fast responses off the scheduler race."""

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import ClassVar

import pytest


@pytest.mark.parametrize("deadline", [timedelta(seconds=90), None])
def test_provider_wait_defers_once_with_the_original_deadline_and_request(
    monkeypatch: pytest.MonkeyPatch,
    deadline: timedelta | None,
) -> None:
    now = datetime(2026, 10, 30, tzinfo=UTC)
    request_writes: list[object] = []
    deferred: list[dict[str, object]] = []

    class AwaitingInput(BaseException):
        timeout = deadline

    class Deferred(BaseException):
        pass

    class Provider:
        template_fields = ()
        options: ClassVar = ["Approve", "Reject"]
        defaults: ClassVar = ["Reject"]
        serialized_params: ClassVar = {"approval_note": {"value": ""}}
        multiple = False

        def __init__(self, **kwargs: object) -> None:
            pass

        def execute(self, context: dict[str, object]) -> None:
            request_writes.append(context)
            raise AwaitingInput()

        def defer(self, **kwargs: object) -> None:
            deferred.append(kwargs)
            raise Deferred()

    modules = {
        "airflow.providers.standard.operators.hitl": {"ApprovalOperator": Provider},
        "airflow.providers.standard.triggers.hitl": {"HITLTrigger": lambda **kwargs: kwargs},
        "airflow.sdk.exceptions": {"TaskAwaitingInput": AwaitingInput},
        "airflow.sdk.timezone": {"utcnow": lambda: now},
    }
    for name, attributes in modules.items():
        module = ModuleType(name)
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, module)
    path = Path(__file__).resolve().parents[2] / "src/airflow_dq_agent/airflow_hitl.py"
    spec = importlib.util.spec_from_file_location("wait_adapter_under_test", path)
    assert spec is not None and spec.loader is not None
    adapter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(adapter)
    operator = adapter.AuditedApprovalOperator(
        quality_run_id="quality-run",
        predecessor_event_id="review-event",
        approver_ids={"reviewer"},
        audit_dsn=None,
    )
    context = {"task_instance": SimpleNamespace(id="task-instance")}

    with pytest.raises(Deferred):
        operator.execute(context)

    assert request_writes == [context]
    assert deferred == [
        {
            "method_name": "execute_complete",
            "trigger": {
                "ti_id": "task-instance",
                "options": ["Approve", "Reject"],
                "defaults": ["Reject"],
                "params": Provider.serialized_params,
                "multiple": False,
                "timeout_datetime": now + deadline if deadline else None,
            },
        }
    ]
