"""DAG parse-time topology is stable across LLM_MODE and APPLY_MODE.

The DAG file is loaded with a minimal Airflow stub so task registration can be
inspected without an Airflow install. APPLY_MODE=off is shadow: the HITL chain
is still registered, then skipped at runtime so mutation cannot run.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    CheckResult,
    CheckStatus,
    Dimension,
    EvalReport,
    ExecutablePlanItem,
    HumanDecision,
    QualityEvidence,
    QualitySuiteReport,
    RemediationPlan,
    TargetSet,
)

DAG_PATH = Path(__file__).resolve().parents[2] / "dags" / "dq_daily.py"

LLM_MODES = ("stub", "replay", "live")
APPLY_MODES = ("off", "hitl")

STABLE_TASK_IDS = frozenset(
    {
        "run_suite_task",
        "propose_task",
        "audit_candidate_task",
        "prepare_plan_task",
        "require_approval",
        "approve_remediation_plan",
        "admit_apply_task",
        "apply_after_admission_task",
    }
)


class _TaskFlowRef:
    """Stand-in for an Airflow XComArg so DAG wiring (`>>`, `.output`) can parse."""

    def __init__(self, name: str) -> None:
        self.__name__ = name
        self.output = self

    def __rshift__(self, other: object) -> object:
        return other

    def __rrshift__(self, other: object) -> object:
        return self


def load_dq_daily(
    monkeypatch: pytest.MonkeyPatch,
    *,
    llm_mode: str,
    apply_mode: str,
    module_name: str,
) -> tuple[types.ModuleType, dict[str, Callable[..., Any]], set[str]]:
    """Parse dags/dq_daily.py with stubbed Airflow and record registered task ids."""
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql+psycopg://dq:dq@localhost:1/unused-warehouse")
    monkeypatch.delenv("READ_DSN", raising=False)
    monkeypatch.delenv("AUDIT_DSN", raising=False)
    monkeypatch.delenv("APPLY_DSN", raising=False)
    monkeypatch.delenv("TRACE_POSTGRES", raising=False)
    monkeypatch.setenv("LLM_MODE", llm_mode)
    monkeypatch.setenv("APPLY_MODE", apply_mode)
    monkeypatch.setenv("HITL_APPROVER_IDS", "airflow")

    exceptions_module = types.ModuleType("airflow.exceptions")

    class AirflowSkipException(Exception): ...

    exceptions_module.AirflowSkipException = AirflowSkipException
    sdk_module = types.ModuleType("airflow.sdk")
    tasks: dict[str, Callable[..., Any]] = {}
    operator_task_ids: set[str] = set()

    def _stub_dag(
        *_args: Any, **_kwargs: Any
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
            return fn

        return wrap

    def _stub_task(
        fn: Callable[..., Any] | None = None, **_kwargs: Any
    ) -> Callable[..., Any] | Callable[[Callable[..., Any]], Callable[..., Any]]:
        def register(candidate: Callable[..., Any]) -> Callable[..., Any]:
            tasks[candidate.__name__] = candidate

            def xcom_reference(*_call_args: Any, **_call_kwargs: Any) -> _TaskFlowRef:
                return _TaskFlowRef(candidate.__name__)

            xcom_reference.__name__ = candidate.__name__
            return xcom_reference

        return register if fn is None else register(fn)

    sdk_module.dag = _stub_dag
    sdk_module.task = _stub_task

    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.exceptions", exceptions_module)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_module)

    import airflow_dq_agent.airflow_hitl as airflow_hitl

    class _RecordingApprovalOperator:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.task_id = kwargs.get("task_id", args[0] if args else None)
            if not self.task_id:
                raise AssertionError("AuditedApprovalOperator must be given task_id")
            operator_task_ids.add(str(self.task_id))
            self.output = _TaskFlowRef(str(self.task_id))

        def __rshift__(self, other: object) -> object:
            return other

        def __rrshift__(self, other: object) -> object:
            return self

    monkeypatch.setattr(airflow_hitl, "AuditedApprovalOperator", _RecordingApprovalOperator)

    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, tasks, set(tasks) | operator_task_ids


def _passing_evaluation_payload() -> dict[str, Any]:
    plan = RemediationPlan(
        quality_run_id="run-1",
        candidate_fingerprint="cand-1",
        policy_fingerprint="pol-1",
        items=[
            ExecutablePlanItem(
                item_id="item-1",
                action_id="quarantine_nulls",
                table="fact_orders",
                evidence=[QualityEvidence(check_id="c1", contract_id="t1")],
                target_set=TargetSet(count=1, fingerprint="ts-1"),
                policy_fingerprint="pol-1",
            )
        ],
        blocked=False,
        warehouse_environment_id="staging.example:5432/warehouse",
        fingerprint="plan-fp-1",
    )
    evaluation = EvalReport(
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        passed=True,
        scores=[],
        fingerprint="eval-fp-1",
    )
    return {
        "plan": plan.model_dump(mode="json"),
        "plan_event_id": "plan-event-1",
        "evaluation": evaluation.model_dump(mode="json"),
        "evaluation_event_id": "eval-event-1",
    }


def _blocked_evaluation_payload() -> dict[str, Any]:
    payload = _passing_evaluation_payload()
    plan = RemediationPlan.model_validate(payload["plan"])
    payload["plan"] = plan.model_copy(
        update={"blocked": True, "blocked_reasons": ["blocked for test"]}
    ).model_dump(mode="json")
    return payload


def _failed_evaluation_payload() -> dict[str, Any]:
    payload = _passing_evaluation_payload()
    evaluation = EvalReport.model_validate(payload["evaluation"])
    payload["evaluation"] = evaluation.model_copy(update={"passed": False}).model_dump(mode="json")
    return payload


def _report_payload() -> dict[str, Any]:
    return QualitySuiteReport(
        run_id="run-1",
        checks=[
            CheckResult(
                check_id="c1",
                table="fact_orders",
                dimension=Dimension.COMPLETENESS,
                status=CheckStatus.FAIL,
                message="total_amount is null",
                contract_id="t1",
            )
        ],
    ).model_dump(mode="json")


def _approve_decision() -> dict[str, Any]:
    return HumanDecision(
        decision="Approve",
        actor="airflow",
        note="Reviewed the whole plan.",
        audit_event_id="decision-event-1",
    ).model_dump(mode="json")


def _reject_decision() -> dict[str, Any]:
    return HumanDecision(
        decision="Reject",
        actor="airflow",
        note="Scope is too broad.",
        audit_event_id="decision-event-1",
    ).model_dump(mode="json")


def _admission_payload() -> dict[str, Any]:
    from datetime import UTC, datetime, timedelta

    now = datetime(2026, 1, 1, tzinfo=UTC)
    return ApplyAdmission(
        quality_run_id="run-1",
        plan_id="plan-1",
        plan_fingerprint="plan-fp-1",
        evaluation_id="eval-1",
        evaluation_fingerprint="eval-fp-1",
        decision_id="dec-1",
        decision_event_id="decision-event-1",
        policy_fingerprint="pol-1",
        warehouse_environment_id="staging.example:5432/warehouse",
        issued_at=now,
        expires_at=now + timedelta(hours=24),
        fingerprint="adm-fp-1",
    ).model_dump(mode="json")


@pytest.mark.integration
def test_apply_mode_off_and_hitl_register_the_same_task_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, _, off_ids = load_dq_daily(
        monkeypatch, llm_mode="stub", apply_mode="off", module_name="dq_daily_topology_off"
    )
    _, _, hitl_ids = load_dq_daily(
        monkeypatch, llm_mode="stub", apply_mode="hitl", module_name="dq_daily_topology_hitl"
    )
    assert off_ids == hitl_ids, (
        "APPLY_MODE must not add or remove DAG tasks: "
        f"off={sorted(off_ids)} hitl={sorted(hitl_ids)} "
        f"hitl_only={sorted(hitl_ids - off_ids)} off_only={sorted(off_ids - hitl_ids)}"
    )


@pytest.mark.integration
@pytest.mark.parametrize("llm_mode", LLM_MODES)
@pytest.mark.parametrize("apply_mode", APPLY_MODES)
def test_dq_daily_task_graph_is_stable_for_every_mode_pair(
    monkeypatch: pytest.MonkeyPatch, llm_mode: str, apply_mode: str
) -> None:
    _, _, task_ids = load_dq_daily(
        monkeypatch,
        llm_mode=llm_mode,
        apply_mode=apply_mode,
        module_name=f"dq_daily_topology_{llm_mode}_{apply_mode}",
    )
    assert task_ids == set(STABLE_TASK_IDS)


@pytest.mark.integration
def test_require_approval_skips_when_apply_mode_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    module, tasks, _ = load_dq_daily(
        monkeypatch, llm_mode="stub", apply_mode="off", module_name="dq_daily_skip_approval_off"
    )
    with pytest.raises(module.AirflowSkipException):
        tasks["require_approval"](_passing_evaluation_payload())


@pytest.mark.integration
def test_admit_apply_skips_when_apply_mode_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    module, tasks, _ = load_dq_daily(
        monkeypatch, llm_mode="stub", apply_mode="off", module_name="dq_daily_skip_admit_off"
    )
    with pytest.raises(module.AirflowSkipException):
        tasks["admit_apply_task"](
            _report_payload(), _passing_evaluation_payload(), _approve_decision()
        )


@pytest.mark.integration
def test_apply_after_admission_skips_without_calling_apply_plan_when_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, tasks, _ = load_dq_daily(
        monkeypatch, llm_mode="stub", apply_mode="off", module_name="dq_daily_skip_apply_off"
    )

    def _forbid_apply(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("apply_plan must not run when APPLY_MODE=off")

    monkeypatch.setattr(module, "apply_plan", _forbid_apply)
    with pytest.raises(module.AirflowSkipException):
        tasks["apply_after_admission_task"](
            _report_payload(), _passing_evaluation_payload(), _admission_payload()
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "payload_factory", [_blocked_evaluation_payload, _failed_evaluation_payload]
)
def test_require_approval_skips_blocked_or_failed_plan_in_hitl(
    monkeypatch: pytest.MonkeyPatch, payload_factory: Callable[[], dict[str, Any]]
) -> None:
    module, tasks, _ = load_dq_daily(
        monkeypatch,
        llm_mode="stub",
        apply_mode="hitl",
        module_name=f"dq_daily_skip_approval_hitl_{payload_factory.__name__}",
    )
    with pytest.raises(module.AirflowSkipException):
        tasks["require_approval"](payload_factory())


@pytest.mark.integration
def test_admit_apply_skips_when_hitl_decision_is_not_approve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, tasks, _ = load_dq_daily(
        monkeypatch, llm_mode="stub", apply_mode="hitl", module_name="dq_daily_skip_admit_reject"
    )
    with pytest.raises(module.AirflowSkipException):
        tasks["admit_apply_task"](
            _report_payload(), _passing_evaluation_payload(), _reject_decision()
        )


@pytest.mark.integration
def test_require_approval_allows_passing_unblocked_plan_in_hitl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, tasks, _ = load_dq_daily(
        monkeypatch, llm_mode="stub", apply_mode="hitl", module_name="dq_daily_allow_approval_hitl"
    )
    tasks["require_approval"](_passing_evaluation_payload())


@pytest.mark.integration
def test_apply_after_admission_invokes_apply_plan_when_hitl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, tasks, _ = load_dq_daily(
        monkeypatch, llm_mode="stub", apply_mode="hitl", module_name="dq_daily_apply_hitl"
    )
    calls: list[str] = []

    class _Result:
        def model_dump(self, mode: str = "json") -> dict[str, str]:
            return {"status": "applied"}

    def _record_apply(*_args: Any, **_kwargs: Any) -> _Result:
        calls.append("apply_plan")
        return _Result()

    monkeypatch.setattr(module, "apply_plan", _record_apply)
    payload = tasks["apply_after_admission_task"](
        _report_payload(), _passing_evaluation_payload(), _admission_payload()
    )
    assert calls == ["apply_plan"]
    assert payload == {"status": "applied"}
