"""DAG-level proof: no XCom payload carries row samples and governance still works.

The DAG file is loaded with a minimal Airflow stub so the real task bodies can be
executed without an Airflow install. Calling a recorded task function returns the
value Airflow would store as that task's XCom payload.
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
    EvalReport,
    ExecutablePlanItem,
    Proposal,
    QualitySuiteReport,
    RemediationPlan,
)
from airflow_dq_agent.quality import run_quality_suite
from airflow_dq_agent.warehouse.defects import EXPECTED_DEFECTS
from airflow_dq_agent.warehouse.seed import seed_warehouse

DAG_PATH = Path(__file__).resolve().parents[2] / "dags" / "dq_daily.py"

# Raw seeded row values that must never surface in any XCom payload.
SEALED_ROW_VALUES: tuple[object, ...] = (
    "SHIPPPED",
    "SUBJ-DUPE",
    "lethal",
    999001,
    999501,
    "c101.invalid",
    "c102.invalid",
)


def _assert_payload_is_sample_free(node: object) -> None:
    if isinstance(node, dict):
        assert "sample_failures" not in node
        for value in node.values():
            _assert_payload_is_sample_free(value)
    elif isinstance(node, list):
        for item in node:
            _assert_payload_is_sample_free(item)
    else:
        assert node not in SEALED_ROW_VALUES


@pytest.fixture()
def dag_tasks(monkeypatch: pytest.MonkeyPatch, warehouse_dsn: str) -> dict[str, Callable[..., Any]]:
    """Load dags/dq_daily.py against the throwaway warehouse with a stubbed airflow."""
    monkeypatch.setenv("WAREHOUSE_DSN", warehouse_dsn)
    monkeypatch.delenv("READ_DSN", raising=False)
    monkeypatch.delenv("AUDIT_DSN", raising=False)
    monkeypatch.delenv("APPLY_DSN", raising=False)
    monkeypatch.delenv("TRACE_POSTGRES", raising=False)
    monkeypatch.setenv("LLM_MODE", "stub")
    monkeypatch.setenv("APPLY_MODE", "off")

    exceptions_module = types.ModuleType("airflow.exceptions")

    class AirflowSkipException(Exception): ...

    exceptions_module.AirflowSkipException = AirflowSkipException
    sdk_module = types.ModuleType("airflow.sdk")
    tasks: dict[str, Callable[..., Any]] = {}

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

            def xcom_reference(*_call_args: Any, **_call_kwargs: Any) -> None:
                # A TaskFlow call at DAG-definition time wires an XCom reference;
                # the recorded function is what a task run executes.
                return None

            xcom_reference.__name__ = candidate.__name__
            return xcom_reference

        # Support both the bare @task form and the @task(...) form.
        return register if fn is None else register(fn)

    sdk_module.dag = _stub_dag
    sdk_module.task = _stub_task

    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.exceptions", exceptions_module)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_module)

    spec = importlib.util.spec_from_file_location("dq_daily_under_test", DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert "run_suite_task" in tasks
    return tasks


@pytest.mark.integration
def test_dag_xcom_payloads_are_sample_free_and_governance_survives(
    dag_tasks: dict[str, Callable[..., Any]], warehouse_dsn: str
) -> None:
    seed_warehouse(warehouse_dsn)
    direct = run_quality_suite(warehouse_dsn)
    failing = [check for check in direct.checks if check.failed]
    assert failing
    assert any(check.sample_failures for check in failing)

    report_payload = dag_tasks["run_suite_task"]()
    proposal_payload = dag_tasks["propose_stub_task"](report_payload)
    candidate_payload = dag_tasks["audit_candidate_task"](report_payload, proposal_payload)
    compiled_payload = dag_tasks["compile_plan_task"](report_payload, candidate_payload)
    evaluated_payload = dag_tasks["evaluate_plan_task"](compiled_payload)

    for payload in (
        report_payload,
        proposal_payload,
        candidate_payload,
        compiled_payload,
        evaluated_payload,
    ):
        _assert_payload_is_sample_free(payload)

    revived = QualitySuiteReport.model_validate(report_payload)
    assert revived.audit_event_id is not None
    assert revived.observed_columns
    direct_by_id = {check.check_id: check for check in direct.checks}
    for check in revived.checks:
        assert check.n_failed == direct_by_id[check.check_id].n_failed
        assert check.n_total == direct_by_id[check.check_id].n_total
    for check_id, defect in EXPECTED_DEFECTS.items():
        check = revived.get(check_id)
        assert check is not None and check.n_failed == defect.n_rows

    proposal = Proposal.model_validate(proposal_payload)
    assert proposal.candidate_actions
    assert EvalReport.model_validate(candidate_payload["candidate_evaluation"]).passed

    plan = RemediationPlan.model_validate(compiled_payload["plan"])
    assert plan.items
    executable = [item for item in plan.items if isinstance(item, ExecutablePlanItem)]
    assert executable == plan.items
    assert all(item.target_set.count >= 0 for item in executable)
    assert sum(item.target_set.count for item in executable) > 0

    evaluation = EvalReport.model_validate(evaluated_payload["evaluation"])
    assert evaluation.passed
    assert evaluated_payload["plan_event_id"] == compiled_payload["plan_event_id"]
