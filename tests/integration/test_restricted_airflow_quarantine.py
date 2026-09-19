"""Restricted-credential Airflow quarantine of one external completeness table."""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    DecisionBinding,
    EvalReport,
    ExecutablePlanItem,
    HumanDecision,
    QualitySuiteReport,
    RemediationPlan,
)
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.hitl import audit_then_complete_approval
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.traces import PostgresAuditRepository
from airflow_dq_agent.warehouse.db import make_engine

EXAMPLE_DAG = Path(__file__).resolve().parents[2] / "examples" / "dq_external_invoice.py"


@pytest.fixture(scope="module")
def required_warehouse_dsn() -> Iterator[str]:
    """PostgreSQL for #41. Fail, do not skip, when the proof cannot run."""
    configured = os.getenv("TEST_WAREHOUSE_DSN")
    if configured:
        yield configured
        return
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:16")
        container.start()
    except Exception as exc:
        pytest.fail(
            "PostgreSQL is required for Airflow rejection/timeout acceptance "
            f"(issue #41); a skip is incomplete: {exc}"
        )
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql+psycopg")
    finally:
        container.stop()


@pytest.fixture
def only_external_invoice() -> Iterator[None]:
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    TABLE_CONTRACTS.clear()
    CHECK_SPECS.clear()
    yield
    TABLE_CONTRACTS.clear()
    TABLE_CONTRACTS.update(contracts)
    CHECK_SPECS.clear()
    CHECK_SPECS.update(checks)


def _load_example_dag(
    monkeypatch: pytest.MonkeyPatch,
    *,
    warehouse_dsn: str,
    read_dsn: str,
    audit_dsn: str,
    apply_dsn: str,
) -> tuple[types.ModuleType, dict[str, Callable[..., Any]]]:
    monkeypatch.setenv("WAREHOUSE_DSN", warehouse_dsn)
    monkeypatch.setenv("READ_DSN", read_dsn)
    monkeypatch.setenv("AUDIT_DSN", audit_dsn)
    monkeypatch.setenv("APPLY_DSN", apply_dsn)
    monkeypatch.setenv("LLM_MODE", "stub")
    monkeypatch.setenv("APPLY_MODE", "hitl")
    monkeypatch.setenv("HITL_APPROVER_IDS", "airflow")
    monkeypatch.setenv("TRACE_POSTGRES", "true")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)

    exceptions_module = types.ModuleType("airflow.exceptions")

    class AirflowSkipException(Exception):
        pass

    exceptions_module.AirflowSkipException = AirflowSkipException
    sdk_module = types.ModuleType("airflow.sdk")
    tasks: dict[str, Callable[..., Any]] = {}

    class _XComRef:
        def __init__(self, name: str) -> None:
            self.__name__ = name

        def __call__(self, *_args: Any, **_kwargs: Any) -> _XComRef:
            return self

        def __rshift__(self, other: object) -> object:
            return other

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
            return _XComRef(candidate.__name__)

        return register if fn is None else register(fn)

    sdk_module.dag = _stub_dag
    sdk_module.task = _stub_task
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.exceptions", exceptions_module)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_module)

    class _FakeApproval:
        output = "approval-xcom"

        def __init__(self, **kwargs: Any) -> None:
            del kwargs

        def __rshift__(self, other: object) -> object:
            return other

    import airflow_dq_agent.airflow_hitl as hitl

    monkeypatch.setattr(hitl, "AuditedApprovalOperator", _FakeApproval)

    spec = importlib.util.spec_from_file_location("dq_external_invoice_integration", EXAMPLE_DAG)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, tasks


def _privilege_denied(engine: Any, sql: str) -> None:
    with pytest.raises(ProgrammingError), engine.begin() as connection:
        connection.execute(text(sql))


def _seed_restricted_example(warehouse_dsn: str):
    from airflow_dq_agent.adoption import (
        apply_governance_schema,
        provision_restricted_logins,
        seed_external_invoice,
    )

    apply_governance_schema(warehouse_dsn)
    credentials = provision_restricted_logins(warehouse_dsn)
    seed_external_invoice(warehouse_dsn)
    return credentials


def _evaluate_example(
    monkeypatch: pytest.MonkeyPatch, warehouse_dsn: str, credentials: Any
) -> tuple[Any, dict[str, Callable[..., Any]], dict[str, Any], dict[str, Any]]:
    module, tasks = _load_example_dag(
        monkeypatch,
        warehouse_dsn=warehouse_dsn,
        read_dsn=credentials.read_dsn,
        audit_dsn=credentials.audit_dsn,
        apply_dsn=credentials.apply_dsn,
    )
    report_payload = tasks["run_suite_task"]()
    proposal_payload = tasks["propose_task"](report_payload)
    candidate_payload = tasks["audit_candidate_task"](report_payload, proposal_payload)
    compiled_payload = tasks["compile_plan_task"](report_payload, candidate_payload)
    evaluated_payload = tasks["evaluate_plan_task"](compiled_payload)
    return module, tasks, report_payload, evaluated_payload


def _record_hitl_decision(
    *,
    provider_event: dict[str, Any],
    report: QualitySuiteReport,
    plan: RemediationPlan,
    evaluation: EvalReport,
    evaluated_payload: dict[str, Any],
    audit_dsn: str,
) -> HumanDecision:
    def persist(event: object) -> None:
        from airflow_dq_agent.contracts.models import AuditEvent
        from airflow_dq_agent.traces import append_event

        assert isinstance(event, AuditEvent)
        append_event(event, dsn=audit_dsn, mirror_postgres=True)

    return audit_then_complete_approval(
        provider_event,
        approver_ids={"airflow"},
        quality_run_id=report.run_id,
        predecessor=str(evaluated_payload["review_event_id"]),
        persist=persist,
        complete_provider=lambda: None,
        binding=DecisionBinding(
            plan_id=plan.plan_id,
            plan_fingerprint=plan.fingerprint,
            review_fingerprint=evaluated_payload["approval_review"]["fingerprint"],
            evaluation_id=evaluation.evaluation_id,
            evaluation_fingerprint=evaluation.fingerprint,
        ),
    )


def _source_amounts(connection: Any) -> dict[int, float | None]:
    return {
        int(row[0]): row[1]
        for row in connection.execute(
            text("SELECT invoice_id, amount FROM warehouse.ext_invoice ORDER BY invoice_id")
        )
    }


def _trace_kinds(connection: Any, run_id: str) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            text(
                "SELECT kind FROM dq.traces WHERE body ->> 'quality_run_id' = :run_id ORDER BY seq"
            ),
            {"run_id": run_id},
        )
    ]


def _quarantine_invoice_ids(connection: Any, run_id: str) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            text(
                "SELECT pk_json ->> 'invoice_id' FROM dq.quarantine_rows "
                "WHERE table_name = 'warehouse.ext_invoice' AND run_id = :run_id "
                "ORDER BY pk_json ->> 'invoice_id'"
            ),
            {"run_id": run_id},
        )
    ]


def _assert_seeded_source_unchanged(source: dict[int, float | None]) -> None:
    from airflow_dq_agent.adoption import COMPLETE_INVOICE_IDS, MISSING_INVOICE_IDS

    assert source[101] == 10.0
    assert source[102] is None
    assert source[103] == 20.0
    assert source[104] is None
    assert set(source) == set(COMPLETE_INVOICE_IDS + MISSING_INVOICE_IDS)


@pytest.mark.integration
def test_approved_external_invoice_quarantine_copies_authorized_keys(
    warehouse_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    only_external_invoice: None,
) -> None:
    from airflow_dq_agent.adoption import EXTERNAL_INVOICE_CHECK_ID, MISSING_INVOICE_IDS

    credentials = _seed_restricted_example(warehouse_dsn)
    owner = make_engine(warehouse_dsn)
    read = make_engine(credentials.read_dsn)
    apply = make_engine(credentials.apply_dsn)
    with read.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM warehouse.ext_invoice")).scalar_one() == 4
        )
    _privilege_denied(
        read, "INSERT INTO warehouse.ext_invoice (invoice_id, amount) VALUES (999, 1.0)"
    )
    _privilege_denied(
        read,
        "INSERT INTO dq.quarantine_rows (run_id, table_name, pk_json, reason, payload) "
        "VALUES ('x', 'warehouse.ext_invoice', '{}', 'no', '{}')",
    )
    _privilege_denied(apply, "UPDATE warehouse.ext_invoice SET amount = 0 WHERE invoice_id = 101")
    _privilege_denied(apply, "DELETE FROM warehouse.ext_invoice WHERE invoice_id = 101")
    _privilege_denied(apply, "UPDATE dq.traces SET body = '{}'::jsonb")
    _privilege_denied(apply, "DELETE FROM dq.traces")
    _privilege_denied(
        apply,
        "INSERT INTO dq.traces (trace_id, kind, body) VALUES ('forged', 'quality_report', '{}')",
    )

    module, tasks, report_payload, evaluated_payload = _evaluate_example(
        monkeypatch, warehouse_dsn, credentials
    )
    assert module.settings.llm_mode == "stub"
    assert module.settings.openai_api_key is None

    report = QualitySuiteReport.model_validate(report_payload)
    check = report.get(EXTERNAL_INVOICE_CHECK_ID)
    assert check is not None and check.failed
    assert check.n_failed == len(MISSING_INVOICE_IDS)
    assert "sample_failures" not in report_payload.get("checks", [{}])[0]

    plan = RemediationPlan.model_validate(evaluated_payload["plan"])
    evaluation = EvalReport.model_validate(evaluated_payload["evaluation"])
    assert evaluation.passed
    executable = [item for item in plan.items if isinstance(item, ExecutablePlanItem)]
    assert len(executable) == 1
    assert executable[0].action_id == "quarantine_nulls"
    assert executable[0].target_set.count == len(MISSING_INVOICE_IDS)

    decision = _record_hitl_decision(
        provider_event={
            "chosen_options": ["Approve"],
            "responded_by_user": {"id": "airflow"},
            "params_input": {
                "approval_note": "Copy the authorized missing-amount invoices into quarantine."
            },
            "timedout": False,
        },
        report=report,
        plan=plan,
        evaluation=evaluation,
        evaluated_payload=evaluated_payload,
        audit_dsn=credentials.audit_dsn,
    )
    assert decision.decision == "Approve"

    admission_payload = tasks["admit_apply_task"](
        report_payload, evaluated_payload, decision.model_dump(mode="json")
    )
    admission = ApplyAdmission.model_validate(admission_payload)
    result = tasks["apply_after_admission_task"](
        report_payload, evaluated_payload, admission_payload
    )
    assert result["dry_run"] is False
    assert result["audit_event_id"]

    with owner.connect() as connection:
        copied = _quarantine_invoice_ids(connection, result["run_id"])
        source = _source_amounts(connection)
        kinds = _trace_kinds(connection, report.run_id)
    assert copied == [str(key) for key in MISSING_INVOICE_IDS]
    assert set(int(key) for key in copied) == set(MISSING_INVOICE_IDS)
    _assert_seeded_source_unchanged(source)
    assert "quality_report" in kinds
    assert "plan_compiled" in kinds
    assert "evaluation" in kinds
    assert "human_approved" in kinds
    assert "apply_succeeded" in kinds
    audit = PostgresAuditRepository(credentials.audit_dsn)
    assert audit.get(report.audit_event_id or "") is not None
    assert audit.get(admission.decision_event_id) is not None
    assert audit.get(result["audit_event_id"]) is not None


@pytest.mark.integration
def test_rejected_external_invoice_writes_zero_quarantine_rows(
    required_warehouse_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    only_external_invoice: None,
) -> None:
    from airflow_dq_agent.adoption import EXTERNAL_INVOICE_CHECK_ID

    credentials = _seed_restricted_example(required_warehouse_dsn)
    owner = make_engine(required_warehouse_dsn)
    module, tasks, report_payload, evaluated_payload = _evaluate_example(
        monkeypatch, required_warehouse_dsn, credentials
    )
    assert module.settings.llm_mode == "stub"
    assert module.settings.openai_api_key is None

    report = QualitySuiteReport.model_validate(report_payload)
    plan = RemediationPlan.model_validate(evaluated_payload["plan"])
    evaluation = EvalReport.model_validate(evaluated_payload["evaluation"])
    assert report.get(EXTERNAL_INVOICE_CHECK_ID) is not None
    assert evaluation.passed

    decision = _record_hitl_decision(
        provider_event={
            "chosen_options": ["Reject"],
            "responded_by_user": {"id": "airflow"},
            "params_input": {"approval_note": "Do not copy invoices into quarantine."},
            "timedout": False,
        },
        report=report,
        plan=plan,
        evaluation=evaluation,
        evaluated_payload=evaluated_payload,
        audit_dsn=credentials.audit_dsn,
    )
    assert decision.decision == "Reject"

    with pytest.raises(module.AirflowSkipException, match="did not approve"):
        tasks["admit_apply_task"](
            report_payload, evaluated_payload, decision.model_dump(mode="json")
        )

    with owner.connect() as connection:
        copied = _quarantine_invoice_ids(connection, report.run_id)
        source = _source_amounts(connection)
        kinds = _trace_kinds(connection, report.run_id)
    assert copied == []
    _assert_seeded_source_unchanged(source)
    assert "human_rejected" in kinds
    assert "human_approved" not in kinds
    assert "human_timed_out" not in kinds
    assert "apply_succeeded" not in kinds
    audit = PostgresAuditRepository(credentials.audit_dsn)
    recorded = audit.get(decision.audit_event_id or "")
    assert recorded is not None
    assert recorded.kind == "human_rejected"


@pytest.mark.integration
def test_timed_out_external_invoice_writes_zero_quarantine_rows(
    required_warehouse_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    only_external_invoice: None,
) -> None:
    credentials = _seed_restricted_example(required_warehouse_dsn)
    owner = make_engine(required_warehouse_dsn)
    module, tasks, report_payload, evaluated_payload = _evaluate_example(
        monkeypatch, required_warehouse_dsn, credentials
    )
    assert module.settings.llm_mode == "stub"
    assert module.settings.openai_api_key is None

    report = QualitySuiteReport.model_validate(report_payload)
    plan = RemediationPlan.model_validate(evaluated_payload["plan"])
    evaluation = EvalReport.model_validate(evaluated_payload["evaluation"])

    decision = _record_hitl_decision(
        provider_event={
            "responded_by_user": None,
            "params_input": {},
            "timedout": True,
        },
        report=report,
        plan=plan,
        evaluation=evaluation,
        evaluated_payload=evaluated_payload,
        audit_dsn=credentials.audit_dsn,
    )
    assert decision.decision == "Timeout"
    assert decision.actor == "airflow-timeout"

    with pytest.raises(module.AirflowSkipException, match="did not approve"):
        tasks["admit_apply_task"](
            report_payload, evaluated_payload, decision.model_dump(mode="json")
        )

    with owner.connect() as connection:
        copied = _quarantine_invoice_ids(connection, report.run_id)
        source = _source_amounts(connection)
        kinds = _trace_kinds(connection, report.run_id)
        timeout_actors = [
            row[0]
            for row in connection.execute(
                text(
                    "SELECT body ->> 'decision_actor' FROM dq.traces "
                    "WHERE body ->> 'quality_run_id' = :run_id AND kind = 'human_timed_out'"
                ),
                {"run_id": report.run_id},
            )
        ]
    assert copied == []
    _assert_seeded_source_unchanged(source)
    assert "human_timed_out" in kinds
    assert "human_approved" not in kinds
    assert "human_rejected" not in kinds
    assert "apply_succeeded" not in kinds
    assert timeout_actors == ["airflow-timeout"]
    audit = PostgresAuditRepository(credentials.audit_dsn)
    recorded = audit.get(decision.audit_event_id or "")
    assert recorded is not None
    assert recorded.kind == "human_timed_out"
