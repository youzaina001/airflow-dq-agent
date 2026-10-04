"""Adopter example: one external table, completeness Check Policy, Airflow apply path."""

from __future__ import annotations

import importlib.util
import inspect
import sys
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from airflow_dq_agent.contracts.models import (
    Dimension,
    ExecutablePlanItem,
    QualityEvidence,
    TargetSet,
)
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.planning.integrity import warehouse_environment_id
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import CHECK_SPECS

REPO = Path(__file__).resolve().parents[2]
EXAMPLE_DAG = REPO / "examples" / "dq_external_invoice.py"
README = REPO / "README.md"
DDL = REPO / "src" / "airflow_dq_agent" / "demo" / "ddl.sql"


@pytest.fixture(autouse=True)
def restore_registries() -> None:
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    yield
    TABLE_CONTRACTS.clear()
    TABLE_CONTRACTS.update(contracts)
    CHECK_SPECS.clear()
    CHECK_SPECS.update(checks)


@pytest.fixture
def only_external_invoice() -> None:
    TABLE_CONTRACTS.clear()
    CHECK_SPECS.clear()
    from airflow_dq_agent.adoption import register_external_invoice

    register_external_invoice()
    yield


def test_register_external_invoice_is_single_pk_completeness_quarantine(
    only_external_invoice: None,
) -> None:
    from airflow_dq_agent.adoption import (
        EXTERNAL_INVOICE_CHECK_ID,
        EXTERNAL_INVOICE_TABLE,
        MISSING_INVOICE_IDS,
    )
    from airflow_dq_agent.contracts.tables import get_table_contract
    from airflow_dq_agent.quality.registry import get_check_spec

    contract = get_table_contract(EXTERNAL_INVOICE_TABLE)
    assert contract.primary_key == ["invoice_id"]
    assert len(contract.primary_key) == 1
    spec = get_check_spec(EXTERNAL_INVOICE_CHECK_ID)
    assert spec.table == EXTERNAL_INVOICE_TABLE
    assert spec.column == "amount"
    assert spec.dimension is Dimension.COMPLETENESS
    assert spec.rule_for("quarantine_nulls") is not None
    assert "fact_orders" not in TABLE_CONTRACTS
    assert MISSING_INVOICE_IDS == (102, 104)

    report = run_suite_on_frames(
        {
            EXTERNAL_INVOICE_TABLE: pl.DataFrame(
                {
                    "invoice_id": [101, 102, 103, 104],
                    "amount": [10.0, None, 20.0, None],
                }
            )
        }
    )
    check = report.get(EXTERNAL_INVOICE_CHECK_ID)
    assert check is not None
    assert check.failed
    assert check.n_failed == 2
    assert {row["invoice_id"] for row in check.sample_failures} == {102, 104}


def test_restricted_login_dsn_keeps_warehouse_identity_without_secrets() -> None:
    from airflow_dq_agent.adoption import login_dsn

    admin = "postgresql+psycopg://dq:owner-secret@db.example:5432/warehouse"
    read = login_dsn(admin, user="dq_read_login", password="read-secret")
    apply = login_dsn(admin, user="dq_apply_login", password="apply-secret")

    assert "dq_read_login" in read
    assert "read-secret" in read
    assert "owner-secret" not in read
    assert warehouse_environment_id(read) == warehouse_environment_id(apply)
    assert warehouse_environment_id(read) == "db.example:5432/warehouse"
    assert "secret" not in warehouse_environment_id(read)


def test_apply_target_recheck_does_not_require_row_lock_privilege() -> None:
    item = ExecutablePlanItem(
        item_id="lock-share",
        action_id="quarantine_nulls",
        table="fact_orders",
        params={"column": "total_amount", "pk_column": "order_id"},
        evidence=(
            QualityEvidence(
                check_id="fact_orders.total_amount.completeness",
                contract_id="warehouse.fact_orders",
            ),
        ),
        target_set=TargetSet(count=1, fingerprint="targets:lock"),
        policy_fingerprint="policy:lock",
    )
    recorded: list[str] = []

    class _Connection:
        def execute(self, statement: object, params: object = None) -> list[tuple[int]]:
            del params
            recorded.append(str(statement))
            return []

    PostgresTargetSetResolver(
        dsn="postgresql+psycopg://dq_apply_login:x@localhost:5433/warehouse"
    ).lock_and_resolve(_Connection(), item)  # type: ignore[arg-type]

    assert recorded
    sql = recorded[0].upper()
    # PostgreSQL requires UPDATE to take FOR SHARE/FOR UPDATE; dq_apply is SELECT-only.
    assert "FOR SHARE" not in sql
    assert "FOR UPDATE" not in sql


def test_governance_ddl_keeps_apply_from_writing_source_or_audit() -> None:
    ddl = DDL.read_text(encoding="utf-8")
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO dq_read, dq_apply" in ddl
    assert "GRANT UPDATE ON ALL TABLES IN SCHEMA warehouse" not in ddl
    assert "GRANT INSERT ON dq.quarantine_rows TO dq_apply" in ddl
    assert "REVOKE ALL ON dq.traces, dq.check_runs, dq.apply_log FROM dq_apply" in ddl
    assert "REVOKE UPDATE, DELETE ON dq.traces, dq.check_runs FROM dq_audit" in ddl


def _load_example_dag(
    monkeypatch: pytest.MonkeyPatch,
    *,
    apply_mode: str = "hitl",
    llm_mode: str = "stub",
    module_name: str = "dq_external_invoice_under_test",
) -> tuple[types.ModuleType, dict[str, Callable[..., Any]], list[dict[str, Any]]]:
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql+psycopg://dq:dq@localhost:5433/warehouse")
    monkeypatch.setenv(
        "READ_DSN", "postgresql+psycopg://dq_read_login:read@localhost:5433/warehouse"
    )
    monkeypatch.setenv(
        "AUDIT_DSN", "postgresql+psycopg://dq_audit_login:audit@localhost:5433/warehouse"
    )
    monkeypatch.setenv(
        "APPLY_DSN", "postgresql+psycopg://dq_apply_login:apply@localhost:5433/warehouse"
    )
    monkeypatch.setenv("LLM_MODE", llm_mode)
    monkeypatch.setenv("APPLY_MODE", apply_mode)
    monkeypatch.setenv("HITL_APPROVER_IDS", "airflow")
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

    operator_kwargs: list[dict[str, Any]] = []

    class _FakeApproval:
        output = "approval-xcom"

        def __init__(self, **kwargs: Any) -> None:
            operator_kwargs.append(kwargs)

        def __rshift__(self, other: object) -> object:
            return other

    import airflow_dq_agent.airflow_hitl as hitl

    monkeypatch.setattr(hitl, "AuditedApprovalOperator", _FakeApproval)

    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, EXAMPLE_DAG)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, tasks, operator_kwargs


def test_example_dag_registers_external_invoice_not_demo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = EXAMPLE_DAG.read_text(encoding="utf-8")
    assert "register_external_invoice()" in source
    assert "from airflow_dq_agent.demo import register_demo" not in source
    assert "LLM_MODE" not in source or "stub" in source
    module, tasks, operator_kwargs = _load_example_dag(monkeypatch)
    assert module.settings.llm_mode == "stub"
    assert module.settings.apply_mode == "hitl"
    assert module.settings.openai_api_key is None
    assert "run_suite_task" in tasks
    assert "apply_after_admission_task" in tasks
    assert operator_kwargs
    assert operator_kwargs[0]["audit_dsn"] == module.settings.audit_dsn
    assert "dq_audit_login" in str(operator_kwargs[0]["audit_dsn"])


def test_example_dag_binds_restricted_read_audit_and_apply_dsns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _module, tasks, operator_kwargs = _load_example_dag(monkeypatch)
    suite_src = inspect.getsource(tasks["run_suite_task"])
    admit_src = inspect.getsource(tasks["admit_apply_task"])
    apply_src = inspect.getsource(tasks["apply_after_admission_task"])
    assert "read_dsn" in suite_src
    assert "audit_dsn" in admit_src
    assert "apply_dsn" in apply_src
    assert "dry_run=False" in apply_src
    assert operator_kwargs[0]["audit_dsn"] is not None


def test_example_dag_hitl_timeout_uses_execution_timeout_compatible_with_provider_1_12_1() -> None:
    """Pinned apache-airflow-providers-standard==1.12.1 has no response_timeout."""
    source = EXAMPLE_DAG.read_text(encoding="utf-8")
    assert "response_timeout=" not in source
    assert "execution_timeout=" in source
    assert "DQ_HITL_TIMEOUT_SECONDS" in source


_EXAMPLE_TASK_IDS = frozenset(
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


def _example_task_ids(
    tasks: dict[str, Callable[..., Any]], operator_kwargs: list[dict[str, Any]]
) -> set[str]:
    return set(tasks) | {str(item["task_id"]) for item in operator_kwargs}


@pytest.mark.parametrize("llm_mode", ("stub", "replay", "live"))
@pytest.mark.parametrize("apply_mode", ("off", "hitl"))
def test_example_dag_task_ids_stay_stable_across_modes(
    monkeypatch: pytest.MonkeyPatch, llm_mode: str, apply_mode: str
) -> None:
    _module, tasks, operator_kwargs = _load_example_dag(
        monkeypatch,
        apply_mode=apply_mode,
        llm_mode=llm_mode,
        module_name=f"invoice_topology_{llm_mode}_{apply_mode}",
    )
    assert _example_task_ids(tasks, operator_kwargs) == _EXAMPLE_TASK_IDS


def test_example_apply_skips_before_mutation_when_apply_mode_is_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime, timedelta

    from airflow_dq_agent.contracts.models import (
        ApplyAdmission,
        CheckResult,
        CheckStatus,
        EvalReport,
        ExecutablePlanItem,
        HumanDecision,
        QualityEvidence,
        QualitySuiteReport,
        RemediationPlan,
    )

    module, tasks, _operator_kwargs = _load_example_dag(
        monkeypatch, apply_mode="off", module_name="invoice_skip_off"
    )
    plan = RemediationPlan(
        quality_run_id="run-1",
        candidate_fingerprint="cand-1",
        policy_fingerprint="pol-1",
        items=(
            ExecutablePlanItem(
                item_id="item-1",
                action_id="quarantine_nulls",
                table="external_invoice",
                evidence=(QualityEvidence(check_id="c1", contract_id="t1"),),
                target_set=TargetSet(count=1, fingerprint="ts-1"),
                policy_fingerprint="pol-1",
            ),
        ),
        blocked=False,
        warehouse_environment_id="localhost:5433/warehouse",
        fingerprint="plan-fp-1",
    )
    evaluation = EvalReport(
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        passed=True,
        scores=[],
        fingerprint="eval-fp-1",
    )
    report = QualitySuiteReport(
        run_id="run-1",
        checks=[
            CheckResult(
                check_id="c1",
                table="external_invoice",
                dimension=Dimension.COMPLETENESS,
                status=CheckStatus.FAIL,
                message="amount is null",
                contract_id="t1",
            )
        ],
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    admission = ApplyAdmission(
        quality_run_id="run-1",
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint="eval-fp-1",
        decision_id="dec-1",
        decision_event_id="decision-event-1",
        policy_fingerprint="pol-1",
        warehouse_environment_id="localhost:5433/warehouse",
        issued_at=now,
        expires_at=now + timedelta(hours=24),
        fingerprint="adm-fp-1",
    )
    evaluation_payload = {
        "plan": plan.model_dump(mode="json"),
        "evaluation": evaluation.model_dump(mode="json"),
    }
    decision = HumanDecision(
        decision="Approve",
        actor="airflow",
        note="Reviewed the whole plan.",
        audit_event_id="decision-event-1",
    ).model_dump(mode="json")

    def _forbid_apply(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("apply_plan must not run when APPLY_MODE=off")

    monkeypatch.setattr(module, "apply_plan", _forbid_apply)
    with pytest.raises(module.AirflowSkipException):
        tasks["require_approval"](evaluation_payload)
    with pytest.raises(module.AirflowSkipException):
        tasks["admit_apply_task"](report.model_dump(mode="json"), evaluation_payload, decision)
    with pytest.raises(module.AirflowSkipException):
        tasks["apply_after_admission_task"](
            report.model_dump(mode="json"),
            evaluation_payload,
            admission.model_dump(mode="json"),
        )


def test_example_dag_skips_apply_on_reject_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _module, tasks, _kwargs = _load_example_dag(monkeypatch)
    admit_src = inspect.getsource(tasks["admit_apply_task"])
    assert 'if parsed_decision.decision in {"Reject", "Timeout"}:' in admit_src
    assert "AirflowSkipException" in admit_src


def test_documentation_describes_quarantine_copy_not_source_repair() -> None:
    readme = README.read_text(encoding="utf-8")
    dag = EXAMPLE_DAG.read_text(encoding="utf-8")
    combined = f"{readme}\n{dag}"
    assert "examples/dq_external_invoice.py" in readme
    assert "register_external_invoice" in combined
    assert "quarantine cop" in combined.lower()
    assert "source" in combined.lower() and "unchanged" in combined.lower()
    assert "READ_DSN" in readme and "AUDIT_DSN" in readme and "APPLY_DSN" in readme
    assert "APPLY_MODE=hitl" in readme
    assert "LLM_MODE=stub" in readme
    assert "repair" in combined.lower()


def test_documentation_describes_how_to_exercise_rejection_and_timeout() -> None:
    readme = README.read_text(encoding="utf-8")
    assert "Rejection and timeout are out of scope" not in readme
    assert "human_rejected" in readme
    assert "human_timed_out" in readme
    assert "airflow-timeout" in readme
    assert "dq.quarantine_rows" in readme
    assert "Reject" in readme
    assert "Timeout" in readme or "timeout" in readme
    assert (REPO / "scripts" / "compose-hitl-reject-timeout.sh").is_file()


def test_documentation_describes_airflow_crash_retry_of_committed_quarantine() -> None:
    readme = README.read_text(encoding="utf-8")
    dag = EXAMPLE_DAG.read_text(encoding="utf-8")
    assert "examples/dq_external_invoice.py" in readme
    assert "compose-hitl-crash-retry.sh" in readme
    assert "apply_succeeded" in readme
    assert "lost" in readme.lower() or "crash" in readme.lower()
    assert "retry" in readme.lower()
    assert "102" in readme and "104" in readme
    assert (REPO / "scripts" / "compose-hitl-crash-retry.sh").is_file()
    decorator_block = dag[: dag.index("def apply_after_admission_task")]
    apply_decorator = decorator_block[decorator_block.rfind("@task") :]
    assert "retries=" in apply_decorator
    assert "retry_delay" in apply_decorator
    assert "DQ_COMPOSE_CRASH" not in dag
    executor = (REPO / "src" / "airflow_dq_agent" / "apply" / "executor.py").read_text(
        encoding="utf-8"
    )
    assert "DQ_COMPOSE_CRASH" not in executor
    assert "SIGKILL" not in executor


def test_readme_uses_human_decision_and_apply_admission_not_approval_aliases() -> None:
    readme = README.read_text(encoding="utf-8")
    lowered = readme.lower()
    assert "audited approval" not in lowered
    assert "approval configuration" not in lowered
    assert "consumed-admission failure" not in lowered
    assert "Human Decision" in readme
    assert "Apply Admission" in readme


def test_crash_retry_proof_binds_admission_xcom_and_apply_log_identity() -> None:
    script = (REPO / "scripts" / "compose-hitl-crash-retry.sh").read_text(encoding="utf-8")
    assert "admit_apply_task" in script
    assert "apply_after_admission_task" in script
    assert "xcomEntries/return_value" in script
    assert "admission_id" in script
    assert "apply_result_id" in script
    assert "-ge 1" not in script
    assert "FROM dq.apply_log WHERE admission_id" in script
    assert "body ->> 'apply_result_id'" in script


@pytest.mark.parametrize("dag_path", [EXAMPLE_DAG, REPO / "dags" / "dq_daily.py"])
@pytest.mark.parametrize("outcome", ["passing", "blocked", "candidate_failed", "audit_failed"])
def test_dag_preparation_preserves_lineage_privacy_and_approval_gate(
    monkeypatch: pytest.MonkeyPatch,
    dag_path: Path,
    outcome: str,
) -> None:
    import json

    from airflow_dq_agent.agent import AgentRun, run_proposal_agent
    from airflow_dq_agent.contracts.fingerprints import report_payload_fingerprint
    from airflow_dq_agent.contracts.models import AuditEvent
    from airflow_dq_agent.demo import seeded_failure_report
    from airflow_dq_agent.quality import sample_free_report

    monkeypatch.setattr(sys.modules[__name__], "EXAMPLE_DAG", dag_path)
    module, tasks, operator_kwargs = _load_example_dag(monkeypatch)
    raw = seeded_failure_report()
    report = raw.model_copy(
        update={"audit_event_id": "existing-root", "fingerprint": report_payload_fingerprint(raw)}
    )
    echoed = run_proposal_agent(report).proposal.model_copy(
        update={
            "proposal_id": "secret-row",
            "fingerprint": "secret-row",
            "summary": "secret-row",
            "root_cause_hypothesis": "secret-row",
            "do_not_apply_reasons": ["secret-row"],
        }
    )
    monkeypatch.setattr(
        module,
        "run_proposal_agent",
        lambda _: AgentRun(proposal=echoed, prompt="secret-row", tool_calls=[], llm_mode="live"),
    )
    events: list[AuditEvent] = []

    def persist(event: AuditEvent) -> None:
        if outcome == "audit_failed" and event.kind == "approval_review":
            raise RuntimeError("audit unavailable")
        events.append(event)

    class Targets:
        def __init__(self, *, engine: object) -> None:
            assert engine == module.settings.read_dsn or engine == module.settings.warehouse_dsn

        def resolve(self, **_: object) -> TargetSet:
            if outcome == "blocked":
                raise ValueError("secret-row")
            return TargetSet(count=5, fingerprint="targets:dag")

    monkeypatch.setattr(module, "append_event", persist)
    monkeypatch.setattr(module, "make_engine", lambda dsn: dsn)
    monkeypatch.setattr(module, "PostgresTargetSetResolver", Targets)
    report_payload = sample_free_report(report)
    proposal = tasks["propose_task"](report_payload)
    candidate = tasks["audit_candidate_task"](report_payload, proposal)
    if outcome == "candidate_failed":
        candidate["candidate_evaluation"]["passed"] = False
        with pytest.raises(
            module.AirflowSkipException, match="Candidate Proposal evaluation failed"
        ):
            tasks["prepare_plan_task"](report_payload, candidate)
        assert len(events) == 1
        return
    if outcome == "audit_failed":
        with pytest.raises(RuntimeError, match="audit unavailable"):
            tasks["prepare_plan_task"](report_payload, candidate)
        assert [event.kind for event in events] == [
            "candidate_proposal",
            "plan_compiled",
            "evaluation",
        ]
        return
    prepared = tasks["prepare_plan_task"](report_payload, candidate)
    assert [event.kind for event in events] == [
        "candidate_proposal",
        "plan_blocked" if outcome == "blocked" else "plan_compiled",
        "evaluation",
        "approval_review",
    ]
    assert events[0].predecessor_ids == [report.audit_event_id]
    assert all(
        event.predecessor_ids == [events[i - 1].event_id] for i, event in enumerate(events) if i
    )
    assert prepared["approval_review"] == events[-1].review_payload
    assert prepared["plan"]["candidate_fingerprint"] == events[0].candidate_fingerprint
    body = json.dumps([proposal, candidate, prepared, [e.model_dump(mode="json") for e in events]])
    assert "secret-row" not in body
    assert "sample_failures" not in body
    assert "prepare_plan_task" in operator_kwargs[0]["body"]
    if outcome == "blocked":
        with pytest.raises(module.AirflowSkipException, match="No passing executable"):
            tasks["require_approval"](prepared)
    else:
        tasks["require_approval"](prepared)
