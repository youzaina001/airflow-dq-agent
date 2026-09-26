"""The bundled shadow DAG stays idle until one adopter registry is selected."""

from __future__ import annotations

import importlib.util
import sys
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from airflow_dq_agent.contracts.fingerprints import report_payload_fingerprint
from airflow_dq_agent.contracts.models import AuditEvent, QualitySuiteReport, TargetSet
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import CHECK_SPECS

REPO = Path(__file__).resolve().parents[2]
DAG_PATH = REPO / "dags" / "dq_shadow.py"
INVOICE_REGISTRY = """
tables:
  - table: invoice
    schema_name: billing
    grain: one row per invoice_id
    description: Adopter invoice.
    primary_key: [invoice_id]
    columns:
      - name: invoice_id
        dtype: int64
        unique: true
      - name: amount
        dtype: float64
        nullable: true
checks:
  - check_id: invoice.amount.completeness
    table: invoice
    column: amount
    dimension: completeness
    description: amount must be present
    policies:
      - action_id: quarantine_nulls
"""


@pytest.fixture(autouse=True)
def restore_catalogs() -> None:
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    yield
    TABLE_CONTRACTS.clear()
    TABLE_CONTRACTS.update(contracts)
    CHECK_SPECS.clear()
    CHECK_SPECS.update(checks)


def _stub_airflow(monkeypatch: pytest.MonkeyPatch) -> dict[str, Callable[..., Any]]:
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
    return tasks


def _load(
    monkeypatch: pytest.MonkeyPatch, module_name: str
) -> tuple[Any, dict[str, Callable[..., Any]]]:
    tasks = _stub_airflow(monkeypatch)
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, tasks


def test_shadow_dag_is_idle_without_a_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    assert "register_demo" not in source
    assert "create_apply_admission" not in source
    assert "AuditedApprovalOperator" not in source
    assert "prepare_plan_review" in source
    monkeypatch.delenv("REGISTRY_PATH", raising=False)
    module, tasks = _load(monkeypatch, "dq_shadow_idle")
    assert module.dq_shadow is None
    assert tasks == {}


def _invoice_report() -> QualitySuiteReport:
    raw = run_suite_on_frames(
        {"invoice": pl.DataFrame({"invoice_id": [1, 2], "amount": [10.0, None]})}
    )
    return raw.model_copy(
        update={
            "audit_event_id": "existing-root",
            "fingerprint": report_payload_fingerprint(raw),
        }
    )


def test_shadow_dag_refuses_jsonl_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(INVOICE_REGISTRY, encoding="utf-8")
    monkeypatch.setenv("REGISTRY_PATH", str(registry))
    monkeypatch.setenv("TRACE_POSTGRES", "false")
    monkeypatch.setenv("READ_DSN", "postgresql+psycopg://reader:x@localhost/warehouse")
    monkeypatch.setenv("AUDIT_DSN", "postgresql+psycopg://auditor:x@localhost/warehouse")
    with pytest.raises(RuntimeError, match="TRACE_POSTGRES=true"):
        _load(monkeypatch, "dq_shadow_jsonl")


def test_shadow_dag_prepares_one_registry_and_no_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = tmp_path / "registry.yaml"
    registry.write_text(INVOICE_REGISTRY, encoding="utf-8")
    monkeypatch.setenv("REGISTRY_PATH", str(registry))
    monkeypatch.setenv("LLM_MODE", "stub")
    monkeypatch.setenv("APPLY_MODE", "off")
    monkeypatch.setenv("TRACE_POSTGRES", "true")
    monkeypatch.setenv("READ_DSN", "postgresql+psycopg://reader:x@localhost/warehouse")
    monkeypatch.setenv("AUDIT_DSN", "postgresql+psycopg://auditor:x@localhost/warehouse")
    module, tasks = _load(monkeypatch, "dq_shadow_run")
    assert set(tasks) == {
        "run_suite_task",
        "propose_task",
        "audit_candidate_task",
        "prepare_plan_task",
    }
    events: list[AuditEvent] = []

    class Targets:
        def __init__(self, *, dsn: str) -> None:
            assert dsn.startswith("postgresql+psycopg://reader")

        def resolve(self, **_: object) -> TargetSet:
            return TargetSet(count=1, fingerprint="targets:invoice")

    monkeypatch.setattr(module, "run_quality_suite", lambda dsn: _invoice_report())
    monkeypatch.setattr(module, "append_event", events.append)
    monkeypatch.setattr(module, "PostgresTargetSetResolver", Targets)
    report = tasks["run_suite_task"]()
    proposal = tasks["propose_task"](report)
    candidate = tasks["audit_candidate_task"](report, proposal)
    prepared = tasks["prepare_plan_task"](report, candidate)
    assert "billing.invoice" in prepared["shadow_review"]
    assert "No Human Decision" in prepared["shadow_review"]
    assert "does not repair source data" in prepared["shadow_review"]
    assert [event.kind for event in events] == [
        "candidate_proposal",
        "plan_compiled",
        "evaluation",
        "approval_review",
    ]
    assert "fact_orders.total_amount.completeness" not in CHECK_SPECS
