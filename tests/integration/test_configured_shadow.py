"""Fresh-process Shadow Review of one non-default schema table on disposable Postgres."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from airflow_dq_agent.adoption import apply_governance_schema, provision_restricted_logins
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.warehouse.db import make_engine

REPO = Path(__file__).resolve().parents[2]
DAG_PATH = REPO / "dags" / "dq_shadow.py"
REGISTRY = """
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
FORBIDDEN_KINDS = {
    "human_approved",
    "human_rejected",
    "human_timed_out",
    "dry_run",
    "apply_succeeded",
    "apply_failed",
}


@pytest.fixture(autouse=True)
def restore_catalogs() -> None:
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    yield
    TABLE_CONTRACTS.clear()
    TABLE_CONTRACTS.update(contracts)
    CHECK_SPECS.clear()
    CHECK_SPECS.update(checks)


def _prepare(admin_dsn: str) -> tuple[str, str]:
    apply_governance_schema(admin_dsn)
    credentials = provision_restricted_logins(admin_dsn)
    engine = make_engine(admin_dsn)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS billing"))
        connection.execute(
            text(
                """
                CREATE TABLE billing.invoice (
                    invoice_id BIGINT PRIMARY KEY,
                    amount DOUBLE PRECISION
                )
                """
            )
        )
        connection.execute(text("TRUNCATE billing.invoice"))
        connection.execute(
            text("INSERT INTO billing.invoice (invoice_id, amount) VALUES (1, 10), (2, NULL)")
        )
        connection.execute(text("GRANT USAGE ON SCHEMA billing TO dq_read"))
        connection.execute(text("GRANT SELECT ON billing.invoice TO dq_read"))
    return credentials.read_dsn, credentials.audit_dsn


def _stub_and_load(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, dict[str, Callable[..., Any]]]:
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
    sys.modules.pop("dq_shadow_integration", None)
    spec = importlib.util.spec_from_file_location("dq_shadow_integration", DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, tasks


@pytest.mark.integration
def test_configured_shadow_reviews_one_billing_table_without_a_decision(
    warehouse_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    read_dsn, audit_dsn = _prepare(warehouse_dsn)
    registry = tmp_path / "registry.yaml"
    registry.write_text(REGISTRY, encoding="utf-8")
    env = os.environ.copy()
    env.pop("OPENAI_API_KEY", None)
    env.pop("OPENAI_BASE_URL", None)
    env.update(
        {
            "PYTHONPATH": str(REPO / "src"),
            "LLM_MODE": "stub",
            "APPLY_MODE": "off",
            "TRACE_POSTGRES": "true",
            "READ_DSN": read_dsn,
            "AUDIT_DSN": audit_dsn,
            "WAREHOUSE_DSN": "postgresql+psycopg://nope:nope@127.0.0.1:1/nope",
            "TRACES_DIR": str(tmp_path / "traces"),
        }
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "airflow_dq_agent.cli",
            "shadow",
            "--registry",
            str(registry),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 1, completed.stderr
    assert "Traceback" not in completed.stderr
    assert "Traceback" not in completed.stdout
    assert "billing.invoice" in completed.stdout
    assert "invoice.amount.completeness" in completed.stdout
    assert "Exact target count: 1" in completed.stdout
    assert "does not repair source data" in completed.stdout
    assert "No Human Decision" in completed.stdout
    assert "Audit details:" in completed.stdout
    head = completed.stdout.split("Audit details:", maxsplit=1)[0]
    assert head.index("Table:") < head.index("Exact target count:")

    for name, value in env.items():
        if name in {"PYTHONPATH", "WAREHOUSE_DSN"}:
            continue
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("REGISTRY_PATH", str(registry))
    _module, tasks = _stub_and_load(monkeypatch)
    report = tasks["run_suite_task"]()
    proposal = tasks["propose_task"](report)
    candidate = tasks["audit_candidate_task"](report, proposal)
    prepared = tasks["prepare_plan_task"](report, candidate)
    assert "billing.invoice" in prepared["shadow_review"]
    assert "Exact target count: 1" in prepared["shadow_review"]
    assert "No Human Decision" in prepared["shadow_review"]

    engine = make_engine(warehouse_dsn)
    with engine.connect() as connection:
        amount = connection.execute(
            text("SELECT amount FROM billing.invoice WHERE invoice_id = 2")
        ).scalar_one()
        quarantine = connection.execute(
            text("SELECT count(*) FROM dq.quarantine_rows")
        ).scalar_one()
        apply_rows = connection.execute(text("SELECT count(*) FROM dq.apply_log")).scalar_one()
        kinds = connection.execute(text("SELECT kind FROM dq.traces ORDER BY seq")).scalars().all()
    assert amount is None
    assert quarantine == 0
    assert apply_rows == 0
    assert kinds.count("approval_review") == 2
    assert not FORBIDDEN_KINDS.intersection(kinds)
