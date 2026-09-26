"""Configured Shadow Review: one adopter registry, no demo catalog, no decision."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from airflow_dq_agent.cli import build_parser, main
from airflow_dq_agent.contracts.fingerprints import report_payload_fingerprint
from airflow_dq_agent.contracts.models import AuditEvent, CheckStatus, QualitySuiteReport, TargetSet
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.demo import seeded_failure_report
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.warehouse.db import READ_CONNECTION_FAILED

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


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "registry.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def _configure(monkeypatch: pytest.MonkeyPatch, registry: Path) -> None:
    monkeypatch.setenv("LLM_MODE", "stub")
    monkeypatch.setenv("APPLY_MODE", "off")
    monkeypatch.setenv("TRACE_POSTGRES", "true")
    monkeypatch.setenv("READ_DSN", "postgresql+psycopg://reader:x@localhost/warehouse")
    monkeypatch.setenv("AUDIT_DSN", "postgresql+psycopg://auditor:x@localhost/warehouse")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)


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


def test_shadow_help_names_the_registry_selector() -> None:
    help_text = build_parser().format_help()
    assert "--registry" in help_text
    assert "TRACE_POSTGRES" in help_text


def test_readme_states_shadow_registry_audit_and_quarantine_copy() -> None:
    text = Path(__file__).resolve().parents[2].joinpath("README.md").read_text(encoding="utf-8")
    assert "shadow --registry" in text
    assert "TRACE_POSTGRES=true" in text
    assert "distinct" in text
    assert "does not repair source data" in text
    assert "No Human Decision" in text or "no Human Decision" in text


def test_invalid_registry_does_not_review(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(tmp_path, ":\n")
    assert main(["shadow", "--registry", str(path)]) == 2
    output = capsys.readouterr().out
    assert "registry:" in output
    assert "do not review a remediation plan" in output
    assert "Traceback" not in output


def test_unknown_action_is_an_unsupported_contract(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(tmp_path, INVOICE_REGISTRY.replace("quarantine_nulls", "drop_table"))
    assert main(["shadow", "--registry", str(path)]) == 2
    output = capsys.readouterr().out
    assert "drop_table" in output
    assert "do not review a remediation plan" in output
    assert "Traceback" not in output


def test_jsonl_only_is_not_a_completed_shadow_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(tmp_path, INVOICE_REGISTRY)
    _configure(monkeypatch, path)
    monkeypatch.setenv("TRACE_POSTGRES", "false")
    assert main(["shadow", "--registry", str(path)]) == 2
    output = capsys.readouterr().out
    assert "TRACE_POSTGRES=true" in output
    assert "do not review a remediation plan" in output
    assert "fact_orders.total_amount.completeness" not in CHECK_SPECS


def test_read_and_audit_logins_must_be_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(tmp_path, INVOICE_REGISTRY)
    _configure(monkeypatch, path)
    monkeypatch.setenv("AUDIT_DSN", "postgresql+psycopg://reader:other@localhost/warehouse")
    assert main(["shadow", "--registry", str(path)]) == 2
    assert "distinct read and audit logins" in capsys.readouterr().out


def test_empty_suite_stops_before_a_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(tmp_path, "tables: []\nchecks: []\n")
    _configure(monkeypatch, path)

    def fail_suite(_dsn: str) -> QualitySuiteReport:
        raise AssertionError("empty suite must not open a read connection")

    monkeypatch.setattr("airflow_dq_agent.cli.run_quality_suite", fail_suite)
    assert main(["shadow", "--registry", str(path)]) == 2
    output = capsys.readouterr().out
    assert "no checks ran" in output
    assert "do not review a remediation plan" in output


def test_unavailable_read_connection_names_the_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(tmp_path, INVOICE_REGISTRY)
    _configure(monkeypatch, path)

    def fail_suite(_dsn: str) -> QualitySuiteReport:
        raise RuntimeError(READ_CONNECTION_FAILED)

    monkeypatch.setattr("airflow_dq_agent.cli.run_quality_suite", fail_suite)
    assert main(["shadow", "--registry", str(path)]) == 2
    output = capsys.readouterr().out
    assert READ_CONNECTION_FAILED in output
    assert "do not review a remediation plan" in output
    assert "Traceback" not in output


def test_check_errors_do_not_prepare_a_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(tmp_path, INVOICE_REGISTRY)
    _configure(monkeypatch, path)
    failed = seeded_failure_report().checks[0]
    errored = failed.model_copy(
        update={"status": CheckStatus.ERROR, "n_failed": 0, "sample_failures": []}
    )
    report = QualitySuiteReport(checks=[errored], observed_columns={})
    monkeypatch.setattr("airflow_dq_agent.cli.run_quality_suite", lambda _dsn: report)
    assert main(["shadow", "--registry", str(path)]) == 2
    output = capsys.readouterr().out
    assert "incomplete" in output
    assert "Proposed effect:" not in output
    assert "Traceback" not in output


def test_composite_primary_key_is_refused_before_review(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    body = INVOICE_REGISTRY.replace(
        "primary_key: [invoice_id]", "primary_key: [invoice_id, amount]"
    )
    path = _write(tmp_path, body)
    assert main(["shadow", "--registry", str(path)]) == 2
    output = capsys.readouterr().out
    assert "does not have a single-column primary key" in output
    assert "do not review a remediation plan" in output
    assert "Traceback" not in output


def test_passing_shadow_review_leads_with_the_adopter_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _write(tmp_path, INVOICE_REGISTRY)
    _configure(monkeypatch, path)
    events: list[AuditEvent] = []

    def suite(dsn: str) -> QualitySuiteReport:
        assert dsn.startswith("postgresql+psycopg://reader")
        return _invoice_report()

    class Targets:
        def __init__(self, *, dsn: str) -> None:
            assert dsn.startswith("postgresql+psycopg://reader")

        def resolve(self, **_: object) -> TargetSet:
            return TargetSet(count=1, fingerprint="targets:invoice")

    monkeypatch.setattr("airflow_dq_agent.cli.run_quality_suite", suite)
    monkeypatch.setattr("airflow_dq_agent.cli.append_event", events.append)
    monkeypatch.setattr("airflow_dq_agent.cli.PostgresTargetSetResolver", Targets)
    assert main(["shadow", "--registry", str(path)]) == 1
    output = capsys.readouterr().out
    assert "Traceback" not in output
    assert "10.0" not in output
    assert "sample_failures" not in output
    head, audit = output.split("Audit details:", maxsplit=1)
    labels = [
        "Table:",
        "Failed checks:",
        "Proposed effect:",
        "Exact target count:",
        "Risk:",
        "Reversibility:",
        "Expiry guidance:",
    ]
    positions = [head.index(label) for label in labels]
    assert positions == sorted(positions)
    assert "billing.invoice" in head
    assert "invoice.amount.completeness" in head
    assert "Exact target count: 1" in head
    assert "Risk: medium" in head
    assert "Reversibility: reversible" in head
    assert "does not repair source data" in head
    assert "No Human Decision" in head
    assert "Plan fingerprint:" in audit
    assert [event.kind for event in events] == [
        "candidate_proposal",
        "plan_compiled",
        "evaluation",
        "approval_review",
    ]
    assert events[0].predecessor_ids == ["existing-root"]
    assert "fact_orders.total_amount.completeness" not in CHECK_SPECS
