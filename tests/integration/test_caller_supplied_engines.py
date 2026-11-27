"""Exercise caller-owned quality and audit engines against PostgreSQL."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from tests.settings_isolation import use_settings_without_dotenv

from airflow_dq_agent.contracts.models import AuditEvent, QualitySuiteReport
from airflow_dq_agent.demo import seed_warehouse
from airflow_dq_agent.demo.defects import EXPECTED_DEFECTS
from airflow_dq_agent.quality import run_quality_suite
from airflow_dq_agent.traces import PostgresAuditSink
from airflow_dq_agent.warehouse.db import make_engine

UNREACHABLE_DSN = "postgresql+psycopg://bad:bad@127.0.0.1:1/unreachable"


@pytest.fixture(autouse=True)
def _ignore_developer_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    use_settings_without_dotenv(monkeypatch)
    monkeypatch.setenv("TRACE_POSTGRES", "false")
    monkeypatch.setenv("APPLY_MODE", "off")
    monkeypatch.setenv("WAREHOUSE_DSN", UNREACHABLE_DSN)
    monkeypatch.setenv("AUDIT_DSN", UNREACHABLE_DSN)
    monkeypatch.delenv("READ_DSN", raising=False)


def _assert_expected_defects(report: QualitySuiteReport) -> None:
    failures = {check.check_id: check.n_failed for check in report.failed_checks}
    assert failures == {check_id: defect.n_rows for check_id, defect in EXPECTED_DEFECTS.items()}


def _assert_postgres_audit(engine: Engine, report: QualitySuiteReport) -> None:
    assert report.audit_event_id is not None
    with engine.connect() as connection:
        trace = connection.execute(
            text(
                "SELECT kind, body ->> 'quality_run_id' FROM dq.traces WHERE trace_id = :trace_id"
            ),
            {"trace_id": report.audit_event_id},
        ).one()
        check_runs = connection.execute(
            text(
                "SELECT check_id, status, n_failed, n_total FROM dq.check_runs "
                "WHERE run_id = :run_id"
            ),
            {"run_id": report.run_id},
        ).all()

    assert tuple(trace) == ("quality_report", report.run_id)
    assert sorted(tuple(row) for row in check_runs) == sorted(
        (check.check_id, check.status.value, check.n_failed, check.n_total)
        for check in report.checks
    )


@pytest.mark.integration
def test_run_quality_suite_with_caller_supplied_engines_against_postgres(
    warehouse_dsn: str, tmp_path: Path
) -> None:
    seed_warehouse(warehouse_dsn)
    read_engine = make_engine(warehouse_dsn)
    audit_engine = make_engine(warehouse_dsn)
    try:
        report = run_quality_suite(
            read_engine=read_engine, audit_sink=PostgresAuditSink(engine=audit_engine)
        )

        _assert_expected_defects(report)
        _assert_postgres_audit(audit_engine, report)
        trace_json = (tmp_path / "agent-traces.jsonl").read_text(encoding="utf-8")
        trace = AuditEvent.model_validate_json(trace_json)
        assert trace.event_id == report.audit_event_id
        assert trace.quality_run_id == report.run_id
    finally:
        audit_engine.dispose()
        read_engine.dispose()


@pytest.mark.integration
def test_run_quality_suite_caller_engine_fails_closed_on_audit_error(
    warehouse_dsn: str, tmp_path: Path
) -> None:
    seed_warehouse(warehouse_dsn)
    read_engine = make_engine(warehouse_dsn)
    bad_audit_engine = make_engine(UNREACHABLE_DSN)
    try:
        with pytest.raises(SQLAlchemyError):
            run_quality_suite(
                read_engine=read_engine, audit_sink=PostgresAuditSink(engine=bad_audit_engine)
            )

        assert not (tmp_path / "agent-traces.jsonl").exists()
    finally:
        bad_audit_engine.dispose()
        read_engine.dispose()


@pytest.mark.integration
def test_run_quality_suite_preserves_caller_engine_connection_hook(
    warehouse_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seed_warehouse(warehouse_dsn)
    read_engine = make_engine(warehouse_dsn)
    audit_engine = make_engine(warehouse_dsn)
    statements: list[str] = []

    def capture_statement(
        _connection: Connection, _cursor: object, statement: str, *_: object
    ) -> None:
        statements.append(statement)

    def no_default_engine(dsn: str | None = None) -> Engine:
        raise AssertionError(f"make_engine called with {dsn!r}")

    try:
        event.listen(read_engine, "before_cursor_execute", capture_statement)
        monkeypatch.setattr("airflow_dq_agent.quality.suite.make_engine", no_default_engine)
        monkeypatch.setattr("airflow_dq_agent.traces.writer.make_engine", no_default_engine)

        report = run_quality_suite(
            read_engine=read_engine, audit_sink=PostgresAuditSink(engine=audit_engine)
        )

        assert any("warehouse.fact_orders" in statement for statement in statements)
        _assert_expected_defects(report)
        _assert_postgres_audit(audit_engine, report)
    finally:
        audit_engine.dispose()
        read_engine.dispose()
