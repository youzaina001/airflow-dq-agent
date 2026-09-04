"""Append-only audit adapters with minimized lineage payloads."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from sqlalchemy import text

from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts.models import (
    AuditEvent,
    EvalReport,
    HumanDecision,
    QualitySuiteReport,
)
from airflow_dq_agent.traces.lineage import (
    candidate_proposal_event,
    decision_event,
    quality_report_event,
)
from airflow_dq_agent.warehouse.db import make_engine

if TYPE_CHECKING:
    from airflow_dq_agent.agent.runner import AgentRun

TRACE_FILENAME = "agent-traces.jsonl"


class AuditSink(Protocol):
    """An append-only audit adapter at the durable lineage seam."""

    def append(self, event: AuditEvent) -> None: ...

    def record_check_runs(self, report: QualitySuiteReport) -> None: ...


def _trace_path(directory: Path | None = None) -> Path:
    settings = get_settings()
    resolved = directory or settings.traces_dir
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved / TRACE_FILENAME


def _append_jsonl(event: AuditEvent, path: Path) -> None:
    payload = (
        json.dumps(event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")) + "\n"
    )
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    descriptor = os.open(path, flags, 0o644)
    try:
        os.write(descriptor, payload.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _check_run_body(report: QualitySuiteReport, check_index: int) -> dict[str, object]:
    check = report.checks[check_index]
    return {
        "quality_run_id": report.run_id,
        "report_id": report.report_id,
        "check_id": check.check_id,
        "contract_id": check.contract_id,
        "status": check.status.value,
        "n_failed": check.n_failed,
        "n_total": check.n_total,
    }


class JsonlAuditSink:
    """Supplementary append-only local audit for shadow mode and diagnostics."""

    def __init__(self, directory: Path | None = None) -> None:
        self._directory = directory

    def append(self, event: AuditEvent) -> None:
        _append_jsonl(event, _trace_path(self._directory))

    def record_check_runs(self, report: QualitySuiteReport) -> None:
        del report


class PostgresAuditSink:
    """The source-of-truth append-only audit adapter for HITL mode."""

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn

    def append(self, event: AuditEvent) -> None:
        body = json.dumps(event.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        engine = make_engine(self._dsn)
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO dq.traces (trace_id, kind, body) "
                    "VALUES (:trace_id, :kind, CAST(:body AS jsonb))"
                ),
                {"trace_id": event.event_id, "kind": event.kind, "body": body},
            )

    def record_check_runs(self, report: QualitySuiteReport) -> None:
        engine = make_engine(self._dsn)
        with engine.begin() as connection:
            for index, check in enumerate(report.checks):
                body = _check_run_body(report, index)
                connection.execute(
                    text(
                        "INSERT INTO dq.check_runs "
                        "(run_id, check_id, status, n_failed, n_total, body) "
                        "VALUES (:run_id, :check_id, :status, :n_failed, :n_total, "
                        "CAST(:body AS jsonb))"
                    ),
                    {
                        "run_id": report.run_id,
                        "check_id": check.check_id,
                        "status": check.status.value,
                        "n_failed": check.n_failed,
                        "n_total": check.n_total,
                        "body": json.dumps(body, sort_keys=True, separators=(",", ":")),
                    },
                )


def _postgres_required(mirror_postgres: bool | None) -> bool:
    settings = get_settings()
    return settings.apply_mode == "hitl" or (
        settings.trace_postgres if mirror_postgres is None else mirror_postgres
    )


def append_event(
    event: AuditEvent,
    *,
    directory: Path | None = None,
    dsn: str | None = None,
    mirror_postgres: bool | None = None,
) -> Path:
    """Persist an event; HITL fails closed if its Postgres audit write fails."""
    settings = get_settings()
    postgres_required = _postgres_required(mirror_postgres)
    postgres_dsn = dsn or settings.audit_dsn or settings.warehouse_dsn
    if postgres_required:
        PostgresAuditSink(postgres_dsn).append(event)
    path = _trace_path(directory)
    _append_jsonl(event, path)
    return path


def record_quality_report(
    report: QualitySuiteReport,
    *,
    directory: Path | None = None,
    dsn: str | None = None,
    mirror_postgres: bool | None = None,
) -> AuditEvent:
    """Append the report event and its indexed, sample-free per-check records."""
    event = quality_report_event(report)
    settings = get_settings()
    postgres_required = _postgres_required(mirror_postgres)
    postgres_dsn = dsn or settings.audit_dsn or settings.warehouse_dsn
    if postgres_required:
        sink = PostgresAuditSink(postgres_dsn)
        sink.record_check_runs(report)
        sink.append(event)
    JsonlAuditSink(directory).append(event)
    return event


def trace_agent_run(
    agent_run: AgentRun,
    report: QualitySuiteReport,
    evaluation: EvalReport | None = None,
    *,
    dag_id: str | None = None,
    directory: Path | None = None,
    dsn: str | None = None,
) -> AuditEvent:
    """Append report and candidate lineage without raw agent payloads."""
    del dag_id, evaluation
    report_event = record_quality_report(report, directory=directory, dsn=dsn)
    candidate_event = candidate_proposal_event(report, agent_run.proposal, report_event)
    append_event(candidate_event, directory=directory, dsn=dsn)
    return candidate_event


def append_human_decision(
    quality_run_id: str,
    predecessor: AuditEvent,
    decision: HumanDecision,
    *,
    plan_id: str | None = None,
    plan_fingerprint: str | None = None,
    directory: Path | None = None,
    dsn: str | None = None,
) -> AuditEvent:
    """Persist an attributable decision after identity and note validation."""
    event = decision_event(
        quality_run_id,
        decision,
        predecessor,
        plan_id=plan_id,
        plan_fingerprint=plan_fingerprint,
    )
    append_event(event, directory=directory, dsn=dsn)
    return event


__all__ = [
    "AuditSink",
    "JsonlAuditSink",
    "PostgresAuditSink",
    "append_event",
    "append_human_decision",
    "record_quality_report",
    "trace_agent_run",
]
