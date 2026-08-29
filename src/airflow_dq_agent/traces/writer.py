"""Append traces locally first, then optionally mirror them to Postgres."""

from __future__ import annotations

import json
import os
from pathlib import Path

from sqlalchemy import text

from airflow_dq_agent.agent.runner import AgentRun
from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts.models import (
    EvalReport,
    HumanDecision,
    QualitySuiteReport,
    TraceRecord,
)
from airflow_dq_agent.warehouse.db import make_engine

TRACE_FILENAME = "agent-traces.jsonl"


def _trace_path(directory: Path | None = None) -> Path:
    settings = get_settings()
    resolved = directory or settings.traces_dir
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved / TRACE_FILENAME


def _append_jsonl(record: TraceRecord, path: Path) -> None:
    """Use one append-only write; existing trace lines are never revisited."""
    payload = (
        json.dumps(record.model_dump(mode="json"), sort_keys=True, separators=(",", ":")) + "\n"
    )
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    descriptor = os.open(path, flags, 0o644)
    try:
        os.write(descriptor, payload.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _mirror_postgres(record: TraceRecord, dsn: str | None = None) -> None:
    body = json.dumps(record.model_dump(mode="json"))
    engine = make_engine(dsn)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO dq.traces (trace_id, kind, body) VALUES (:trace_id, :kind, CAST(:body AS jsonb))"
            ),
            {"trace_id": record.trace_id, "kind": record.kind, "body": body},
        )


def append_trace(
    record: TraceRecord,
    *,
    directory: Path | None = None,
    dsn: str | None = None,
    mirror_postgres: bool | None = None,
) -> Path:
    """Append local JSONL before an optional mirror; preserve local evidence on mirror failure."""
    path = _trace_path(directory)
    _append_jsonl(record, path)
    should_mirror = get_settings().trace_postgres if mirror_postgres is None else mirror_postgres
    if should_mirror:
        _mirror_postgres(record, dsn)
    return path


def trace_agent_run(
    agent_run: AgentRun,
    report: QualitySuiteReport,
    evaluation: EvalReport | None = None,
    *,
    dag_id: str | None = None,
    directory: Path | None = None,
    dsn: str | None = None,
) -> TraceRecord:
    """Create and append the audit event for one agent run."""
    settings = get_settings()
    record = TraceRecord(
        kind="agent_run",
        dag_id=dag_id,
        run_id=report.run_id,
        llm_mode=agent_run.llm_mode,
        apply_mode=settings.apply_mode,
        llm_model=settings.llm_model,
        prompt=agent_run.prompt,
        tool_calls=agent_run.tool_calls,
        proposal=agent_run.proposal,
        eval_scores=evaluation,
        quality_run_id=report.run_id,
    )
    append_trace(record, directory=directory, dsn=dsn)
    return record


def append_human_decision(
    parent_trace_id: str,
    decision: HumanDecision,
    *,
    directory: Path | None = None,
    dsn: str | None = None,
) -> TraceRecord:
    """Record a distinct immutable HITL event linked to its agent-run trace."""
    settings = get_settings()
    record = TraceRecord(
        parent_trace_id=parent_trace_id,
        kind="human_decision",
        llm_mode=settings.llm_mode,
        apply_mode=settings.apply_mode,
        llm_model=settings.llm_model,
        human_decision=decision,
    )
    append_trace(record, directory=directory, dsn=dsn)
    return record
