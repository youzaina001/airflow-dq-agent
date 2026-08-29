"""Transactional execution of already evaluated and human-approved renderings."""

from __future__ import annotations

from uuid import uuid4

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine

from airflow_dq_agent.apply.renderer import RenderedStep, render_proposal
from airflow_dq_agent.contracts.models import EvalReport, HumanDecision, Proposal
from airflow_dq_agent.warehouse.db import make_engine


class AppliedStep(BaseModel):
    rendered: RenderedStep
    estimated_rows: int | None = None
    rowcount: int | None = None


class ApplyResult(BaseModel):
    run_id: str
    dry_run: bool
    steps: list[AppliedStep] = Field(default_factory=list)


def _require_authorization(
    proposal: Proposal,
    passed_eval: EvalReport,
    approval: HumanDecision | None,
    *,
    dry_run: bool,
) -> None:
    if not passed_eval.passed:
        raise PermissionError("Refusing apply: EvalReport did not pass")
    if not dry_run and proposal.steps and (approval is None or approval.decision != "Approve"):
        raise PermissionError("Refusing apply: proposal-level HITL approval is required")


def _estimate(connection: object, rendered: RenderedStep) -> int | None:
    if rendered.estimate_sql is None:
        return 0
    result = connection.execute(text(rendered.estimate_sql), rendered.estimate_params)  # type: ignore[attr-defined]
    value = result.scalar_one()
    return int(value)


def _log_step(
    connection: object, run_id: str, rendered: RenderedStep, rowcount: int | None
) -> None:
    connection.execute(  # type: ignore[attr-defined]
        text(
            "INSERT INTO dq.apply_log (run_id, action_id, table_name, sql_text, rowcount) "
            "VALUES (:run_id, :action_id, :table_name, :sql_text, :rowcount)"
        ),
        {
            "run_id": run_id,
            "action_id": rendered.action_id,
            "table_name": rendered.table,
            "sql_text": rendered.sql,
            "rowcount": rowcount,
        },
    )


def apply_proposal(
    proposal: Proposal,
    passed_eval: EvalReport,
    *,
    approval: HumanDecision | None = None,
    dry_run: bool = True,
    engine: Engine | None = None,
    dsn: str | None = None,
    run_id: str | None = None,
) -> ApplyResult:
    """Apply controlled SQL plus audit rows in one transaction, or render a dry run.

    The render path never accepts SQL from ``proposal.sql_preview``.  Real
    mutations require both a passing deterministic eval and one proposal-level
    HITL ``Approve`` decision.
    """
    _require_authorization(proposal, passed_eval, approval, dry_run=dry_run)
    resolved_run_id = run_id or uuid4().hex
    rendered_steps = render_proposal(proposal, run_id=resolved_run_id)
    database = engine or make_engine(dsn)
    applied: list[AppliedStep] = []
    with database.begin() as connection:
        for rendered in rendered_steps:
            estimate = _estimate(connection, rendered)
            if dry_run:
                applied.append(AppliedStep(rendered=rendered, estimated_rows=estimate))
                continue
            rowcount: int | None = 0
            if rendered.estimate_sql is not None:
                result = connection.execute(text(rendered.sql), rendered.params)
                rowcount = int(result.rowcount) if result.rowcount is not None else None
            _log_step(connection, resolved_run_id, rendered, rowcount)
            applied.append(
                AppliedStep(rendered=rendered, estimated_rows=estimate, rowcount=rowcount)
            )
    return ApplyResult(run_id=resolved_run_id, dry_run=dry_run, steps=applied)
