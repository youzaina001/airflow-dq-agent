"""Transactional execution of already evaluated and human-approved renderings."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine

from airflow_dq_agent.apply.renderer import RenderedStep, render_plan_item, render_proposal
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    EvalReport,
    ExecutablePlanItem,
    HumanDecision,
    Proposal,
    RemediationPlan,
)
from airflow_dq_agent.planning import current_policy_fingerprint
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.warehouse.db import make_engine


class AppliedStep(BaseModel):
    rendered: RenderedStep
    estimated_rows: int | None = None
    rowcount: int | None = None


class ApplyResult(BaseModel):
    run_id: str
    dry_run: bool
    plan_id: str | None = None
    admission_id: str | None = None
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
            "sql_text": "controlled SQL redacted from durable audit",
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


def _require_plan_admission(
    plan: RemediationPlan,
    evaluation: EvalReport,
    admission: ApplyAdmission,
    *,
    now: datetime,
) -> None:
    if plan.blocked or any(not isinstance(item, ExecutablePlanItem) for item in plan.items):
        raise PermissionError("Refusing apply: remediation plan is blocked")
    if not evaluation.passed:
        raise PermissionError("Refusing apply: remediation-plan evaluation did not pass")
    if evaluation.plan_id != plan.plan_id or evaluation.plan_fingerprint != plan.fingerprint:
        raise PermissionError("Refusing apply: evaluation does not belong to this remediation plan")
    if (
        admission.plan_id != plan.plan_id
        or admission.plan_fingerprint != plan.fingerprint
        or admission.quality_run_id != plan.quality_run_id
        or admission.evaluation_id != evaluation.evaluation_id
        or admission.evaluation_fingerprint != evaluation.fingerprint
    ):
        raise PermissionError("Refusing apply: admission does not authorize this evaluated plan")
    if now > admission.expires_at:
        raise PermissionError("Refusing apply: apply admission has expired")
    current_policy = current_policy_fingerprint(plan)
    if current_policy != plan.policy_fingerprint or current_policy != admission.policy_fingerprint:
        raise PermissionError("Refusing apply: policy snapshot drifted after admission")


def apply_plan(
    plan: RemediationPlan,
    evaluation: EvalReport,
    admission: ApplyAdmission,
    *,
    dry_run: bool = True,
    engine: Engine | None = None,
    dsn: str | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> ApplyResult:
    """Recheck a whole-plan admission, lock targets, and mutate only matching rows.

    The apply path never accepts a candidate proposal.  It validates the immutable
    admission first, then recomputes each controlled target set in the same database
    transaction that either records a dry run or performs all mutation statements.
    """
    applied_at = now or datetime.now(UTC)
    _require_plan_admission(plan, evaluation, admission, now=applied_at)
    database = engine or make_engine(dsn)
    resolved_run_id = run_id or uuid4().hex
    executable = [item for item in plan.items if isinstance(item, ExecutablePlanItem)]
    resolver = PostgresTargetSetResolver(engine=database)
    applied: list[AppliedStep] = []
    with database.begin() as connection:
        if dry_run:
            connection.execute(text("SET TRANSACTION READ ONLY"))
        for item in executable:
            actual_targets = (
                resolver.resolve_item(connection, item)
                if dry_run
                else resolver.lock_and_resolve(connection, item)
            )
            if actual_targets != item.target_set:
                raise PermissionError(
                    "Refusing apply: controlled target set changed after plan compilation"
                )
        for item in executable:
            rendered = render_plan_item(item, run_id=resolved_run_id)
            estimate = item.target_set.count
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
    return ApplyResult(
        run_id=resolved_run_id,
        dry_run=dry_run,
        plan_id=plan.plan_id,
        admission_id=admission.admission_id,
        steps=applied,
    )
