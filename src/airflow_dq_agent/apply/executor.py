"""Transactional execution of already evaluated and human-approved renderings."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine

from airflow_dq_agent.apply.renderer import RenderedStep, render_plan_item
from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    AuditEvent,
    EvalReport,
    ExecutablePlanItem,
    RemediationPlan,
)
from airflow_dq_agent.planning import current_policy_fingerprint
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.traces.lineage import apply_result_event
from airflow_dq_agent.traces.writer import JsonlAuditSink, append_event
from airflow_dq_agent.warehouse.db import make_engine


class AppliedStep(BaseModel):
    rendered: RenderedStep
    estimated_rows: int | None = None
    rowcount: int | None = None


class ApplyResult(BaseModel):
    apply_result_id: str = Field(default_factory=lambda: uuid4().hex)
    fingerprint: str | None = None
    audit_event_id: str | None = None
    run_id: str
    dry_run: bool
    plan_id: str | None = None
    admission_id: str | None = None
    steps: list[AppliedStep] = Field(default_factory=list)


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


def _require_dry_run(plan: RemediationPlan, evaluation: EvalReport) -> None:
    if plan.blocked or any(not isinstance(item, ExecutablePlanItem) for item in plan.items):
        raise PermissionError("Refusing dry run: remediation plan is blocked")
    if not evaluation.passed:
        raise PermissionError("Refusing dry run: remediation-plan evaluation did not pass")
    if evaluation.plan_id != plan.plan_id or evaluation.plan_fingerprint != plan.fingerprint:
        raise PermissionError(
            "Refusing dry run: evaluation does not belong to this remediation plan"
        )
    if current_policy_fingerprint(plan) != plan.policy_fingerprint:
        raise PermissionError("Refusing dry run: policy snapshot drifted after evaluation")


def _result_fingerprint(
    plan: RemediationPlan,
    admission: ApplyAdmission | None,
    run_id: str,
    dry_run: bool,
    steps: list[AppliedStep],
) -> str:
    return canonical_fingerprint(
        {
            "quality_run_id": plan.quality_run_id,
            "plan_fingerprint": plan.fingerprint,
            "admission_fingerprint": admission.fingerprint if admission else None,
            "run_id": run_id,
            "dry_run": dry_run,
            "steps": [
                {
                    "action_id": step.rendered.action_id,
                    "table": step.rendered.table,
                    "estimated_rows": step.estimated_rows,
                    "rowcount": step.rowcount,
                }
                for step in steps
            ],
        }
    )


def _set_controlled_transaction_mode(connection: object, *, dry_run: bool) -> None:
    """Freeze the target snapshot before locking/re-rendering controlled predicates.

    The target summary deliberately never retains raw primary keys.  Serializable
    isolation therefore makes the subsequent controlled DML observe the same target
    snapshot that was fingerprinted and locked; a conflicting phantom causes the
    whole transaction to abort rather than expanding the approved target set.
    """
    connection.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))  # type: ignore[attr-defined]
    if dry_run:
        connection.execute(text("SET TRANSACTION READ ONLY"))  # type: ignore[attr-defined]


def _record_apply_result(
    connection: object,
    *,
    event: AuditEvent,
    result: ApplyResult,
    plan: RemediationPlan,
    admission: ApplyAdmission,
) -> None:
    target_count = sum(
        item.target_set.count for item in plan.items if isinstance(item, ExecutablePlanItem)
    )
    target_fingerprint = canonical_fingerprint(
        [item.target_set.fingerprint for item in plan.items if isinstance(item, ExecutablePlanItem)]
    )
    rowcounts = [step.rowcount for step in result.steps if step.rowcount is not None]
    connection.execute(  # type: ignore[attr-defined]
        text(
            "SELECT dq.record_apply_result("
            ":event_id, :kind, CAST(:body AS jsonb), :run_id, :plan_id, :admission_id, "
            ":item_id, :action_id, :table_name, :target_count, :target_fingerprint, :rowcount)"
        ),
        {
            "event_id": event.event_id,
            "kind": event.kind,
            "body": json.dumps(
                event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
            ),
            "run_id": result.run_id,
            "plan_id": plan.plan_id,
            "admission_id": admission.admission_id,
            "item_id": "whole-plan",
            "action_id": "whole_plan",
            "table_name": "multiple",
            "target_count": target_count,
            "target_fingerprint": target_fingerprint,
            "rowcount": sum(rowcounts) if rowcounts else None,
        },
    )


def apply_plan(
    plan: RemediationPlan,
    evaluation: EvalReport,
    admission: ApplyAdmission | None = None,
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
    if dry_run:
        _require_dry_run(plan, evaluation)
    else:
        if admission is None:
            raise PermissionError("Refusing apply: mutation requires an apply admission")
        _require_plan_admission(plan, evaluation, admission, now=applied_at)
    database = engine or make_engine(dsn or get_settings().apply_dsn)
    resolved_run_id = run_id or uuid4().hex
    executable = [item for item in plan.items if isinstance(item, ExecutablePlanItem)]
    resolver = PostgresTargetSetResolver(engine=database)
    result = ApplyResult(
        run_id=resolved_run_id,
        dry_run=dry_run,
        plan_id=plan.plan_id,
        admission_id=admission.admission_id if admission else None,
    )
    try:
        with database.begin() as connection:
            _set_controlled_transaction_mode(connection, dry_run=dry_run)
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
                if dry_run:
                    result.steps.append(
                        AppliedStep(rendered=rendered, estimated_rows=item.target_set.count)
                    )
                    continue
                rowcount: int | None = 0
                if rendered.estimate_sql is not None:
                    mutation = connection.execute(text(rendered.sql), rendered.params)
                    rowcount = int(mutation.rowcount) if mutation.rowcount is not None else None
                result.steps.append(
                    AppliedStep(
                        rendered=rendered, estimated_rows=item.target_set.count, rowcount=rowcount
                    )
                )
            result.fingerprint = _result_fingerprint(
                plan, admission, resolved_run_id, dry_run, result.steps
            )
            event = apply_result_event(
                plan,
                evaluation,
                admission,
                result_id=result.apply_result_id,
                result_fingerprint=result.fingerprint,
                dry_run=dry_run,
            )
            result.audit_event_id = event.event_id
            if not dry_run:
                assert admission is not None
                _record_apply_result(
                    connection, event=event, result=result, plan=plan, admission=admission
                )
        if dry_run:
            append_event(event)
        else:
            JsonlAuditSink().append(event)
    except Exception:
        failure = apply_result_event(
            plan,
            evaluation,
            admission,
            result_id=result.apply_result_id,
            result_fingerprint=_result_fingerprint(
                plan, admission, resolved_run_id, dry_run, result.steps
            ),
            dry_run=True,
            reasons=["controlled apply failed before a result could be admitted"],
        ).model_copy(update={"kind": "apply_failed"})
        append_event(failure)
        raise
    return result
