"""Whole-plan, time-bounded authorization for controlled mutation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    EvalReport,
    HumanDecision,
    QualitySuiteReport,
    RemediationPlan,
)
from airflow_dq_agent.planning.integrity import (
    admission_payload_fingerprint,
    decision_payload_fingerprint,
    verify_evaluation_integrity,
    verify_executable_params,
    verify_plan_integrity,
    verify_report_integrity,
)
from airflow_dq_agent.planning.review import build_approval_review
from airflow_dq_agent.traces.repository import AuditLineageLookup


def _verify_durable_approval(
    plan: RemediationPlan,
    evaluation: EvalReport,
    decision: HumanDecision,
    *,
    ttl: timedelta,
    audit_repository: AuditLineageLookup | None,
) -> str:
    """Resolve the Human Decision against Audit Lineage and the reviewed payload."""
    audit_event_id = decision.audit_event_id
    if not audit_event_id or not audit_event_id.strip():
        raise PermissionError("Refusing admission: human decision has no durable audit event")
    if audit_repository is None:
        raise PermissionError("Refusing admission: audit lineage lookup is required")
    event = audit_repository.get(audit_event_id)
    if event is None or event.event_id != audit_event_id:
        raise PermissionError("Refusing admission: human decision audit event was not found")
    if event.kind != "human_approved":
        raise PermissionError("Refusing admission: audit event is not a human approval")
    if event.quality_run_id != plan.quality_run_id:
        raise PermissionError(
            "Refusing admission: human decision does not belong to this quality run"
        )
    if event.plan_id != plan.plan_id or event.plan_fingerprint != plan.fingerprint:
        raise PermissionError(
            "Refusing admission: human decision does not belong to this remediation plan"
        )
    if event.decision_actor != decision.actor:
        raise PermissionError(
            "Refusing admission: human decision actor does not match audit lineage"
        )
    expected = decision_payload_fingerprint(
        decision_id=decision.decision_id,
        decision=decision.decision,
        actor=decision.actor,
        note=decision.note,
        decided_at=decision.decided_at,
    )
    if event.decision_fingerprint != expected:
        raise PermissionError(
            "Refusing admission: human decision fingerprint does not match received payload"
        )
    review = build_approval_review(plan, evaluation, ttl=ttl)
    bound = decision.fingerprint
    if not bound or bound != review.fingerprint:
        raise PermissionError("Refusing admission: human decision does not bind the reviewed plan")
    return audit_event_id


def create_apply_admission(
    plan: RemediationPlan,
    evaluation: EvalReport,
    decision: HumanDecision,
    *,
    report: QualitySuiteReport,
    now: datetime | None = None,
    ttl: timedelta = timedelta(hours=24),
    audit_repository: AuditLineageLookup | None = None,
) -> ApplyAdmission:
    """Create admission only for one fresh, approved, fully executable plan."""
    issued_at = now or datetime.now(UTC)
    verify_report_integrity(report, refusing="admission")
    verify_plan_integrity(plan, refusing="admission")
    if plan.blocked:
        raise PermissionError("Refusing admission: remediation plan is blocked")
    verify_executable_params(plan, report=report, refusing="admission")
    if not evaluation.passed:
        raise PermissionError("Refusing admission: remediation-plan evaluation did not pass")
    evaluation_fingerprint = verify_evaluation_integrity(plan, evaluation, refusing="admission")
    if decision.decision != "Approve":
        raise PermissionError("Refusing admission: human decision is not an approval")
    if not decision.actor.strip() or not decision.note or not decision.note.strip():
        raise PermissionError("Refusing admission: approval requires an actor and non-empty note")
    audit_event_id = _verify_durable_approval(
        plan,
        evaluation,
        decision,
        ttl=ttl,
        audit_repository=audit_repository,
    )
    expires_at = issued_at + ttl
    admission_id = uuid4().hex
    fingerprint = admission_payload_fingerprint(
        admission_id=admission_id,
        quality_run_id=plan.quality_run_id,
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation_fingerprint,
        decision_id=decision.decision_id,
        decision_event_id=audit_event_id,
        policy_fingerprint=plan.policy_fingerprint,
        warehouse_environment_id=plan.warehouse_environment_id,
        issued_at=issued_at,
        expires_at=expires_at,
    )
    return ApplyAdmission(
        admission_id=admission_id,
        quality_run_id=plan.quality_run_id,
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation_fingerprint,
        decision_id=decision.decision_id,
        decision_event_id=audit_event_id,
        policy_fingerprint=plan.policy_fingerprint,
        warehouse_environment_id=plan.warehouse_environment_id,
        issued_at=issued_at,
        expires_at=expires_at,
        fingerprint=fingerprint,
    )
