"""Whole-plan, time-bounded authorization for controlled mutation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    EvalReport,
    HumanDecision,
    RemediationPlan,
)


def create_apply_admission(
    plan: RemediationPlan,
    evaluation: EvalReport,
    decision: HumanDecision,
    *,
    now: datetime | None = None,
    ttl: timedelta = timedelta(hours=24),
) -> ApplyAdmission:
    """Create admission only for one fresh, approved, fully executable plan."""
    issued_at = now or datetime.now(UTC)
    if plan.blocked:
        raise PermissionError("Refusing admission: remediation plan is blocked")
    if not evaluation.passed:
        raise PermissionError("Refusing admission: remediation-plan evaluation did not pass")
    if evaluation.plan_id != plan.plan_id or evaluation.plan_fingerprint != plan.fingerprint:
        raise PermissionError(
            "Refusing admission: evaluation does not belong to this remediation plan"
        )
    if not evaluation.fingerprint:
        raise PermissionError("Refusing admission: evaluation has no immutable fingerprint")
    if decision.decision != "Approve":
        raise PermissionError("Refusing admission: human decision is not an approval")
    if not decision.actor.strip() or not decision.note or not decision.note.strip():
        raise PermissionError("Refusing admission: approval requires an actor and non-empty note")
    if not decision.audit_event_id:
        raise PermissionError("Refusing admission: human decision has no durable audit event")
    expires_at = issued_at + ttl
    fingerprint = canonical_fingerprint(
        {
            "quality_run_id": plan.quality_run_id,
            "plan_id": plan.plan_id,
            "plan_fingerprint": plan.fingerprint,
            "evaluation_id": evaluation.evaluation_id,
            "evaluation_fingerprint": evaluation.fingerprint,
            "decision_id": decision.decision_id,
            "decision_event_id": decision.audit_event_id,
            "policy_fingerprint": plan.policy_fingerprint,
            "issued_at": issued_at,
            "expires_at": expires_at,
        }
    )
    return ApplyAdmission(
        quality_run_id=plan.quality_run_id,
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation.fingerprint,
        decision_id=decision.decision_id,
        decision_event_id=decision.audit_event_id,
        policy_fingerprint=plan.policy_fingerprint,
        issued_at=issued_at,
        expires_at=expires_at,
        fingerprint=fingerprint,
    )
