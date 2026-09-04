"""Sanitized immutable audit-lineage event factories."""

from __future__ import annotations

from typing import Any, Literal

from airflow_dq_agent.contracts.fingerprints import (
    canonical_fingerprint,
    report_payload_fingerprint,
)
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    ApprovalReview,
    AuditEvent,
    EvalReport,
    ExecutablePlanItem,
    HumanDecision,
    Proposal,
    QualitySuiteReport,
    RemediationPlan,
)
from airflow_dq_agent.planning.integrity import decision_payload_fingerprint


def _report_fingerprint(report: QualitySuiteReport) -> str:
    """Fingerprint quality results without storing samples or row values."""
    return report_payload_fingerprint(report)


def _event(
    kind: Literal[
        "quality_report",
        "candidate_proposal",
        "plan_compiled",
        "plan_blocked",
        "evaluation",
        "approval_review",
        "human_approved",
        "human_rejected",
        "human_timed_out",
        "dry_run",
        "apply_succeeded",
        "apply_failed",
    ],
    *,
    quality_run_id: str,
    predecessor_ids: list[str] | None = None,
    **fields: Any,
) -> AuditEvent:
    payload: dict[str, Any] = {
        "kind": kind,
        "quality_run_id": quality_run_id,
        "predecessor_ids": predecessor_ids or [],
        "fingerprint": "pending",
        **fields,
    }
    draft = AuditEvent.model_validate(payload)
    return draft.model_copy(
        update={
            "fingerprint": canonical_fingerprint(
                draft.model_dump(mode="json", exclude={"fingerprint"})
            )
        }
    )


def _predecessor_id(predecessor: AuditEvent | str) -> str:
    return predecessor.event_id if isinstance(predecessor, AuditEvent) else predecessor


def quality_report_event(report: QualitySuiteReport) -> AuditEvent:
    fingerprint = report.fingerprint or _report_fingerprint(report)
    return _event(
        "quality_report",
        quality_run_id=report.run_id,
        report_id=report.report_id,
        report_fingerprint=fingerprint,
    )


def candidate_proposal_event(
    report: QualitySuiteReport, proposal: Proposal, predecessor: AuditEvent | str
) -> AuditEvent:
    candidate_fingerprint = proposal.fingerprint or canonical_fingerprint(proposal)
    return _event(
        "candidate_proposal",
        quality_run_id=report.run_id,
        predecessor_ids=[_predecessor_id(predecessor)],
        proposal_id=proposal.proposal_id,
        candidate_fingerprint=candidate_fingerprint,
    )


def plan_event(plan: RemediationPlan, predecessor: AuditEvent | str) -> AuditEvent:
    executable = [item for item in plan.items if isinstance(item, ExecutablePlanItem)]
    return _event(
        "plan_blocked" if plan.blocked else "plan_compiled",
        quality_run_id=plan.quality_run_id,
        predecessor_ids=[_predecessor_id(predecessor)],
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        policy_fingerprint=plan.policy_fingerprint,
        target_count=sum(item.target_set.count for item in executable),
        target_set_fingerprint=canonical_fingerprint(
            [item.target_set.fingerprint for item in executable]
        ),
        reasons=plan.blocked_reasons,
    )


def evaluation_event(
    plan: RemediationPlan, evaluation: EvalReport, predecessor: AuditEvent | str
) -> AuditEvent:
    return _event(
        "evaluation",
        quality_run_id=plan.quality_run_id,
        predecessor_ids=[_predecessor_id(predecessor)],
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        policy_fingerprint=plan.policy_fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation.fingerprint,
        reasons=evaluation.blocked_reasons,
    )


def review_event(
    review: ApprovalReview,
    evaluation: EvalReport,
    predecessor: AuditEvent | str,
) -> AuditEvent:
    """Persist the sample-free review shown before a Human Decision."""
    return _event(
        "approval_review",
        quality_run_id=review.quality_run_id,
        predecessor_ids=[_predecessor_id(predecessor)],
        plan_id=review.plan_id,
        plan_fingerprint=review.plan_fingerprint,
        policy_fingerprint=review.policy_fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation.fingerprint,
        review_fingerprint=review.fingerprint,
        review_payload=review.model_dump(mode="json"),
    )


def decision_event(
    quality_run_id: str,
    decision: HumanDecision,
    predecessor: AuditEvent | str,
    *,
    plan_id: str | None = None,
    plan_fingerprint: str | None = None,
    evaluation_id: str | None = None,
    evaluation_fingerprint: str | None = None,
) -> AuditEvent:
    kind = {
        "Approve": "human_approved",
        "Reject": "human_rejected",
        "Timeout": "human_timed_out",
    }.get(decision.decision, "human_rejected")
    decision_fingerprint = decision_payload_fingerprint(
        decision_id=decision.decision_id,
        decision=decision.decision,
        actor=decision.actor,
        note=decision.note,
        decided_at=decision.decided_at,
    )
    return _event(
        kind,  # type: ignore[arg-type]
        quality_run_id=quality_run_id,
        predecessor_ids=[_predecessor_id(predecessor)],
        plan_id=plan_id,
        plan_fingerprint=plan_fingerprint,
        evaluation_id=evaluation_id,
        evaluation_fingerprint=evaluation_fingerprint,
        review_fingerprint=decision.review_fingerprint,
        decision_id=decision.decision_id,
        decision_fingerprint=decision_fingerprint,
        decision_outcome=decision.decision,
        decision_actor=decision.actor,
        decision_note=decision.note,
        reasons=[f"decision_fingerprint={decision_fingerprint}"],
    )


def apply_result_event(
    plan: RemediationPlan,
    evaluation: EvalReport,
    admission: ApplyAdmission | None,
    *,
    result_id: str,
    result_fingerprint: str,
    dry_run: bool,
    reasons: list[str] | None = None,
    failed: bool = False,
) -> AuditEvent:
    """Build the terminal event body that dq_apply records atomically with mutation."""
    kind: Literal["dry_run", "apply_succeeded", "apply_failed"]
    if failed:
        kind = "apply_failed"
    elif dry_run:
        kind = "dry_run"
    else:
        kind = "apply_succeeded"
    return _event(
        kind,
        quality_run_id=plan.quality_run_id,
        predecessor_ids=[
            admission.decision_event_id
            if admission
            else (evaluation.audit_event_id or evaluation.evaluation_id)
        ],
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        policy_fingerprint=admission.policy_fingerprint if admission else plan.policy_fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation.fingerprint,
        decision_id=admission.decision_id if admission else None,
        apply_result_id=result_id,
        apply_result_fingerprint=result_fingerprint,
        reasons=reasons or [],
    )
