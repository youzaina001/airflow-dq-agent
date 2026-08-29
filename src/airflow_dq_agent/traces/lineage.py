"""Sanitized immutable audit-lineage event factories."""

from __future__ import annotations

from typing import Any, Literal

from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import (
    AuditEvent,
    EvalReport,
    HumanDecision,
    Proposal,
    QualitySuiteReport,
    RemediationPlan,
)


def _report_fingerprint(report: QualitySuiteReport) -> str:
    """Fingerprint quality results without storing samples or row values."""
    return canonical_fingerprint(
        {
            "quality_run_id": report.run_id,
            "checks": [
                {
                    "check_id": check.check_id,
                    "contract_id": check.contract_id,
                    "status": check.status,
                    "n_failed": check.n_failed,
                    "n_total": check.n_total,
                }
                for check in report.checks
            ],
        }
    )


def _event(
    kind: Literal[
        "quality_report",
        "candidate_proposal",
        "plan_compiled",
        "plan_blocked",
        "evaluation",
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


def quality_report_event(report: QualitySuiteReport) -> AuditEvent:
    fingerprint = report.fingerprint or _report_fingerprint(report)
    return _event(
        "quality_report",
        quality_run_id=report.run_id,
        report_id=report.report_id,
        report_fingerprint=fingerprint,
    )


def candidate_proposal_event(
    report: QualitySuiteReport, proposal: Proposal, predecessor: AuditEvent
) -> AuditEvent:
    candidate_fingerprint = proposal.fingerprint or canonical_fingerprint(proposal)
    return _event(
        "candidate_proposal",
        quality_run_id=report.run_id,
        predecessor_ids=[predecessor.event_id],
        proposal_id=proposal.proposal_id,
        candidate_fingerprint=candidate_fingerprint,
    )


def plan_event(plan: RemediationPlan, predecessor: AuditEvent) -> AuditEvent:
    return _event(
        "plan_blocked" if plan.blocked else "plan_compiled",
        quality_run_id=plan.quality_run_id,
        predecessor_ids=[predecessor.event_id],
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        reasons=plan.blocked_reasons,
    )


def evaluation_event(
    plan: RemediationPlan, evaluation: EvalReport, predecessor: AuditEvent
) -> AuditEvent:
    return _event(
        "evaluation",
        quality_run_id=plan.quality_run_id,
        predecessor_ids=[predecessor.event_id],
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation.fingerprint,
        reasons=evaluation.blocked_reasons,
    )


def decision_event(
    quality_run_id: str, decision: HumanDecision, predecessor: AuditEvent
) -> AuditEvent:
    kind = {
        "Approve": "human_approved",
        "Reject": "human_rejected",
        "Timeout": "human_timed_out",
    }.get(decision.decision, "human_rejected")
    decision_fingerprint = decision.fingerprint or canonical_fingerprint(
        {
            "decision_id": decision.decision_id,
            "decision": decision.decision,
            "actor": decision.actor,
            "note": decision.note,
            "decided_at": decision.decided_at,
        }
    )
    return _event(
        kind,  # type: ignore[arg-type]
        quality_run_id=quality_run_id,
        predecessor_ids=[predecessor.event_id],
        decision_id=decision.decision_id,
        reasons=[f"decision_fingerprint={decision_fingerprint}"],
    )
