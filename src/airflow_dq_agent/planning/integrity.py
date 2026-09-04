"""Recompute governed fingerprints from received artifacts at durable seams."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    EvalReport,
    EvalScore,
    ExecutablePlanItem,
    NonExecutablePlanItem,
    RemediationPlan,
)
from airflow_dq_agent.quality.registry import get_check_spec

PlanItem = ExecutablePlanItem | NonExecutablePlanItem


def plan_payload_fingerprint(
    *,
    quality_run_id: str,
    candidate_fingerprint: str,
    policy_fingerprint: str,
    items: Sequence[PlanItem],
) -> str:
    """Canonical fingerprint of a received Remediation Plan payload."""
    return canonical_fingerprint(
        {
            "quality_run_id": quality_run_id,
            "candidate_fingerprint": candidate_fingerprint,
            "policy_fingerprint": policy_fingerprint,
            "items": [item.model_dump(mode="json") for item in items],
        }
    )


def evaluation_payload_fingerprint(
    *,
    plan_id: str | None,
    plan_fingerprint: str | None,
    passed: bool,
    scores: Sequence[EvalScore],
    blocked_reasons: Sequence[str],
) -> str:
    """Canonical fingerprint of a received plan evaluation payload."""
    return canonical_fingerprint(
        {
            "plan_id": plan_id,
            "plan_fingerprint": plan_fingerprint,
            "passed": passed,
            "scores": [score.model_dump(mode="json") for score in scores],
            "blocked_reasons": list(blocked_reasons),
        }
    )


def admission_payload_fingerprint(
    *,
    quality_run_id: str,
    plan_id: str,
    plan_fingerprint: str,
    evaluation_id: str,
    evaluation_fingerprint: str,
    decision_id: str,
    decision_event_id: str,
    policy_fingerprint: str,
    issued_at: datetime,
    expires_at: datetime,
) -> str:
    """Canonical fingerprint of a received Apply Admission payload."""
    return canonical_fingerprint(
        {
            "quality_run_id": quality_run_id,
            "plan_id": plan_id,
            "plan_fingerprint": plan_fingerprint,
            "evaluation_id": evaluation_id,
            "evaluation_fingerprint": evaluation_fingerprint,
            "decision_id": decision_id,
            "decision_event_id": decision_event_id,
            "policy_fingerprint": policy_fingerprint,
            "issued_at": issued_at,
            "expires_at": expires_at,
        }
    )


def verify_plan_integrity(plan: RemediationPlan, *, refusing: str) -> None:
    expected = plan_payload_fingerprint(
        quality_run_id=plan.quality_run_id,
        candidate_fingerprint=plan.candidate_fingerprint,
        policy_fingerprint=plan.policy_fingerprint,
        items=plan.items,
    )
    if expected != plan.fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: remediation plan fingerprint does not match received payload"
        )


def verify_evaluation_integrity(
    plan: RemediationPlan, evaluation: EvalReport, *, refusing: str
) -> str:
    fingerprint = evaluation.fingerprint
    if not fingerprint:
        raise PermissionError(f"Refusing {refusing}: evaluation has no immutable fingerprint")
    expected = evaluation_payload_fingerprint(
        plan_id=evaluation.plan_id,
        plan_fingerprint=evaluation.plan_fingerprint,
        passed=evaluation.passed,
        scores=evaluation.scores,
        blocked_reasons=evaluation.blocked_reasons,
    )
    if expected != fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: evaluation fingerprint does not match received payload"
        )
    if evaluation.plan_id != plan.plan_id or evaluation.plan_fingerprint != plan.fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: evaluation does not belong to this remediation plan"
        )
    return fingerprint


def verify_admission_integrity(
    plan: RemediationPlan,
    evaluation: EvalReport,
    admission: ApplyAdmission,
    *,
    refusing: str,
) -> None:
    expected = admission_payload_fingerprint(
        quality_run_id=admission.quality_run_id,
        plan_id=admission.plan_id,
        plan_fingerprint=admission.plan_fingerprint,
        evaluation_id=admission.evaluation_id,
        evaluation_fingerprint=admission.evaluation_fingerprint,
        decision_id=admission.decision_id,
        decision_event_id=admission.decision_event_id,
        policy_fingerprint=admission.policy_fingerprint,
        issued_at=admission.issued_at,
        expires_at=admission.expires_at,
    )
    if expected != admission.fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: apply admission fingerprint does not match received payload"
        )
    if (
        admission.plan_id != plan.plan_id
        or admission.plan_fingerprint != plan.fingerprint
        or admission.quality_run_id != plan.quality_run_id
        or admission.evaluation_id != evaluation.evaluation_id
        or admission.evaluation_fingerprint != evaluation.fingerprint
    ):
        raise PermissionError(
            f"Refusing {refusing}: admission does not authorize this evaluated plan"
        )


def verify_executable_params(plan: RemediationPlan, *, refusing: str) -> None:
    """Re-derive item parameters from Quality Evidence and Check Policy."""
    for item in plan.items:
        if not isinstance(item, ExecutablePlanItem):
            continue
        try:
            specs = [get_check_spec(evidence.check_id) for evidence in item.evidence]
            if not specs:
                raise ValueError("executable item has no quality evidence")
            action = get_governed_action(item.action_id)
            derived = action.derive_params(specs[0])
            if any(action.derive_params(spec) != derived for spec in specs[1:]):
                raise ValueError("evidence requires incompatible controlled parameter values")
        except (KeyError, ValueError) as exc:
            raise PermissionError(
                f"Refusing {refusing}: item parameters do not match Check Policy"
            ) from exc
        if derived != item.params:
            raise PermissionError(f"Refusing {refusing}: item parameters do not match Check Policy")
