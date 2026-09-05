"""Sample-free whole-plan review shown before a Human Decision."""

from __future__ import annotations

from datetime import timedelta

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import (
    ApprovalReview,
    ApprovalReviewItem,
    ApprovalReviewScore,
    EvalReport,
    ExecutablePlanItem,
    RemediationPlan,
)


def _ttl_hours(ttl: timedelta) -> float:
    hours = ttl / timedelta(hours=1)
    if hours <= 0:
        raise ValueError("Apply Admission TTL must be positive")
    return hours


def _expiry_guidance(ttl_hours: float) -> str:
    return (
        f"Apply Admission expires {ttl_hours:g} hours after the Human Decision. "
        "If it expires or Remediation Target Sets drift, recompile the Remediation Plan "
        "and request a new Human Decision; do not apply."
    )


def build_approval_review(
    plan: RemediationPlan,
    evaluation: EvalReport,
    *,
    ttl: timedelta = timedelta(hours=24),
) -> ApprovalReview:
    """Build the canonical sample-free payload a human reviews before deciding."""
    ttl_hours = _ttl_hours(ttl)
    items = []
    for item in plan.items:
        if not isinstance(item, ExecutablePlanItem):
            continue
        action = get_governed_action(item.action_id)
        items.append(
            ApprovalReviewItem(
                action_id=item.action_id,
                table=item.table,
                evidence_check_ids=[evidence.check_id for evidence in item.evidence],
                target_count=item.target_set.count,
                target_fingerprint=item.target_set.fingerprint,
                mutates=action.mutates,
                reversible=action.metadata.reversible,
            )
        )
    scores = [
        ApprovalReviewScore(
            name=score.name,
            score=score.score,
            passed=score.passed,
            rationale=score.rationale,
        )
        for score in evaluation.scores
    ]
    if not evaluation.fingerprint:
        raise ValueError("evaluation has no immutable fingerprint")
    payload = {
        "plan_id": plan.plan_id,
        "plan_fingerprint": plan.fingerprint,
        "policy_fingerprint": plan.policy_fingerprint,
        "quality_run_id": plan.quality_run_id,
        "items": [item.model_dump(mode="json") for item in items],
        "evaluation_id": evaluation.evaluation_id,
        "evaluation_fingerprint": evaluation.fingerprint,
        "evaluation_passed": evaluation.passed,
        "evaluation_scores": [score.model_dump(mode="json") for score in scores],
        "admission_ttl_hours": ttl_hours,
        "expiry_guidance": _expiry_guidance(ttl_hours),
    }
    return ApprovalReview.model_validate({**payload, "fingerprint": canonical_fingerprint(payload)})


def render_approval_review_body(review: ApprovalReview) -> str:
    """Render the review as Airflow HITL body text with no row samples."""
    lines = [
        f"Remediation Plan {review.plan_id}",
        f"Plan fingerprint: {review.plan_fingerprint}",
        f"Check Policy fingerprint: {review.policy_fingerprint}",
        f"Quality run: {review.quality_run_id}",
        f"Review fingerprint: {review.fingerprint}",
        "",
        f"Evaluation: {'passed' if review.evaluation_passed else 'failed'}",
        f"Evaluation id: {review.evaluation_id}",
        f"Evaluation fingerprint: {review.evaluation_fingerprint}",
    ]
    for score in review.evaluation_scores:
        status = "passed" if score.passed else "failed"
        lines.append(f"- {score.name}: {score.score} ({status}) — {score.rationale}")
    lines.append("")
    lines.append("Executable items:")
    if not review.items:
        lines.append("- none")
    for item in review.items:
        mutates = "yes" if item.mutates else "no"
        reversible = "yes" if item.reversible else "no"
        checks = ", ".join(item.evidence_check_ids)
        lines.append(
            f"- {item.action_id} on {item.table}; mutates={mutates}; reversible={reversible}"
        )
        lines.append(f"  Quality Evidence: {checks}")
        lines.append(
            "  Remediation Target Set "
            f"count={item.target_count} fingerprint={item.target_fingerprint}"
        )
    lines.extend(
        [
            "",
            f"Apply Admission TTL: {review.admission_ttl_hours:g} hours after the Human Decision.",
            review.expiry_guidance,
            "",
            "Approve the whole plan or reject it. A note is required.",
        ]
    )
    return "\n".join(lines)
