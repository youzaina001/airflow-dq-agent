"""Shared sequencing of an already audited candidate into a sample-free review."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from airflow_dq_agent.contracts.models import AuditEvent, EvalReport, Proposal, QualitySuiteReport
from airflow_dq_agent.evals import evaluate_plan
from airflow_dq_agent.planning.compiler import TargetSetResolver, compile_remediation_plan
from airflow_dq_agent.planning.integrity import verify_report_integrity
from airflow_dq_agent.planning.review import build_approval_review, render_approval_review_body
from airflow_dq_agent.traces.lineage import evaluation_event, plan_event, review_event


class CandidateEvaluationFailed(ValueError):
    """A refused candidate cannot proceed to compilation or Human Decision."""


def prepare_plan_review(
    report: QualitySuiteReport,
    proposal: Proposal,
    *,
    candidate_evaluation: EvalReport,
    candidate_event_id: str,
    target_sets: TargetSetResolver,
    persist: Callable[[AuditEvent], object],
    ttl: timedelta = timedelta(hours=24),
) -> dict[str, Any]:
    """Reuse the caller's report root and candidate predecessor; persist before returning."""
    verify_report_integrity(report, refusing="plan compilation")
    if report.audit_event_id is None:
        raise RuntimeError("quality report has no persisted audit root")
    if not candidate_evaluation.passed:
        raise CandidateEvaluationFailed("Candidate Proposal evaluation failed")
    plan = compile_remediation_plan(report, proposal, target_sets=target_sets)
    compiled = plan_event(plan, candidate_event_id)
    persist(compiled)
    evaluation = evaluate_plan(plan)
    evaluated = evaluation_event(plan, evaluation, compiled)
    persist(evaluated)
    evaluation = evaluation.model_copy(update={"audit_event_id": evaluated.event_id})
    review = build_approval_review(plan, evaluation, ttl=ttl)
    reviewed = review_event(review, evaluation, evaluated)
    persist(reviewed)
    return {
        "plan": plan.model_dump(mode="json"),
        "plan_event_id": compiled.event_id,
        "evaluation": evaluation.model_dump(mode="json"),
        "evaluation_event_id": evaluated.event_id,
        "approval_review": review.model_dump(mode="json"),
        "approval_review_body": render_approval_review_body(review),
        "review_event_id": reviewed.event_id,
    }
