import json
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from airflow_dq_agent.contracts import (
    ApprovalReview,
    CandidateAction,
    EvalReport,
    Proposal,
    QualityEvidence,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.evals import evaluate_plan
from airflow_dq_agent.hitl import (
    audit_approval_decision,
    audit_then_complete_approval,
    parse_approval_output,
)
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.planning.review import build_approval_review, render_approval_review_body
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.traces import quality_report_event


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:orders-null-v1")


def _evaluated_plan() -> tuple[RemediationPlan, EvalReport]:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    plan = compile_remediation_plan(
        report.model_copy(update={"checks": [failed]}),
        Proposal(
            summary="Quarantine failed rows.",
            root_cause_hypothesis="A required value was omitted.",
            candidate_actions=[
                CandidateAction(
                    action_id="quarantine_nulls",
                    evidence=[
                        QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                    ],
                    rationale="Preserve source rows for review.",
                )
            ],
            confidence=0.9,
        ),
        target_sets=_TargetSets(),
    )
    return plan, evaluate_plan(plan)


def test_structured_approval_requires_allowlisted_actor_and_note() -> None:
    decision = parse_approval_output(
        {
            "chosen_options": ["Approve"],
            "params_input": {"approval_note": "Reviewed exact target counts."},
            "responded_by_user": {"id": "approver-1", "name": "Reviewer"},
            "responded_at": "2026-08-30T12:00:00Z",
            "timedout": False,
        },
        approver_ids={"approver-1"},
    )

    assert decision.decision == "Approve"
    assert decision.actor == "approver-1"
    assert decision.note == "Reviewed exact target counts."


def test_structured_timeout_is_not_a_human_approval() -> None:
    decision = parse_approval_output(
        {
            "chosen_options": ["Reject"],
            "params_input": {"approval_note": ""},
            "responded_by_user": None,
            "timedout": True,
        },
        approver_ids={"approver-1"},
    )

    assert decision.decision == "Timeout"
    assert decision.actor == "airflow-timeout"


def test_audited_approval_persists_the_actor_and_note_before_returning() -> None:
    report = seeded_failure_report()
    events = []
    decision = audit_approval_decision(
        {
            "chosen_options": ["Approve"],
            "params_input": {"approval_note": "Reviewed exact target counts."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=report.run_id,
        predecessor=quality_report_event(report),
        persist=events.append,
    )

    assert decision.audit_event_id == events[0].event_id
    assert events[0].decision_actor == "approver-1"
    assert events[0].decision_note == "Reviewed exact target counts."
    assert events[0].decision_fingerprint
    assert decision.fingerprint
    assert decision.fingerprint == events[0].decision_fingerprint


def test_audited_approval_returns_fingerprint_matching_persisted_event() -> None:
    report = seeded_failure_report()
    events = []
    decision = audit_approval_decision(
        {
            "chosen_options": ["Approve"],
            "params_input": {"approval_note": "Reviewed exact target counts."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=report.run_id,
        predecessor=quality_report_event(report),
        persist=events.append,
    )

    assert decision.fingerprint
    assert decision.fingerprint == events[0].decision_fingerprint


def test_audited_rejection_is_persisted_before_airflow_can_skip_downstream_tasks() -> None:
    report = seeded_failure_report()
    events = []
    decision = audit_approval_decision(
        {
            "chosen_options": ["Reject"],
            "params_input": {"approval_note": "Target scope needs review."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=report.run_id,
        predecessor=quality_report_event(report),
        persist=events.append,
    )

    assert decision.decision == "Reject"
    assert decision.audit_event_id == events[0].event_id
    assert events[0].decision_outcome == "Reject"
    assert decision.fingerprint
    assert decision.fingerprint == events[0].decision_fingerprint


def test_rejection_is_persisted_before_the_provider_branch_callback() -> None:
    report = seeded_failure_report()
    order: list[str] = []

    decision = audit_then_complete_approval(
        {
            "chosen_options": ["Reject"],
            "params_input": {"approval_note": "Target scope needs review."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=report.run_id,
        predecessor=quality_report_event(report),
        persist=lambda event: order.append(f"audit:{event.event_id}"),
        complete_provider=lambda: order.append("provider-branch"),
    )

    assert decision.decision == "Reject"
    assert order[0].startswith("audit:")
    assert order[1] == "provider-branch"


def test_approval_review_is_sample_free_and_names_the_executable_plan() -> None:
    plan, evaluation = _evaluated_plan()
    review = build_approval_review(plan, evaluation, ttl=timedelta(hours=24))
    payload = json.dumps(review.model_dump(mode="json"))
    item = review.items[0]

    assert review.plan_id == plan.plan_id
    assert review.plan_fingerprint == plan.fingerprint
    assert review.policy_fingerprint == plan.policy_fingerprint
    assert review.quality_run_id == plan.quality_run_id
    assert review.evaluation_passed is True
    assert review.evaluation_id == evaluation.evaluation_id
    assert review.evaluation_fingerprint == evaluation.fingerprint
    assert {score.name for score in review.evaluation_scores} == {
        score.name for score in evaluation.scores
    }
    assert item.action_id == "quarantine_nulls"
    assert item.table == "fact_orders"
    assert item.evidence_check_ids == ["fact_orders.total_amount.completeness"]
    assert item.target_count == 5
    assert item.target_fingerprint == "targets:orders-null-v1"
    assert item.mutates is True
    assert review.admission_ttl_hours == 24
    assert "recompile" in review.expiry_guidance.lower()
    assert "sample_failures" not in payload
    assert "params" not in payload
    assert '"order_id": 9001' not in payload
    assert "SHIPPPED" not in payload

    with pytest.raises(ValidationError):
        ApprovalReview.model_validate({**review.model_dump(mode="json"), "sample_failures": []})


def test_approval_review_body_renders_the_canonical_payload() -> None:
    plan, evaluation = _evaluated_plan()
    review = build_approval_review(plan, evaluation, ttl=timedelta(hours=24))
    body = render_approval_review_body(review)

    assert plan.plan_id in body
    assert plan.fingerprint in body
    assert plan.policy_fingerprint in body
    assert "quarantine_nulls" in body
    assert "fact_orders" in body
    assert "fact_orders.total_amount.completeness" in body
    assert "count=5" in body
    assert "targets:orders-null-v1" in body
    assert "mutates=yes" in body
    assert "Evaluation: passed" in body
    assert "Apply Admission TTL: 24 hours" in body
    assert "Approve the whole plan or reject it. A note is required." in body
    assert "sample_failures" not in body
    assert "9001" not in body
    assert "SHIPPPED" not in body


def test_audited_approval_binds_the_shown_review_fingerprint() -> None:
    plan, evaluation = _evaluated_plan()
    review = build_approval_review(plan, evaluation)
    events = []
    decision = audit_approval_decision(
        {
            "chosen_options": ["Approve"],
            "params_input": {"approval_note": "Reviewed exact target counts."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=plan.quality_run_id,
        predecessor=quality_report_event(seeded_failure_report()),
        persist=events.append,
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        review_fingerprint=review.fingerprint,
    )

    assert decision.review_fingerprint == review.fingerprint
    assert decision.fingerprint != review.fingerprint
    assert events[0].review_fingerprint == review.fingerprint
    assert events[0].decision_fingerprint == decision.fingerprint
    assert events[0].plan_id == plan.plan_id
    assert events[0].plan_fingerprint == plan.fingerprint
    assert events[0].kind == "human_approved"


def test_dag_uses_sample_free_approval_review_body() -> None:
    source = Path(__file__).resolve().parents[2] / "dags" / "dq_daily.py"
    text = source.read_text(encoding="utf-8")
    assert "Evaluation passed. Approve the whole plan or reject it. A note is required." not in text
    assert "build_approval_review" in text
    assert "approval_review_body" in text
    assert "review_event" in text
    assert "review_event_id" in text
    assert "PostgresAuditRepository" in text
