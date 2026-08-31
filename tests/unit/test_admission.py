from datetime import UTC, datetime, timedelta

import pytest

from airflow_dq_agent.apply import apply_plan
from airflow_dq_agent.contracts import (
    CandidateAction,
    EvalReport,
    HumanDecision,
    Proposal,
    QualityEvidence,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.evals import evaluate_plan
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.planning.admission import create_apply_admission
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.quality.registry import CHECK_SPECS


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:orders-null-v1")


def _evaluated_plan() -> tuple[RemediationPlan, EvalReport]:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped_report = report.model_copy(update={"checks": [failed]})
    candidate = Proposal(
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
    )
    plan = compile_remediation_plan(scoped_report, candidate, target_sets=_TargetSets())
    return plan, evaluate_plan(plan)


def test_unaudited_human_decision_cannot_create_apply_admission() -> None:
    plan, evaluation = _evaluated_plan()

    with pytest.raises(PermissionError, match="human decision has no durable audit event"):
        create_apply_admission(
            plan,
            evaluation,
            HumanDecision(decision="Approve", actor="approver-1", note="Reviewed target set."),
        )


def test_audited_approval_receives_time_bounded_apply_admission() -> None:
    plan, evaluation = _evaluated_plan()
    now = datetime(2026, 8, 30, tzinfo=UTC)

    admission = create_apply_admission(
        plan,
        evaluation,
        HumanDecision(
            decision="Approve",
            actor="approver-1",
            note="Reviewed target set.",
            audit_event_id="decision-event-1",
        ),
        now=now,
        ttl=timedelta(hours=24),
    )

    assert admission.plan_id == plan.plan_id
    assert admission.plan_fingerprint == plan.fingerprint
    assert admission.policy_fingerprint == plan.policy_fingerprint
    assert admission.decision_event_id == "decision-event-1"
    assert admission.expires_at == now + timedelta(hours=24)
    assert admission.fingerprint

    with pytest.raises(PermissionError, match="expired"):
        apply_plan(
            plan,
            evaluation,
            admission,
            dry_run=False,
            now=admission.expires_at + timedelta(seconds=1),
        )

    with pytest.raises(PermissionError, match="requires an apply admission"):
        apply_plan(plan, evaluation, dry_run=False)


def test_policy_drift_blocks_mutation_before_a_database_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, evaluation = _evaluated_plan()
    admission = create_apply_admission(
        plan,
        evaluation,
        HumanDecision(
            decision="Approve",
            actor="approver-1",
            note="Reviewed target set.",
            audit_event_id="decision-event-1",
        ),
    )
    check_id = "fact_orders.total_amount.completeness"
    spec = CHECK_SPECS[check_id]
    monkeypatch.setitem(CHECK_SPECS, check_id, spec.model_copy(update={"description": "drift"}))

    with pytest.raises(PermissionError, match="policy snapshot drifted"):
        apply_plan(plan, evaluation, admission, dry_run=False)
