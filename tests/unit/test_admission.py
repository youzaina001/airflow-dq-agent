from datetime import UTC, datetime, timedelta

import pytest

from airflow_dq_agent.apply import apply_plan
from airflow_dq_agent.contracts import (
    CandidateAction,
    EvalReport,
    HumanDecision,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.contracts.fingerprints import report_payload_fingerprint
from airflow_dq_agent.evals import evaluate_plan
from airflow_dq_agent.hitl import audit_approval_decision
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.planning.admission import create_apply_admission
from airflow_dq_agent.planning.integrity import decision_payload_fingerprint
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.traces.lineage import quality_report_event


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:orders-null-v1")


def _evaluated_plan() -> tuple[RemediationPlan, EvalReport, QualitySuiteReport]:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped_report = report.model_copy(update={"checks": [failed]})
    scoped_report = scoped_report.model_copy(
        update={"fingerprint": report_payload_fingerprint(scoped_report)}
    )
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
    return plan, evaluate_plan(plan), scoped_report


def _with_decision_fingerprint(decision: HumanDecision) -> HumanDecision:
    return decision.model_copy(
        update={
            "fingerprint": decision_payload_fingerprint(
                decision_id=decision.decision_id,
                decision=decision.decision,
                actor=decision.actor,
                note=decision.note,
                decided_at=decision.decided_at,
            )
        }
    )


def _audited_approval() -> HumanDecision:
    return _with_decision_fingerprint(
        HumanDecision(
            decision="Approve",
            actor="approver-1",
            note="Reviewed target set.",
            audit_event_id="decision-event-1",
        )
    )


def test_unaudited_human_decision_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()

    with pytest.raises(PermissionError, match="human decision has no durable audit event"):
        create_apply_admission(
            plan,
            evaluation,
            HumanDecision(decision="Approve", actor="approver-1", note="Reviewed target set."),
            report=report,
        )


@pytest.mark.parametrize("audit_event_id", ["", " ", "\t\n"])
def test_blank_audit_event_id_cannot_create_apply_admission(audit_event_id: str) -> None:
    plan, evaluation, report = _evaluated_plan()

    with pytest.raises(PermissionError, match="human decision has no durable audit event"):
        create_apply_admission(
            plan,
            evaluation,
            HumanDecision(
                decision="Approve",
                actor="approver-1",
                note="Reviewed target set.",
                audit_event_id=audit_event_id,
            ),
            report=report,
        )


def test_audited_approval_receives_time_bounded_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    now = datetime(2026, 8, 30, tzinfo=UTC)

    admission = create_apply_admission(
        plan,
        evaluation,
        _audited_approval(),
        report=report,
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
            report=report,
            dry_run=False,
            now=admission.expires_at + timedelta(seconds=1),
        )

    with pytest.raises(PermissionError, match="requires an apply admission"):
        apply_plan(plan, evaluation, report=report, dry_run=False)


def test_policy_drift_blocks_mutation_before_a_database_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, evaluation, report = _evaluated_plan()
    admission = create_apply_admission(
        plan,
        evaluation,
        _audited_approval(),
        report=report,
    )
    check_id = "fact_orders.total_amount.completeness"
    spec = CHECK_SPECS[check_id]
    monkeypatch.setitem(CHECK_SPECS, check_id, spec.model_copy(update={"description": "drift"}))

    with pytest.raises(PermissionError, match="policy snapshot drifted"):
        apply_plan(plan, evaluation, admission, report=report, dry_run=False)


def test_audited_reject_rewritten_to_approve_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    predecessor = quality_report_event(report)
    events = []
    rejected = audit_approval_decision(
        {
            "chosen_options": ["Reject"],
            "params_input": {"approval_note": "Target scope needs review."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=report.run_id,
        predecessor=predecessor,
        persist=events.append,
    )
    rewritten = rejected.model_copy(
        update={
            "decision": "Approve",
            "note": rejected.note or "Reviewed target set.",
        }
    )
    assert rewritten.fingerprint == rejected.fingerprint

    with pytest.raises(PermissionError):
        create_apply_admission(plan, evaluation, rewritten, report=report)

    approved = audit_approval_decision(
        {
            "chosen_options": ["Approve"],
            "params_input": {"approval_note": "Reviewed target set."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=report.run_id,
        predecessor=predecessor,
        persist=events.append,
    )
    admission = create_apply_admission(plan, evaluation, approved, report=report)
    assert admission.decision_id == approved.decision_id
    assert admission.decision_event_id == approved.audit_event_id


def test_rewritten_actor_or_note_after_audit_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    events = []
    approved = audit_approval_decision(
        {
            "chosen_options": ["Approve"],
            "params_input": {"approval_note": "Reviewed target set."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=report.run_id,
        predecessor=quality_report_event(report),
        persist=events.append,
    )

    for rewritten in (
        approved.model_copy(update={"actor": "forged-approver"}),
        approved.model_copy(update={"note": "forged approval note"}),
    ):
        assert rewritten.fingerprint == approved.fingerprint
        with pytest.raises(PermissionError):
            create_apply_admission(plan, evaluation, rewritten, report=report)


@pytest.mark.parametrize("fingerprint", [None, "", " "])
def test_missing_or_blank_decision_fingerprint_cannot_create_apply_admission(
    fingerprint: str | None,
) -> None:
    plan, evaluation, report = _evaluated_plan()
    with pytest.raises(PermissionError, match="fingerprint"):
        create_apply_admission(
            plan,
            evaluation,
            HumanDecision(
                decision="Approve",
                actor="approver-1",
                note="Reviewed target set.",
                audit_event_id="decision-event-1",
                fingerprint=fingerprint,
            ),
            report=report,
        )
