from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest

from airflow_dq_agent.apply import apply_plan
from airflow_dq_agent.contracts import (
    AuditEvent,
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
from airflow_dq_agent.planning.review import build_approval_review
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.traces import InMemoryAuditRepository, PostgresAuditRepository
from airflow_dq_agent.traces.lineage import decision_event, quality_report_event, review_event


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


def _bound_approval(
    plan: RemediationPlan,
    evaluation: EvalReport,
    *,
    actor: str = "approver-1",
    note: str = "Reviewed target set.",
    ttl: timedelta = timedelta(hours=24),
    quality_run_id: str | None = None,
    plan_id: str | None = None,
    plan_fingerprint: str | None = None,
    review_fingerprint: str | None = None,
    outcome: Literal["Approve", "Reject", "Timeout"] = "Approve",
) -> tuple[HumanDecision, InMemoryAuditRepository]:
    review = build_approval_review(plan, evaluation, ttl=ttl)
    shown = review_event(review, evaluation, "evaluation-event-1")
    decision = HumanDecision(
        decision=outcome,
        actor=actor,
        note=note,
        review_fingerprint=review_fingerprint
        if review_fingerprint is not None
        else review.fingerprint,
    )
    event = decision_event(
        quality_run_id or plan.quality_run_id,
        decision,
        shown,
        plan_id=plan.plan_id if plan_id is None else plan_id,
        plan_fingerprint=plan.fingerprint if plan_fingerprint is None else plan_fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation.fingerprint,
    )
    return (
        decision.model_copy(update={"audit_event_id": event.event_id}),
        InMemoryAuditRepository([shown, event]),
    )


def _assert_refusal_is_safe(exc: BaseException) -> None:
    message = str(exc)
    lowered = message.lower()
    assert "sample" not in lowered
    assert "sample_failures" not in lowered
    assert "9001" not in message
    assert "shippped" not in lowered


def test_unaudited_human_decision_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()

    with pytest.raises(PermissionError, match="human decision has no durable audit event"):
        create_apply_admission(
            plan,
            evaluation,
            HumanDecision(decision="Approve", actor="approver-1", note="Reviewed target set."),
            report=report,
            audit_repository=InMemoryAuditRepository(),
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
            audit_repository=InMemoryAuditRepository(),
        )


def test_admission_fails_closed_without_audit_lineage_lookup() -> None:
    plan, evaluation, report = _evaluated_plan()
    review = build_approval_review(plan, evaluation)

    with pytest.raises(PermissionError, match="audit lineage lookup is required") as refused:
        create_apply_admission(
            plan,
            evaluation,
            HumanDecision(
                decision="Approve",
                actor="approver-1",
                note="Reviewed target set.",
                review_fingerprint=review.fingerprint,
                audit_event_id="decision-event-1",
            ),
            report=report,
        )
    _assert_refusal_is_safe(refused.value)


def test_fabricated_decision_event_is_refused_without_a_repository_hit() -> None:
    plan, evaluation, report = _evaluated_plan()
    review = build_approval_review(plan, evaluation)

    with pytest.raises(PermissionError, match="audit event was not found") as refused:
        create_apply_admission(
            plan,
            evaluation,
            HumanDecision(
                decision="Approve",
                actor="approver-1",
                note="Reviewed target set.",
                review_fingerprint=review.fingerprint,
                audit_event_id="decision-event-1",
            ),
            report=report,
            audit_repository=InMemoryAuditRepository(),
        )
    _assert_refusal_is_safe(refused.value)


@pytest.mark.parametrize("outcome", ["Reject", "Timeout"])
def test_non_approval_cannot_create_apply_admission(outcome: str) -> None:
    plan, evaluation, report = _evaluated_plan()
    decision, repository = _bound_approval(plan, evaluation, outcome=outcome)

    with pytest.raises(PermissionError, match="not an approval") as refused:
        create_apply_admission(
            plan, evaluation, decision, report=report, audit_repository=repository
        )
    _assert_refusal_is_safe(refused.value)


def test_reject_audit_event_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    review = build_approval_review(plan, evaluation)
    rejected = HumanDecision(
        decision="Reject",
        actor="approver-1",
        note="Needs another look.",
        fingerprint=review.fingerprint,
    )
    event = decision_event(
        plan.quality_run_id,
        rejected,
        "evaluation-event-1",
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
    )
    forged = rejected.model_copy(update={"decision": "Approve", "audit_event_id": event.event_id})

    with pytest.raises(PermissionError, match="not a human approval") as refused:
        create_apply_admission(
            plan,
            evaluation,
            forged,
            report=report,
            audit_repository=InMemoryAuditRepository([event]),
        )
    _assert_refusal_is_safe(refused.value)


def test_timeout_audit_event_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    review = build_approval_review(plan, evaluation)
    timed_out = HumanDecision(
        decision="Timeout",
        actor="airflow-timeout",
        note="waited",
        fingerprint=review.fingerprint,
    )
    event = decision_event(
        plan.quality_run_id,
        timed_out,
        "evaluation-event-1",
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
    )
    forged = HumanDecision(
        decision="Approve",
        actor="approver-1",
        note="Reviewed target set.",
        fingerprint=review.fingerprint,
        audit_event_id=event.event_id,
    )

    with pytest.raises(PermissionError, match="not a human approval") as refused:
        create_apply_admission(
            plan,
            evaluation,
            forged,
            report=report,
            audit_repository=InMemoryAuditRepository([event]),
        )
    _assert_refusal_is_safe(refused.value)


def test_approval_for_another_plan_or_run_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    decision, other_run = _bound_approval(plan, evaluation, quality_run_id="other-quality-run")
    with pytest.raises(PermissionError, match="does not belong to this quality run") as refused:
        create_apply_admission(
            plan, evaluation, decision, report=report, audit_repository=other_run
        )
    _assert_refusal_is_safe(refused.value)

    decision, other_plan = _bound_approval(plan, evaluation, plan_id="other-plan")
    with pytest.raises(
        PermissionError, match="does not belong to this remediation plan"
    ) as refused:
        create_apply_admission(
            plan, evaluation, decision, report=report, audit_repository=other_plan
        )
    _assert_refusal_is_safe(refused.value)

    decision, other_fp = _bound_approval(plan, evaluation, plan_fingerprint="sha256:other")
    with pytest.raises(
        PermissionError, match="does not belong to this remediation plan"
    ) as refused:
        create_apply_admission(plan, evaluation, decision, report=report, audit_repository=other_fp)
    _assert_refusal_is_safe(refused.value)


def test_approval_for_another_evaluation_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    other = evaluation.model_copy(update={"evaluation_id": "other-evaluation"})
    assert other.fingerprint == evaluation.fingerprint
    decision, repository = _bound_approval(plan, evaluation)

    with pytest.raises(PermissionError, match="evaluation") as refused:
        create_apply_admission(plan, other, decision, report=report, audit_repository=repository)
    _assert_refusal_is_safe(refused.value)


def test_approval_with_mismatched_decision_id_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    decision, repository = _bound_approval(plan, evaluation)
    swapped = decision.model_copy(update={"decision_id": "forged-decision"})

    with pytest.raises(PermissionError, match="does not match audit lineage") as refused:
        create_apply_admission(
            plan, evaluation, swapped, report=report, audit_repository=repository
        )
    _assert_refusal_is_safe(refused.value)


def test_approval_event_with_non_approve_outcome_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    decision, repository = _bound_approval(plan, evaluation)
    event = repository.get(decision.audit_event_id or "")
    assert event is not None
    lying = event.model_copy(update={"decision_outcome": "Reject"})

    with pytest.raises(PermissionError, match="not a human approval") as refused:
        create_apply_admission(
            plan,
            evaluation,
            decision,
            report=report,
            audit_repository=InMemoryAuditRepository([lying]),
        )
    _assert_refusal_is_safe(refused.value)


def test_postgres_audit_repository_returns_none_for_missing_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _EmptyResult:
        def first(self) -> None:
            return None

    class _Connection:
        def execute(self, statement: object, params: object = None) -> _EmptyResult:
            del statement, params
            return _EmptyResult()

        def __enter__(self) -> object:
            return self

        def __exit__(self, *_: object) -> None:
            return None

    class _Engine:
        def connect(self) -> _Connection:
            return _Connection()

    monkeypatch.setattr(
        "airflow_dq_agent.traces.repository.make_engine", lambda *_a, **_k: _Engine()
    )

    assert PostgresAuditRepository("postgresql+psycopg://unused").get("decision-event-1") is None


def test_approval_from_another_actor_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    decision, repository = _bound_approval(plan, evaluation, actor="approver-1")
    swapped = decision.model_copy(update={"actor": "approver-2"})

    with pytest.raises(PermissionError, match="actor does not match audit lineage") as refused:
        create_apply_admission(
            plan, evaluation, swapped, report=report, audit_repository=repository
        )
    _assert_refusal_is_safe(refused.value)


def test_approval_with_mismatched_review_fingerprint_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    decision, repository = _bound_approval(
        plan, evaluation, review_fingerprint="sha256:not-the-review"
    )

    with pytest.raises(PermissionError, match="does not bind the reviewed plan") as refused:
        create_apply_admission(
            plan, evaluation, decision, report=report, audit_repository=repository
        )
    _assert_refusal_is_safe(refused.value)


def test_audited_approval_of_the_shown_review_receives_time_bounded_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    now = datetime(2026, 8, 30, tzinfo=UTC)
    review = build_approval_review(plan, evaluation, ttl=timedelta(hours=24))
    review_audit = review_event(review, evaluation, "evaluation-event-1")
    events: list[AuditEvent] = [review_audit]
    decision = audit_approval_decision(
        {
            "chosen_options": ["Approve"],
            "params_input": {"approval_note": "Reviewed target set."},
            "responded_by_user": {"id": "approver-1"},
            "timedout": False,
        },
        approver_ids={"approver-1"},
        quality_run_id=plan.quality_run_id,
        predecessor=review_audit,
        persist=events.append,
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        review_fingerprint=review.fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation.fingerprint,
    )

    admission = create_apply_admission(
        plan,
        evaluation,
        decision,
        now=now,
        ttl=timedelta(hours=24),
        report=report,
        audit_repository=InMemoryAuditRepository(events),
    )

    assert admission.plan_id == plan.plan_id
    assert admission.plan_fingerprint == plan.fingerprint
    assert admission.policy_fingerprint == plan.policy_fingerprint
    assert admission.decision_event_id == decision.audit_event_id
    assert admission.expires_at == now + timedelta(hours=24)
    assert admission.fingerprint
    assert decision.review_fingerprint == review.fingerprint
    assert decision.fingerprint != review.fingerprint
    assert events[-1].decision_fingerprint == decision.fingerprint
    assert events[-1].review_fingerprint == review.fingerprint
    assert events[-1].evaluation_id == evaluation.evaluation_id

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
    decision, repository = _bound_approval(plan, evaluation)
    admission = create_apply_admission(
        plan, evaluation, decision, report=report, audit_repository=repository
    )
    check_id = "fact_orders.total_amount.completeness"
    spec = CHECK_SPECS[check_id]
    monkeypatch.setitem(CHECK_SPECS, check_id, spec.model_copy(update={"description": "drift"}))

    with pytest.raises(PermissionError, match="policy snapshot drifted"):
        apply_plan(plan, evaluation, admission, report=report, dry_run=False)


def test_audited_reject_rewritten_to_approve_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    rejected, repository = _bound_approval(plan, evaluation, outcome="Reject")
    rewritten = rejected.model_copy(
        update={
            "decision": "Approve",
            "note": rejected.note or "Reviewed target set.",
        }
    )
    assert rewritten.review_fingerprint == rejected.review_fingerprint

    with pytest.raises(PermissionError):
        create_apply_admission(
            plan, evaluation, rewritten, report=report, audit_repository=repository
        )

    approved, approved_repository = _bound_approval(plan, evaluation)
    admission = create_apply_admission(
        plan, evaluation, approved, report=report, audit_repository=approved_repository
    )
    assert admission.decision_id == approved.decision_id
    assert admission.decision_event_id == approved.audit_event_id


def test_rewritten_actor_or_note_after_audit_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    events = []
    review = build_approval_review(plan, evaluation)
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
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        review_fingerprint=review.fingerprint,
    )

    for rewritten in (
        approved.model_copy(update={"actor": "forged-approver"}),
        approved.model_copy(update={"note": "forged approval note"}),
    ):
        assert rewritten.fingerprint == approved.fingerprint
        with pytest.raises(PermissionError):
            create_apply_admission(
                plan,
                evaluation,
                rewritten,
                report=report,
                audit_repository=InMemoryAuditRepository(events),
            )


def test_forged_decision_fingerprint_cannot_create_apply_admission() -> None:
    plan, evaluation, report = _evaluated_plan()
    decision, repository = _bound_approval(plan, evaluation)
    forged = decision.model_copy(update={"fingerprint": "sha256:forged-decision"})

    with pytest.raises(PermissionError, match="fingerprint") as refused:
        create_apply_admission(
            plan, evaluation, forged, report=report, audit_repository=repository
        )
    _assert_refusal_is_safe(refused.value)
