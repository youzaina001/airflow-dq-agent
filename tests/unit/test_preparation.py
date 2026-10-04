"""The shared preparation seam returns the review actually persisted in Audit Lineage."""

from datetime import timedelta

import pytest

from airflow_dq_agent.agent import run_proposal_agent, safe_proposal_for_xcom
from airflow_dq_agent.contracts.fingerprints import (
    canonical_fingerprint,
    report_payload_fingerprint,
)
from airflow_dq_agent.contracts.models import AuditEvent, Proposal, TargetSet
from airflow_dq_agent.demo import seeded_failure_report
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.planning.preparation import prepare_plan_review
from airflow_dq_agent.traces import candidate_proposal_event


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:preparation")


def _inputs():
    report = seeded_failure_report()
    report = report.model_copy(
        update={
            "audit_event_id": "existing-root",
            "fingerprint": report_payload_fingerprint(report),
        }
    )
    proposal = Proposal.model_validate(
        safe_proposal_for_xcom(report, run_proposal_agent(report).proposal)
    )
    candidate = candidate_proposal_event(report, proposal, report.audit_event_id)
    return report, proposal, candidate


def test_preparation_persists_the_displayed_review_in_order_with_existing_lineage() -> None:
    report, proposal, candidate = _inputs()
    events: list[AuditEvent] = []

    prepared = prepare_plan_review(
        report,
        proposal,
        candidate_evaluation=evaluate_proposal(report, proposal),
        candidate_event_id=candidate.event_id,
        target_sets=_TargetSets(),
        persist=events.append,
        ttl=timedelta(hours=3),
    )

    assert [event.kind for event in events] == ["plan_compiled", "evaluation", "approval_review"]
    assert [event.predecessor_ids for event in events] == [
        [candidate.event_id],
        [events[0].event_id],
        [events[1].event_id],
    ]
    assert all(event.quality_run_id == report.run_id for event in events)
    assert all(event.plan_id == prepared["plan"]["plan_id"] for event in events)
    assert all(event.plan_fingerprint == prepared["plan"]["fingerprint"] for event in events)
    assert prepared["plan"]["candidate_fingerprint"] == candidate.candidate_fingerprint
    assert prepared["plan_event_id"] == events[0].event_id
    assert (
        prepared["evaluation_event_id"]
        == prepared["evaluation"]["audit_event_id"]
        == events[1].event_id
    )
    assert prepared["review_event_id"] == events[2].event_id
    review = prepared["approval_review"]
    assert review == events[2].review_payload
    assert review["evaluation_fingerprint"] == events[1].evaluation_fingerprint
    assert review["evaluation_id"] == events[1].evaluation_id
    assert review["evaluation_passed"] is True
    assert review["admission_ttl_hours"] == 3
    assert review["fingerprint"] == canonical_fingerprint(
        {k: v for k, v in review.items() if k != "fingerprint"}
    )
    assert review["fingerprint"] in prepared["approval_review_body"]


def test_failed_candidate_cannot_be_prepared_or_audited() -> None:
    report, proposal, candidate = _inputs()
    events: list[AuditEvent] = []
    evaluation = evaluate_proposal(report, proposal).model_copy(update={"passed": False})
    with pytest.raises(ValueError, match="Candidate Proposal evaluation failed"):
        prepare_plan_review(
            report,
            proposal,
            candidate_evaluation=evaluation,
            candidate_event_id=candidate.event_id,
            target_sets=_TargetSets(),
            persist=events.append,
        )
    assert events == []


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"audit_event_id": None}, "quality report has no persisted audit root"),
        ({"fingerprint": "tampered"}, "quality report fingerprint does not match"),
    ],
)
def test_preparation_requires_the_existing_intact_report_root(change, reason) -> None:
    report, proposal, candidate = _inputs()
    events: list[AuditEvent] = []
    with pytest.raises((RuntimeError, PermissionError), match=reason):
        prepare_plan_review(
            report.model_copy(update=change),
            proposal,
            candidate_evaluation=evaluate_proposal(report, proposal),
            candidate_event_id=candidate.event_id,
            target_sets=_TargetSets(),
            persist=events.append,
        )
    assert events == []


@pytest.mark.parametrize("failure_at", ["plan_compiled", "evaluation", "approval_review"])
def test_preparation_stops_on_each_audit_failure(failure_at: str) -> None:
    report, proposal, candidate = _inputs()
    kinds: list[str] = []

    def persist(event: AuditEvent) -> None:
        kinds.append(event.kind)
        if event.kind == failure_at:
            raise OSError("audit unavailable")

    with pytest.raises(OSError, match="audit unavailable"):
        prepare_plan_review(
            report,
            proposal,
            candidate_evaluation=evaluate_proposal(report, proposal),
            candidate_event_id=candidate.event_id,
            target_sets=_TargetSets(),
            persist=persist,
        )
    expected = ["plan_compiled", "evaluation", "approval_review"]
    assert kinds == expected[: expected.index(failure_at) + 1]


def test_blocked_preparation_keeps_controlled_reason_and_sample_free_review() -> None:
    import json

    report, proposal, candidate = _inputs()
    events: list[AuditEvent] = []

    class FailingTargets:
        def resolve(self, **_: object) -> TargetSet:
            raise ValueError("secret sampled row")

    prepared = prepare_plan_review(
        report,
        proposal,
        candidate_evaluation=evaluate_proposal(report, proposal),
        candidate_event_id=candidate.event_id,
        target_sets=FailingTargets(),
        persist=events.append,
    )
    assert events[0].kind == "plan_blocked"
    assert "remediation target set could not be resolved" in prepared["plan"]["blocked_reasons"]
    assert prepared["approval_review"]["evaluation_passed"] is False
    assert prepared["approval_review"]["items"] == []
    body = json.dumps([prepared, [event.model_dump(mode="json") for event in events]])
    assert "secret sampled row" not in body
    assert "sample_failures" not in body
