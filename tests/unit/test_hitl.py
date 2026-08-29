from airflow_dq_agent.hitl import audit_approval_decision, parse_approval_output
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.traces import quality_report_event


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
