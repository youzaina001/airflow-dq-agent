"""Live proposal output must be bounded before Airflow persists it in XCom."""

import json

import pytest

from airflow_dq_agent.agent import run_proposal_agent, safe_proposal_for_xcom
from airflow_dq_agent.contracts import Proposal, QualitySuiteReport
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.quality import seeded_failure_report

SAMPLED_VALUE = "customer-raw-value@example.invalid"


def _echoing_proposal() -> tuple[QualitySuiteReport, Proposal]:
    report = seeded_failure_report()
    base = run_proposal_agent(report).proposal
    actions = [
        action.model_copy(update={"rationale": SAMPLED_VALUE}) for action in base.candidate_actions
    ]
    proposal = base.model_copy(
        update={
            "proposal_id": SAMPLED_VALUE,
            "fingerprint": SAMPLED_VALUE,
            "summary": SAMPLED_VALUE,
            "root_cause_hypothesis": SAMPLED_VALUE,
            "candidate_actions": actions,
            "do_not_apply_reasons": [SAMPLED_VALUE],
            "confidence": 0.53,
        }
    )
    return report, proposal


def test_model_authored_values_are_removed_from_xcom_proposal() -> None:
    report, proposal = _echoing_proposal()

    payload = safe_proposal_for_xcom(report, proposal)

    assert SAMPLED_VALUE not in json.dumps(payload)
    assert payload["proposal_id"] != proposal.proposal_id
    assert payload["fingerprint"] is None
    assert payload["confidence"] == 0.0
    revived = Proposal.model_validate(payload)
    assert evaluate_proposal(report, revived).passed
    assert [action.action_id for action in revived.candidate_actions] == [
        action.action_id for action in proposal.candidate_actions
    ]


@pytest.mark.parametrize("field", ["action_id", "check_id", "contract_id"])
def test_unbounded_authority_identifier_fails_before_xcom(field: str) -> None:
    report, proposal = _echoing_proposal()
    first = proposal.candidate_actions[0]
    evidence = first.evidence[0]
    if field == "action_id":
        first = first.model_copy(update={"action_id": SAMPLED_VALUE})
    elif field == "check_id":
        first = first.model_copy(
            update={"evidence": [evidence.model_copy(update={"check_id": SAMPLED_VALUE})]}
        )
    else:
        first = first.model_copy(
            update={"evidence": [evidence.model_copy(update={"contract_id": SAMPLED_VALUE})]}
        )
    proposal = proposal.model_copy(update={"candidate_actions": [first]})

    with pytest.raises(PermissionError, match="unbounded candidate proposal") as exc_info:
        safe_proposal_for_xcom(report, proposal)

    assert SAMPLED_VALUE not in str(exc_info.value)
