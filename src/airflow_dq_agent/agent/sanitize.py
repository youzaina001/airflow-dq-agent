"""Bound model output before it crosses the durable Airflow XCom boundary."""

from __future__ import annotations

from typing import Any

from airflow_dq_agent import check_policy
from airflow_dq_agent.check_policy import PolicyRefusal
from airflow_dq_agent.contracts.models import (
    CandidateAction,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
)


def safe_proposal_for_xcom(report: QualitySuiteReport, proposal: Proposal) -> dict[str, Any]:
    """Return an authority-only proposal payload with no model-authored text.

    A live proposer can read bounded row samples. Its free-text fields must therefore
    remain transient: the model could repeat a sampled value in any of them. Only
    identifiers that can be reconstructed from reviewed registries and the current
    quality report cross into XCom. Unexpected identifiers fail closed without being
    included in the error message or a task return value.
    """
    report_failures = {check.check_id: check for check in report.failed_checks}
    safe_actions: list[CandidateAction] = []
    for requested in proposal.candidate_actions:
        try:
            justification = check_policy.justify_action(
                action_id=requested.action_id,
                evidence=requested.evidence,
                report_failures=report_failures,
            )
        except PolicyRefusal as exc:
            raise PermissionError("Refusing to persist an unbounded candidate proposal") from exc

        safe_actions.append(
            CandidateAction(
                action_id=requested.action_id,
                evidence=[
                    QualityEvidence(
                        check_id=spec.check_id,
                        contract_id=report_failures[spec.check_id].contract_id,
                    )
                    for spec in justification.specs
                ],
                rationale="Requested for the cited Quality Evidence.",
            )
        )

    safe_proposal = Proposal(
        summary=f"Candidate Proposal contains {len(safe_actions)} governed action request(s).",
        root_cause_hypothesis="Model-authored narrative is not retained in durable task data.",
        candidate_actions=safe_actions,
        do_not_apply_reasons=["Deterministic evaluation and policy compilation are required."],
        confidence=0.0,
    )
    return safe_proposal.model_dump(mode="json")
