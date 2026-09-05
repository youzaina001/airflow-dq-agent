"""Bound model output before it crosses the durable Airflow XCom boundary."""

from __future__ import annotations

from typing import Any

from airflow_dq_agent.action_definitions import is_governed_action
from airflow_dq_agent.contracts.models import (
    CandidateAction,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
)
from airflow_dq_agent.quality.registry import get_check_spec


def safe_proposal_for_xcom(report: QualitySuiteReport, proposal: Proposal) -> dict[str, Any]:
    """Return an authority-only proposal payload with no model-authored text.

    A live proposer can read bounded row samples. Its free-text fields must therefore
    remain transient: the model could repeat a sampled value in any of them. Only
    identifiers that can be reconstructed from reviewed registries and the current
    quality report cross into XCom. Unexpected identifiers fail closed without being
    included in the error message or a task return value.
    """
    report_checks = {check.check_id: check for check in report.checks}
    safe_actions: list[CandidateAction] = []
    for requested in proposal.candidate_actions:
        if not is_governed_action(requested.action_id):
            raise PermissionError("Refusing to persist an unbounded candidate proposal")

        safe_evidence: list[QualityEvidence] = []
        for evidence in requested.evidence:
            check = report_checks.get(evidence.check_id)
            if check is None or evidence.contract_id != check.contract_id:
                raise PermissionError("Refusing to persist an unbounded candidate proposal")
            try:
                spec = get_check_spec(check.check_id)
            except KeyError as exc:
                raise PermissionError(
                    "Refusing to persist an unbounded candidate proposal"
                ) from exc
            if spec.rule_for(requested.action_id) is None:
                raise PermissionError("Refusing to persist an unbounded candidate proposal")
            safe_evidence.append(
                QualityEvidence(check_id=check.check_id, contract_id=check.contract_id)
            )

        safe_actions.append(
            CandidateAction(
                action_id=requested.action_id,
                evidence=safe_evidence,
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
