"""Deterministic candidate and remediation-plan evaluation gates."""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from airflow_dq_agent.action_definitions import is_governed_action
from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts.models import (
    EvalReport,
    EvalScore,
    Proposal,
    QualitySuiteReport,
    RemediationPlan,
)
from airflow_dq_agent.planning import current_policy_fingerprint
from airflow_dq_agent.planning.integrity import (
    evaluation_payload_fingerprint,
    verify_plan_integrity,
)

_DESTRUCTIVE_TOKENS = ("DROP", "TRUNCATE", "ALTER", "DELETE")


def _score(name: str, score: float, threshold: float, rationale: str, **details: Any) -> EvalScore:
    return EvalScore(
        name=name,
        score=score,
        passed=score >= threshold,
        rationale=rationale,
        details=details,
    )


def _parse_proposal(proposal: Proposal | dict[str, Any]) -> tuple[Proposal | None, EvalScore]:
    try:
        parsed = Proposal.model_validate(proposal)
    except ValidationError as exc:
        return None, _score(
            "schema_validity",
            0.0,
            1.0,
            "Candidate Proposal does not satisfy the Pydantic output contract.",
            errors=exc.errors(include_url=False),
        )
    return parsed, _score(
        "schema_validity", 1.0, 1.0, "Candidate Proposal satisfies the Pydantic output contract."
    )


def _evidence_score(report: QualitySuiteReport, proposal: Proposal, threshold: float) -> EvalScore:
    failures = {check.check_id: check.contract_id for check in report.failed_checks}
    evidence = [item for action in proposal.candidate_actions for item in action.evidence]
    unknown = sorted(item.check_id for item in evidence if item.check_id not in failures)
    mismatched = sorted(
        item.check_id
        for item in evidence
        if item.check_id in failures and item.contract_id != failures[item.check_id]
    )
    if not failures:
        valid = not proposal.candidate_actions
        score = 1.0 if valid else 0.0
    elif unknown or mismatched:
        score = 0.0
    else:
        cited = {item.check_id for item in evidence}
        score = len(cited & set(failures)) / len(failures)
    return _score(
        "citation_quality",
        score,
        threshold,
        "Candidate evidence refers only to failed checks with matching contract identities."
        if score >= threshold
        else "Candidate evidence is incomplete, invented, or has a mismatched contract identity.",
        unknown=unknown,
        mismatched_contracts=mismatched,
    )


def _groundedness_score(
    report: QualitySuiteReport, proposal: Proposal, threshold: float
) -> EvalScore:
    errors: list[str] = []
    if not report.failed_count and proposal.candidate_actions:
        errors.append("green quality report cannot have candidate actions")
    for action in proposal.candidate_actions:
        if not action.evidence:
            errors.append("candidate action has no quality evidence")
        if not action.action_id.strip() or any(char.isspace() for char in action.action_id):
            errors.append("candidate action ID is not a non-empty slug")
    return _score(
        "groundedness",
        1.0 if not errors else 0.0,
        threshold,
        "Every candidate action is attached to report-scoped quality evidence."
        if not errors
        else "Candidate actions are not grounded in this quality report.",
        errors=errors,
    )


def _destructive_score(proposal: Proposal) -> EvalScore:
    findings = [
        action.action_id
        for action in proposal.candidate_actions
        if any(token in action.action_id.upper() for token in _DESTRUCTIVE_TOKENS)
    ]
    return _score(
        "destructive_risk",
        0.0 if findings else 1.0,
        1.0,
        "No candidate action identifier has destructive SQL semantics."
        if not findings
        else "Candidate requested an action with destructive SQL semantics.",
        action_ids=findings,
    )


def _allowlist_score(proposal: Proposal) -> EvalScore:
    unknown = sorted(
        action.action_id
        for action in proposal.candidate_actions
        if not is_governed_action(action.action_id)
    )
    return _score(
        "allowlist_compliance",
        0.0 if unknown else 1.0,
        1.0,
        "Every candidate action is catalogued; check-policy enforcement happens during compilation."
        if not unknown
        else "Candidate requested an action outside the remediation catalog.",
        action_ids=unknown,
    )


def evaluate_proposal(
    report: QualitySuiteReport, proposal: Proposal | dict[str, Any]
) -> EvalReport:
    """Evaluate candidate structure and evidence; compilation remains the authority gate."""
    settings = get_settings()
    parsed, schema = _parse_proposal(proposal)
    if parsed is None:
        scores = [
            schema,
            _score("citation_quality", 0.0, settings.eval_citation_threshold, "Invalid candidate."),
            _score("groundedness", 0.0, settings.eval_groundedness_threshold, "Invalid candidate."),
            _score("destructive_risk", 0.0, 1.0, "Invalid candidate."),
            _score("allowlist_compliance", 0.0, 1.0, "Invalid candidate."),
        ]
    else:
        scores = [
            schema,
            _evidence_score(report, parsed, settings.eval_citation_threshold),
            _groundedness_score(report, parsed, settings.eval_groundedness_threshold),
            _destructive_score(parsed),
            _allowlist_score(parsed),
        ]
    blocked = [f"{score.name}: {score.rationale}" for score in scores if not score.passed]
    return EvalReport(
        passed=not blocked,
        scores=scores,
        blocked_reasons=blocked,
        summary_markdown="## Candidate Proposal evaluation\n\n"
        + (
            "PASS — compile this candidate."
            if not blocked
            else "BLOCKED — do not compile this candidate."
        ),
    )


def evaluate_plan(plan: RemediationPlan) -> EvalReport:
    """Evaluate a compiled plan, never a candidate's text, SQL, or parameters."""
    verify_plan_integrity(plan, refusing="evaluation")
    executable = [item for item in plan.items if item.kind == "executable"]
    compilation_ok = not plan.blocked and len(executable) == len(plan.items)
    compilation = _score(
        "plan_compilation",
        1.0 if compilation_ok else 0.0,
        1.0,
        "Every failed check is covered by an executable controlled plan item."
        if compilation_ok
        else "The remediation plan contains explicit blocked or omitted outcomes.",
        blocked_reasons=plan.blocked_reasons,
    )
    policy_ok = current_policy_fingerprint(plan) == plan.policy_fingerprint
    policy = _score(
        "policy_snapshot",
        1.0 if policy_ok else 0.0,
        1.0,
        "The compiled plan still matches the current check policy and renderer."
        if policy_ok
        else "Check policy, table contract, remediation rule, or renderer drifted after compilation.",
    )
    target_errors = [
        item.item_id
        for item in executable
        if item.target_set.count < 0 or not item.target_set.fingerprint
    ]
    targets = _score(
        "target_set_integrity",
        1.0 if not target_errors else 0.0,
        1.0,
        "Every executable item has an exact target-set count and fingerprint."
        if not target_errors
        else "An executable plan item has no valid target-set summary.",
        item_ids=target_errors,
    )
    scores = [compilation, policy, targets]
    blocked = [f"{score.name}: {score.rationale}" for score in scores if not score.passed]
    passed = not blocked
    fingerprint = evaluation_payload_fingerprint(
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        passed=passed,
        scores=scores,
        blocked_reasons=blocked,
    )
    return EvalReport(
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        fingerprint=fingerprint,
        passed=passed,
        scores=scores,
        blocked_reasons=blocked,
        summary_markdown="## Remediation plan evaluation\n\n"
        + (
            "PASS — eligible for whole-plan HITL."
            if passed
            else "BLOCKED — do not admit this plan."
        ),
    )
