"""Proposal evals that hold the agent accountable to the quality report and contracts."""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import (
    FORBIDDEN_SQL_TOKENS,
    DestructiveRank,
    EvalReport,
    EvalScore,
    Proposal,
    QualitySuiteReport,
    RemediationPlan,
)
from airflow_dq_agent.contracts.remediations import REMEDIATION_CATALOG, validate_step_params
from airflow_dq_agent.contracts.tables import get_table_contract
from airflow_dq_agent.planning import current_policy_fingerprint

_DESTRUCTIVE_TOKENS = (*FORBIDDEN_SQL_TOKENS, "DELETE")


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
            "Proposal does not satisfy the Pydantic output contract.",
            errors=exc.errors(include_url=False),
        )
    return parsed, _score(
        "schema_validity", 1.0, 1.0, "Proposal satisfies the Pydantic output contract."
    )


def _citation_score(report: QualitySuiteReport, proposal: Proposal, threshold: float) -> EvalScore:
    failing = set(report.failing_check_ids)
    report_ids = report.check_ids
    cited = proposal.cited_check_ids()
    missing = sorted(failing - cited)
    unknown = sorted(cited - report_ids)
    mismatched_contracts = sorted(
        citation.check_id
        for citation in proposal.citations
        if (check := report.get(citation.check_id)) is not None
        and citation.contract_id != check.contract_id
    )
    if not failing:
        valid = not cited and not proposal.failing_check_ids
        score = 1.0 if valid else 0.0
    elif unknown or mismatched_contracts:
        score = 0.0
    else:
        score = (len(failing) - len(missing)) / len(failing)
    rationale = "All failed checks are cited with report-backed contract IDs."
    if score < threshold:
        rationale = "Citations are incomplete, invented, or use a mismatched contract ID."
    return _score(
        "citation_quality",
        score,
        threshold,
        rationale,
        missing=missing,
        unknown=unknown,
        mismatched_contracts=mismatched_contracts,
    )


def _groundedness_score(
    report: QualitySuiteReport, proposal: Proposal, threshold: float
) -> EvalScore:
    errors: list[str] = []
    failing = set(report.failing_check_ids)
    declared = set(proposal.failing_check_ids)
    invented = sorted(declared - report.check_ids)
    non_failing = sorted(declared - failing)
    if invented:
        errors.append(f"invented failing check IDs: {invented}")
    if non_failing:
        errors.append(f"proposal labels passing checks as failing: {non_failing}")
    if not report.failed_count and proposal.steps:
        errors.append("green quality report cannot have remediation steps")
    for step in proposal.steps:
        step_check_id = step.params.get("check_id")
        if step_check_id is not None:
            check = report.get(step_check_id) if isinstance(step_check_id, str) else None
            if check is None or not check.failed:
                errors.append(
                    f"{step.action_id} references a check that is not failed in this report"
                )
        try:
            contract = get_table_contract(step.table)
        except KeyError as exc:
            errors.append(str(exc))
            continue
        for key in ("column", "pk_column", "fk_column", "business_key"):
            value = step.params.get(key)
            values = value if key == "business_key" and isinstance(value, list) else [value]
            for column in values:
                if column is not None and (
                    not isinstance(column, str) or not contract.has_column(column)
                ):
                    errors.append(f"{step.table}.{column!r} is not a contracted identifier")
        violations = validate_step_params(step.action_id, step.table, step.params)
        errors.extend(f"{step.action_id}: {violation}" for violation in violations)
    score = 1.0 if not errors else 0.0
    return _score(
        "groundedness",
        score,
        threshold,
        "All steps are grounded in report evidence and table contracts."
        if not errors
        else "Steps or claimed failures are not grounded in report evidence/contracts.",
        errors=errors,
    )


def _destructive_score(proposal: Proposal) -> EvalScore:
    findings: list[str] = []
    for step in proposal.steps:
        preview = step.sql_preview.upper()
        tokens = [token for token in _DESTRUCTIVE_TOKENS if token in preview]
        if tokens:
            findings.append(f"{step.action_id}: forbidden preview token(s) {tokens}")
        if any(token in step.action_id.upper() for token in _DESTRUCTIVE_TOKENS):
            findings.append(f"{step.action_id}: forbidden action identifier")
        if step.destructive_rank == DestructiveRank.CRITICAL:
            findings.append(f"{step.action_id}: CRITICAL destructive rank")
    score = 0.0 if findings else 1.0
    return _score(
        "destructive_risk",
        score,
        1.0,
        "No forbidden SQL semantics or critical actions were proposed."
        if not findings
        else "Proposal contains forbidden SQL semantics or a critical action.",
        findings=findings,
    )


def _allowlist_score(proposal: Proposal) -> EvalScore:
    errors: list[str] = []
    for step in proposal.steps:
        action = REMEDIATION_CATALOG.get(step.action_id)
        if action is None:
            errors.append(f"{step.action_id!r} is not allow-listed")
            continue
        table = step.table.split(".")[-1]
        if table not in action.allowed_tables:
            errors.append(f"{step.action_id} is not allow-listed for {table}")
    score = 1.0 if not errors else 0.0
    return _score(
        "allowlist_compliance",
        score,
        1.0,
        "Every action/table pair is in the remediation allow-list."
        if not errors
        else "Proposal uses an action/table pair outside the allow-list.",
        errors=errors,
    )


def evaluate_proposal(
    report: QualitySuiteReport, proposal: Proposal | dict[str, Any]
) -> EvalReport:
    """Score a structured proposal without executing any proposal SQL."""
    settings = get_settings()
    parsed, schema = _parse_proposal(proposal)
    if parsed is None:
        zero_scores = [
            _score("citation_quality", 0.0, settings.eval_citation_threshold, "Invalid proposal."),
            _score("groundedness", 0.0, settings.eval_groundedness_threshold, "Invalid proposal."),
            _score("destructive_risk", 0.0, 1.0, "Invalid proposal."),
            _score("allowlist_compliance", 0.0, 1.0, "Invalid proposal."),
        ]
        scores = [schema, *zero_scores]
    else:
        scores = [
            schema,
            _citation_score(report, parsed, settings.eval_citation_threshold),
            _groundedness_score(report, parsed, settings.eval_groundedness_threshold),
            _destructive_score(parsed),
            _allowlist_score(parsed),
        ]
    blocked = [f"{score.name}: {score.rationale}" for score in scores if not score.passed]
    passed = not blocked
    summary = (
        "## Proposal evaluation\n\n"
        + (
            "PASS — safe to present for HITL."
            if passed
            else "BLOCKED — do not apply this proposal."
        )
        + "\n\n"
        + "\n".join(
            f"- {'PASS' if score.passed else 'FAIL'} `{score.name}`: {score.score:.2f}"
            for score in scores
        )
    )
    return EvalReport(
        passed=passed, scores=scores, blocked_reasons=blocked, summary_markdown=summary
    )


def evaluate_plan(plan: RemediationPlan) -> EvalReport:
    """Evaluate a compiled plan, never a candidate's text, SQL, or parameters."""
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
    summary = "## Remediation plan evaluation\n\n" + (
        "PASS — eligible for whole-plan HITL." if passed else "BLOCKED — do not admit this plan."
    )
    fingerprint = canonical_fingerprint(
        {
            "plan_id": plan.plan_id,
            "plan_fingerprint": plan.fingerprint,
            "passed": passed,
            "scores": [score.model_dump(mode="json") for score in scores],
            "blocked_reasons": blocked,
        }
    )
    return EvalReport(
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        fingerprint=fingerprint,
        passed=passed,
        scores=scores,
        blocked_reasons=blocked,
        summary_markdown=summary,
    )
