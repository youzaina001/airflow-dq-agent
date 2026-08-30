"""Compile an untrusted candidate proposal into a controlled remediation plan."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from airflow_dq_agent.action_definitions import derive_action_params, get_action_definition
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import (
    CandidateAction,
    ExecutablePlanItem,
    NonExecutablePlanItem,
    Proposal,
    QualityEvidence,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.contracts.tables import get_table_contract
from airflow_dq_agent.quality.registry import CheckSpec, get_check_spec

RENDERER_VERSION = "controlled-renderer-v3"


class TargetSetResolver(Protocol):
    """The seam that obtains an exact, non-durable remediation target summary."""

    def resolve(
        self,
        *,
        report_run_id: str,
        check_id: str,
        action_id: str,
        table: str,
        params: dict[str, object],
    ) -> TargetSet: ...


def _policy_fingerprint(specs: Sequence[CheckSpec], action_id: str) -> str:
    action = get_action_definition(action_id).metadata
    return canonical_fingerprint(
        {
            "contracts": [get_table_contract(spec.table).model_dump(mode="json") for spec in specs],
            "check_policies": [spec.model_dump(mode="json") for spec in specs],
            "remediation_rule": action.model_dump(mode="json"),
            "renderer_version": RENDERER_VERSION,
        }
    )


def current_policy_fingerprint(plan: RemediationPlan) -> str:
    """Fingerprint the policy currently governing an already compiled plan."""
    item_fingerprints: list[str] = []
    for item in plan.items:
        if not isinstance(item, ExecutablePlanItem):
            continue
        specs = [get_check_spec(evidence.check_id) for evidence in item.evidence]
        item_fingerprints.append(_policy_fingerprint(specs, item.action_id))
    return canonical_fingerprint(item_fingerprints)


def _validated_evidence(
    report_run_id: str, action: CandidateAction, report_failures: dict[str, str]
) -> tuple[list[QualityEvidence], list[CheckSpec]]:
    evidence = action.evidence
    if not evidence:
        raise ValueError("candidate action has no quality evidence")
    specs: list[CheckSpec] = []
    for item in evidence:
        contract_id = report_failures.get(item.check_id)
        if contract_id is None or contract_id != item.contract_id:
            raise ValueError("candidate evidence does not refer to a failed check in this report")
        specs.append(get_check_spec(item.check_id))
    if len({spec.table for spec in specs}) != 1:
        raise ValueError("one plan item cannot target more than one contracted table")
    return evidence, specs


def _blocked_item(
    *,
    index: int,
    evidence: list[QualityEvidence],
    reason: str,
) -> NonExecutablePlanItem:
    return NonExecutablePlanItem(item_id=f"candidate-{index}", evidence=evidence, reason=reason)


def compile_remediation_plan(
    report: object,
    candidate: Proposal,
    *,
    target_sets: TargetSetResolver,
) -> RemediationPlan:
    """Compile one candidate action into one plan item without inventing mutations.

    Any unavailable action, invalid evidence, target lookup failure, or omitted failed
    check becomes an explicit non-executable outcome.  The returned plan is therefore
    complete enough to audit even when it is blocked from evaluation and admission.
    """
    from airflow_dq_agent.contracts.models import QualitySuiteReport

    if not isinstance(report, QualitySuiteReport):
        report = QualitySuiteReport.model_validate(report)
    report_failures = {check.check_id: check.contract_id for check in report.failed_checks}
    items: list[ExecutablePlanItem | NonExecutablePlanItem] = []
    covered: set[str] = set()

    for index, requested in enumerate(candidate.candidate_actions):
        evidence = list(requested.evidence)
        try:
            evidence, specs = _validated_evidence(report.run_id, requested, report_failures)
            if any(spec.rule_for(requested.action_id) is None for spec in specs):
                raise ValueError("requested action is not declared by the check policy")
            if any(spec.table != specs[0].table for spec in specs):
                raise ValueError("one plan item cannot target more than one contracted table")
            params = derive_action_params(specs[0], requested.action_id)
            if any(derive_action_params(spec, requested.action_id) != params for spec in specs[1:]):
                raise ValueError("evidence requires incompatible controlled parameter values")
            target_set = target_sets.resolve(
                report_run_id=report.run_id,
                check_id=specs[0].check_id,
                action_id=requested.action_id,
                table=specs[0].table,
                params=params,
            )
            item = ExecutablePlanItem(
                item_id=f"candidate-{index}",
                action_id=requested.action_id,
                table=specs[0].table,
                params=params,
                evidence=evidence,
                target_set=target_set,
                policy_fingerprint=_policy_fingerprint(specs, requested.action_id),
            )
            items.append(item)
            covered.update(entry.check_id for entry in evidence)
        except (KeyError, ValueError):
            items.append(
                _blocked_item(
                    index=index,
                    evidence=evidence,
                    reason="candidate action is unavailable under the controlled policy",
                )
            )
            covered.update(
                entry.check_id for entry in evidence if entry.check_id in report_failures
            )

    omitted = [
        QualityEvidence(check_id=check_id, contract_id=contract_id)
        for check_id, contract_id in report_failures.items()
        if check_id not in covered
    ]
    if omitted:
        items.append(
            NonExecutablePlanItem(
                item_id="omitted-failures",
                evidence=omitted,
                reason="candidate proposal omitted failed-check coverage",
            )
        )
    blocked_reasons = [item.reason for item in items if isinstance(item, NonExecutablePlanItem)]
    policy_fingerprint = canonical_fingerprint(
        [item.policy_fingerprint for item in items if isinstance(item, ExecutablePlanItem)]
    )
    candidate_fingerprint = canonical_fingerprint(candidate)
    plan_fingerprint = canonical_fingerprint(
        {
            "quality_run_id": report.run_id,
            "candidate_fingerprint": candidate_fingerprint,
            "policy_fingerprint": policy_fingerprint,
            "items": [item.model_dump(mode="json") for item in items],
        }
    )
    return RemediationPlan(
        quality_run_id=report.run_id,
        candidate_fingerprint=candidate_fingerprint,
        policy_fingerprint=policy_fingerprint,
        items=items,
        blocked=bool(blocked_reasons),
        blocked_reasons=blocked_reasons,
        fingerprint=plan_fingerprint,
    )
