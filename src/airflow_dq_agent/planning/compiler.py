"""Compile an untrusted candidate proposal into a controlled remediation plan."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Protocol
from uuid import uuid4

from airflow_dq_agent import check_policy
from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.config import get_settings
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
from airflow_dq_agent.planning.integrity import (
    plan_payload_fingerprint,
)
from airflow_dq_agent.planning.integrity import (
    warehouse_environment_id as environment_id_from_dsn,
)
from airflow_dq_agent.quality.registry import CheckSpec, get_check_spec

RENDERER_VERSION = "controlled-renderer-v2"


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
    action = get_governed_action(action_id).metadata
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


def _blocked_item(
    *,
    index: int,
    evidence: Sequence[QualityEvidence],
    reason: str,
) -> NonExecutablePlanItem:
    return NonExecutablePlanItem(
        item_id=f"candidate-{index}", evidence=tuple(evidence), reason=reason
    )


def candidate_action_identity(action: CandidateAction) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Identity used to refuse duplicate (action_id, Quality Evidence) requests."""
    evidence = tuple(sorted((item.check_id, item.contract_id) for item in action.evidence))
    return (action.action_id, evidence)


def compile_remediation_plan(
    report: object,
    candidate: Proposal,
    *,
    target_sets: TargetSetResolver,
    warehouse_environment_id: str | None = None,
) -> RemediationPlan:
    """Compile one candidate action into one plan item without inventing mutations.

    Any unavailable action, invalid evidence, target lookup failure, or omitted failed
    check becomes an explicit non-executable outcome.  The returned plan is therefore
    complete enough to audit even when it is blocked from evaluation and admission.
    """
    from airflow_dq_agent.contracts.models import QualitySuiteReport

    if not isinstance(report, QualitySuiteReport):
        report = QualitySuiteReport.model_validate(report)
    report_failures = {check.check_id: check for check in report.failed_checks}
    items: list[ExecutablePlanItem | NonExecutablePlanItem] = []
    covered: set[str] = set()
    duplicate_identities = {
        identity
        for identity, count in Counter(
            candidate_action_identity(requested) for requested in candidate.candidate_actions
        ).items()
        if count > 1
    }

    for index, requested in enumerate(candidate.candidate_actions):
        evidence = list(requested.evidence)
        if candidate_action_identity(requested) in duplicate_identities:
            items.append(
                _blocked_item(
                    index=index,
                    evidence=evidence,
                    reason="duplicate candidate action and quality evidence",
                )
            )
            covered.update(
                entry.check_id for entry in evidence if entry.check_id in report_failures
            )
            continue
        try:
            justification = check_policy.justify_action(
                action_id=requested.action_id,
                evidence=requested.evidence,
                report_failures=report_failures,
            )
            specs = justification.specs
            if any(spec.table != specs[0].table for spec in specs):
                raise ValueError("one plan item cannot target more than one contracted table")
            target_set = target_sets.resolve(
                report_run_id=report.run_id,
                check_id=specs[0].check_id,
                action_id=requested.action_id,
                table=specs[0].table,
                params=justification.params,
            )
            item = ExecutablePlanItem(
                item_id=f"candidate-{index}",
                action_id=requested.action_id,
                table=specs[0].table,
                params=justification.params,
                evidence=tuple(evidence),
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
        QualityEvidence(check_id=check_id, contract_id=failed.contract_id)
        for check_id, failed in report_failures.items()
        if check_id not in covered
    ]
    if omitted:
        items.append(
            NonExecutablePlanItem(
                item_id="omitted-failures",
                evidence=tuple(omitted),
                reason="candidate proposal omitted failed-check coverage",
            )
        )
    blocked_reasons = [item.reason for item in items if isinstance(item, NonExecutablePlanItem)]
    policy_fingerprint = canonical_fingerprint(
        [item.policy_fingerprint for item in items if isinstance(item, ExecutablePlanItem)]
    )
    candidate_fingerprint = canonical_fingerprint(candidate)
    plan_id = uuid4().hex
    if warehouse_environment_id is not None:
        bound_environment_id = warehouse_environment_id
    else:
        inferred = getattr(target_sets, "warehouse_environment_id", None)
        bound_environment_id = (
            inferred
            if isinstance(inferred, str) and inferred
            else environment_id_from_dsn(get_settings().warehouse_dsn)
        )
    plan_fingerprint = plan_payload_fingerprint(
        plan_id=plan_id,
        quality_run_id=report.run_id,
        candidate_fingerprint=candidate_fingerprint,
        policy_fingerprint=policy_fingerprint,
        warehouse_environment_id=bound_environment_id,
        items=items,
    )
    return RemediationPlan(
        plan_id=plan_id,
        quality_run_id=report.run_id,
        candidate_fingerprint=candidate_fingerprint,
        policy_fingerprint=policy_fingerprint,
        items=tuple(items),
        blocked=bool(blocked_reasons),
        blocked_reasons=blocked_reasons,
        warehouse_environment_id=bound_environment_id,
        fingerprint=plan_fingerprint,
    )
