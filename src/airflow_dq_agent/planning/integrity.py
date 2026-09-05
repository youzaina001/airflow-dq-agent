"""Recompute governed fingerprints from received artifacts at durable seams."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from sqlalchemy.engine import make_url

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.fingerprints import (
    canonical_fingerprint,
    report_payload_fingerprint,
)
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    EvalReport,
    EvalScore,
    ExecutablePlanItem,
    NonExecutablePlanItem,
    QualitySuiteReport,
    RemediationPlan,
)
from airflow_dq_agent.quality.registry import get_check_spec

PlanItem = ExecutablePlanItem | NonExecutablePlanItem


def warehouse_environment_id(dsn: str) -> str:
    """Stable non-secret warehouse identity: host:port/database."""
    url = make_url(dsn)
    host = url.host or ""
    port = "" if url.port is None else str(url.port)
    database = url.database or ""
    return f"{host}:{port}/{database}"


def plan_payload_fingerprint(
    *,
    plan_id: str,
    quality_run_id: str,
    candidate_fingerprint: str,
    policy_fingerprint: str,
    warehouse_environment_id: str,
    items: Sequence[PlanItem],
) -> str:
    """Canonical fingerprint of a received Remediation Plan payload."""
    return canonical_fingerprint(
        {
            "plan_id": plan_id,
            "quality_run_id": quality_run_id,
            "candidate_fingerprint": candidate_fingerprint,
            "policy_fingerprint": policy_fingerprint,
            "warehouse_environment_id": warehouse_environment_id,
            "items": [item.model_dump(mode="json") for item in items],
        }
    )


def evaluation_payload_fingerprint(
    *,
    evaluation_id: str,
    plan_id: str | None,
    plan_fingerprint: str | None,
    passed: bool,
    scores: Sequence[EvalScore],
    blocked_reasons: Sequence[str],
) -> str:
    """Canonical fingerprint of a received plan evaluation payload."""
    return canonical_fingerprint(
        {
            "evaluation_id": evaluation_id,
            "plan_id": plan_id,
            "plan_fingerprint": plan_fingerprint,
            "passed": passed,
            "scores": [score.model_dump(mode="json") for score in scores],
            "blocked_reasons": list(blocked_reasons),
        }
    )


def decision_payload_fingerprint(
    *,
    decision_id: str,
    decision: str,
    actor: str,
    note: str | None,
    decided_at: datetime,
) -> str:
    """Canonical fingerprint of a received Human Decision payload."""
    return canonical_fingerprint(
        {
            "decision_id": decision_id,
            "decision": decision,
            "actor": actor,
            "note": note,
            "decided_at": decided_at,
        }
    )


def admission_payload_fingerprint(
    *,
    admission_id: str,
    quality_run_id: str,
    plan_id: str,
    plan_fingerprint: str,
    evaluation_id: str,
    evaluation_fingerprint: str,
    decision_id: str,
    decision_event_id: str,
    policy_fingerprint: str,
    warehouse_environment_id: str,
    issued_at: datetime,
    expires_at: datetime,
) -> str:
    """Canonical fingerprint of a received Apply Admission payload."""
    return canonical_fingerprint(
        {
            "admission_id": admission_id,
            "quality_run_id": quality_run_id,
            "plan_id": plan_id,
            "plan_fingerprint": plan_fingerprint,
            "evaluation_id": evaluation_id,
            "evaluation_fingerprint": evaluation_fingerprint,
            "decision_id": decision_id,
            "decision_event_id": decision_event_id,
            "policy_fingerprint": policy_fingerprint,
            "warehouse_environment_id": warehouse_environment_id,
            "issued_at": issued_at,
            "expires_at": expires_at,
        }
    )


def verify_report_integrity(report: QualitySuiteReport, *, refusing: str) -> None:
    """Recompute the received Quality Evidence payload against its durable fingerprint."""
    fingerprint = report.fingerprint
    if not fingerprint:
        raise PermissionError(f"Refusing {refusing}: quality report has no immutable fingerprint")
    expected = report_payload_fingerprint(report)
    if expected != fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: quality report fingerprint does not match received payload"
        )


def verify_plan_integrity(plan: RemediationPlan, *, refusing: str) -> None:
    expected = plan_payload_fingerprint(
        plan_id=plan.plan_id,
        quality_run_id=plan.quality_run_id,
        candidate_fingerprint=plan.candidate_fingerprint,
        policy_fingerprint=plan.policy_fingerprint,
        warehouse_environment_id=plan.warehouse_environment_id,
        items=plan.items,
    )
    if expected != plan.fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: remediation plan fingerprint does not match received payload"
        )


def verify_evaluation_integrity(
    plan: RemediationPlan, evaluation: EvalReport, *, refusing: str
) -> str:
    fingerprint = evaluation.fingerprint
    if not fingerprint:
        raise PermissionError(f"Refusing {refusing}: evaluation has no immutable fingerprint")
    expected = evaluation_payload_fingerprint(
        evaluation_id=evaluation.evaluation_id,
        plan_id=evaluation.plan_id,
        plan_fingerprint=evaluation.plan_fingerprint,
        passed=evaluation.passed,
        scores=evaluation.scores,
        blocked_reasons=evaluation.blocked_reasons,
    )
    if expected != fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: evaluation fingerprint does not match received payload"
        )
    if evaluation.plan_id != plan.plan_id or evaluation.plan_fingerprint != plan.fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: evaluation does not belong to this remediation plan"
        )
    return fingerprint


def verify_admission_integrity(
    plan: RemediationPlan,
    evaluation: EvalReport,
    admission: ApplyAdmission,
    *,
    refusing: str,
) -> None:
    expected = admission_payload_fingerprint(
        admission_id=admission.admission_id,
        quality_run_id=admission.quality_run_id,
        plan_id=admission.plan_id,
        plan_fingerprint=admission.plan_fingerprint,
        evaluation_id=admission.evaluation_id,
        evaluation_fingerprint=admission.evaluation_fingerprint,
        decision_id=admission.decision_id,
        decision_event_id=admission.decision_event_id,
        policy_fingerprint=admission.policy_fingerprint,
        warehouse_environment_id=admission.warehouse_environment_id,
        issued_at=admission.issued_at,
        expires_at=admission.expires_at,
    )
    if expected != admission.fingerprint:
        raise PermissionError(
            f"Refusing {refusing}: apply admission fingerprint does not match received payload"
        )
    if (
        admission.plan_id != plan.plan_id
        or admission.plan_fingerprint != plan.fingerprint
        or admission.quality_run_id != plan.quality_run_id
        or admission.evaluation_id != evaluation.evaluation_id
        or admission.evaluation_fingerprint != evaluation.fingerprint
        or admission.warehouse_environment_id != plan.warehouse_environment_id
    ):
        raise PermissionError(
            f"Refusing {refusing}: admission does not authorize this evaluated plan"
        )


def verify_executable_params(
    plan: RemediationPlan, *, report: QualitySuiteReport, refusing: str
) -> None:
    """Re-derive item parameters from originating Quality Evidence and Check Policy."""
    if report.run_id != plan.quality_run_id:
        raise PermissionError(
            f"Refusing {refusing}: quality report does not belong to this remediation plan"
        )
    report_failures = {check.check_id: check for check in report.failed_checks}
    covered: set[str] = set()
    seen_identities: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    for item in plan.items:
        if not isinstance(item, ExecutablePlanItem):
            continue
        identity = (
            item.action_id,
            tuple(sorted((evidence.check_id, evidence.contract_id) for evidence in item.evidence)),
        )
        if identity in seen_identities:
            raise PermissionError(
                f"Refusing {refusing}: duplicate executable action and quality evidence"
            )
        seen_identities.add(identity)
        try:
            if not item.evidence:
                raise ValueError("executable item has no quality evidence")
            specs = []
            for evidence in item.evidence:
                failed = report_failures.get(evidence.check_id)
                if failed is None or failed.contract_id != evidence.contract_id:
                    raise ValueError("evidence is not a failed check in this quality run")
                spec = get_check_spec(evidence.check_id)
                if spec.table != item.table or spec.contract_id != evidence.contract_id:
                    raise ValueError("evidence does not match the contracted table")
                if failed.table != item.table:
                    raise ValueError("evidence does not match the contracted table")
                specs.append(spec)
                covered.add(evidence.check_id)
            action = get_governed_action(item.action_id)
            derived = action.derive_params(specs[0])
            if any(action.derive_params(spec) != derived for spec in specs[1:]):
                raise ValueError("evidence requires incompatible controlled parameter values")
        except (KeyError, ValueError) as exc:
            raise PermissionError(
                f"Refusing {refusing}: quality evidence is not a failed check in this quality run"
            ) from exc
        if derived != item.params:
            raise PermissionError(f"Refusing {refusing}: item parameters do not match Check Policy")
    if not plan.blocked and covered != set(report_failures):
        raise PermissionError(
            f"Refusing {refusing}: plan does not cover every failed check in this quality run"
        )
