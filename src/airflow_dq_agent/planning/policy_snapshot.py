"""Independent Policy Snapshot construction and drift comparison."""

from __future__ import annotations

from collections.abc import Sequence

from airflow_dq_agent.action_definitions import GovernedAction, get_governed_action
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import ApplyAdmission, ExecutablePlanItem, RemediationPlan
from airflow_dq_agent.contracts.tables import get_table_contract
from airflow_dq_agent.quality.registry import CheckSpec, get_check_spec

RENDERER_VERSION = "controlled-renderer-v2"


def _referenced_contract_payloads(
    governed: GovernedAction, specs: Sequence[CheckSpec]
) -> list[object]:
    ref_tables: list[str] = []
    drift_entries: list[dict[str, str]] = []
    for spec in specs:
        try:
            ref_tables.extend(governed.referenced_tables(spec))
        except ValueError as exc:
            # Record missing FK bindings explicitly so contract drift still fails closed without crashing evals.
            drift_entries.append({"missing_reference": spec.check_id, "error": str(exc)})
    return [
        *[get_table_contract(table).model_dump(mode="json") for table in dict.fromkeys(ref_tables)],
        *drift_entries,
    ]


def policy_snapshot_fingerprint(specs: Sequence[CheckSpec], action_id: str) -> str:
    """Compute the Policy Snapshot fingerprint for one governed remediation item."""
    governed = get_governed_action(action_id)
    return canonical_fingerprint(
        {
            "contracts": [get_table_contract(spec.table).model_dump(mode="json") for spec in specs],
            "referenced_contracts": _referenced_contract_payloads(governed, specs),
            "check_policies": [spec.model_dump(mode="json") for spec in specs],
            "remediation_rule": governed.metadata.model_dump(mode="json"),
            "renderer_version": RENDERER_VERSION,
        }
    )


def current_policy_fingerprint(plan: RemediationPlan) -> str:
    """Recompute the Policy Snapshot currently governing a compiled plan."""
    item_fingerprints: list[str] = []
    for item in plan.items:
        if not isinstance(item, ExecutablePlanItem):
            continue
        specs = [get_check_spec(evidence.check_id) for evidence in item.evidence]
        item_fingerprints.append(policy_snapshot_fingerprint(specs, item.action_id))
    return canonical_fingerprint(item_fingerprints)


def verify_policy_snapshot(
    plan: RemediationPlan,
    *,
    admission: ApplyAdmission | None = None,
    refusing: str,
    after: str = "compilation",
) -> str:
    """Recompute the live Policy Snapshot and refuse if anything drifted."""
    current_policy = current_policy_fingerprint(plan)
    if current_policy != plan.policy_fingerprint or (
        admission is not None and current_policy != admission.policy_fingerprint
    ):
        raise PermissionError(f"Refusing {refusing}: policy snapshot drifted after {after}")
    return current_policy
