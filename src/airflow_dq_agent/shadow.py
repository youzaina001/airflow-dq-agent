"""Configured Shadow Review wording shared by the CLI and the bundled DAG."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy.engine import make_url

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.config import Settings
from airflow_dq_agent.contracts.models import ApprovalReview
from airflow_dq_agent.contracts.tables import get_table_contract

EMPTY_SUITE = (
    "suite: incomplete: 0 passed, 0 failed, 0 errors; no checks ran\n"
    "next: correct check execution or configuration; do not review a remediation plan"
)


def configuration_error(settings: Settings) -> str | None:
    """Return the reason a configured Shadow Review must not start."""
    if not settings.trace_postgres:
        return (
            "Shadow Review requires PostgreSQL Audit Lineage; "
            "set TRACE_POSTGRES=true. JSONL-only is not a completed Shadow Review."
        )
    if not settings.read_dsn or not settings.audit_dsn:
        return "Shadow Review requires READ_DSN and AUDIT_DSN"
    try:
        read_user = make_url(settings.read_dsn).username
        audit_user = make_url(settings.audit_dsn).username
    except Exception:
        return "Shadow Review requires READ_DSN and AUDIT_DSN"
    if not read_user or not audit_user or read_user == audit_user:
        return "Shadow Review requires distinct read and audit logins"
    return None


def render_shadow_review(prepared: Mapping[str, Any]) -> str:
    """Lead with the adopter effect; keep fingerprints as audit details."""
    review = ApprovalReview.model_validate(prepared["approval_review"])
    plan = prepared["plan"]
    reasons = plan.get("blocked_reasons", []) if isinstance(plan, dict) else []
    lines = ["Shadow Review"]
    if review.items:
        for item in review.items:
            action = get_governed_action(item.action_id)
            lines.append(f"Table: {get_table_contract(item.table).qualified}")
            lines.append(f"Failed checks: {', '.join(item.evidence_check_ids)}")
            lines.append(f"Proposed effect: {action.metadata.description}")
            lines.append(f"Exact target count: {item.target_count}")
            lines.append(f"Risk: {action.metadata.destructive_rank.value}")
            lines.append(
                "Reversibility: " + ("reversible" if item.reversible else "not reversible")
            )
    else:
        lines.extend(
            [
                "Table: none",
                "Failed checks: none",
                "Proposed effect: none",
                "Exact target count: 0",
                "Risk: none",
                "Reversibility: not applicable",
            ]
        )
    if reasons:
        lines.append("Blocked: " + "; ".join(str(reason) for reason in reasons))
    lines.append(f"Expiry guidance: {review.expiry_guidance}")
    if review.evaluation_passed and review.items:
        lines.append(
            "Next: the Shadow Review was recorded. No Human Decision or Apply Admission "
            "was created. Quarantine copies rows into dq.quarantine_rows and does not "
            "repair source data."
        )
    elif review.evaluation_passed:
        lines.append(
            "Next: no remediation is required. No Human Decision or Apply Admission was created."
        )
    else:
        lines.append("Next: do not request a Human Decision.")
    lines.extend(
        [
            "Audit details:",
            f"Plan id: {review.plan_id}",
            f"Plan fingerprint: {review.plan_fingerprint}",
            f"Review fingerprint: {review.fingerprint}",
            f"Quality run: {review.quality_run_id}",
            f"Evaluation id: {review.evaluation_id}",
            f"Evaluation fingerprint: {review.evaluation_fingerprint}",
        ]
    )
    return "\n".join(lines)
