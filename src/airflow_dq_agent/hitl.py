"""Validated, auditable translation of Airflow ApprovalOperator output."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Set
from datetime import UTC, datetime
from typing import Any

from airflow_dq_agent.contracts.models import AuditEvent, HumanDecision
from airflow_dq_agent.traces.lineage import decision_event


def _actor_id(value: object) -> str | None:
    if isinstance(value, Mapping):
        actor = value.get("id")
        return str(actor) if actor is not None else None
    actor = getattr(value, "id", None)
    return str(actor) if actor is not None else None


def _note(params_input: object) -> str | None:
    if not isinstance(params_input, Mapping):
        return None
    value = params_input.get("approval_note")
    if isinstance(value, Mapping):
        value = value.get("value")
    return value.strip() if isinstance(value, str) else None


def _responded_at(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(UTC)


def parse_approval_output(output: Mapping[str, Any], *, approver_ids: Set[str]) -> HumanDecision:
    """Parse the provider's structured output and fail closed on malformed identity."""
    if bool(output.get("timedout")):
        return HumanDecision(
            decision="Timeout",
            actor="airflow-timeout",
            note=None,
            decided_at=_responded_at(output.get("responded_at")),
        )
    chosen = output.get("chosen_options")
    if not isinstance(chosen, list) or not all(isinstance(option, str) for option in chosen):
        raise PermissionError("Malformed ApprovalOperator output: chosen_options is required")
    actor = _actor_id(output.get("responded_by_user"))
    if actor is None or actor not in approver_ids:
        raise PermissionError("ApprovalOperator responder is not an allow-listed Airflow user")
    note = _note(output.get("params_input"))
    if "Approve" in chosen and not note:
        raise PermissionError("ApprovalOperator approval requires a non-empty approval_note")
    return HumanDecision(
        decision="Approve" if "Approve" in chosen else "Reject",
        actor=actor,
        note=note,
        decided_at=_responded_at(output.get("responded_at")),
    )


def audit_approval_decision(
    output: Mapping[str, Any],
    *,
    approver_ids: Set[str],
    quality_run_id: str,
    predecessor: AuditEvent | str,
    persist: Callable[[AuditEvent], None],
) -> HumanDecision:
    """Persist a parsed decision before returning it to any downstream admission path."""
    decision = parse_approval_output(output, approver_ids=approver_ids)
    event = decision_event(quality_run_id, decision, predecessor)
    persist(event)
    return decision.model_copy(update={"audit_event_id": event.event_id})


def audit_then_complete_approval(
    output: Mapping[str, Any],
    *,
    approver_ids: Set[str],
    quality_run_id: str,
    predecessor: AuditEvent | str,
    persist: Callable[[AuditEvent], None],
    complete_provider: Callable[[], object],
) -> HumanDecision:
    """Durably audit the outcome before the provider can branch or skip tasks."""
    decision = audit_approval_decision(
        output,
        approver_ids=approver_ids,
        quality_run_id=quality_run_id,
        predecessor=predecessor,
        persist=persist,
    )
    complete_provider()
    return decision
