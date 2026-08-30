"""Compatibility adapters for the governed remediation-action module."""

from __future__ import annotations

from typing import Any

from airflow_dq_agent.action_definitions import RenderedStep as _RenderedStep
from airflow_dq_agent.action_definitions import get_action_definition
from airflow_dq_agent.contracts.models import (
    FORBIDDEN_SQL_TOKENS,
    DestructiveRank,
    ExecutablePlanItem,
    NonExecutablePlanItem,
    RemediationStep,
)

_PREVIEW_BLOCKED = (*FORBIDDEN_SQL_TOKENS, "DELETE")
RenderedStep = _RenderedStep


def _ensure_preview_is_safe(step: RemediationStep) -> None:
    preview = step.sql_preview.upper()
    bad = [token for token in _PREVIEW_BLOCKED if token in preview]
    if bad or step.destructive_rank == DestructiveRank.CRITICAL:
        raise ValueError(
            f"Refusing unsafe proposal preview for {step.action_id}: {bad or ['CRITICAL rank']}"
        )


def render_step(step: RemediationStep, *, run_id: str = "dry-run") -> RenderedStep:
    """Render a legacy preview step through the same governed action interface."""
    _ensure_preview_is_safe(step)
    action = get_action_definition(step.action_id)
    return action.render(table=step.table, params=step.params, run_id=run_id)


def render_plan_item(
    item: ExecutablePlanItem | NonExecutablePlanItem, *, run_id: str = "dry-run"
) -> RenderedStep:
    """Render one compiled item without accepting candidate text or SQL previews."""
    if not isinstance(item, ExecutablePlanItem):
        raise ValueError("Cannot render a non-executable remediation plan item")
    return render_controlled_action(
        action_id=item.action_id, table=item.table, params=item.params, run_id=run_id
    )


def render_controlled_action(
    *, action_id: str, table: str, params: dict[str, Any], run_id: str = "dry-run"
) -> RenderedStep:
    """Render policy-derived fields through the action-owned controlled renderer."""
    return get_action_definition(action_id).render(table=table, params=params, run_id=run_id)
