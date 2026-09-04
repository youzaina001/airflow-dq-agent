"""Airflow adapter that durably audits HITL output before provider branching."""

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from airflow_dq_agent.contracts.models import AuditEvent
from airflow_dq_agent.hitl import audit_then_complete_approval
from airflow_dq_agent.traces import append_event

if TYPE_CHECKING:

    class _ProviderApprovalOperator:
        template_fields: tuple[str, ...]

        def __init__(self, **_: Any) -> None: ...

        def execute_complete(self, context: dict[str, Any], event: dict[str, Any]) -> Any: ...

else:
    try:  # Keep the core package importable in the lightweight local test environment.
        from airflow.providers.standard.operators.hitl import (
            ApprovalOperator as _ProviderApprovalOperator,
        )
    except ModuleNotFoundError:

        class _ProviderApprovalOperator:
            """Type-compatible placeholder used only when the optional extra is absent."""

            template_fields: tuple[str, ...] = ()

            def __init__(self, **_: Any) -> None:
                raise RuntimeError("AuditedApprovalOperator requires the airflow extra")

            def execute_complete(self, context: dict[str, Any], event: dict[str, Any]) -> Any:
                raise RuntimeError("AuditedApprovalOperator requires the airflow extra")


class AuditedApprovalOperator(_ProviderApprovalOperator):
    """Persist an attributable decision before ``ApprovalOperator`` can skip tasks.

    The provider deliberately skips direct downstream tasks on rejection.  Auditing
    in a separate task would therefore lose those decisions.  This adapter validates
    and records the provider's structured response before delegating to that branch
    behavior.  On approval it returns the audited ``HumanDecision`` payload, which
    is the only value supplied to plan admission.
    """

    template_fields = tuple(
        dict.fromkeys(
            (
                *_ProviderApprovalOperator.template_fields,
                "body",
                "quality_run_id",
                "predecessor_event_id",
                "plan_id",
                "plan_fingerprint",
                "review_fingerprint",
            )
        )
    )

    def __init__(
        self,
        *,
        quality_run_id: str,
        predecessor_event_id: str,
        approver_ids: Collection[str],
        audit_dsn: str | None,
        plan_id: str = "",
        plan_fingerprint: str = "",
        review_fingerprint: str = "",
        **kwargs: Any,
    ) -> None:
        self.quality_run_id = quality_run_id
        self.predecessor_event_id = predecessor_event_id
        # Keep custom-operator state DAG-serialization friendly; a set is not JSON data.
        self.approver_ids = sorted(set(approver_ids))
        self.audit_dsn = audit_dsn
        self.plan_id = plan_id
        self.plan_fingerprint = plan_fingerprint
        self.review_fingerprint = review_fingerprint
        super().__init__(**kwargs)

    def execute_complete(self, context: dict[str, Any], event: dict[str, Any]) -> Any:
        # This must precede ``super``: it can skip direct downstream tasks for Reject.
        def persist(audit_event: AuditEvent) -> None:
            append_event(
                audit_event,
                dsn=self.audit_dsn,
                mirror_postgres=True,
            )

        decision = audit_then_complete_approval(
            event,
            approver_ids=set(self.approver_ids),
            quality_run_id=self.quality_run_id,
            predecessor=self.predecessor_event_id,
            persist=persist,
            complete_provider=lambda: super(AuditedApprovalOperator, self).execute_complete(
                context=context, event=event
            ),
            plan_id=self.plan_id,
            plan_fingerprint=self.plan_fingerprint,
            review_fingerprint=self.review_fingerprint,
        )
        return decision.model_dump(mode="json")
