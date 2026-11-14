"""Trusted remediation-plan compilation and Policy Snapshot verification."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow_dq_agent.planning.policy_snapshot import (
    RENDERER_VERSION,
    current_policy_fingerprint,
    policy_snapshot_fingerprint,
    verify_policy_snapshot,
)

if TYPE_CHECKING:
    from airflow_dq_agent.planning.compiler import (
        TargetSetResolver,
        compile_remediation_plan,
    )

__all__ = [
    "RENDERER_VERSION",
    "TargetSetResolver",
    "compile_remediation_plan",
    "current_policy_fingerprint",
    "policy_snapshot_fingerprint",
    "verify_policy_snapshot",
]


def __getattr__(name: str) -> object:
    if name in ("TargetSetResolver", "compile_remediation_plan"):
        from airflow_dq_agent.planning import compiler

        return getattr(compiler, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
