"""The one in-process Check Policy justification for proposals and plans.

Whether Quality Evidence is a failed check, the action is declared by Check Policy,
and controlled parameters may be derived is decided here once. Candidate evaluation,
XCom persistence, compilation, and apply recompute all refuse through this module, so
changing this rule changes every governed refusal.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.models import CheckResult, QualityEvidence
from airflow_dq_agent.quality.registry import CheckSpec, get_check_spec


class PolicyRefusal(ValueError):
    """A requested action is not justified by the Check Policy."""


@dataclass(frozen=True)
class CheckPolicyJustification:
    """The cited Check Specs and derived controlled parameters of one justified action."""

    specs: tuple[CheckSpec, ...]
    params: dict[str, Any] = field(default_factory=dict)


def justify_action(
    *,
    action_id: str,
    evidence: Sequence[QualityEvidence],
    report_failures: Mapping[str, CheckResult],
) -> CheckPolicyJustification:
    """Justify one requested action against one report's failed checks.

    Quality Evidence must name failed checks in the report with matching contract
    identities, the action must be declared by every cited check's policy, and
    controlled parameters must derive identically from every cited check.
    """
    if not evidence:
        raise PolicyRefusal("requested action has no quality evidence")
    specs: list[CheckSpec] = []
    for item in evidence:
        failed = report_failures.get(item.check_id)
        if failed is None or failed.contract_id != item.contract_id:
            raise PolicyRefusal("quality evidence is not a failed check in this report")
        try:
            specs.append(get_check_spec(item.check_id))
        except KeyError as exc:
            raise PolicyRefusal("quality evidence does not name a declared check") from exc
    try:
        governed = get_governed_action(action_id)
    except KeyError as exc:
        raise PolicyRefusal("requested action is outside the governed catalog") from exc
    if any(spec.rule_for(action_id) is None for spec in specs):
        raise PolicyRefusal("requested action is not declared by the check policy")
    try:
        params = governed.derive_params(specs[0])
        consistent = all(governed.derive_params(spec) == params for spec in specs[1:])
    except ValueError as exc:
        raise PolicyRefusal("check policy does not produce a bindable controlled action") from exc
    if not consistent:
        raise PolicyRefusal("evidence requires incompatible controlled parameter values")
    return CheckPolicyJustification(specs=tuple(specs), params=params)
