"""One Check Policy justification governs evaluation, XCom persistence, compile, and apply."""

import pytest

from airflow_dq_agent import check_policy
from airflow_dq_agent.agent import safe_proposal_for_xcom
from airflow_dq_agent.check_policy import CheckPolicyJustification, PolicyRefusal
from airflow_dq_agent.contracts import (
    CandidateAction,
    ExecutablePlanItem,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.demo import seeded_failure_report
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.planning.integrity import (
    plan_payload_fingerprint,
    verify_executable_params,
)


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:orders-null-v1")


def _completeness_scenario() -> tuple[QualitySuiteReport, QualityEvidence]:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped = report.model_copy(update={"checks": [failed]})
    evidence = QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
    return scoped, evidence


def _declared_proposal(scoped: QualitySuiteReport, evidence: QualityEvidence) -> Proposal:
    del scoped
    return Proposal(
        summary="Quarantine the failed completeness rows.",
        root_cause_hypothesis="The source omitted a required total.",
        candidate_actions=[
            CandidateAction(
                action_id="quarantine_nulls",
                evidence=[evidence],
                rationale="Request the reviewed action declared by this Check Policy.",
            )
        ],
        confidence=0.9,
    )


def _undeclared_proposal(scoped: QualitySuiteReport, evidence: QualityEvidence) -> Proposal:
    del scoped
    return Proposal(
        summary="Fill missing totals.",
        root_cause_hypothesis="A catalogued action was requested without a declared policy.",
        candidate_actions=[
            CandidateAction(
                action_id="null_fill",
                evidence=[evidence],
                rationale="Catalogued but not declared by this Check Policy.",
            )
        ],
        confidence=0.1,
    )


def _compiled_plan(scoped: QualitySuiteReport, evidence: QualityEvidence) -> RemediationPlan:
    return compile_remediation_plan(
        scoped,
        _declared_proposal(scoped, evidence),
        target_sets=_TargetSets(),
    )


def _recomputed_plan(plan: RemediationPlan, *, updates: dict) -> RemediationPlan:
    original = plan.items[0]
    assert isinstance(original, ExecutablePlanItem)
    items = [original.model_copy(update=updates)]
    return plan.model_copy(
        update={
            "items": items,
            "fingerprint": plan_payload_fingerprint(
                plan_id=plan.plan_id,
                quality_run_id=plan.quality_run_id,
                candidate_fingerprint=plan.candidate_fingerprint,
                policy_fingerprint=plan.policy_fingerprint,
                warehouse_environment_id=plan.warehouse_environment_id,
                items=items,
            ),
        }
    )


def test_declared_evidence_backed_action_passes_every_governed_seam() -> None:
    scoped, evidence = _completeness_scenario()
    proposal = _declared_proposal(scoped, evidence)

    evaluation = evaluate_proposal(scoped, proposal)
    payload = safe_proposal_for_xcom(scoped, proposal)
    plan = _compiled_plan(scoped, evidence)
    verify_executable_params(plan, report=scoped, refusing="apply")

    assert evaluation.passed
    assert payload["candidate_actions"][0]["action_id"] == "quarantine_nulls"
    assert plan.blocked is False
    item = plan.items[0]
    assert isinstance(item, ExecutablePlanItem)
    assert item.action_id == "quarantine_nulls"
    assert item.params == {"column": "total_amount", "pk_column": "order_id"}


def test_undeclared_action_is_refused_at_every_governed_seam() -> None:
    scoped, evidence = _completeness_scenario()
    proposal = _undeclared_proposal(scoped, evidence)

    evaluation = evaluate_proposal(scoped, proposal)
    assert evaluation.passed is False
    check_policy_score = evaluation.get("check_policy")
    assert check_policy_score is not None
    assert check_policy_score.passed is False

    with pytest.raises(PermissionError, match="unbounded candidate proposal"):
        safe_proposal_for_xcom(scoped, proposal)

    plan = compile_remediation_plan(scoped, proposal, target_sets=_TargetSets())
    assert plan.blocked is True
    assert plan.items[0].kind == "non_executable"

    tampered = _recomputed_plan(
        _compiled_plan(scoped, evidence), updates={"action_id": "null_fill"}
    )
    with pytest.raises(PermissionError, match="not a failed check in this quality run"):
        verify_executable_params(tampered, report=scoped, refusing="apply")


def test_changing_the_legality_rule_changes_all_four_refusals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scoped, evidence = _completeness_scenario()
    proposal = _declared_proposal(scoped, evidence)

    evaluation = evaluate_proposal(scoped, proposal)
    plan = _compiled_plan(scoped, evidence)
    assert evaluation.passed
    assert plan.blocked is False
    safe_proposal_for_xcom(scoped, proposal)
    verify_executable_params(plan, report=scoped, refusing="apply")

    def _refuse_every_action(
        *,
        action_id: str,
        evidence: object,
        report_failures: object,
    ) -> CheckPolicyJustification:
        del action_id, evidence, report_failures
        raise PolicyRefusal("the changed legality rule refuses every action")

    monkeypatch.setattr(check_policy, "justify_action", _refuse_every_action)

    changed_evaluation = evaluate_proposal(scoped, proposal)
    assert changed_evaluation.get("check_policy") is not None
    assert changed_evaluation.get("check_policy").passed is False  # type: ignore[union-attr]

    with pytest.raises(PermissionError, match="unbounded candidate proposal"):
        safe_proposal_for_xcom(scoped, proposal)

    changed_plan = compile_remediation_plan(scoped, proposal, target_sets=_TargetSets())
    assert changed_plan.blocked is True

    with pytest.raises(PermissionError, match="not a failed check in this quality run"):
        verify_executable_params(plan, report=scoped, refusing="apply")


def test_tampered_params_are_refused_at_apply_recompute() -> None:
    scoped, evidence = _completeness_scenario()
    plan = _compiled_plan(scoped, evidence)
    tampered = _recomputed_plan(
        plan, updates={"params": {"column": "customer_sk", "pk_column": "order_id"}}
    )

    with pytest.raises(PermissionError, match="item parameters do not match Check Policy"):
        verify_executable_params(tampered, report=scoped, refusing="apply")


def test_the_rule_cites_only_declared_failed_evidence() -> None:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    passing = report.get("dim_product.sku.uniqueness")
    assert passing is not None and not passing.failed
    failures = {check.check_id: check for check in report.failed_checks}
    evidence = QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)

    justified = check_policy.justify_action(
        action_id="quarantine_nulls",
        evidence=[evidence],
        report_failures=failures,
    )
    assert [cited.check_id for cited in justified.specs] == [evidence.check_id]
    assert justified.params == {"column": "total_amount", "pk_column": "order_id"}

    with pytest.raises(PolicyRefusal):
        check_policy.justify_action(
            action_id="quarantine_nulls",
            evidence=[QualityEvidence(check_id=passing.check_id, contract_id=passing.contract_id)],
            report_failures=failures,
        )
    with pytest.raises(PolicyRefusal):
        check_policy.justify_action(
            action_id="null_fill",
            evidence=[evidence],
            report_failures=failures,
        )
    with pytest.raises(PolicyRefusal):
        check_policy.justify_action(
            action_id="not_in_catalog",
            evidence=[evidence],
            report_failures=failures,
        )
