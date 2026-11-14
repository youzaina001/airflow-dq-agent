from __future__ import annotations

import dataclasses
import importlib
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from airflow_dq_agent import action_definitions
from airflow_dq_agent.apply import apply_plan
from airflow_dq_agent.contracts import (
    ApplyAdmission,
    CandidateAction,
    DecisionBinding,
    EvalReport,
    HumanDecision,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.contracts.fingerprints import report_payload_fingerprint
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS, ColumnContract
from airflow_dq_agent.demo import seeded_failure_report
from airflow_dq_agent.evals import evaluate_plan
from airflow_dq_agent.planning import compile_remediation_plan, current_policy_fingerprint
from airflow_dq_agent.planning.admission import create_apply_admission
from airflow_dq_agent.planning.review import build_approval_review
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.traces import InMemoryAuditRepository
from airflow_dq_agent.traces.lineage import decision_event, review_event

NOW = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:policy-snapshot-v1")


def _compile_and_evaluate(
    check_id: str, action_id: str
) -> tuple[RemediationPlan, EvalReport, QualitySuiteReport]:
    report = seeded_failure_report()
    failed = report.get(check_id)
    assert failed is not None
    scoped = report.model_copy(update={"checks": [failed]})
    scoped = scoped.model_copy(update={"fingerprint": report_payload_fingerprint(scoped)})
    candidate = Proposal(
        summary=f"Apply {action_id} to {check_id}.",
        root_cause_hypothesis="A governed quality check failed.",
        candidate_actions=[
            CandidateAction(
                action_id=action_id,
                evidence=[
                    QualityEvidence(
                        check_id=failed.check_id,
                        contract_id=CHECK_SPECS[check_id].contract_id,
                    )
                ],
                rationale="Quarantine non-conforming rows under Check Policy.",
            )
        ],
        confidence=0.9,
    )
    plan = compile_remediation_plan(scoped, candidate, target_sets=_TargetSets())
    evaluation = evaluate_plan(plan)
    assert not plan.blocked
    assert evaluation.passed
    return plan, evaluation, scoped


def _bound_approval(
    plan: RemediationPlan, evaluation: EvalReport
) -> tuple[HumanDecision, InMemoryAuditRepository]:
    review = build_approval_review(plan, evaluation, ttl=timedelta(hours=24))
    shown = review_event(review, evaluation, "evaluation-event-1")
    decision = HumanDecision(
        decision="Approve",
        actor="approver-1",
        note="Reviewed target set.",
        review_fingerprint=review.fingerprint,
        decided_at=NOW,
    )
    event = decision_event(
        plan.quality_run_id,
        decision,
        shown,
        binding=DecisionBinding(
            plan_id=plan.plan_id,
            plan_fingerprint=plan.fingerprint,
            evaluation_id=evaluation.evaluation_id,
            evaluation_fingerprint=evaluation.fingerprint,
        ),
    )
    return (
        decision.model_copy(update={"audit_event_id": event.event_id}),
        InMemoryAuditRepository([shown, event]),
    )


def _assert_safe_refusal(exc: BaseException) -> None:
    lowered = str(exc).lower()
    assert "sample" not in lowered
    assert "9001" not in lowered


def _drift_check_description(monkeypatch: pytest.MonkeyPatch, check_id: str) -> None:
    spec = CHECK_SPECS[check_id]
    monkeypatch.setitem(CHECK_SPECS, check_id, spec.model_copy(update={"description": "drifted"}))


def _drift_remove_check_policy(monkeypatch: pytest.MonkeyPatch, check_id: str) -> None:
    spec = CHECK_SPECS[check_id]
    monkeypatch.setitem(CHECK_SPECS, check_id, spec.model_copy(update={"policies": []}))


def _drift_table_contract(monkeypatch: pytest.MonkeyPatch, table: str, column_name: str) -> None:
    contract = TABLE_CONTRACTS[table]
    extra = ColumnContract(name=column_name, dtype="utf8", nullable=True)
    monkeypatch.setitem(
        TABLE_CONTRACTS,
        table,
        contract.model_copy(update={"columns": [*contract.columns, extra]}),
    )


def _drift_action_metadata(monkeypatch: pytest.MonkeyPatch, action_id: str) -> None:
    governed = action_definitions.get_governed_action(action_id)
    drifted = dataclasses.replace(
        governed,
        metadata=governed.metadata.model_copy(update={"notes": "drifted governed action notes"}),
    )
    monkeypatch.setitem(action_definitions._GOVERNED_ACTIONS, action_id, drifted)


def _drift_renderer_version(monkeypatch: pytest.MonkeyPatch) -> None:
    policy_snapshot = importlib.import_module("airflow_dq_agent.planning.policy_snapshot")
    monkeypatch.setattr(policy_snapshot, "RENDERER_VERSION", "controlled-renderer-v999")


def test_policy_snapshot_module_and_consumers_do_not_import_compiler() -> None:
    code = """
import json, sys
from airflow_dq_agent.planning import policy_snapshot, admission
from airflow_dq_agent.evals import harness
from airflow_dq_agent.apply import executor

assert hasattr(policy_snapshot, "policy_snapshot_fingerprint")
assert hasattr(policy_snapshot, "current_policy_fingerprint")
assert hasattr(policy_snapshot, "verify_policy_snapshot")
assert hasattr(policy_snapshot, "RENDERER_VERSION")
loaded = "airflow_dq_agent.planning.compiler" in sys.modules
print(json.dumps({"compiler_loaded": loaded}))
"""
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    assert '"compiler_loaded": false' in proc.stdout


def test_referenced_contract_drift_changes_orphan_snapshot_and_blocks_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_id = "fact_order_items.product_sk.referential_integrity"
    plan, evaluation, report = _compile_and_evaluate(check_id, "quarantine_orphans")
    decision, repository = _bound_approval(plan, evaluation)
    before = current_policy_fingerprint(plan)
    assert before == plan.policy_fingerprint

    # Unrelated dimension contract drift must not invalidate fact_order_items.product_sk -> dim_product.
    _drift_table_contract(monkeypatch, "dim_customer", "drifted_unrelated_col")
    assert current_policy_fingerprint(plan) == before

    # Referenced table contract drift (dim_product) must invalidate the Policy Snapshot.
    _drift_table_contract(monkeypatch, "dim_product", "drifted_ref_col")
    after = current_policy_fingerprint(plan)
    assert after != before

    re_eval = evaluate_plan(plan)
    assert not re_eval.passed
    policy_score = next(score for score in re_eval.scores if score.name == "policy_snapshot")
    assert policy_score.score == 0.0

    with pytest.raises(PermissionError, match="policy snapshot drifted") as refused:
        create_apply_admission(
            plan,
            evaluation,
            decision,
            now=NOW,
            report=report,
            audit_repository=repository,
        )
    _assert_safe_refusal(refused.value)


def test_referenced_contract_drift_blocks_already_issued_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_id = "fact_order_items.product_sk.referential_integrity"
    plan, evaluation, report = _compile_and_evaluate(check_id, "quarantine_orphans")
    decision, repository = _bound_approval(plan, evaluation)
    admission = create_apply_admission(
        plan, evaluation, decision, now=NOW, report=report, audit_repository=repository
    )
    assert isinstance(admission, ApplyAdmission)

    _drift_table_contract(monkeypatch, "dim_product", "drifted_ref_col")

    with pytest.raises(PermissionError, match="policy snapshot drifted") as refused:
        apply_plan(plan, evaluation, admission, report=report, dry_run=False, now=NOW)
    _assert_safe_refusal(refused.value)


@pytest.mark.parametrize(
    ("check_id", "action_id", "mutate"),
    [
        (
            "fact_orders.total_amount.completeness",
            "quarantine_nulls",
            lambda mp: _drift_check_description(mp, "fact_orders.total_amount.completeness"),
        ),
        (
            "fact_orders.total_amount.completeness",
            "quarantine_nulls",
            lambda mp: _drift_remove_check_policy(mp, "fact_orders.total_amount.completeness"),
        ),
        (
            "fact_orders.total_amount.completeness",
            "quarantine_nulls",
            lambda mp: _drift_table_contract(mp, "fact_orders", "drifted_source_col"),
        ),
        (
            "fact_order_items.product_sk.referential_integrity",
            "quarantine_orphans",
            lambda mp: _drift_remove_check_policy(
                mp, "fact_order_items.product_sk.referential_integrity"
            ),
        ),
        (
            "fact_order_items.product_sk.referential_integrity",
            "quarantine_orphans",
            lambda mp: _drift_table_contract(mp, "dim_product", "drifted_ref_col"),
        ),
        (
            "fact_orders.total_amount.completeness",
            "quarantine_nulls",
            lambda mp: _drift_action_metadata(mp, "quarantine_nulls"),
        ),
        (
            "fact_orders.total_amount.completeness",
            "quarantine_nulls",
            _drift_renderer_version,
        ),
    ],
    ids=[
        "check_description",
        "check_policy_removed",
        "source_contract",
        "orphan_policy_removed",
        "referenced_contract",
        "action_metadata",
        "renderer_version",
    ],
)
def test_policy_snapshot_drift_refuses_admission_evaluation_and_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    check_id: str,
    action_id: str,
    mutate: Callable[[pytest.MonkeyPatch], None],
) -> None:
    plan, evaluation, report = _compile_and_evaluate(check_id, action_id)
    decision, repository = _bound_approval(plan, evaluation)

    mutate(monkeypatch)

    drifted_eval = evaluate_plan(plan)
    assert not drifted_eval.passed
    policy_score = next(score for score in drifted_eval.scores if score.name == "policy_snapshot")
    assert policy_score.score == 0.0

    with pytest.raises(PermissionError, match="policy snapshot drifted") as refused_admission:
        create_apply_admission(
            plan,
            evaluation,
            decision,
            now=NOW,
            report=report,
            audit_repository=repository,
        )
    _assert_safe_refusal(refused_admission.value)

    with pytest.raises(PermissionError, match="policy snapshot drifted") as refused_dry_run:
        apply_plan(plan, evaluation, report=report, dry_run=True, now=NOW)
    _assert_safe_refusal(refused_dry_run.value)


def test_renderer_version_and_orphan_sql_shape_preserved() -> None:
    policy_snapshot = importlib.import_module("airflow_dq_agent.planning.policy_snapshot")
    assert policy_snapshot.RENDERER_VERSION == "controlled-renderer-v2"

    spec = CHECK_SPECS["fact_order_items.product_sk.referential_integrity"]
    governed = action_definitions.get_governed_action("quarantine_orphans")
    assert governed.referenced_tables(spec) == ("dim_product",)

    params = governed.derive_params(spec)
    step = governed.render(table="fact_order_items", params=params, run_id="run-1")
    assert 'LEFT JOIN "warehouse"."dim_product" r ON r."product_sk" = t."product_sk"' in step.sql

    planning = importlib.import_module("airflow_dq_agent.planning")
    assert hasattr(planning, "TargetSetResolver")
    with pytest.raises(AttributeError):
        _ = planning.does_not_exist


def test_verify_policy_snapshot_rejects_mismatched_admission_policy() -> None:
    policy_snapshot = importlib.import_module("airflow_dq_agent.planning.policy_snapshot")
    plan, evaluation, report = _compile_and_evaluate(
        "fact_orders.total_amount.completeness", "quarantine_nulls"
    )
    decision, repository = _bound_approval(plan, evaluation)
    admission = create_apply_admission(
        plan, evaluation, decision, now=NOW, report=report, audit_repository=repository
    )
    stale_admission = admission.model_copy(update={"policy_fingerprint": "sha256:stale-admission"})

    with pytest.raises(PermissionError, match="policy snapshot drifted after admission"):
        policy_snapshot.verify_policy_snapshot(
            plan,
            admission=stale_admission,
            refusing="apply",
            after="admission",
        )


def test_source_fk_removal_fails_eval_and_refuses_admission_without_crashing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_id = "fact_order_items.product_sk.referential_integrity"
    plan, evaluation, report = _compile_and_evaluate(check_id, "quarantine_orphans")
    decision, repository = _bound_approval(plan, evaluation)
    before = current_policy_fingerprint(plan)

    contract = TABLE_CONTRACTS["fact_order_items"]
    monkeypatch.setitem(
        TABLE_CONTRACTS,
        "fact_order_items",
        contract.model_copy(update={"foreign_keys": []}),
    )

    assert current_policy_fingerprint(plan) != before
    drifted_eval = evaluate_plan(plan)
    assert not drifted_eval.passed
    policy_score = next(score for score in drifted_eval.scores if score.name == "policy_snapshot")
    assert policy_score.score == 0.0

    with pytest.raises(PermissionError, match="policy snapshot drifted") as refused:
        create_apply_admission(
            plan,
            evaluation,
            decision,
            now=NOW,
            report=report,
            audit_repository=repository,
        )
    _assert_safe_refusal(refused.value)
