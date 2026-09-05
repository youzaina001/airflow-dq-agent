import pytest

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts import (
    CandidateAction,
    CheckResult,
    ExecutablePlanItem,
    Proposal,
    QualityEvidence,
    TargetSet,
)
from airflow_dq_agent.contracts.models import CheckStatus
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.planning.integrity import plan_payload_fingerprint, verify_executable_params
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.quality.registry import get_check_spec


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:orders-null-v1")


def test_compiler_creates_an_executable_item_from_declared_check_policy() -> None:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    candidate = Proposal(
        summary="Quarantine the rows that failed the declared completeness check.",
        root_cause_hypothesis="The source omitted a required total.",
        candidate_actions=[
            CandidateAction(
                action_id="quarantine_nulls",
                evidence=[
                    QualityEvidence(
                        check_id=failed.check_id,
                        contract_id=failed.contract_id,
                    )
                ],
                rationale="Preserve source rows while routing the failed target set for review.",
            )
        ],
        confidence=0.8,
    )

    plan = compile_remediation_plan(report, candidate, target_sets=_TargetSets())

    assert plan.blocked is True  # Other failures were deliberately not proposed in this slice.
    item = plan.items[0]
    assert item.kind == "executable"
    assert item.action_id == "quarantine_nulls"
    assert item.table == "fact_orders"
    assert item.params == {"column": "total_amount", "pk_column": "order_id"}
    assert item.evidence == (
        QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id),
    )
    assert item.target_set == TargetSet(count=5, fingerprint="targets:orders-null-v1")
    assert item.policy_fingerprint
    assert "target_keys" not in plan.model_dump_json()


def test_compiler_blocks_catalogued_but_unreviewed_null_fill_without_target_lookup() -> None:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    one_failure_report = report.model_copy(update={"checks": [failed]})

    class NoTargetLookup:
        def resolve(self, **_: object) -> TargetSet:
            raise AssertionError("unreviewed actions must not resolve a target set")

    candidate = Proposal(
        summary="Fill the missing values.",
        root_cause_hypothesis="An unsafe automated fill was requested.",
        candidate_actions=[
            CandidateAction(
                action_id="null_fill",
                evidence=[
                    QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                ],
                rationale="This catalogued action has no reviewed policy.",
            )
        ],
        confidence=0.1,
    )

    plan = compile_remediation_plan(one_failure_report, candidate, target_sets=NoTargetLookup())

    assert plan.blocked is True
    assert plan.items[0].kind == "non_executable"
    assert plan.blocked_reasons == ["candidate action is unavailable under the controlled policy"]


def test_governed_action_renders_an_executable_compiled_item() -> None:
    item = ExecutablePlanItem(
        item_id="candidate-0",
        action_id="quarantine_nulls",
        table="fact_orders",
        params={"column": "total_amount", "pk_column": "order_id"},
        evidence=[
            QualityEvidence(
                check_id="fact_orders.total_amount.completeness",
                contract_id="warehouse.fact_orders",
            )
        ],
        target_set=TargetSet(count=5, fingerprint="targets:orders-null-v1"),
        policy_fingerprint="policy:orders-null-v1",
    )

    rendered = get_governed_action(item.action_id).render(
        table=item.table, params=item.params, run_id="test-run"
    )

    assert (
        rendered.target_sql
        == 'SELECT t."order_id" FROM "warehouse"."fact_orders" t WHERE t."total_amount" IS NULL ORDER BY t."order_id"'
    )
    assert "FOR UPDATE" not in rendered.target_sql
    assert rendered.params["run_id"] == "test-run"


def test_compiler_does_not_treat_error_status_as_executable_evidence() -> None:
    report = seeded_failure_report()
    errored = report.get("fact_orders.total_amount.completeness")
    assert errored is not None
    errored = errored.model_copy(
        update={"status": CheckStatus.ERROR, "n_failed": 0, "sample_failures": []}
    )
    assert not errored.failed
    scoped_report = report.model_copy(update={"checks": [errored]})
    assert scoped_report.failed_count == 0

    candidate = Proposal(
        summary="Quarantine rows for a check that could not be evaluated.",
        root_cause_hypothesis="ERROR is not failed-check Quality Evidence.",
        candidate_actions=[
            CandidateAction(
                action_id="quarantine_nulls",
                evidence=[
                    QualityEvidence(check_id=errored.check_id, contract_id=errored.contract_id)
                ],
                rationale="This check never produced failed-row evidence.",
            )
        ],
        confidence=0.1,
    )

    plan = compile_remediation_plan(scoped_report, candidate, target_sets=_TargetSets())

    assert plan.blocked is True
    assert plan.items[0].kind == "non_executable"
    assert not any(item.kind == "executable" for item in plan.items)


def test_red_dim_product_uniqueness_does_not_block_executable_completeness() -> None:
    report = seeded_failure_report()
    completeness = report.get("fact_orders.total_amount.completeness")
    uniqueness_spec = get_check_spec("dim_product.sku.uniqueness")
    assert completeness is not None
    uniqueness = CheckResult(
        check_id=uniqueness_spec.check_id,
        table=uniqueness_spec.table,
        column=uniqueness_spec.column,
        dimension=uniqueness_spec.dimension,
        status=CheckStatus.FAIL,
        n_failed=2,
        n_total=40,
        message="sku is unique",
        contract_id=uniqueness_spec.contract_id,
        predicate=uniqueness_spec.description,
    )
    scoped_report = report.model_copy(update={"checks": [completeness, uniqueness]})
    candidate = Proposal(
        summary="Remediate the failed completeness and uniqueness checks.",
        root_cause_hypothesis="A required total was omitted and a product sku collided.",
        candidate_actions=[
            CandidateAction(
                action_id=get_check_spec(completeness.check_id).policies[0].action_id,
                evidence=[
                    QualityEvidence(
                        check_id=completeness.check_id, contract_id=completeness.contract_id
                    )
                ],
                rationale="Request the reviewed completeness action.",
            ),
            CandidateAction(
                action_id=uniqueness_spec.policies[0].action_id,
                evidence=[
                    QualityEvidence(
                        check_id=uniqueness.check_id, contract_id=uniqueness.contract_id
                    )
                ],
                rationale="Request the reviewed uniqueness action.",
            ),
        ],
        confidence=0.9,
    )

    plan = compile_remediation_plan(scoped_report, candidate, target_sets=_TargetSets())

    completeness_item = next(
        item
        for item in plan.items
        if any(evidence.check_id == completeness.check_id for evidence in item.evidence)
    )
    uniqueness_item = next(
        item
        for item in plan.items
        if any(evidence.check_id == uniqueness.check_id for evidence in item.evidence)
    )
    assert completeness_item.kind == "executable"
    assert completeness_item.action_id == "quarantine_nulls"
    assert uniqueness_item.kind == "executable"
    assert uniqueness_item.action_id == "no_op_alert"
    assert plan.blocked is False


def test_compiler_refuses_duplicate_action_and_evidence() -> None:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped_report = report.model_copy(update={"checks": [failed]})
    evidence = [QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)]
    duplicate = CandidateAction(
        action_id="quarantine_nulls",
        evidence=evidence,
        rationale="Preserve source rows for review.",
    )
    candidate = Proposal(
        summary="Quarantine the same failed rows twice.",
        root_cause_hypothesis="Duplicate candidate actions must not compile into two inserts.",
        candidate_actions=[duplicate, duplicate],
        confidence=0.9,
    )

    plan = compile_remediation_plan(scoped_report, candidate, target_sets=_TargetSets())

    assert plan.blocked is True
    assert [item.kind for item in plan.items] == ["non_executable", "non_executable"]
    assert not any(item.kind == "executable" for item in plan.items)


def test_verify_executable_params_refuses_duplicate_action_and_evidence() -> None:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped_report = report.model_copy(update={"checks": [failed]})
    plan = compile_remediation_plan(
        scoped_report,
        Proposal(
            summary="Quarantine failed rows.",
            root_cause_hypothesis="A required value was omitted.",
            candidate_actions=[
                CandidateAction(
                    action_id="quarantine_nulls",
                    evidence=[
                        QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                    ],
                    rationale="Preserve source rows for review.",
                )
            ],
            confidence=0.9,
        ),
        target_sets=_TargetSets(),
    )
    original = plan.items[0]
    assert isinstance(original, ExecutablePlanItem)
    duplicate = original.model_copy(update={"item_id": "candidate-1"})
    items = [original, duplicate]
    tampered = plan.model_copy(
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

    with pytest.raises(PermissionError, match="duplicate"):
        verify_executable_params(tampered, report=scoped_report, refusing="admission")
