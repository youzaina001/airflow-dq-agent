from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts import (
    CandidateAction,
    ExecutablePlanItem,
    Proposal,
    QualityEvidence,
    TargetSet,
)
from airflow_dq_agent.contracts.models import CheckStatus
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.quality.fixtures import seeded_failure_report


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
