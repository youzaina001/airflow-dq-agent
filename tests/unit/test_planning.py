from airflow_dq_agent.contracts import CandidateAction, Proposal, QualityEvidence, TargetSet
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
    assert item.evidence == [
        QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
    ]
    assert item.target_set == TargetSet(count=5, fingerprint="targets:orders-null-v1")
    assert item.policy_fingerprint
    assert "target_keys" not in plan.model_dump_json()
