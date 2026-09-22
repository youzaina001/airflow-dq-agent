import json

from airflow_dq_agent.contracts import (
    CandidateAction,
    Proposal,
    QualityEvidence,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.contracts.models import CheckResult, QualitySuiteReport
from airflow_dq_agent.demo import seeded_failure_report
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.traces.lineage import plan_event

UNKNOWN_CHECK_REASON = "unknown check is not in the quality catalog"
INVALID_EVIDENCE_REASON = "candidate evidence is invalid for this quality report"
UNSUPPORTED_ACTION_REASON = "requested action is unsupported by the check policy"
TARGET_RESOLUTION_REASON = "remediation target set could not be resolved"
GENERIC_UNAVAILABLE_REASON = "candidate action is unavailable under the controlled policy"
OMITTED_COVERAGE_REASON = "candidate proposal omitted failed-check coverage"

_FORBIDDEN_LINEAGE_TOKENS = ("sample_failures", "9001", "SHIPPPED", "prompt")


class _NoTargetLookup:
    def resolve(self, **_: object) -> TargetSet:
        raise AssertionError("unreviewed actions must not resolve a target set")


def _one_failure_report() -> tuple[QualitySuiteReport, CheckResult]:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    return report.model_copy(update={"checks": [failed]}), failed


def _candidate(action_id: str, check_id: str, contract_id: str) -> Proposal:
    return Proposal(
        summary="Quarantine the rows that failed the declared completeness check.",
        root_cause_hypothesis="The source omitted a required total.",
        candidate_actions=[
            CandidateAction(
                action_id=action_id,
                evidence=[QualityEvidence(check_id=check_id, contract_id=contract_id)],
                rationale="Preserve source rows while routing the failed target set for review.",
            )
        ],
        confidence=0.1,
    )


def _assert_sample_free_blocked_lineage(plan: RemediationPlan, reason: str) -> None:
    assert plan.blocked is True
    assert reason in plan.blocked_reasons
    assert GENERIC_UNAVAILABLE_REASON not in plan.blocked_reasons
    for blocked in plan.blocked_reasons:
        for token in _FORBIDDEN_LINEAGE_TOKENS:
            assert token not in blocked
        assert "@" not in blocked
        assert "secret" not in blocked.lower()
        assert "c101.invalid" not in blocked
    event = plan_event(plan, "quality-report-predecessor")
    assert event.kind == "plan_blocked"
    assert reason in event.reasons
    body = json.dumps(event.model_dump(mode="json"))
    for token in _FORBIDDEN_LINEAGE_TOKENS:
        assert token not in body
    assert "c101.invalid" not in body
    assert "secret" not in body.lower()


def test_compiler_blocks_catalogued_but_unreviewed_null_fill_without_target_lookup() -> None:
    one_failure_report, failed = _one_failure_report()
    candidate = _candidate("null_fill", failed.check_id, failed.contract_id)

    plan = compile_remediation_plan(one_failure_report, candidate, target_sets=_NoTargetLookup())

    assert plan.items[0].kind == "non_executable"
    assert plan.blocked_reasons == [UNSUPPORTED_ACTION_REASON]
    _assert_sample_free_blocked_lineage(plan, UNSUPPORTED_ACTION_REASON)


def test_compiler_blocks_unknown_check_not_in_the_quality_catalog() -> None:
    one_failure_report, failed = _one_failure_report()
    candidate = _candidate(
        "quarantine_nulls",
        "not.a.catalogued.quality.check",
        failed.contract_id,
    )

    plan = compile_remediation_plan(one_failure_report, candidate, target_sets=_NoTargetLookup())

    assert plan.items[0].kind == "non_executable"
    assert UNKNOWN_CHECK_REASON in plan.blocked_reasons
    assert INVALID_EVIDENCE_REASON not in plan.blocked_reasons
    assert OMITTED_COVERAGE_REASON in plan.blocked_reasons
    _assert_sample_free_blocked_lineage(plan, UNKNOWN_CHECK_REASON)


def test_compiler_blocks_invalid_evidence_for_this_quality_report() -> None:
    one_failure_report, failed = _one_failure_report()
    candidate = _candidate("quarantine_nulls", failed.check_id, "warehouse.dim_customer")

    plan = compile_remediation_plan(one_failure_report, candidate, target_sets=_NoTargetLookup())

    assert plan.items[0].kind == "non_executable"
    assert plan.blocked_reasons == [INVALID_EVIDENCE_REASON]
    assert UNKNOWN_CHECK_REASON not in plan.blocked_reasons
    _assert_sample_free_blocked_lineage(plan, INVALID_EVIDENCE_REASON)


def test_compiler_blocks_when_remediation_target_set_cannot_be_resolved() -> None:
    one_failure_report, failed = _one_failure_report()
    candidate = _candidate("quarantine_nulls", failed.check_id, failed.contract_id)

    class FailingTargets:
        def resolve(self, **_: object) -> TargetSet:
            raise ValueError("target lookup failed for order_id 9001 SHIPPPED")

    plan = compile_remediation_plan(one_failure_report, candidate, target_sets=FailingTargets())

    assert plan.items[0].kind == "non_executable"
    assert plan.blocked_reasons == [TARGET_RESOLUTION_REASON]
    assert "target lookup failed" not in "".join(plan.blocked_reasons)
    _assert_sample_free_blocked_lineage(plan, TARGET_RESOLUTION_REASON)
