import json
from pathlib import Path

import pytest

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.cli import _drop_table_proposal, _spurious_green_proposal
from airflow_dq_agent.contracts.models import (
    CandidateAction,
    CheckStatus,
    Proposal,
    QualityEvidence,
)
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.quality.fixtures import green_report, seeded_failure_report


def test_stub_proposal_is_allow_list_grounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_MODE", "stub")
    report = seeded_failure_report()
    agent_run = run_proposal_agent(report)
    evaluation = evaluate_proposal(report, agent_run.proposal)
    assert agent_run.llm_mode == "stub"
    assert {
        evidence.check_id
        for action in agent_run.proposal.candidate_actions
        for evidence in action.evidence
    } == set(report.failing_check_ids)
    assert evaluation.passed


def test_green_report_rejects_spurious_remediation() -> None:
    evaluation = evaluate_proposal(green_report(), _spurious_green_proposal())
    assert not evaluation.passed
    assert evaluation.get("groundedness").passed is False  # type: ignore[union-attr]


def test_drop_table_is_blocked_by_destructive_and_allow_list_evals() -> None:
    report = seeded_failure_report()
    evaluation = evaluate_proposal(report, _drop_table_proposal(report))
    assert evaluation.get("destructive_risk").score == 0.0  # type: ignore[union-attr]
    assert evaluation.get("allowlist_compliance").score == 0.0  # type: ignore[union-attr]


def test_replay_revalidates_trace_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = Path(__file__).parents[2] / "evals/fixtures/traces/replay-proposal.json"
    monkeypatch.setenv("LLM_MODE", "replay")
    monkeypatch.setenv("REPLAY_TRACE_PATH", str(fixture))
    report = seeded_failure_report()
    run = run_proposal_agent(report)
    assert run.llm_mode == "replay"
    assert evaluate_proposal(report, run.proposal).passed


def test_evaluate_proposal_fails_null_fill_on_red_completeness() -> None:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped_report = report.model_copy(update={"checks": [failed]})
    evaluation = evaluate_proposal(
        scoped_report,
        Proposal(
            summary="Fill missing totals.",
            root_cause_hypothesis="An unreviewed fill was requested for red completeness.",
            candidate_actions=[
                CandidateAction(
                    action_id="null_fill",
                    evidence=[
                        QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                    ],
                    rationale="Catalogued but not declared by this Check Policy.",
                )
            ],
            confidence=0.1,
        ),
    )

    assert evaluation.passed is False
    assert "PASS — compile this candidate." not in evaluation.summary_markdown
    check_policy = evaluation.get("check_policy")
    assert check_policy is not None
    assert check_policy.passed is False
    assert check_policy.score == 0.0


def test_evaluate_proposal_fails_dedupe_on_dim_product_uniqueness() -> None:
    report = seeded_failure_report()
    uniqueness = report.get("dim_product.sku.uniqueness")
    assert uniqueness is not None
    failed = uniqueness.model_copy(
        update={"status": CheckStatus.FAIL, "n_failed": 2, "message": "sku is unique"}
    )
    scoped_report = report.model_copy(update={"checks": [failed]})
    evaluation = evaluate_proposal(
        scoped_report,
        Proposal(
            summary="Dedupe product skus.",
            root_cause_hypothesis="Uniqueness on dim_product is not a bindable dedupe policy.",
            candidate_actions=[
                CandidateAction(
                    action_id="dedupe_keep_min_pk",
                    evidence=[
                        QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                    ],
                    rationale="This action is not declared by the dim_product uniqueness Check Policy.",
                )
            ],
            confidence=0.1,
        ),
    )

    assert evaluation.passed is False
    check_policy = evaluation.get("check_policy")
    assert check_policy is not None
    assert check_policy.passed is False


def test_evaluate_proposal_omits_unexpected_field_input_from_eval_report() -> None:
    leaked = "leaked-sample-9001@example.invalid"
    evaluation = evaluate_proposal(
        seeded_failure_report(),
        {
            "summary": "Quarantine failed rows.",
            "root_cause_hypothesis": "A required value was omitted.",
            "confidence": 0.9,
            "secret_rows": leaked,
        },
    )
    assert not evaluation.passed
    durable = json.dumps(evaluation.model_dump(mode="json"))
    assert leaked not in durable
    assert leaked not in evaluation.summary_markdown
    for score in evaluation.scores:
        assert leaked not in json.dumps(score.details)
        assert leaked not in score.rationale
