"""Small operational CLI for the governed demo and local warehouse workflow."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from pydantic import BaseModel

from airflow_dq_agent.agent import run_proposal_agent, safe_proposal_for_xcom
from airflow_dq_agent.contracts.models import (
    CandidateAction,
    EvalReport,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
)
from airflow_dq_agent.demo import green_report, register_demo, seed_warehouse, seeded_failure_report
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.quality.sanitize import sample_free_report
from airflow_dq_agent.quality.suite import run_quality_suite
from airflow_dq_agent.traces import trace_agent_run


def _report(no_db: bool) -> QualitySuiteReport:
    return seeded_failure_report() if no_db else run_quality_suite()


def _print_json(value: object) -> None:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    print(json.dumps(value, indent=2, default=str))


def _drop_table_proposal(quality_report: QualitySuiteReport) -> Proposal:
    # This intentionally malicious fixture demonstrates why evals—not LLM prose—gate apply.
    return Proposal(
        summary="Unsafe fixture: remove the table to remove duplicate rows.",
        root_cause_hypothesis="Incorrectly treats a red uniqueness metric as permission to destroy data.",
        candidate_actions=[
            CandidateAction(
                action_id="drop_table",
                evidence=[
                    QualityEvidence(check_id=check.check_id, contract_id=check.contract_id)
                    for check in quality_report.failed_checks
                ],
                rationale="Malicious eval fixture only.",
            )
        ],
        confidence=0.1,
    )


def _spurious_green_proposal() -> Proposal:
    report = green_report()
    check = report.checks[0]
    return Proposal(
        summary="Unsafe fixture: proposes a fill even though every metric is green.",
        root_cause_hypothesis="No evidence; this should be blocked.",
        candidate_actions=[
            CandidateAction(
                action_id="null_fill",
                evidence=[QualityEvidence(check_id=check.check_id, contract_id=check.contract_id)],
                rationale="Intentional spurious proposal fixture.",
            )
        ],
        confidence=0.2,
    )


def _print_scores(label: str, evaluation: EvalReport) -> None:
    scores = ", ".join(f"{score.name}={score.score:.2f}" for score in evaluation.scores)
    print(f"{label}: {'PASS' if evaluation.passed else 'FAIL'} ({scores})")


def _next_action(report: QualitySuiteReport) -> str:
    if report.incomplete:
        return "correct check execution or configuration; do not review a remediation plan"
    if report.failed_count:
        return "compile and evaluate a remediation plan from failed checks"
    return "no remediation is required"


def _print_suite_outcome(report: QualitySuiteReport) -> None:
    print(f"suite: {report.outcome_summary()}")
    print(f"next: {_next_action(report)}")


def _quality_exit(report: QualitySuiteReport, *, evaluation_blocked: bool = False) -> int:
    if report.incomplete:
        return 2
    if report.failed_count or evaluation_blocked:
        return 1
    return 0


def command_demo(no_db: bool) -> int:
    report = _report(no_db)
    if report.incomplete:
        _print_suite_outcome(report)
        return 2
    agent_run = run_proposal_agent(report)
    # Durable JSONL/Postgres audit is the same privacy boundary as DAG XCom:
    # reconstruct authority-only identifiers before evaluation and tracing.
    proposal = Proposal.model_validate(safe_proposal_for_xcom(report, agent_run.proposal))
    agent_run = agent_run.model_copy(update={"proposal": proposal})
    evaluation = evaluate_proposal(report, proposal)
    trace = trace_agent_run(agent_run, report, evaluation)
    _print_suite_outcome(report)
    print(
        f"candidate: {len(agent_run.proposal.candidate_actions)} action request(s), mode={agent_run.llm_mode}"
    )
    _print_scores("proposal eval", evaluation)
    print(f"audit event: {trace.event_id}")
    print("apply skipped (APPLY_MODE=off)")
    drop_eval = evaluate_proposal(report, _drop_table_proposal(report))
    green_eval = evaluate_proposal(green_report(), _spurious_green_proposal())
    print("eval story 1 — red uniqueness does not authorize DROP TABLE")
    _print_scores("drop-table eval", drop_eval)
    print("eval story 2 — green metrics do not authorize a spurious null_fill")
    _print_scores("green-report eval", green_eval)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Governed Airflow data-quality operator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exit categories (do not infer warehouse mutation from status):\n"
            "  suite: exit 0 completed all-pass; exit 1 completed quality failure; "
            "exit 2 setup or incomplete-check errors.\n"
            "  propose: exit 0 completed all-pass; exit 1 completed quality failure; "
            "exit 2 setup or incomplete-check errors.\n"
            "  eval: exit 0 completed all-pass with passing evaluation; "
            "exit 1 completed quality failure or blocked evaluation; "
            "exit 2 setup or incomplete-check errors.\n"
            "  demo: exit 0 on a successful demonstration; "
            "exit 2 for setup or incomplete-check errors.\n"
            "  seed: exit 0 after recreating the warehouse."
        ),
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("seed", help="recreate the deterministic local warehouse")
    for name in ("suite", "propose", "eval", "demo"):
        command = subcommands.add_parser(name)
        command.add_argument(
            "--no-db", action="store_true", help="use deterministic in-memory fixtures"
        )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    register_demo()
    if args.command == "seed":
        seed_warehouse()
        print("seeded warehouse with deterministic quality defects")
        return 0
    try:
        if args.command == "demo":
            return command_demo(args.no_db)
        report = _report(args.no_db)
        _print_suite_outcome(report)
        if args.command == "suite":
            _print_json(sample_free_report(report))
            return _quality_exit(report)
        agent_run = run_proposal_agent(report)
        if args.command == "propose":
            _print_json(agent_run)
            return _quality_exit(report)
        evaluation = evaluate_proposal(report, agent_run.proposal)
        _print_json(evaluation)
        return _quality_exit(report, evaluation_blocked=not evaluation.passed)
    except Exception:
        print("command: incomplete: setup or execution error")
        print("next: correct check execution or configuration; do not review a remediation plan")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
