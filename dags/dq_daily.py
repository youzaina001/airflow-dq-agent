"""Daily governed DQ flow: detect → propose → evaluate → optional HITL apply."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException

from airflow.providers.standard.operators.hitl import ApprovalOperator

from airflow_dq_agent.agent import build_read_only_toolset, run_proposal_agent
from airflow_dq_agent.agent.runner import AgentRun, build_prompt
from airflow_dq_agent.apply import apply_proposal
from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts import EvalReport, HumanDecision, Proposal, QualitySuiteReport
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.quality import run_quality_suite
from airflow_dq_agent.traces import trace_agent_run

# Proposal is deliberately imported at module scope so Airflow can serialize it through XCom.
__all__ = ["Proposal"]

settings = get_settings()


@dag(
    dag_id="dq_daily",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["data-quality", "governed-ai"],
)
def dq_daily() -> None:
    @task
    def run_suite_task() -> dict[str, Any]:
        return run_quality_suite().model_dump(mode="json")

    @task
    def propose_stub_task(report_data: dict[str, Any]) -> dict[str, Any]:
        report = QualitySuiteReport.model_validate(report_data)
        return run_proposal_agent(report).proposal.model_dump(mode="json")

    # We deliberately do not give the live agent SQLToolset.query (or any arbitrary SQL tool).
    # FunctionToolset only contains catalog reads, controlled check sampling, and schema reads.
    if settings.llm_mode == "live":

        @task.agent(
            llm_conn_id="pydanticai_default",
            output_type=Proposal,
            serialize_output=True,
            toolsets=[build_read_only_toolset()],
        )
        def propose_live_task(report_data: dict[str, Any]) -> str:
            return build_prompt(QualitySuiteReport.model_validate(report_data))

        propose_task = propose_live_task
    else:
        propose_task = propose_stub_task

    @task
    def evaluate_task(report_data: dict[str, Any], proposal_data: dict[str, Any]) -> dict[str, Any]:
        report = QualitySuiteReport.model_validate(report_data)
        proposal = Proposal.model_validate(proposal_data)
        return evaluate_proposal(report, proposal).model_dump(mode="json")

    @task
    def write_trace_task(
        report_data: dict[str, Any], proposal_data: dict[str, Any], evaluation_data: dict[str, Any]
    ) -> str:
        report = QualitySuiteReport.model_validate(report_data)
        proposal = Proposal.model_validate(proposal_data)
        evaluation = EvalReport.model_validate(evaluation_data)
        # The Airflow @task.agent path has provider-side tool logs; this event preserves its output contract.
        agent_run = AgentRun(
            proposal=proposal, prompt=build_prompt(report), llm_mode=settings.llm_mode
        )
        trace = trace_agent_run(agent_run, report, evaluation, dag_id="dq_daily")
        return trace.trace_id

    report = run_suite_task()
    proposal = propose_task(report)
    evaluation = evaluate_task(report, proposal)
    write_trace_task(report, proposal, evaluation)

    if settings.apply_mode == "hitl":

        @task
        def require_approval(
            report_data: dict[str, Any],
            proposal_data: dict[str, Any],
            evaluation_data: dict[str, Any],
        ) -> None:
            report = QualitySuiteReport.model_validate(report_data)
            proposal_value = Proposal.model_validate(proposal_data)
            evaluation = EvalReport.model_validate(evaluation_data)
            if not evaluation.passed or not proposal_value.steps or not report.failed_count:
                raise AirflowSkipException(
                    "No passing proposal with remediation steps requires approval"
                )

        @task
        def apply_after_approval(
            proposal_data: dict[str, Any], evaluation_data: dict[str, Any], approval_value: str
        ) -> dict[str, Any]:
            if str(approval_value).lower() != "approve":
                # This is the TaskFlow equivalent of SkipMixin behavior: rejection never reaches apply.
                raise AirflowSkipException("HITL rejected the proposal")
            decision = HumanDecision(decision="Approve")
            result = apply_proposal(
                Proposal.model_validate(proposal_data),
                EvalReport.model_validate(evaluation_data),
                approval=decision,
                dry_run=False,
            )
            return result.model_dump(mode="json")

        approval_gate = require_approval(report, proposal, evaluation)
        approval = ApprovalOperator(
            task_id="approve_proposal",
            subject="Approve governed DQ remediation proposal",
            body="Evaluation passed. Approve the whole proposal or reject it.",
            defaults="Reject",
            fail_on_reject=False,
        )
        approval_gate >> approval
        apply_after_approval(proposal, evaluation, approval.output)


dq_daily()
