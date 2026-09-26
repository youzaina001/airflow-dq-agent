"""Daily governed DQ flow: report → candidate → plan → eval → audited HITL → apply."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from airflow_dq_agent.agent import run_proposal_agent, safe_proposal_for_xcom
from airflow_dq_agent.airflow_hitl import AuditedApprovalOperator
from airflow_dq_agent.apply import apply_plan
from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts import (
    ApplyAdmission,
    EvalReport,
    HumanDecision,
    Proposal,
    QualitySuiteReport,
    RemediationPlan,
)
from airflow_dq_agent.demo import register_demo
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.planning.preparation import CandidateEvaluationFailed, prepare_plan_review
from airflow_dq_agent.planning.admission import create_apply_admission
from airflow_dq_agent.planning.integrity import verify_report_integrity
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.quality import run_quality_suite, sample_free_report
from airflow_dq_agent.traces import PostgresAuditRepository, append_event, candidate_proposal_event
from airflow_dq_agent.warehouse.db import make_engine

# Synthetic demo warehouse. HITL/apply against an adopter table is
# examples/dq_external_invoice.py, not this file with a swapped register_*.
register_demo()
settings = get_settings()
if settings.apply_mode == "hitl" and not settings.hitl_approver_id_set:
    raise RuntimeError("APPLY_MODE=hitl requires at least one HITL_APPROVER_IDS identity")


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
        # XCom is durable storage, like JSONL and Postgres audit lineage. The
        # report crosses this boundary through an allow-list of named fields:
        # IDs, counts, messages, and observed columns; never sample_failures.
        return sample_free_report(run_quality_suite(settings.read_dsn or settings.warehouse_dsn))

    @task
    def propose_task(report_data: dict[str, Any]) -> dict[str, Any]:
        report = QualitySuiteReport.model_validate(report_data)
        verify_report_integrity(report, refusing="proposal")
        # The raw model result and any bounded tool samples remain transient inside
        # this task. Only canonical authority identifiers and controlled text are
        # reconstructed for the durable XCom return value.
        return safe_proposal_for_xcom(report, run_proposal_agent(report).proposal)

    @task
    def audit_candidate_task(
        report_data: dict[str, Any], proposal_data: dict[str, Any]
    ) -> dict[str, Any]:
        report = QualitySuiteReport.model_validate(report_data)
        proposal = Proposal.model_validate(proposal_data)
        verify_report_integrity(report, refusing="candidate audit")
        if report.audit_event_id is None:
            raise RuntimeError("quality report has no persisted audit root")
        event = candidate_proposal_event(report, proposal, report.audit_event_id)
        append_event(event)
        candidate_evaluation = evaluate_proposal(report, proposal)
        return {
            "proposal": proposal.model_dump(mode="json"),
            "candidate_event_id": event.event_id,
            "candidate_evaluation": candidate_evaluation.model_dump(mode="json"),
        }

    @task
    def prepare_plan_task(
        report_data: dict[str, Any], candidate_data: dict[str, Any]
    ) -> dict[str, Any]:
        report = QualitySuiteReport.model_validate(report_data)
        proposal = Proposal.model_validate(candidate_data["proposal"])
        candidate_evaluation = EvalReport.model_validate(candidate_data["candidate_evaluation"])
        try:
            return prepare_plan_review(
                report,
                proposal,
                candidate_evaluation=candidate_evaluation,
                candidate_event_id=str(candidate_data["candidate_event_id"]),
                target_sets=PostgresTargetSetResolver(
                    engine=make_engine(settings.read_dsn or settings.warehouse_dsn)
                ),
                persist=append_event,
                ttl=settings.apply_admission_ttl,
            )
        except CandidateEvaluationFailed as exc:
            raise AirflowSkipException(str(exc)) from exc

    report = run_suite_task()
    proposal = propose_task(report)
    candidate = audit_candidate_task(report, proposal)
    evaluated = prepare_plan_task(report, candidate)

    @task
    def require_approval(evaluation_data: dict[str, Any]) -> None:
        if settings.apply_mode != "hitl":
            raise AirflowSkipException("APPLY_MODE=off does not request human approval")
        plan = RemediationPlan.model_validate(evaluation_data["plan"])
        evaluation = EvalReport.model_validate(evaluation_data["evaluation"])
        if plan.blocked or not evaluation.passed or not plan.items:
            raise AirflowSkipException("No passing executable remediation plan requires approval")

    @task
    def admit_apply_task(
        report_data: dict[str, Any],
        evaluation_data: dict[str, Any],
        decision_data: dict[str, Any],
    ) -> dict[str, Any]:
        if settings.apply_mode != "hitl":
            raise AirflowSkipException("APPLY_MODE=off refuses apply admission")
        # Honest Reject/Timeout skip the apply branch; invalid decisions fail loudly.
        report = QualitySuiteReport.model_validate(report_data)
        plan = RemediationPlan.model_validate(evaluation_data["plan"])
        evaluation = EvalReport.model_validate(evaluation_data["evaluation"])
        parsed_decision = HumanDecision.model_validate(decision_data)
        if parsed_decision.decision in {"Reject", "Timeout"}:
            raise AirflowSkipException("HITL did not approve this remediation plan")
        return create_apply_admission(
            plan,
            evaluation,
            parsed_decision,
            report=report,
            ttl=settings.apply_admission_ttl,
            audit_repository=PostgresAuditRepository(settings.audit_dsn or settings.warehouse_dsn),
        ).model_dump(mode="json")

    @task
    def apply_after_admission_task(
        report_data: dict[str, Any],
        evaluation_data: dict[str, Any],
        admission_data: dict[str, Any],
    ) -> dict[str, Any]:
        if settings.apply_mode != "hitl":
            raise AirflowSkipException("APPLY_MODE=off refuses mutation")
        report = QualitySuiteReport.model_validate(report_data)
        plan = RemediationPlan.model_validate(evaluation_data["plan"])
        evaluation = EvalReport.model_validate(evaluation_data["evaluation"])
        admission = ApplyAdmission.model_validate(admission_data)
        result = apply_plan(
            plan,
            evaluation,
            admission,
            report=report,
            dry_run=False,
            engine=make_engine(settings.apply_dsn or settings.warehouse_dsn),
        )
        return result.model_dump(mode="json")

    approval_gate = require_approval(evaluated)
    approval = AuditedApprovalOperator(
        task_id="approve_remediation_plan",
        subject="Approve governed DQ remediation plan",
        # HITL body is a string; the sample-free review is rendered upstream.
        body="{{ ti.xcom_pull(task_ids='prepare_plan_task')['approval_review_body'] }}",
        quality_run_id="{{ ti.xcom_pull(task_ids='run_suite_task')['run_id'] }}",
        predecessor_event_id=(
            "{{ ti.xcom_pull(task_ids='prepare_plan_task')['review_event_id'] }}"
        ),
        plan_id="{{ ti.xcom_pull(task_ids='prepare_plan_task')['plan']['plan_id'] }}",
        plan_fingerprint=(
            "{{ ti.xcom_pull(task_ids='prepare_plan_task')['plan']['fingerprint'] }}"
        ),
        review_fingerprint=(
            "{{ ti.xcom_pull(task_ids='prepare_plan_task')['approval_review']['fingerprint'] }}"
        ),
        evaluation_id=(
            "{{ ti.xcom_pull(task_ids='prepare_plan_task')['evaluation']['evaluation_id'] }}"
        ),
        evaluation_fingerprint=(
            "{{ ti.xcom_pull(task_ids='prepare_plan_task')['evaluation']['fingerprint'] }}"
        ),
        approver_ids=settings.hitl_approver_id_set,
        audit_dsn=settings.audit_dsn,
        defaults="Reject",
        fail_on_reject=False,
        assigned_users=settings.hitl_assigned_users,
        params={
            "approval_note": {
                "type": "string",
                "title": "Approval note",
                "minLength": 1,
            }
        },
        response_timeout=timedelta(hours=24),
    )
    approval_gate >> approval
    admission = admit_apply_task(report, evaluated, approval.output)
    apply_after_admission_task(report, evaluated, admission)


dq_daily()
