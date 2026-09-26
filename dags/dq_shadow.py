"""Configured Shadow Review. Idle unless REGISTRY_PATH selects one adopter registry.

This DAG does not load the demo catalog and does not register a Human Decision
or Apply Admission. Quarantine copies rows and does not repair source data;
this DAG does not perform that copy.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from airflow_dq_agent.agent import run_proposal_agent, safe_proposal_for_xcom
from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts import EvalReport, Proposal, QualitySuiteReport
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.load import use_registry
from airflow_dq_agent.planning.integrity import verify_report_integrity
from airflow_dq_agent.planning.preparation import CandidateEvaluationFailed, prepare_plan_review
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.quality import run_quality_suite, sample_free_report
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.shadow import EMPTY_SUITE, configuration_error, render_shadow_review
from airflow_dq_agent.traces import append_event, candidate_proposal_event

settings = get_settings()
dq_shadow = None

if settings.registry_path is not None:
    try:
        use_registry(settings.registry_path)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"registry: {exc}\nnext: correct the registry; do not review a remediation plan"
        ) from None
    problem = configuration_error(settings)
    if problem:
        raise RuntimeError(
            f"{problem}\nnext: correct read and audit configuration; "
            "do not review a remediation plan"
        ) from None
    if not CHECK_SPECS:
        raise RuntimeError(EMPTY_SUITE)
    read_dsn = settings.read_dsn
    assert read_dsn is not None

    @dag(
        dag_id="dq_shadow",
        schedule=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        is_paused_upon_creation=True,
        tags=["data-quality", "shadow-review"],
    )
    def dq_shadow() -> None:  # type: ignore[no-redef]
        @task
        def run_suite_task() -> dict[str, Any]:
            return sample_free_report(run_quality_suite(read_dsn))

        @task
        def propose_task(report_data: dict[str, Any]) -> dict[str, Any]:
            report = QualitySuiteReport.model_validate(report_data)
            verify_report_integrity(report, refusing="proposal")
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
                prepared = prepare_plan_review(
                    report,
                    proposal,
                    candidate_evaluation=candidate_evaluation,
                    candidate_event_id=str(candidate_data["candidate_event_id"]),
                    target_sets=PostgresTargetSetResolver(dsn=read_dsn),
                    persist=append_event,
                    ttl=settings.apply_admission_ttl,
                )
            except CandidateEvaluationFailed as exc:
                raise AirflowSkipException(str(exc)) from exc
            return {**prepared, "shadow_review": render_shadow_review(prepared)}

        report = run_suite_task()
        proposal = propose_task(report)
        candidate = audit_candidate_task(report, proposal)
        prepare_plan_task(report, candidate)

    dq_shadow()
