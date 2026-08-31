"""Real Postgres coverage; skips cleanly when no Docker engine is available."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import text

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.apply import apply_plan
from airflow_dq_agent.contracts.models import ExecutablePlanItem, HumanDecision
from airflow_dq_agent.evals import evaluate_plan, evaluate_proposal
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.planning.admission import create_apply_admission
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.quality import run_quality_suite
from airflow_dq_agent.traces import append_event, append_human_decision, candidate_proposal_event
from airflow_dq_agent.traces.lineage import evaluation_event, plan_event
from airflow_dq_agent.warehouse.db import make_engine
from airflow_dq_agent.warehouse.defects import EXPECTED_DEFECTS
from airflow_dq_agent.warehouse.seed import seed_warehouse


@pytest.fixture(autouse=True)
def isolated_trace_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep host integration runs independent of Compose-mounted trace ownership."""
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))


@pytest.fixture(scope="module")
def warehouse_dsn() -> str:
    configured = os.getenv("TEST_WAREHOUSE_DSN")
    if configured:
        return configured
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:16")
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker/Postgres unavailable: {exc}")
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql+psycopg")
    finally:
        container.stop()


@pytest.mark.integration
def test_seed_suite_dry_run_and_copy_quarantine(warehouse_dsn: str) -> None:
    seed_warehouse(warehouse_dsn)
    report = run_quality_suite(warehouse_dsn)
    failures = {check.check_id: check.n_failed for check in report.failed_checks}
    for check_id, defect in EXPECTED_DEFECTS.items():
        assert failures[check_id] == defect.n_rows

    proposal = run_proposal_agent(report).proposal
    assert evaluate_proposal(report, proposal).passed
    assert report.audit_event_id is not None
    candidate_audit_event = candidate_proposal_event(report, proposal, report.audit_event_id)
    append_event(candidate_audit_event)
    engine = make_engine(warehouse_dsn)
    plan = compile_remediation_plan(
        report, proposal, target_sets=PostgresTargetSetResolver(engine=engine)
    )
    plan_audit_event = plan_event(plan, candidate_audit_event)
    append_event(plan_audit_event)
    evaluation = evaluate_plan(plan)
    assert evaluation.passed
    evaluation_audit_event = evaluation_event(plan, evaluation, plan_audit_event)
    append_event(evaluation_audit_event)
    decision = HumanDecision(
        decision="Approve", actor="integration-test", note="Reviewed deterministic target sets."
    )
    with pytest.raises(PermissionError, match="human decision has no durable audit event"):
        create_apply_admission(plan, evaluation, decision)

    decision_audit_event = append_human_decision(report.run_id, evaluation_audit_event, decision)
    audited_decision = decision.model_copy(update={"audit_event_id": decision_audit_event.event_id})
    admission = create_apply_admission(
        plan,
        evaluation,
        audited_decision,
    )
    assert decision_audit_event.predecessor_ids == [evaluation_audit_event.event_id]
    assert admission.decision_event_id == decision_audit_event.event_id
    dry_run = apply_plan(
        plan, evaluation, admission, dry_run=True, engine=engine, run_id="integration-dry"
    )
    assert sorted(step.estimated_rows for step in dry_run.steps) == sorted(
        item.target_set.count for item in plan.items if isinstance(item, ExecutablePlanItem)
    )

    applied = apply_plan(
        plan,
        evaluation,
        admission,
        dry_run=False,
        engine=engine,
        run_id="integration-apply",
    )
    assert applied.steps
    with engine.connect() as connection:
        copied = connection.execute(
            text("SELECT count(*) FROM dq.quarantine_rows WHERE run_id = 'integration-apply'")
        ).scalar_one()
        source_nulls = connection.execute(
            text("SELECT count(*) FROM warehouse.fact_orders WHERE total_amount IS NULL")
        ).scalar_one()
    assert copied >= 5
    assert source_nulls == 5
