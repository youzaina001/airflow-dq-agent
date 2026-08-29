"""Real Postgres coverage; skips cleanly when no Docker engine is available."""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.apply import apply_proposal
from airflow_dq_agent.contracts.models import HumanDecision
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.quality import run_quality_suite
from airflow_dq_agent.warehouse.db import make_engine
from airflow_dq_agent.warehouse.defects import EXPECTED_DEFECTS
from airflow_dq_agent.warehouse.seed import seed_warehouse


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
    evaluation = evaluate_proposal(report, proposal)
    assert evaluation.passed
    engine = make_engine(warehouse_dsn)
    dry_run = apply_proposal(
        proposal, evaluation, dry_run=True, engine=engine, run_id="integration-dry"
    )
    assert any(step.estimated_rows == 5 for step in dry_run.steps)

    applied = apply_proposal(
        proposal,
        evaluation,
        approval=HumanDecision(decision="Approve", actor="integration-test"),
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
