"""Issue #29 proof: recovery and concurrency against real PostgreSQL."""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterator
from datetime import timedelta

import pytest
from sqlalchemy import event, text

from airflow_dq_agent.adoption import (
    RestrictedCredentials,
    apply_governance_schema,
    provision_restricted_logins,
)
from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.apply import apply_plan
from airflow_dq_agent.apply.executor import ApplyResult
from airflow_dq_agent.contracts.models import (
    DecisionBinding,
    HumanDecision,
)
from airflow_dq_agent.demo import seed_warehouse
from airflow_dq_agent.evals import evaluate_plan, evaluate_proposal
from airflow_dq_agent.hitl import record_human_decision
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.planning.admission import create_apply_admission
from airflow_dq_agent.planning.review import build_approval_review
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.quality import run_quality_suite
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.traces import (
    PostgresAuditRepository,
    append_event,
    candidate_proposal_event,
)
from airflow_dq_agent.traces.lineage import evaluation_event, plan_event, review_event
from airflow_dq_agent.warehouse.db import make_engine


@pytest.fixture(autouse=True)
def completeness_check_only(monkeypatch: pytest.MonkeyPatch) -> None:
    # Recovery proves the adopted single-check quarantine journey, not conflicting actions.
    for check_id in list(CHECK_SPECS):
        if check_id != "fact_orders.total_amount.completeness":
            monkeypatch.delitem(CHECK_SPECS, check_id)


@pytest.fixture(scope="module")
def required_warehouse_dsn() -> Iterator[str]:
    """PostgreSQL for #29. Fail, do not skip, when the proof cannot run."""
    configured = os.getenv("TEST_WAREHOUSE_DSN")
    if configured:
        yield configured
        return
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:16")
        container.start()
    except Exception as exc:
        pytest.fail(
            "PostgreSQL is required for apply-recovery acceptance (issue #29); "
            f"a skip is incomplete: {exc}"
        )
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql+psycopg")
    finally:
        container.stop()


def _admitted_plan(
    warehouse_dsn: str,
    *,
    ttl: timedelta = timedelta(hours=24),
) -> tuple[object, object, object, object, RestrictedCredentials]:
    """Provision schema and logins, then drive suite → plan → audited decision → admission."""
    apply_governance_schema(warehouse_dsn)
    credentials = provision_restricted_logins(warehouse_dsn)
    seed_warehouse(warehouse_dsn)
    report = run_quality_suite(warehouse_dsn)
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
    evaluation = evaluation.model_copy(update={"audit_event_id": evaluation_audit_event.event_id})
    review = build_approval_review(plan, evaluation, ttl=ttl)
    review_audit_event = review_event(review, evaluation, evaluation_audit_event)
    append_event(review_audit_event, dsn=credentials.audit_dsn, mirror_postgres=True)
    decision = HumanDecision(
        decision="Approve",
        actor="integration-recovery",
        note="Recovery proof decision.",
        review_fingerprint=review.fingerprint,
    )

    def persist_decision(event: object) -> None:
        append_event(event, dsn=credentials.audit_dsn, mirror_postgres=True)

    audited_decision = record_human_decision(
        decision,
        approver_ids={"integration-recovery"},
        quality_run_id=report.run_id,
        predecessor=review_audit_event,
        persist=persist_decision,
        binding=DecisionBinding(
            plan_id=plan.plan_id,
            plan_fingerprint=plan.fingerprint,
            evaluation_id=evaluation.evaluation_id,
            evaluation_fingerprint=evaluation.fingerprint,
        ),
    )
    audit_repository = PostgresAuditRepository(credentials.audit_dsn)
    decision_audit_event = audit_repository.get(audited_decision.audit_event_id)
    assert decision_audit_event is not None
    admission = create_apply_admission(
        plan,
        evaluation,
        audited_decision,
        report=report,
        audit_repository=audit_repository,
        ttl=ttl,
    )
    return plan, evaluation, admission, report, credentials


def _apply_log_rows(connection: object, admission_id: str) -> list[tuple[object, ...]]:
    return list(
        connection.execute(  # type: ignore[attr-defined]
            text(
                "SELECT run_id, event_id, rowcount FROM dq.apply_log "
                "WHERE admission_id = :admission_id"
            ),
            {"admission_id": admission_id},
        )
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("waited_stage", "lock_statement"),
    [
        ("target", "LOCK TABLE warehouse.fact_orders IN ACCESS EXCLUSIVE MODE"),
        ("result", "LOCK TABLE dq.apply_log IN SHARE MODE"),
    ],
)
def test_admission_deadline_cancels_a_real_wait_without_committing(
    required_warehouse_dsn: str,
    waited_stage: str,
    lock_statement: str,
) -> None:
    plan, evaluation, admission, report, credentials = _admitted_plan(
        required_warehouse_dsn, ttl=timedelta(seconds=3)
    )
    owner = make_engine(required_warehouse_dsn)
    apply_engine = make_engine(credentials.apply_dsn)
    errors: list[BaseException] = []
    run_id = f"expiry-{waited_stage}-wait"
    checksum = text(
        "SELECT md5(string_agg(row_to_json(t)::text, ',' ORDER BY order_id)) "
        "FROM warehouse.fact_orders t"
    )

    def attempt() -> None:
        try:
            apply_plan(
                plan,
                evaluation,
                admission,
                report=report,
                dry_run=False,
                engine=apply_engine,
                run_id=run_id,
            )
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=attempt)
    with owner.connect() as blocker:
        original_source = blocker.execute(checksum).scalar_one()
        blocker_pid = blocker.execute(text("SELECT pg_backend_pid()")).scalar_one()
        blocker.execute(text(lock_statement))
        try:
            worker.start()
            observed_wait = False
            deadline = time.monotonic() + 2
            with owner.connect() as observer:
                while worker.is_alive() and time.monotonic() < deadline:
                    observed_wait = observer.execute(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity a "
                            "WHERE a.usename = 'dq_apply_login' AND a.wait_event_type = 'Lock' "
                            "AND :blocker_pid = ANY(pg_blocking_pids(a.pid)) "
                            "AND a.query LIKE :waited_query)"
                        ),
                        {
                            "blocker_pid": blocker_pid,
                            "waited_query": "%record_apply_result%"
                            if waited_stage == "result"
                            else '%FROM "warehouse"."fact_orders"%',
                        },
                    ).scalar_one()
                    observer.commit()
                    if observed_wait:
                        break
                    time.sleep(0.01)
            assert observed_wait, "the restricted apply must reach a real PostgreSQL wait"
            worker.join(timeout=4)
            assert not worker.is_alive(), "database wait exceeded the admission lifetime"
        finally:
            blocker.rollback()
            worker.join(timeout=5)
    assert not worker.is_alive()
    assert len(errors) == 1
    assert getattr(getattr(errors[0], "orig", None), "sqlstate", None) in {"57014", "55P03"}
    with owner.connect() as connection:
        assert connection.execute(checksum).scalar_one() == original_source
        assert (
            connection.execute(
                text("SELECT count(*) FROM dq.quarantine_rows WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).scalar_one()
            == 0
        )
        assert _apply_log_rows(connection, admission.admission_id) == []
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM dq.traces WHERE kind = 'apply_succeeded' "
                    "AND body ->> 'quality_run_id' = :quality_run_id"
                ),
                {"quality_run_id": report.run_id},
            ).scalar_one()
            == 0
        )
    with apply_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT dq.admission_consumed(:admission_id)"),
                {"admission_id": admission.admission_id},
            ).scalar_one()
            is False
        )
        assert (
            connection.execute(
                text("SELECT has_table_privilege(current_user, 'warehouse.fact_orders', 'UPDATE')")
            ).scalar_one()
            is False
        )


@pytest.mark.integration
def test_repeated_apply_returns_the_original_committed_result(
    required_warehouse_dsn: str,
) -> None:
    plan, evaluation, admission, report, credentials = _admitted_plan(required_warehouse_dsn)

    first = apply_plan(
        plan,
        evaluation,
        admission,
        report=report,
        dry_run=False,
        dsn=credentials.apply_dsn,
        run_id="recovery-first",
    )
    second = apply_plan(
        plan,
        evaluation,
        admission,
        report=report,
        dry_run=False,
        dsn=credentials.apply_dsn,
        run_id="recovery-second",
    )
    expired_sql: list[str] = []
    expired_engine = make_engine(credentials.apply_dsn)

    def capture_sql(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        expired_sql.append(statement)

    event.listen(expired_engine, "before_cursor_execute", capture_sql)
    expired = apply_plan(
        plan,
        evaluation,
        admission,
        report=report,
        dry_run=False,
        engine=expired_engine,
        run_id="recovery-expired",
        now=admission.expires_at + timedelta(seconds=1),
    )

    for recovered in (second, expired):
        assert recovered.apply_result_id == first.apply_result_id
        assert recovered.fingerprint == first.fingerprint
        assert recovered.audit_event_id == first.audit_event_id
        assert recovered.run_id == first.run_id
        assert [
            (step.rendered.action_id, step.estimated_rows, step.rowcount)
            for step in recovered.steps
        ] == [(step.rendered.action_id, step.estimated_rows, step.rowcount) for step in first.steps]
    assert "SET TRANSACTION READ ONLY" in expired_sql
    assert any("committed_apply_result" in sql for sql in expired_sql)
    assert not any(
        sql.lstrip().startswith(("INSERT", "UPDATE")) or "record_apply_result" in sql
        for sql in expired_sql
    )

    with make_engine(required_warehouse_dsn).connect() as connection:
        rows = _apply_log_rows(connection, admission.admission_id)
        assert len(rows) == 1
        assert rows[0][0] == first.run_id
        copied = connection.execute(
            text("SELECT count(*) FROM dq.quarantine_rows WHERE run_id = :run_id"),
            {"run_id": "recovery-first"},
        ).scalar_one()
        stray = connection.execute(
            text("SELECT count(*) FROM dq.quarantine_rows WHERE run_id = :run_id"),
            {"run_id": "recovery-second"},
        ).scalar_one()
        expired_copied = connection.execute(
            text("SELECT count(*) FROM dq.quarantine_rows WHERE run_id = :run_id"),
            {"run_id": "recovery-expired"},
        ).scalar_one()
        failures = connection.execute(
            text(
                "SELECT count(*) FROM dq.traces WHERE kind = 'apply_failed' "
                "AND body ->> 'quality_run_id' = :qrid"
            ),
            {"qrid": report.run_id},
        ).scalar_one()
    assert copied == sum(
        item.target_set.count
        for item in plan.items  # type: ignore[union-attr]
    )
    assert stray == 0
    assert expired_copied == 0
    assert failures == 0


@pytest.mark.integration
def test_concurrent_applies_commit_exactly_one_mutation(
    required_warehouse_dsn: str,
) -> None:
    plan, evaluation, admission, report, credentials = _admitted_plan(required_warehouse_dsn)
    outcomes: list[ApplyResult | None] = [None, None]
    errors: list[BaseException | None] = [None, None]

    def _attempt(index: int) -> None:
        try:
            outcomes[index] = apply_plan(
                plan,
                evaluation,
                admission,
                report=report,
                dry_run=False,
                dsn=credentials.apply_dsn,
                run_id=f"recovery-race-{index}",
            )
        except Exception as exc:  # pragma: no cover - exercised by the losing thread
            errors[index] = exc

    threads = [threading.Thread(target=_attempt, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with make_engine(required_warehouse_dsn).connect() as connection:
        rows = _apply_log_rows(connection, admission.admission_id)
        assert len(rows) == 1
        committed_run_id, committed_event_id, _rowcount = rows[0]
        trace = connection.execute(
            text("SELECT body FROM dq.traces WHERE trace_id = :trace_id"),
            {"trace_id": committed_event_id},
        ).first()
        total_copied = connection.execute(
            text(
                "SELECT count(*) FROM dq.quarantine_rows "
                "WHERE run_id IN ('recovery-race-0', 'recovery-race-1')"
            )
        ).scalar_one()
        succeeded = connection.execute(
            text(
                "SELECT count(*) FROM dq.traces WHERE kind = 'apply_succeeded' "
                "AND body ->> 'quality_run_id' = :qrid"
            ),
            {"qrid": report.run_id},
        ).scalar_one()
    assert trace is not None
    body = trace[0] if not isinstance(trace[0], (str, bytes)) else json.loads(str(trace[0]))
    expected_count = sum(
        item.target_set.count
        for item in plan.items  # type: ignore[union-attr]
    )
    assert total_copied == expected_count
    assert succeeded == 1

    returned = [outcome for outcome in outcomes if outcome is not None]
    for outcome in returned:
        assert outcome.apply_result_id == body["apply_result_id"]
        assert outcome.run_id == committed_run_id
        assert outcome.audit_event_id == committed_event_id
    refused = [error for error in errors if error is not None]
    for error in refused:
        assert isinstance(error, PermissionError)
    assert returned or refused
