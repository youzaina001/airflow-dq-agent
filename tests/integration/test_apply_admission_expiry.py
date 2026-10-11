"""Issue #31 proof: a real PostgreSQL wait cannot resume into an unauthorized write."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterator
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text
from tests.integration.test_apply_recovery import _admitted_plan

from airflow_dq_agent.adoption import APPLY_LOGIN
from airflow_dq_agent.apply import apply_plan
from airflow_dq_agent.contracts.models import ExecutablePlanItem
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.warehouse.db import make_engine

_WAITING_LOCK_SQL = """
SELECT count(*)
FROM pg_locks AS held
JOIN pg_class AS relation ON relation.oid = held.relation
JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'warehouse'
  AND relation.relname = 'fact_orders'
  AND NOT held.granted
"""


@pytest.fixture(autouse=True)
def completeness_check_only(monkeypatch: pytest.MonkeyPatch) -> None:
    for check_id in list(CHECK_SPECS):
        if check_id != "fact_orders.total_amount.completeness":
            monkeypatch.delitem(CHECK_SPECS, check_id)


@pytest.fixture(scope="module")
def required_warehouse_dsn() -> Iterator[str]:
    """PostgreSQL for #31. Fail, do not skip, when the proof cannot run."""
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
            "PostgreSQL is required for apply admission-expiry acceptance (issue #31); "
            f"a skip is incomplete: {exc}"
        )
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql+psycopg")
    finally:
        container.stop()


def _rows(connection: object, sql: str) -> list[tuple[object, ...]]:
    return list(connection.execute(text(sql)))  # type: ignore[attr-defined]


def _fact_order_rows(connection: object) -> list[tuple[object, ...]]:
    return _rows(
        connection,
        "SELECT order_id, customer_sk, order_ts, status, total_amount, currency "
        "FROM warehouse.fact_orders ORDER BY order_id",
    )


def _quarantine_rows(connection: object) -> list[tuple[object, ...]]:
    return _rows(
        connection,
        "SELECT run_id, table_name, pk_json::text, reason, payload::text "
        "FROM dq.quarantine_rows ORDER BY quarantine_id",
    )


def _is_bounded_wait_timeout(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        origin = getattr(current, "orig", None)
        sqlstate = getattr(origin, "sqlstate", None) or getattr(current, "sqlstate", None)
        if sqlstate in {"57014", "55P03"}:
            return True
        message = str(current).lower()
        if "statement timeout" in message or "lock timeout" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_expiry_refusal(exc: BaseException) -> bool:
    return isinstance(exc, PermissionError) and "apply admission has expired" in str(exc)


@pytest.mark.integration
def test_source_lock_held_past_admission_lifetime_does_not_mutate(
    required_warehouse_dsn: str,
) -> None:
    plan, evaluation, admission, report, credentials = _admitted_plan(required_warehouse_dsn)
    tables = [
        item.table
        for item in plan.items  # type: ignore[union-attr]
        if isinstance(item, ExecutablePlanItem)
    ]
    assert tables == ["fact_orders"]
    admin = make_engine(required_warehouse_dsn)
    run_id = "expiry-lock-hold"
    with admin.connect() as connection:
        orders_before = _fact_order_rows(connection)
        quarantine_before = _quarantine_rows(connection)
        for role in ("dq_apply", APPLY_LOGIN):
            allowed = connection.execute(
                text("SELECT has_table_privilege(:role, 'warehouse.fact_orders', 'UPDATE')"),
                {"role": role},
            ).scalar_one()
            assert allowed is False
        select_allowed = connection.execute(
            text("SELECT has_table_privilege('dq_apply', 'warehouse.fact_orders', 'SELECT')")
        ).scalar_one()
        assert select_allowed is True

    # Frozen clock: the armed bound is 2s, while `now` is still an hour out.
    # A pre-transaction sample must not stretch the PostgreSQL wait to that hour.
    def clock() -> datetime:
        return admission.expires_at - timedelta(milliseconds=2000)  # type: ignore[union-attr]

    release = threading.Event()
    saw_wait = threading.Event()
    thread_errors: list[BaseException] = []

    def _hold_source_lock() -> None:
        connection = admin.connect()
        transaction = connection.begin()
        try:
            connection.execute(text("LOCK TABLE warehouse.fact_orders IN ACCESS EXCLUSIVE MODE"))
            release.wait(timeout=30)
        except Exception as exc:
            thread_errors.append(exc)
        finally:
            transaction.rollback()
            connection.close()

    def _release_after_blocked_read() -> None:
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and not release.is_set():
                with admin.connect() as connection:
                    waiting = connection.execute(text(_WAITING_LOCK_SQL)).scalar_one()
                if waiting:
                    saw_wait.set()
                    # Stay held until the 2000ms admission bound has elapsed, then release.
                    time.sleep(3)
                    return
                time.sleep(0.05)
        except Exception as exc:
            thread_errors.append(exc)
        finally:
            release.set()

    holder = threading.Thread(target=_hold_source_lock)
    watcher = threading.Thread(target=_release_after_blocked_read)
    holder.start()
    # The lock must be held before apply opens its read.
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and holder.is_alive() and not thread_errors:
        with admin.connect() as connection:
            granted = connection.execute(
                text(
                    """
                    SELECT count(*)
                    FROM pg_locks AS held
                    JOIN pg_class AS relation ON relation.oid = held.relation
                    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname = 'warehouse'
                      AND relation.relname = 'fact_orders'
                      AND held.granted
                      AND held.mode = 'AccessExclusiveLock'
                    """
                )
            ).scalar_one()
        if granted:
            break
        time.sleep(0.05)
    else:
        release.set()
        holder.join(timeout=10)
        raise AssertionError(thread_errors or "source lock was not acquired")
    watcher.start()
    result: object | None = None
    error: Exception | None = None
    try:
        result = apply_plan(
            plan,  # type: ignore[arg-type]
            evaluation,  # type: ignore[arg-type]
            admission,  # type: ignore[arg-type]
            report=report,  # type: ignore[arg-type]
            dry_run=False,
            dsn=credentials.apply_dsn,
            run_id=run_id,
            clock=clock,
            now=admission.expires_at - timedelta(hours=1),  # type: ignore[union-attr]
        )
    except Exception as exc:
        error = exc
    finally:
        release.set()
        watcher.join(timeout=25)
        holder.join(timeout=10)

    assert not thread_errors, thread_errors
    assert saw_wait.is_set(), error
    assert result is None and error is not None, result
    assert _is_bounded_wait_timeout(error) or _is_expiry_refusal(error), error
    with admin.connect() as connection:
        assert _fact_order_rows(connection) == orders_before
        assert _quarantine_rows(connection) == quarantine_before
        assert (
            connection.execute(
                text("SELECT count(*) FROM dq.quarantine_rows WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT count(*) FROM dq.apply_log WHERE admission_id = :admission_id"),
                {"admission_id": admission.admission_id},  # type: ignore[union-attr]
            ).scalar_one()
            == 0
        )
        assert (
            connection.execute(
                text("SELECT dq.admission_consumed(:admission_id)"),
                {"admission_id": admission.admission_id},  # type: ignore[union-attr]
            ).scalar_one()
            is False
        )
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM dq.traces WHERE kind = 'apply_succeeded' "
                    "AND body ->> 'plan_id' = :plan_id"
                ),
                {"plan_id": plan.plan_id},  # type: ignore[union-attr]
            ).scalar_one()
            == 0
        )


@pytest.mark.integration
def test_expired_retry_of_committed_admission_returns_original_result(
    required_warehouse_dsn: str,
) -> None:
    plan, evaluation, admission, report, credentials = _admitted_plan(required_warehouse_dsn)
    admin = make_engine(required_warehouse_dsn)
    with admin.connect() as connection:
        orders_before = _fact_order_rows(connection)

    first = apply_plan(
        plan,  # type: ignore[arg-type]
        evaluation,  # type: ignore[arg-type]
        admission,  # type: ignore[arg-type]
        report=report,  # type: ignore[arg-type]
        dry_run=False,
        dsn=credentials.apply_dsn,
        run_id="expiry-committed-first",
    )
    with admin.connect() as connection:
        quarantine_after_first = _quarantine_rows(connection)
        apply_rows = list(
            connection.execute(
                text(
                    "SELECT run_id, event_id, rowcount FROM dq.apply_log "
                    "WHERE admission_id = :admission_id"
                ),
                {"admission_id": admission.admission_id},  # type: ignore[union-attr]
            )
        )

    second = apply_plan(
        plan,  # type: ignore[arg-type]
        evaluation,  # type: ignore[arg-type]
        admission,  # type: ignore[arg-type]
        report=report,  # type: ignore[arg-type]
        dry_run=False,
        dsn=credentials.apply_dsn,
        run_id="expiry-committed-second",
        now=admission.expires_at + timedelta(seconds=1),  # type: ignore[union-attr]
    )

    assert second.apply_result_id == first.apply_result_id
    assert second.audit_event_id == first.audit_event_id
    assert second.run_id == first.run_id
    assert [
        (step.rendered.action_id, step.estimated_rows, step.rowcount) for step in second.steps
    ] == [(step.rendered.action_id, step.estimated_rows, step.rowcount) for step in first.steps]
    with admin.connect() as connection:
        assert _fact_order_rows(connection) == orders_before
        assert _quarantine_rows(connection) == quarantine_after_first
        assert (
            list(
                connection.execute(
                    text(
                        "SELECT run_id, event_id, rowcount FROM dq.apply_log "
                        "WHERE admission_id = :admission_id"
                    ),
                    {"admission_id": admission.admission_id},  # type: ignore[union-attr]
                )
            )
            == apply_rows
        )
        stray = connection.execute(
            text("SELECT count(*) FROM dq.quarantine_rows WHERE run_id = :run_id"),
            {"run_id": "expiry-committed-second"},
        ).scalar_one()
    assert len(apply_rows) == 1
    assert apply_rows[0][0] == first.run_id
    assert stray == 0
