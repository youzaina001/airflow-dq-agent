"""Guided setup command: one owner connection string prepares a local warehouse."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from airflow_dq_agent.cli import main
from airflow_dq_agent.traces import PostgresAuditRepository
from airflow_dq_agent.warehouse.db import make_engine

_INCOMPLETE = "command: incomplete: setup or execution error"


def _login_dsns(output: str) -> tuple[str, str, str]:
    lines = output.splitlines()
    assert lines[0] == "setup: ready"
    assert len(lines) == 4
    read_dsn = lines[1].removeprefix("read: ")
    audit_dsn = lines[2].removeprefix("audit: ")
    apply_dsn = lines[3].removeprefix("apply: ")
    assert lines[1] == f"read: {read_dsn}"
    assert lines[2] == f"audit: {audit_dsn}"
    assert lines[3] == f"apply: {apply_dsn}"
    assert len({read_dsn, audit_dsn, apply_dsn}) == 3
    return read_dsn, audit_dsn, apply_dsn


@pytest.mark.integration
def test_setup_without_owner_dsn_exits_2_without_ready_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["setup"]) == 2

    output = capsys.readouterr().out
    assert output == f"{_INCOMPLETE}\n"
    assert "setup: ready" not in output


@pytest.mark.integration
def test_setup_refused_dsn_exits_2_without_ready_summary(
    capsys: pytest.CaptureFixture[str],
) -> None:
    refused = "postgresql+psycopg://owner:secret@127.0.0.1:1/warehouse"

    assert main(["setup", "--dsn", refused]) == 2

    output = capsys.readouterr().out
    assert output == f"{_INCOMPLETE}\n"
    assert "setup: ready" not in output
    assert "read:" not in output
    assert "audit:" not in output
    assert "apply:" not in output
    assert refused not in output


@pytest.mark.integration
def test_setup_prints_distinct_logins_and_read_login_sees_known_defects(
    warehouse_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = warehouse_dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    assert main(["setup", "--dsn", owner]) == 0
    read_dsn, audit_dsn, apply_dsn = _login_dsns(capsys.readouterr().out)
    assert read_dsn.startswith("postgresql+psycopg://")

    read_user = make_url(read_dsn).username
    audit_user = make_url(audit_dsn).username
    apply_user = make_url(apply_dsn).username
    assert audit_user != read_user
    assert apply_user != read_user
    assert apply_user != audit_user

    reader = make_engine(read_dsn)
    owner = make_engine(warehouse_dsn)
    try:
        with reader.connect() as connection:
            defects = connection.execute(
                text(
                    "SELECT customer_sk, email FROM warehouse.dim_customer "
                    "WHERE customer_sk IN (101, 102) ORDER BY customer_sk"
                )
            ).all()
        assert defects == [(101, "c101.invalid"), (102, "c102.invalid")]
        with pytest.raises(SQLAlchemyError), reader.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO warehouse.dim_site (site_sk, site_id, country, region) "
                    "VALUES (99999, 'NOPE', 'US', 'x')"
                )
            )
        with owner.connect() as connection:
            assert (
                connection.execute(text("SELECT to_regclass('warehouse.ext_invoice')")).scalar_one()
                is None
            )
    finally:
        reader.dispose()
        owner.dispose()


@pytest.mark.integration
def test_printed_audit_login_records_shadow_lineage_without_model_credentials(
    warehouse_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["setup", "--dsn", warehouse_dsn]) == 0
    read_dsn, audit_dsn, _apply_dsn = _login_dsns(capsys.readouterr().out)
    monkeypatch.setenv("READ_DSN", read_dsn)
    monkeypatch.setenv("AUDIT_DSN", audit_dsn)
    # Neither owner fallback nor apply credentials are available to this review.
    for name in ("WAREHOUSE_DSN", "APPLY_DSN"):
        monkeypatch.setenv(name, "postgresql+psycopg://unused:unused@127.0.0.1:1/unused")
    monkeypatch.setenv("TRACE_POSTGRES", "true")
    monkeypatch.setenv("APPLY_MODE", "off")
    monkeypatch.setenv("LLM_MODE", "stub")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)

    # Known seeded defects return quality-failure status, not an execution error.
    assert main(["shadow"]) == 1
    output = capsys.readouterr().out
    prepared = json.loads(output[output.index("{") :])
    repository = PostgresAuditRepository(audit_dsn)
    event_id = prepared["review_event_id"]
    kinds = []
    while event_id:
        event = repository.get(event_id)
        assert event is not None
        assert event.quality_run_id == prepared["plan"]["quality_run_id"]
        kinds.append(event.kind)
        assert len(event.predecessor_ids) <= 1
        event_id = event.predecessor_ids[0] if event.predecessor_ids else ""
    assert kinds == [
        "approval_review",
        "evaluation",
        "plan_compiled",
        "candidate_proposal",
        "quality_report",
    ]


@pytest.mark.integration
def test_second_setup_replaces_passwords_and_reseeds_known_defects(
    warehouse_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["setup", "--dsn", warehouse_dsn]) == 0
    previous_read, previous_audit, previous_apply = _login_dsns(capsys.readouterr().out)

    assert main(["setup", "--dsn", warehouse_dsn]) == 0
    read_dsn, audit_dsn, apply_dsn = _login_dsns(capsys.readouterr().out)
    assert read_dsn != previous_read
    assert audit_dsn != previous_audit
    assert apply_dsn != previous_apply

    for previous_dsn in (previous_read, previous_audit, previous_apply):
        previous = make_engine(previous_dsn)
        try:
            with pytest.raises(SQLAlchemyError), previous.connect() as connection:
                connection.execute(text("SELECT 1"))
        finally:
            previous.dispose()

    for current_dsn in (read_dsn, audit_dsn, apply_dsn):
        current = make_engine(current_dsn)
        try:
            with current.connect() as connection:
                assert connection.execute(text("SELECT 1")).scalar_one() == 1
        finally:
            current.dispose()

    reader = make_engine(read_dsn)
    try:
        with reader.connect() as connection:
            defects = connection.execute(
                text(
                    "SELECT customer_sk, email FROM warehouse.dim_customer "
                    "WHERE customer_sk IN (101, 102) ORDER BY customer_sk"
                )
            ).all()
        assert defects == [(101, "c101.invalid"), (102, "c102.invalid")]
    finally:
        reader.dispose()


@pytest.mark.integration
def test_seed_takes_no_connection_string_and_prints_no_login_passwords(
    warehouse_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as caught:
        main(["seed", "--dsn", warehouse_dsn])
    assert caught.value.code == 2
    rejected = capsys.readouterr()
    assert "setup: ready" not in rejected.out
    assert "read:" not in rejected.out
    assert "audit:" not in rejected.out
    assert "apply:" not in rejected.out

    monkeypatch.setenv("WAREHOUSE_DSN", warehouse_dsn)
    assert main(["seed"]) == 0
    assert capsys.readouterr().out == "seeded warehouse with deterministic quality defects\n"
