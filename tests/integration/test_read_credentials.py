"""Restricted PostgreSQL reader: selected credentials can read and cannot write."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError

from airflow_dq_agent.agent.runner import (
    _get_observed_schema_tool,
    _sample_failing_rows_tool,
    get_observed_schema,
    sample_failing_rows,
)
from airflow_dq_agent.demo import seed_warehouse
from airflow_dq_agent.quality import run_quality_suite
from airflow_dq_agent.warehouse.db import make_engine

READER_ROLE = "dq_reader_issue67"
READER_PASSWORD = "reader-pass"
SAMPLE_CHECK_ID = "fact_orders.total_amount.completeness"


@pytest.mark.integration
def test_restricted_reader_can_perform_supported_reads_and_cannot_write(
    warehouse_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seed_warehouse(warehouse_dsn)
    admin = make_engine(warehouse_dsn)
    with admin.begin() as connection:
        connection.execute(text(f"DROP ROLE IF EXISTS {READER_ROLE}"))
        connection.execute(
            text(f"CREATE ROLE {READER_ROLE} LOGIN PASSWORD '{READER_PASSWORD}' IN ROLE dq_read")
        )
    read_dsn = (
        make_url(warehouse_dsn)
        .set(username=READER_ROLE, password=READER_PASSWORD)
        .render_as_string(hide_password=False)
    )
    monkeypatch.setenv("READ_DSN", read_dsn)
    monkeypatch.setenv("WAREHOUSE_DSN", warehouse_dsn)

    report = run_quality_suite()
    assert report.checks
    assert report.get("fact_orders.total_amount.completeness") is not None

    samples = sample_failing_rows(SAMPLE_CHECK_ID)
    assert samples
    assert _sample_failing_rows_tool(SAMPLE_CHECK_ID)

    schema = get_observed_schema("dim_customer")
    assert "customer_sk" in schema
    assert "customer_sk" in _get_observed_schema_tool("dim_customer")

    reader = make_engine(read_dsn)
    with pytest.raises(SQLAlchemyError), reader.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO warehouse.dim_site (site_sk, site_id, country, region) "
                "VALUES (99999, 'NOPE', 'US', 'x')"
            )
        )
    reader.dispose()
    admin.dispose()
