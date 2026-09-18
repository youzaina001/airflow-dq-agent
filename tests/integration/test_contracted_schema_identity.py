"""PostgreSQL acceptance: contracted schema wins over a same-name decoy."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.tables import (
    TABLE_CONTRACTS,
    ColumnContract,
    TableContract,
    register_contract,
)
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import CHECK_SPECS, CheckPolicy, CheckSpec, register_check
from airflow_dq_agent.quality.suite import load_frames
from airflow_dq_agent.warehouse.db import make_engine


@pytest.fixture(autouse=True)
def restore_registries() -> None:
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    yield
    TABLE_CONTRACTS.clear()
    TABLE_CONTRACTS.update(contracts)
    CHECK_SPECS.clear()
    CHECK_SPECS.update(checks)


@pytest.mark.integration
def test_suite_sampling_targets_and_rendering_agree_on_contracted_schema(
    warehouse_dsn: str,
) -> None:
    engine = make_engine(warehouse_dsn)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS warehouse"))
        connection.execute(text("DROP TABLE IF EXISTS analytics.ext_invoice"))
        connection.execute(text("DROP TABLE IF EXISTS public.ext_invoice"))
        connection.execute(text("DROP TABLE IF EXISTS warehouse.ext_invoice"))
        connection.execute(
            text(
                "CREATE TABLE analytics.ext_invoice ("
                "invoice_id BIGINT PRIMARY KEY, amount DOUBLE PRECISION)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE public.ext_invoice ("
                "invoice_id BIGINT PRIMARY KEY, amount DOUBLE PRECISION)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE warehouse.ext_invoice ("
                "invoice_id BIGINT PRIMARY KEY, amount DOUBLE PRECISION)"
            )
        )
        connection.execute(
            text("INSERT INTO analytics.ext_invoice VALUES (1, NULL), (2, NULL), (3, 10.0)")
        )
        connection.execute(
            text(
                "INSERT INTO public.ext_invoice VALUES "
                "(1, NULL), (2, 1.0), (3, 1.0), (4, NULL), (5, NULL)"
            )
        )
        connection.execute(
            text("INSERT INTO warehouse.ext_invoice VALUES (1, NULL), (2, NULL), (3, NULL)")
        )

    TABLE_CONTRACTS.clear()
    CHECK_SPECS.clear()
    register_contract(
        TableContract(
            table="ext_invoice",
            schema_name="analytics",
            grain="one row per invoice_id",
            description="Intended adopter table.",
            primary_key=["invoice_id"],
            columns=[
                ColumnContract(name="invoice_id", dtype="int64", unique=True),
                ColumnContract(name="amount", dtype="float64", nullable=True),
            ],
        )
    )
    register_check(
        CheckSpec(
            check_id="ext_invoice.amount.completeness",
            table="ext_invoice",
            column="amount",
            dimension=Dimension.COMPLETENESS,
            description="amount must be present",
            policies=[CheckPolicy(action_id="quarantine_nulls")],
        )
    )

    spec = CHECK_SPECS["ext_invoice.amount.completeness"]
    assert "FROM analytics.ext_invoice " in spec.sample_sql
    assert "warehouse.ext_invoice" not in spec.sample_sql

    rendered = get_governed_action("quarantine_nulls").render(
        table="ext_invoice",
        params={"column": "amount", "pk_column": "invoice_id"},
        run_id="schema-identity",
    )
    assert 'FROM "analytics"."ext_invoice" t' in rendered.sql
    assert rendered.target_sql is not None
    assert 'FROM "analytics"."ext_invoice" t' in rendered.target_sql

    frames = load_frames(engine)
    report = run_suite_on_frames(frames)
    check = report.get("ext_invoice.amount.completeness")
    assert check is not None
    assert check.n_failed == 2
    assert {row["invoice_id"] for row in check.sample_failures} == {1, 2}

    targets = PostgresTargetSetResolver(engine=engine).resolve(
        report_run_id=report.run_id,
        check_id=spec.check_id,
        action_id="quarantine_nulls",
        table="ext_invoice",
        params={"column": "amount", "pk_column": "invoice_id"},
    )
    assert targets.count == 2
