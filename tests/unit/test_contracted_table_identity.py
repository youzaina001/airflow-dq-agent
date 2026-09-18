"""Contracted table identity is used for checks, samples, targets, and rendering."""

from __future__ import annotations

import polars as pl
import pytest
from pydantic import ValidationError

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.tables import (
    TABLE_CONTRACTS,
    ColumnContract,
    TableContract,
    get_table_contract,
    register_contract,
)
from airflow_dq_agent.quality.registry import CHECK_SPECS, CheckPolicy, CheckSpec, register_check
from airflow_dq_agent.quality.suite import load_frames


@pytest.fixture(autouse=True)
def restore_registries() -> None:
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    yield
    TABLE_CONTRACTS.clear()
    TABLE_CONTRACTS.update(contracts)
    CHECK_SPECS.clear()
    CHECK_SPECS.update(checks)


def _invoice_contract(*, schema_name: str = "analytics") -> TableContract:
    return TableContract(
        table="ext_invoice",
        schema_name=schema_name,
        grain="one row per invoice_id",
        description="Adopter-owned invoice header.",
        primary_key=["invoice_id"],
        columns=[
            ColumnContract(name="invoice_id", dtype="int64", unique=True),
            ColumnContract(name="customer_id", dtype="int64"),
            ColumnContract(name="amount", dtype="float64", nullable=True),
        ],
        foreign_keys=[("customer_id", "ext_customer", "customer_id")],
    )


def _customer_contract(*, schema_name: str = "crm") -> TableContract:
    return TableContract(
        table="ext_customer",
        schema_name=schema_name,
        grain="one row per customer_id",
        description="Adopter-owned customer.",
        primary_key=["customer_id"],
        columns=[
            ColumnContract(name="customer_id", dtype="int64", unique=True),
            ColumnContract(name="email", dtype="utf8", nullable=True),
        ],
    )


def test_sample_sql_uses_registered_contract_schema_including_foreign_keys() -> None:
    register_contract(_customer_contract())
    register_contract(_invoice_contract())

    completeness = CheckSpec(
        check_id="ext_invoice.amount.completeness",
        table="ext_invoice",
        column="amount",
        dimension=Dimension.COMPLETENESS,
        description="amount must be present",
        policies=[CheckPolicy(action_id="quarantine_nulls")],
    )
    assert "FROM analytics.ext_invoice " in completeness.sample_sql
    assert "warehouse.ext_invoice" not in completeness.sample_sql

    drift = CheckSpec(
        check_id="ext_invoice.schema_drift",
        table="ext_invoice",
        dimension=Dimension.SCHEMA_DRIFT,
        description="invoice matches contract",
        policies=[CheckPolicy(action_id="schema_drift_ticket")],
    )
    assert "table_schema = 'analytics'" in drift.sample_sql
    assert "table_name = 'ext_invoice'" in drift.sample_sql
    assert "table_schema = 'warehouse'" not in drift.sample_sql

    referential = CheckSpec(
        check_id="ext_invoice.customer_id.referential_integrity",
        table="ext_invoice",
        column="customer_id",
        dimension=Dimension.REFERENTIAL_INTEGRITY,
        description="invoice customer must exist",
        policies=[CheckPolicy(action_id="quarantine_orphans")],
    )
    assert "FROM analytics.ext_invoice t " in referential.sample_sql
    assert "LEFT JOIN crm.ext_customer r " in referential.sample_sql
    assert "warehouse.ext_invoice" not in referential.sample_sql
    assert "warehouse.ext_customer" not in referential.sample_sql


class _StubConnection:
    def __enter__(self) -> _StubConnection:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _StubEngine:
    def connect(self) -> _StubConnection:
        return _StubConnection()


def test_load_frames_reads_the_registered_contract_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_contract(_invoice_contract())
    queries: list[str] = []

    def fake_read(query: str, connection: object) -> pl.DataFrame:
        queries.append(query)
        return pl.DataFrame({"invoice_id": [1], "amount": [1.0]})

    monkeypatch.setattr(pl, "read_database", fake_read)

    frames = load_frames(_StubEngine())  # type: ignore[arg-type]

    assert "ext_invoice" in frames
    assert any("FROM analytics.ext_invoice" in query for query in queries)
    assert all("FROM warehouse.ext_invoice" not in query for query in queries)


def test_controlled_renderer_targets_registered_contract_schema() -> None:
    register_contract(_invoice_contract())

    rendered_nulls = get_governed_action("quarantine_nulls").render(
        table="ext_invoice",
        params={"column": "amount", "pk_column": "invoice_id"},
        run_id="identity-run",
    )
    assert 'FROM "analytics"."ext_invoice" t' in rendered_nulls.sql
    assert rendered_nulls.target_sql is not None
    assert 'FROM "analytics"."ext_invoice" t' in rendered_nulls.target_sql
    assert '"warehouse"."ext_invoice"' not in rendered_nulls.sql
    assert rendered_nulls.params["table_name"] == "analytics.ext_invoice"

    customer = TABLE_CONTRACTS["dim_customer"].model_copy(update={"schema_name": "crm"})
    orders = TABLE_CONTRACTS["fact_orders"].model_copy(update={"schema_name": "analytics"})
    del TABLE_CONTRACTS["dim_customer"]
    del TABLE_CONTRACTS["fact_orders"]
    register_contract(customer)
    register_contract(orders)

    rendered_orphans = get_governed_action("quarantine_orphans").render(
        table="fact_orders",
        params={
            "fk_column": "customer_sk",
            "ref_table": "dim_customer",
            "ref_column": "customer_sk",
            "pk_column": "order_id",
        },
        run_id="identity-run",
    )
    assert 'FROM "analytics"."fact_orders" t' in rendered_orphans.sql
    assert 'LEFT JOIN "crm"."dim_customer" r' in rendered_orphans.sql
    assert '"warehouse"."dim_customer"' not in rendered_orphans.sql


def test_invalid_identifiers_fail_before_a_warehouse_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_args: object, **_kwargs: object) -> pl.DataFrame:
        raise AssertionError("warehouse must not be read")

    monkeypatch.setattr(pl, "read_database", boom)

    with pytest.raises((ValidationError, ValueError), match="identifier"):
        register_contract(_invoice_contract(schema_name="analytics;drop"))
    with pytest.raises((ValidationError, ValueError), match="identifier"):
        register_contract(
            TableContract(
                table="ext-invoice",
                schema_name="analytics",
                grain="one row per invoice_id",
                description="Invalid table name.",
                primary_key=["invoice_id"],
                columns=[ColumnContract(name="invoice_id", dtype="int64", unique=True)],
            )
        )
    with pytest.raises((ValidationError, ValueError), match="identifier"):
        register_contract(
            TableContract(
                table="ext_invoice",
                schema_name="analytics",
                grain="one row per invoice_id",
                description="Invalid column name.",
                primary_key=["invoice_id"],
                columns=[
                    ColumnContract(name="invoice_id", dtype="int64", unique=True),
                    ColumnContract(name="amount;drop", dtype="float64", nullable=True),
                ],
            )
        )


def _boom_warehouse(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_args: object, **_kwargs: object) -> pl.DataFrame:
        raise AssertionError("warehouse must not be read")

    monkeypatch.setattr(pl, "read_database", boom)


def test_missing_contracted_column_fails_before_a_warehouse_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _boom_warehouse(monkeypatch)
    register_contract(_invoice_contract())
    with pytest.raises(ValueError, match="not a column"):
        register_check(
            CheckSpec(
                check_id="ext_invoice.missing.completeness",
                table="ext_invoice",
                column="missing_amount",
                dimension=Dimension.COMPLETENESS,
                description="missing column must not reach the warehouse",
                policies=[CheckPolicy(action_id="quarantine_nulls")],
            )
        )
    with pytest.raises(ValueError, match="pk_column"):
        get_governed_action("quarantine_nulls").render(
            table="ext_invoice",
            params={"column": "amount", "pk_column": "customer_id"},
            run_id="identity-run",
        )


def test_missing_referenced_contract_fails_before_a_warehouse_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _boom_warehouse(monkeypatch)
    register_contract(_invoice_contract())
    with pytest.raises(KeyError, match="ext_customer"):
        CheckSpec(
            check_id="ext_invoice.customer_id.referential_integrity",
            table="ext_invoice",
            column="customer_id",
            dimension=Dimension.REFERENTIAL_INTEGRITY,
            description="invoice customer must exist",
            policies=[CheckPolicy(action_id="quarantine_orphans")],
        )


def test_composite_primary_key_remediation_fails_before_a_warehouse_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _boom_warehouse(monkeypatch)
    register_contract(
        TableContract(
            table="ext_invoice",
            schema_name="analytics",
            grain="one row per invoice line",
            description="Composite identity is not remediable yet.",
            primary_key=["invoice_id", "customer_id"],
            columns=[
                ColumnContract(name="invoice_id", dtype="int64"),
                ColumnContract(name="customer_id", dtype="int64"),
                ColumnContract(name="amount", dtype="float64", nullable=True),
            ],
        )
    )
    with pytest.raises((ValidationError, ValueError), match="primary key"):
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


def test_same_name_multi_schema_registration_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _boom_warehouse(monkeypatch)
    register_contract(_invoice_contract(schema_name="analytics"))
    with pytest.raises(ValueError, match="already registered"):
        register_contract(_invoice_contract(schema_name="ops"))


def test_unambiguous_bare_name_lookup_supports_non_default_schema() -> None:
    register_contract(_invoice_contract())
    loaded = get_table_contract("ext_invoice")
    assert loaded.schema_name == "analytics"
    assert loaded.table == "ext_invoice"
    assert get_table_contract("analytics.ext_invoice").qualified == "analytics.ext_invoice"
