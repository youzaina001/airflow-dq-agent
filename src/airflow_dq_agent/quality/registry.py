"""Allow-listed check catalog. Each CheckSpec owns suite, sample SQL, and apply predicates."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl
from pydantic import BaseModel, Field

from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS, get_table_contract
from airflow_dq_agent.quality.predicates import (
    failed_rows as rows_failing_spec,
)
from airflow_dq_agent.quality.predicates import (
    quarantine_predicate_for,
    sample_sql_for,
)


class CheckPolicy(BaseModel):
    """One controlled remediation rule declared by a quality check."""

    action_id: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class CheckSpec(BaseModel):
    check_id: str
    table: str
    column: str | None
    dimension: Dimension
    description: str
    contract_id: str = ""
    policies: list[CheckPolicy] = Field(default_factory=list)
    contains: str | None = None
    window_start_column: str | None = None
    window_end_column: str | None = None
    business_key: list[str] | None = None
    sample_sql: str = ""
    quarantine_predicate: str | None = None

    def model_post_init(self, __context: object) -> None:
        if not self.contract_id:
            self.contract_id = get_table_contract(self.table).contract_id
        self.sample_sql = sample_sql_for(self)
        self.quarantine_predicate = quarantine_predicate_for(self)
        contract = get_table_contract(self.table)
        if self.dimension is Dimension.COMPLETENESS and self.column is None:
            raise ValueError(f"{self.check_id} completeness check must name a column")
        if self.dimension is Dimension.VALIDITY:
            if self.column is None:
                raise ValueError(f"{self.check_id} validity check must name a column")
            has_rule = self.contains is not None or (
                self.window_start_column is not None and self.window_end_column is not None
            )
            if not has_rule and not contract.column(self.column).allowed_values:
                raise ValueError(f"{self.check_id} has no validity rule")
        if self.dimension is Dimension.UNIQUENESS and not self.business_key:
            raise ValueError(f"{self.check_id} uniqueness check must declare a business_key")
        if self.dimension is Dimension.REFERENTIAL_INTEGRITY and (
            self.column is None
            or not any(foreign_key[0] == self.column for foreign_key in contract.foreign_keys)
        ):
            raise ValueError(f"{self.check_id} column is not a contracted foreign key")

    def rule_for(self, action_id: str) -> CheckPolicy | None:
        return next((policy for policy in self.policies if policy.action_id == action_id), None)

    def failed_rows(self, frames: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
        return rows_failing_spec(self, frames)


def _policies(
    dimension: Dimension,
    *,
    business_key: list[str] | None = None,
) -> list[CheckPolicy]:
    """The only actions the compiler may select for a check.

    ``null_fill`` remains catalogued but intentionally absent until a reviewed check
    policy declares both its target rule and fill value.
    """
    if dimension is Dimension.COMPLETENESS:
        return [CheckPolicy(action_id="quarantine_nulls")]
    if dimension is Dimension.VALIDITY:
        return [CheckPolicy(action_id="quarantine_invalids")]
    if dimension is Dimension.UNIQUENESS:
        if not business_key:
            raise ValueError("uniqueness policy requires a business_key")
        return [
            CheckPolicy(action_id="dedupe_keep_min_pk", parameters={"business_key": business_key})
        ]
    if dimension is Dimension.REFERENTIAL_INTEGRITY:
        return [CheckPolicy(action_id="quarantine_orphans")]
    if dimension is Dimension.SCHEMA_DRIFT:
        return [CheckPolicy(action_id="schema_drift_ticket")]
    return [CheckPolicy(action_id="no_op_alert")]


def _spec(
    check_id: str,
    table: str,
    dimension: Dimension,
    description: str,
    *,
    column: str | None = None,
    contains: str | None = None,
    window_start_column: str | None = None,
    window_end_column: str | None = None,
    business_key: list[str] | None = None,
) -> CheckSpec:
    return CheckSpec(
        check_id=check_id,
        table=table,
        column=column,
        dimension=dimension,
        description=description,
        contains=contains,
        window_start_column=window_start_column,
        window_end_column=window_end_column,
        business_key=business_key,
        policies=_policies(dimension, business_key=business_key),
    )


CHECK_SPECS: dict[str, CheckSpec] = {
    spec.check_id: spec
    for spec in [
        _spec(
            "fact_orders.total_amount.completeness",
            "fact_orders",
            Dimension.COMPLETENESS,
            "total_amount must be present",
            column="total_amount",
        ),
        _spec(
            "fact_orders.status.validity",
            "fact_orders",
            Dimension.VALIDITY,
            "status in placed|paid|shipped|cancelled|returned",
            column="status",
        ),
        _spec(
            "fact_orders.order_nk.uniqueness",
            "fact_orders",
            Dimension.UNIQUENESS,
            "grain (customer_sk, order_ts) must be unique",
            column="order_ts",
            business_key=["customer_sk", "order_ts"],
        ),
        _spec(
            "fact_order_items.product_sk.referential_integrity",
            "fact_order_items",
            Dimension.REFERENTIAL_INTEGRITY,
            "product_sk must exist in dim_product",
            column="product_sk",
        ),
        _spec(
            "dim_customer.email.validity",
            "dim_customer",
            Dimension.VALIDITY,
            "email must contain @",
            column="email",
            contains="@",
        ),
        _spec(
            "dim_patient.sex.completeness",
            "dim_patient",
            Dimension.COMPLETENESS,
            "sex must be present",
            column="sex",
        ),
        _spec(
            "dim_patient.subject_id.uniqueness",
            "dim_patient",
            Dimension.UNIQUENESS,
            "subject_id is the business key",
            column="subject_id",
            business_key=["subject_id"],
        ),
        _spec(
            "fact_visits.visit_date.validity",
            "fact_visits",
            Dimension.VALIDITY,
            "visit_date must fall inside the scheduled window",
            column="visit_date",
            window_start_column="window_start",
            window_end_column="window_end",
        ),
        _spec(
            "fact_visits.patient_sk.referential_integrity",
            "fact_visits",
            Dimension.REFERENTIAL_INTEGRITY,
            "patient_sk must exist in dim_patient",
            column="patient_sk",
        ),
        _spec(
            "fact_adverse_events.term_code.completeness",
            "fact_adverse_events",
            Dimension.COMPLETENESS,
            "term_code must be present",
            column="term_code",
        ),
        _spec(
            "fact_adverse_events.severity.validity",
            "fact_adverse_events",
            Dimension.VALIDITY,
            "severity in mild|moderate|severe",
            column="severity",
        ),
        _spec(
            "fact_orders.customer_sk.referential_integrity",
            "fact_orders",
            Dimension.REFERENTIAL_INTEGRITY,
            "customer_sk must exist in dim_customer",
            column="customer_sk",
        ),
        _spec(
            "dim_product.sku.uniqueness",
            "dim_product",
            Dimension.UNIQUENESS,
            "sku is unique",
            column="sku",
            business_key=["sku"],
        ),
        _spec(
            "dim_patient.site_sk.referential_integrity",
            "dim_patient",
            Dimension.REFERENTIAL_INTEGRITY,
            "site_sk must exist in dim_site",
            column="site_sk",
        ),
        _spec(
            "fact_adverse_events.patient_sk.referential_integrity",
            "fact_adverse_events",
            Dimension.REFERENTIAL_INTEGRITY,
            "patient_sk must exist in dim_patient",
            column="patient_sk",
        ),
        *[
            _spec(
                f"{table}.schema_drift",
                table,
                Dimension.SCHEMA_DRIFT,
                "observed columns must match TABLE_CONTRACTS",
            )
            for table in TABLE_CONTRACTS
        ],
    ]
}


def get_check_spec(check_id: str) -> CheckSpec:
    if check_id not in CHECK_SPECS:
        raise KeyError(f"Unknown check_id {check_id!r}")
    return CHECK_SPECS[check_id]
