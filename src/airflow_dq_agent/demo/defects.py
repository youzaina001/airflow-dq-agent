"""Known injected defects. Eval fixtures and the README story are keyed off these counts."""

from __future__ import annotations

from pydantic import BaseModel

from airflow_dq_agent.contracts.models import Dimension


class InjectedDefect(BaseModel):
    check_id: str
    table: str
    dimension: Dimension
    n_rows: int
    how: str


# Deterministic. Seed uses these IDs so tests can assert exact failure counts.
EXPECTED_DEFECTS: dict[str, InjectedDefect] = {
    "fact_orders.total_amount.completeness": InjectedDefect(
        check_id="fact_orders.total_amount.completeness",
        table="fact_orders",
        dimension=Dimension.COMPLETENESS,
        n_rows=5,
        how="order_id 9001-9005 have NULL total_amount",
    ),
    "fact_orders.status.validity": InjectedDefect(
        check_id="fact_orders.status.validity",
        table="fact_orders",
        dimension=Dimension.VALIDITY,
        n_rows=3,
        how="order_id 9101-9103 have status SHIPPPED (triple P)",
    ),
    "fact_orders.order_nk.uniqueness": InjectedDefect(
        check_id="fact_orders.order_nk.uniqueness",
        table="fact_orders",
        dimension=Dimension.UNIQUENESS,
        n_rows=2,
        how="order_id 9201 and 9202 share (customer_sk=7, order_ts) — a logical duplicate grain",
    ),
    "fact_order_items.product_sk.referential_integrity": InjectedDefect(
        check_id="fact_order_items.product_sk.referential_integrity",
        table="fact_order_items",
        dimension=Dimension.REFERENTIAL_INTEGRITY,
        n_rows=3,
        how="order_item_id 9301-9303 reference product_sk 999001 which does not exist",
    ),
    "dim_customer.email.validity": InjectedDefect(
        check_id="dim_customer.email.validity",
        table="dim_customer",
        dimension=Dimension.VALIDITY,
        n_rows=2,
        how="customer_sk 101, 102 have emails without '@'",
    ),
    "dim_customer.schema_drift": InjectedDefect(
        check_id="dim_customer.schema_drift",
        table="dim_customer",
        dimension=Dimension.SCHEMA_DRIFT,
        n_rows=1,
        how="observed column shadow_segment is not in TABLE_CONTRACTS",
    ),
    "dim_patient.sex.completeness": InjectedDefect(
        check_id="dim_patient.sex.completeness",
        table="dim_patient",
        dimension=Dimension.COMPLETENESS,
        n_rows=1,
        how="patient_sk 501 has NULL sex",
    ),
    "dim_patient.subject_id.uniqueness": InjectedDefect(
        check_id="dim_patient.subject_id.uniqueness",
        table="dim_patient",
        dimension=Dimension.UNIQUENESS,
        n_rows=2,
        how="patient_sk 502 and 503 share subject_id SUBJ-DUPE",
    ),
    "fact_visits.visit_date.validity": InjectedDefect(
        check_id="fact_visits.visit_date.validity",
        table="fact_visits",
        dimension=Dimension.VALIDITY,
        n_rows=4,
        how="visit_id 9401-9404 have visit_date outside [window_start, window_end]",
    ),
    "fact_visits.patient_sk.referential_integrity": InjectedDefect(
        check_id="fact_visits.patient_sk.referential_integrity",
        table="fact_visits",
        dimension=Dimension.REFERENTIAL_INTEGRITY,
        n_rows=2,
        how="visit_id 9501-9502 reference patient_sk 999501",
    ),
    "fact_adverse_events.term_code.completeness": InjectedDefect(
        check_id="fact_adverse_events.term_code.completeness",
        table="fact_adverse_events",
        dimension=Dimension.COMPLETENESS,
        n_rows=3,
        how="ae_id 9601-9603 have NULL term_code",
    ),
    "fact_adverse_events.severity.validity": InjectedDefect(
        check_id="fact_adverse_events.severity.validity",
        table="fact_adverse_events",
        dimension=Dimension.VALIDITY,
        n_rows=2,
        how="ae_id 9701-9702 have severity 'lethal' (not in allow-list)",
    ),
}
