"""Allow-listed check catalog. sample_sql is the only SQL the agent may re-run, via a bound :limit."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.tables import get_table_contract


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
    sample_sql: str
    quarantine_predicate: str | None = None
    contract_id: str = ""
    policies: list[CheckPolicy] = Field(default_factory=list)

    def model_post_init(self, __context: object) -> None:
        if not self.contract_id:
            self.contract_id = get_table_contract(self.table).contract_id

    def rule_for(self, action_id: str) -> CheckPolicy | None:
        return next((policy for policy in self.policies if policy.action_id == action_id), None)


_UNIQUE_BUSINESS_KEYS: dict[str, list[str]] = {
    "fact_orders.order_nk.uniqueness": ["customer_sk", "order_ts"],
    "dim_patient.subject_id.uniqueness": ["subject_id"],
    "dim_product.sku.uniqueness": ["sku"],
}


def _policies(check_id: str, dimension: Dimension) -> list[CheckPolicy]:
    """The only actions the compiler may select for a check.

    ``null_fill`` remains catalogued but intentionally absent until a reviewed check
    policy declares both its target rule and fill value.
    """
    if dimension is Dimension.COMPLETENESS:
        return [CheckPolicy(action_id="quarantine_nulls")]
    if dimension is Dimension.VALIDITY:
        return [CheckPolicy(action_id="quarantine_invalids")]
    if dimension is Dimension.UNIQUENESS:
        return [
            CheckPolicy(
                action_id="dedupe_keep_min_pk",
                parameters={"business_key": _UNIQUE_BUSINESS_KEYS[check_id]},
            )
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
    sample_sql: str,
    column: str | None = None,
    quarantine_predicate: str | None = None,
) -> CheckSpec:
    return CheckSpec(
        check_id=check_id,
        table=table,
        column=column,
        dimension=dimension,
        description=description,
        sample_sql=sample_sql,
        quarantine_predicate=quarantine_predicate,
        policies=_policies(check_id, dimension),
    )


CHECK_SPECS: dict[str, CheckSpec] = {
    spec.check_id: spec
    for spec in [
        _spec(
            "fact_orders.total_amount.completeness",
            "fact_orders",
            Dimension.COMPLETENESS,
            "total_amount must be present",
            "SELECT order_id, customer_sk, total_amount FROM warehouse.fact_orders "
            "WHERE total_amount IS NULL ORDER BY order_id LIMIT :limit",
            column="total_amount",
        ),
        _spec(
            "fact_orders.status.validity",
            "fact_orders",
            Dimension.VALIDITY,
            "status in placed|paid|shipped|cancelled|returned",
            "SELECT order_id, status FROM warehouse.fact_orders "
            "WHERE status NOT IN ('placed','paid','shipped','cancelled','returned') "
            "ORDER BY order_id LIMIT :limit",
            column="status",
            quarantine_predicate=(
                "t.\"status\" NOT IN ('placed', 'paid', 'shipped', 'cancelled', 'returned')"
            ),
        ),
        _spec(
            "fact_orders.order_nk.uniqueness",
            "fact_orders",
            Dimension.UNIQUENESS,
            "grain (customer_sk, order_ts) must be unique",
            "SELECT customer_sk, order_ts, COUNT(*) AS n FROM warehouse.fact_orders "
            "GROUP BY customer_sk, order_ts HAVING COUNT(*) > 1 LIMIT :limit",
            column="order_ts",
        ),
        _spec(
            "fact_order_items.product_sk.referential_integrity",
            "fact_order_items",
            Dimension.REFERENTIAL_INTEGRITY,
            "product_sk must exist in dim_product",
            "SELECT i.order_item_id, i.product_sk FROM warehouse.fact_order_items i "
            "LEFT JOIN warehouse.dim_product p ON p.product_sk = i.product_sk "
            "WHERE p.product_sk IS NULL ORDER BY i.order_item_id LIMIT :limit",
            column="product_sk",
        ),
        _spec(
            "dim_customer.email.validity",
            "dim_customer",
            Dimension.VALIDITY,
            "email must contain @",
            "SELECT customer_sk, email FROM warehouse.dim_customer "
            "WHERE email NOT LIKE '%_@_%.__%' ORDER BY customer_sk LIMIT :limit",
            column="email",
            quarantine_predicate="t.\"email\" !~ '^[^@]+@[^@]+\\\\.[^@]+$'",
        ),
        _spec(
            "dim_customer.schema_drift",
            "dim_customer",
            Dimension.SCHEMA_DRIFT,
            "observed columns must match TABLE_CONTRACTS",
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'warehouse' AND table_name = 'dim_customer' "
            "ORDER BY ordinal_position LIMIT :limit",
        ),
        _spec(
            "dim_patient.sex.completeness",
            "dim_patient",
            Dimension.COMPLETENESS,
            "sex must be present",
            "SELECT patient_sk, subject_id, sex FROM warehouse.dim_patient "
            "WHERE sex IS NULL ORDER BY patient_sk LIMIT :limit",
            column="sex",
        ),
        _spec(
            "dim_patient.subject_id.uniqueness",
            "dim_patient",
            Dimension.UNIQUENESS,
            "subject_id is the business key",
            "SELECT subject_id, COUNT(*) AS n FROM warehouse.dim_patient "
            "GROUP BY subject_id HAVING COUNT(*) > 1 LIMIT :limit",
            column="subject_id",
        ),
        _spec(
            "fact_visits.visit_date.validity",
            "fact_visits",
            Dimension.VALIDITY,
            "visit_date must fall inside the scheduled window",
            "SELECT visit_id, patient_sk, visit_date, window_start, window_end "
            "FROM warehouse.fact_visits "
            "WHERE visit_date IS NOT NULL "
            "AND (visit_date < window_start OR visit_date > window_end) "
            "ORDER BY visit_id LIMIT :limit",
            column="visit_date",
            quarantine_predicate=(
                't."visit_date" IS NOT NULL AND '
                '(t."visit_date" < t."window_start" OR t."visit_date" > t."window_end")'
            ),
        ),
        _spec(
            "fact_visits.patient_sk.referential_integrity",
            "fact_visits",
            Dimension.REFERENTIAL_INTEGRITY,
            "patient_sk must exist in dim_patient",
            "SELECT v.visit_id, v.patient_sk FROM warehouse.fact_visits v "
            "LEFT JOIN warehouse.dim_patient p ON p.patient_sk = v.patient_sk "
            "WHERE p.patient_sk IS NULL ORDER BY v.visit_id LIMIT :limit",
            column="patient_sk",
        ),
        _spec(
            "fact_adverse_events.term_code.completeness",
            "fact_adverse_events",
            Dimension.COMPLETENESS,
            "term_code must be present",
            "SELECT ae_id, patient_sk, term_code FROM warehouse.fact_adverse_events "
            "WHERE term_code IS NULL ORDER BY ae_id LIMIT :limit",
            column="term_code",
        ),
        _spec(
            "fact_adverse_events.severity.validity",
            "fact_adverse_events",
            Dimension.VALIDITY,
            "severity in mild|moderate|severe",
            "SELECT ae_id, severity FROM warehouse.fact_adverse_events "
            "WHERE severity NOT IN ('mild','moderate','severe') "
            "ORDER BY ae_id LIMIT :limit",
            column="severity",
            quarantine_predicate="t.\"severity\" NOT IN ('mild', 'moderate', 'severe')",
        ),
        _spec(
            "fact_orders.customer_sk.referential_integrity",
            "fact_orders",
            Dimension.REFERENTIAL_INTEGRITY,
            "customer_sk must exist in dim_customer",
            "SELECT o.order_id, o.customer_sk FROM warehouse.fact_orders o "
            "LEFT JOIN warehouse.dim_customer c ON c.customer_sk = o.customer_sk "
            "WHERE c.customer_sk IS NULL ORDER BY o.order_id LIMIT :limit",
            column="customer_sk",
        ),
        _spec(
            "dim_product.sku.uniqueness",
            "dim_product",
            Dimension.UNIQUENESS,
            "sku is unique",
            "SELECT sku, COUNT(*) AS n FROM warehouse.dim_product "
            "GROUP BY sku HAVING COUNT(*) > 1 LIMIT :limit",
            column="sku",
        ),
        _spec(
            "dim_patient.site_sk.referential_integrity",
            "dim_patient",
            Dimension.REFERENTIAL_INTEGRITY,
            "site_sk must exist in dim_site",
            "SELECT p.patient_sk, p.site_sk FROM warehouse.dim_patient p "
            "LEFT JOIN warehouse.dim_site s ON s.site_sk = p.site_sk "
            "WHERE s.site_sk IS NULL ORDER BY p.patient_sk LIMIT :limit",
            column="site_sk",
        ),
        _spec(
            "fact_adverse_events.patient_sk.referential_integrity",
            "fact_adverse_events",
            Dimension.REFERENTIAL_INTEGRITY,
            "patient_sk must exist in dim_patient",
            "SELECT a.ae_id, a.patient_sk FROM warehouse.fact_adverse_events a "
            "LEFT JOIN warehouse.dim_patient p ON p.patient_sk = a.patient_sk "
            "WHERE p.patient_sk IS NULL ORDER BY a.ae_id LIMIT :limit",
            column="patient_sk",
        ),
    ]
}


def get_check_spec(check_id: str) -> CheckSpec:
    if check_id not in CHECK_SPECS:
        raise KeyError(f"Unknown check_id {check_id!r}")
    return CHECK_SPECS[check_id]
