"""In-memory QualitySuiteReport matching the seeded defects. Used by evals and `cli demo --no-db`."""

from __future__ import annotations

from airflow_dq_agent.contracts.models import CheckResult, CheckStatus, QualitySuiteReport
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.warehouse.defects import EXPECTED_DEFECTS


def _fail(check_id: str, samples: list[dict[str, object]], n_total: int) -> CheckResult:
    spec = CHECK_SPECS[check_id]
    return CheckResult(
        check_id=spec.check_id,
        table=spec.table,
        column=spec.column,
        dimension=spec.dimension,
        status=CheckStatus.FAIL,
        n_failed=len(samples),
        n_total=n_total,
        sample_failures=samples,
        message=EXPECTED_DEFECTS[check_id].how,
        contract_id=spec.contract_id,
        predicate=spec.description,
    )


def _pass(check_id: str, n_total: int) -> CheckResult:
    spec = CHECK_SPECS[check_id]
    return CheckResult(
        check_id=spec.check_id,
        table=spec.table,
        column=spec.column,
        dimension=spec.dimension,
        status=CheckStatus.PASS,
        n_failed=0,
        n_total=n_total,
        sample_failures=[],
        message=f"{check_id} passed",
        contract_id=spec.contract_id,
        predicate=spec.description,
    )


def seeded_failure_report() -> QualitySuiteReport:
    """Mirrors warehouse/seed.py so evals do not need Postgres."""
    checks = [
        _fail(
            "fact_orders.total_amount.completeness",
            [{"order_id": i, "total_amount": None} for i in range(9001, 9006)],
            n_total=210,
        ),
        _fail(
            "fact_orders.status.validity",
            [{"order_id": i, "status": "SHIPPPED"} for i in range(9101, 9104)],
            n_total=210,
        ),
        _fail(
            "fact_orders.order_nk.uniqueness",
            [
                {"order_id": 9201, "customer_sk": 7, "order_ts": "2025-06-03T12:00:00+00:00"},
                {"order_id": 9202, "customer_sk": 7, "order_ts": "2025-06-03T12:00:00+00:00"},
            ],
            n_total=210,
        ),
        _fail(
            "fact_order_items.product_sk.referential_integrity",
            [{"order_item_id": i, "product_sk": 999001} for i in range(9301, 9304)],
            n_total=450,
        ),
        _fail(
            "dim_customer.email.validity",
            [
                {"customer_sk": 101, "email": "c101.invalid"},
                {"customer_sk": 102, "email": "c102.invalid"},
            ],
            n_total=120,
        ),
        _fail(
            "dim_customer.schema_drift",
            [{"kind": "extra", "column": "shadow_segment"}],
            n_total=7,
        ),
        _fail(
            "dim_patient.sex.completeness",
            [{"patient_sk": 501, "subject_id": "SUBJ-0501", "sex": None}],
            n_total=83,
        ),
        _fail(
            "dim_patient.subject_id.uniqueness",
            [
                {"patient_sk": 502, "subject_id": "SUBJ-DUPE"},
                {"patient_sk": 503, "subject_id": "SUBJ-DUPE"},
            ],
            n_total=83,
        ),
        _fail(
            "fact_visits.visit_date.validity",
            [{"visit_id": i, "visit_date": "2025-03-01"} for i in range(9401, 9405)],
            n_total=406,
        ),
        _fail(
            "fact_visits.patient_sk.referential_integrity",
            [{"visit_id": i, "patient_sk": 999501} for i in range(9501, 9503)],
            n_total=406,
        ),
        _fail(
            "fact_adverse_events.term_code.completeness",
            [{"ae_id": i, "term_code": None} for i in range(9601, 9604)],
            n_total=35,
        ),
        _fail(
            "fact_adverse_events.severity.validity",
            [{"ae_id": i, "severity": "lethal"} for i in range(9701, 9703)],
            n_total=35,
        ),
        _pass("fact_orders.customer_sk.referential_integrity", 210),
        _pass("dim_product.sku.uniqueness", 40),
        _pass("dim_patient.site_sk.referential_integrity", 83),
        _pass("fact_adverse_events.patient_sk.referential_integrity", 35),
    ]
    present = {check.check_id for check in checks}
    checks.extend(
        _pass(check_id, n_total=100) for check_id in CHECK_SPECS if check_id not in present
    )
    observed = {name: list(c.column_names) for name, c in TABLE_CONTRACTS.items()}
    observed["dim_customer"] = [*TABLE_CONTRACTS["dim_customer"].column_names, "shadow_segment"]
    return QualitySuiteReport(checks=checks, observed_columns=observed)


def green_report() -> QualitySuiteReport:
    """All contracted checks pass. Used to prove evals reject a spurious proposal."""
    checks = [_pass(check_id, n_total=100) for check_id in CHECK_SPECS]
    observed = {name: list(c.column_names) for name, c in TABLE_CONTRACTS.items()}
    return QualitySuiteReport(checks=checks, observed_columns=observed)
