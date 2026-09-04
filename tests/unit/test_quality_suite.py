from datetime import date, datetime

import polars as pl

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.models import CheckStatus
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import CHECK_SPECS, get_check_spec


def _contracted_frames() -> dict[str, pl.DataFrame]:
    return {
        "dim_customer": pl.DataFrame(
            {
                "customer_sk": [1],
                "customer_nk": ["C1"],
                "email": ["c1@example.test"],
                "country": ["US"],
                "signup_date": [date(2025, 1, 1)],
                "is_active": [True],
            }
        ),
        "dim_product": pl.DataFrame(
            {
                "product_sk": [1],
                "sku": ["S1"],
                "category": ["devices"],
                "unit_price": [1.0],
                "active_flag": [True],
            }
        ),
        "fact_orders": pl.DataFrame(
            {
                "order_id": [1],
                "customer_sk": [1],
                "order_ts": [datetime(2025, 1, 1)],
                "status": ["paid"],
                "total_amount": [1.0],
                "currency": ["USD"],
            }
        ),
        "fact_order_items": pl.DataFrame(
            {
                "order_item_id": [1],
                "order_id": [1],
                "product_sk": [1],
                "qty": [1],
                "unit_price": [1.0],
            }
        ),
        "dim_site": pl.DataFrame(
            {"site_sk": [1], "site_id": ["site1"], "country": ["US"], "region": ["north"]}
        ),
        "dim_patient": pl.DataFrame(
            {
                "patient_sk": [1],
                "subject_id": ["SUBJ1"],
                "site_sk": [1],
                "sex": ["F"],
                "birth_year": [1990],
                "enrolled_on": [date(2025, 1, 1)],
            }
        ),
        "fact_visits": pl.DataFrame(
            {
                "visit_id": [1],
                "patient_sk": [1],
                "visit_code": ["SCR"],
                "window_start": [date(2025, 1, 1)],
                "window_end": [date(2025, 1, 2)],
                "visit_date": [date(2025, 1, 1)],
                "status": ["completed"],
            }
        ),
        "fact_adverse_events": pl.DataFrame(
            {
                "ae_id": [1],
                "patient_sk": [1],
                "term_code": ["AE-HEADACHE"],
                "severity": ["mild"],
                "onset_date": [date(2025, 1, 1)],
                "related_flag": [True],
            }
        ),
    }


def test_suite_emits_only_catalogued_checks() -> None:
    report = run_suite_on_frames(_contracted_frames())
    assert report.failed_count == 0
    assert report.check_ids == set(CHECK_SPECS)
    assert {f"{table}.schema_drift" for table in TABLE_CONTRACTS} <= set(CHECK_SPECS)


def test_catalogued_completeness_check_fails_null_rows() -> None:
    frames = _contracted_frames()
    frames["fact_orders"] = frames["fact_orders"].with_columns(
        pl.lit(None).cast(pl.Float64).alias("total_amount")
    )
    report = run_suite_on_frames(frames)
    check = report.get("fact_orders.total_amount.completeness")
    assert check is not None
    assert check.n_failed == 1
    assert check.failed


def test_email_validity_is_the_contains_at_rule() -> None:
    frames = _contracted_frames()
    frames["dim_customer"] = pl.DataFrame(
        {
            "customer_sk": [1, 2, 3],
            "customer_nk": ["C1", "C2", "C3"],
            "email": ["c1@example.test", "user@localhost", "c3.invalid"],
            "country": ["US", "US", "US"],
            "signup_date": [date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)],
            "is_active": [True, True, True],
        }
    )
    report = run_suite_on_frames(frames)
    check = report.get("dim_customer.email.validity")
    assert check is not None
    assert check.n_failed == 1
    assert check.sample_failures[0]["email"] == "c3.invalid"

    spec = get_check_spec("dim_customer.email.validity")
    assert spec.sample_sql == (
        "SELECT customer_sk, email FROM warehouse.dim_customer "
        "WHERE email IS NULL OR email NOT LIKE '%@%' "
        "ORDER BY customer_sk LIMIT :limit"
    )
    assert spec.quarantine_predicate == 't."email" IS NULL OR t."email" NOT LIKE \'%@%\''

    rendered = get_governed_action("quarantine_invalids").render(
        table="dim_customer",
        params={
            "check_id": spec.check_id,
            "column": "email",
            "pk_column": "customer_sk",
        },
        run_id="test-run",
    )
    assert spec.quarantine_predicate in rendered.sql
    assert spec.quarantine_predicate in (rendered.target_sql or "")


def test_dropped_column_returns_report_with_error_and_schema_drift() -> None:
    frames = _contracted_frames()
    frames["fact_orders"] = frames["fact_orders"].drop("total_amount")

    report = run_suite_on_frames(frames)

    completeness = report.get("fact_orders.total_amount.completeness")
    assert completeness is not None
    assert completeness.status == CheckStatus.ERROR
    assert not completeness.failed
    assert completeness.sample_failures == []
    assert "total_amount" in completeness.message
    assert len(completeness.message) <= 200

    drift = report.get("fact_orders.schema_drift")
    assert drift is not None
    assert drift.status == CheckStatus.FAIL
    assert "total_amount" in drift.message
    assert any(
        row.get("kind") == "missing" and row.get("column") == "total_amount"
        for row in drift.sample_failures
    )
    assert any(check.status == CheckStatus.ERROR for check in report.checks)
    assert report.check_ids == set(CHECK_SPECS)


def test_missing_table_returns_a_report() -> None:
    frames = _contracted_frames()
    del frames["fact_orders"]

    report = run_suite_on_frames(frames)

    drift = report.get("fact_orders.schema_drift")
    assert drift is not None
    assert drift.status == CheckStatus.ERROR
    assert "fact_orders" in drift.message
    assert drift.sample_failures == []

    completeness = report.get("fact_orders.total_amount.completeness")
    assert completeness is not None
    assert completeness.status == CheckStatus.ERROR
    assert completeness.sample_failures == []
    assert report.check_ids == set(CHECK_SPECS)


def test_extra_column_returns_a_report_with_schema_drift() -> None:
    frames = _contracted_frames()
    frames["fact_orders"] = frames["fact_orders"].with_columns(pl.lit(1).alias("unexpected_col"))

    report = run_suite_on_frames(frames)

    drift = report.get("fact_orders.schema_drift")
    assert drift is not None
    assert drift.status == CheckStatus.FAIL
    assert "unexpected_col" in drift.message
    assert report.check_ids == set(CHECK_SPECS)
