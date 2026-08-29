from datetime import date, datetime

import polars as pl

from airflow_dq_agent.quality import run_suite_on_frames


def test_polars_pandera_suite_runs_on_contracted_frames() -> None:
    frames = {
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
    report = run_suite_on_frames(frames)
    assert report.failed_count == 0
    assert len(report.checks) == 31
