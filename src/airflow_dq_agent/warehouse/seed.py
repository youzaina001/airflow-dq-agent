"""Deterministic synthetic load + the known defects evals are scored against."""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from airflow_dq_agent.warehouse.db import apply_ddl, make_engine

COUNTRIES = ["US", "GB", "DE", "FR", "ES", "CA", "NL"]
CATEGORIES = ["devices", "consumables", "apparel", "lab"]
STATUSES = ["placed", "paid", "shipped", "cancelled", "returned"]
CURRENCIES = ["USD", "EUR", "GBP"]
AE_TERMS = ["AE-HEADACHE", "AE-NAUSEA", "AE-FATIGUE", "AE-RASH", "AE-COUGH"]
VISIT_CODES = ["SCR", "W1", "W4", "W8", "EOT"]
REGIONS = ["north", "south", "east", "west"]


def _wipe(engine: Engine) -> None:
    tables = [
        "dq.apply_log",
        "dq.check_runs",
        "dq.quarantine_rows",
        "warehouse.fact_adverse_events",
        "warehouse.fact_visits",
        "warehouse.fact_order_items",
        "warehouse.fact_orders",
        "warehouse.dim_patient",
        "warehouse.dim_site",
        "warehouse.dim_product",
        "warehouse.dim_customer",
    ]
    with engine.begin() as conn:
        for table in tables:
            conn.execute(text(f"TRUNCATE {table} RESTART IDENTITY CASCADE"))


def seed_warehouse(dsn: str | None = None, *, apply_schema: bool = True) -> None:
    engine = make_engine(dsn)
    if apply_schema:
        apply_ddl(engine)
    _wipe(engine)
    rng = random.Random(42)
    start = date(2025, 1, 1)

    customers = []
    for sk in range(1, 121):
        email = f"c{sk:03d}@example.test"
        if sk in {101, 102}:
            email = f"c{sk:03d}.invalid"
        customers.append(
            {
                "customer_sk": sk,
                "customer_nk": f"CUST-{sk:04d}",
                "email": email,
                "country": COUNTRIES[sk % len(COUNTRIES)],
                "signup_date": start + timedelta(days=sk),
                "is_active": sk % 17 != 0,
                "shadow_segment": "beta" if sk % 11 == 0 else None,
            }
        )

    products = []
    for sk in range(1, 41):
        products.append(
            {
                "product_sk": sk,
                "sku": f"SKU-{sk:04d}",
                "category": CATEGORIES[sk % len(CATEGORIES)],
                "unit_price": round(5.0 + sk * 1.25, 2),
                "active_flag": True,
            }
        )

    orders: list[dict[str, object]] = []
    items: list[dict[str, object]] = []
    item_id = 1
    for oid in range(1, 201):
        ts = datetime(2025, 3, 1, tzinfo=UTC) + timedelta(hours=oid)
        amount = round(20.0 + (oid % 50) * 3.4, 2)
        status = STATUSES[oid % len(STATUSES)]
        orders.append(
            {
                "order_id": oid,
                "customer_sk": 1 + (oid % 100),
                "order_ts": ts,
                "status": status,
                "total_amount": amount,
                "currency": CURRENCIES[oid % 3],
            }
        )
        n_lines = 1 + (oid % 3)
        for _ in range(n_lines):
            psk = 1 + (item_id % 40)
            qty = 1 + (item_id % 4)
            items.append(
                {
                    "order_item_id": item_id,
                    "order_id": oid,
                    "product_sk": psk,
                    "qty": qty,
                    "unit_price": products[psk - 1]["unit_price"],
                }
            )
            item_id += 1

    # Completeness: NULL amounts
    for oid in range(9001, 9006):
        orders.append(
            {
                "order_id": oid,
                "customer_sk": 1,
                "order_ts": datetime(2025, 6, 1, tzinfo=UTC) + timedelta(hours=oid - 9001),
                "status": "placed",
                "total_amount": None,
                "currency": "USD",
            }
        )
        items.append(
            {
                "order_item_id": 8000 + oid,
                "order_id": oid,
                "product_sk": 1,
                "qty": 1,
                "unit_price": 5.0,
            }
        )

    # Validity: illegal status
    for i, oid in enumerate(range(9101, 9104)):
        orders.append(
            {
                "order_id": oid,
                "customer_sk": 2,
                "order_ts": datetime(2025, 6, 2, tzinfo=UTC) + timedelta(hours=i),
                "status": "SHIPPPED",
                "total_amount": 40.0,
                "currency": "USD",
            }
        )

    # Logical duplicates: same customer_sk + order_ts, distinct PKs
    dup_ts = datetime(2025, 6, 3, 12, tzinfo=UTC)
    orders.append(
        {
            "order_id": 9201,
            "customer_sk": 7,
            "order_ts": dup_ts,
            "status": "paid",
            "total_amount": 55.0,
            "currency": "USD",
        }
    )
    orders.append(
        {
            "order_id": 9202,
            "customer_sk": 7,
            "order_ts": dup_ts,
            "status": "paid",
            "total_amount": 55.0,
            "currency": "USD",
        }
    )

    # RI: missing product
    for i, iid in enumerate(range(9301, 9304)):
        items.append(
            {
                "order_item_id": iid,
                "order_id": 10 + i,
                "product_sk": 999001,
                "qty": 1,
                "unit_price": 9.99,
            }
        )

    sites = [
        {
            "site_sk": sk,
            "site_id": f"SITE-{sk:02d}",
            "country": COUNTRIES[sk % len(COUNTRIES)],
            "region": REGIONS[sk % len(REGIONS)],
        }
        for sk in range(1, 21)
    ]

    patients = []
    for sk in range(1, 81):
        patients.append(
            {
                "patient_sk": sk,
                "subject_id": f"SUBJ-{sk:04d}",
                "site_sk": 1 + (sk % 20),
                "sex": ["M", "F", "U"][sk % 3],
                "birth_year": 1960 + (sk % 40),
                "enrolled_on": date(2024, 9, 1) + timedelta(days=sk),
            }
        )
    # Completeness: null sex
    patients.append(
        {
            "patient_sk": 501,
            "subject_id": "SUBJ-0501",
            "site_sk": 1,
            "sex": None,
            "birth_year": 1980,
            "enrolled_on": date(2024, 10, 1),
        }
    )
    # Uniqueness: duplicate subject_id
    patients.append(
        {
            "patient_sk": 502,
            "subject_id": "SUBJ-DUPE",
            "site_sk": 2,
            "sex": "F",
            "birth_year": 1975,
            "enrolled_on": date(2024, 10, 2),
        }
    )
    patients.append(
        {
            "patient_sk": 503,
            "subject_id": "SUBJ-DUPE",
            "site_sk": 2,
            "sex": "M",
            "birth_year": 1976,
            "enrolled_on": date(2024, 10, 3),
        }
    )

    visits = []
    vid = 1
    for p in patients:
        patient_sk = p["patient_sk"]
        if not isinstance(patient_sk, int):
            raise TypeError("patient_sk must be an integer")
        if patient_sk >= 500:
            continue
        enrolled: date = p["enrolled_on"]  # type: ignore[assignment]
        for i, code in enumerate(VISIT_CODES):
            w0 = enrolled + timedelta(days=7 * i)
            w1 = w0 + timedelta(days=5)
            visits.append(
                {
                    "visit_id": vid,
                    "patient_sk": p["patient_sk"],
                    "visit_code": code,
                    "window_start": w0,
                    "window_end": w1,
                    "visit_date": w0 + timedelta(days=1),
                    "status": "completed",
                }
            )
            vid += 1

    # Validity: outside window
    for i, visit_id in enumerate(range(9401, 9405)):
        visits.append(
            {
                "visit_id": visit_id,
                "patient_sk": 1,
                "visit_code": "W99",
                "window_start": date(2025, 1, 1),
                "window_end": date(2025, 1, 7),
                "visit_date": date(2025, 3, 1) if i % 2 == 0 else date(2024, 12, 1),
                "status": "completed",
            }
        )

    # RI: missing patient
    for _i, visit_id in enumerate(range(9501, 9503)):
        visits.append(
            {
                "visit_id": visit_id,
                "patient_sk": 999501,
                "visit_code": "SCR",
                "window_start": date(2025, 1, 1),
                "window_end": date(2025, 1, 7),
                "visit_date": date(2025, 1, 2),
                "status": "scheduled",
            }
        )

    aes = []
    ae_id = 1
    for sk in range(1, 31):
        aes.append(
            {
                "ae_id": ae_id,
                "patient_sk": sk,
                "term_code": AE_TERMS[sk % len(AE_TERMS)],
                "severity": ["mild", "moderate", "severe"][sk % 3],
                "onset_date": date(2025, 2, 1) + timedelta(days=sk),
                "related_flag": sk % 4 == 0,
            }
        )
        ae_id += 1
    for i, aid in enumerate(range(9601, 9604)):
        aes.append(
            {
                "ae_id": aid,
                "patient_sk": 3,
                "term_code": None,
                "severity": "mild",
                "onset_date": date(2025, 4, 1) + timedelta(days=i),
                "related_flag": False,
            }
        )
    for i, aid in enumerate(range(9701, 9703)):
        aes.append(
            {
                "ae_id": aid,
                "patient_sk": 4,
                "term_code": "AE-HEADACHE",
                "severity": "lethal",
                "onset_date": date(2025, 4, 10) + timedelta(days=i),
                "related_flag": True,
            }
        )

    _ = rng  # reserved for future jitter; seed stays fully deterministic
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO warehouse.dim_customer
                (customer_sk, customer_nk, email, country, signup_date, is_active, shadow_segment)
                VALUES (:customer_sk, :customer_nk, :email, :country, :signup_date, :is_active, :shadow_segment)
                """
            ),
            customers,
        )
        conn.execute(
            text(
                """
                INSERT INTO warehouse.dim_product
                (product_sk, sku, category, unit_price, active_flag)
                VALUES (:product_sk, :sku, :category, :unit_price, :active_flag)
                """
            ),
            products,
        )
        conn.execute(
            text(
                """
                INSERT INTO warehouse.fact_orders
                (order_id, customer_sk, order_ts, status, total_amount, currency)
                VALUES (:order_id, :customer_sk, :order_ts, :status, :total_amount, :currency)
                """
            ),
            orders,
        )
        conn.execute(
            text(
                """
                INSERT INTO warehouse.fact_order_items
                (order_item_id, order_id, product_sk, qty, unit_price)
                VALUES (:order_item_id, :order_id, :product_sk, :qty, :unit_price)
                """
            ),
            items,
        )
        conn.execute(
            text(
                """
                INSERT INTO warehouse.dim_site (site_sk, site_id, country, region)
                VALUES (:site_sk, :site_id, :country, :region)
                """
            ),
            sites,
        )
        conn.execute(
            text(
                """
                INSERT INTO warehouse.dim_patient
                (patient_sk, subject_id, site_sk, sex, birth_year, enrolled_on)
                VALUES (:patient_sk, :subject_id, :site_sk, :sex, :birth_year, :enrolled_on)
                """
            ),
            patients,
        )
        conn.execute(
            text(
                """
                INSERT INTO warehouse.fact_visits
                (visit_id, patient_sk, visit_code, window_start, window_end, visit_date, status)
                VALUES (:visit_id, :patient_sk, :visit_code, :window_start, :window_end, :visit_date, :status)
                """
            ),
            visits,
        )
        conn.execute(
            text(
                """
                INSERT INTO warehouse.fact_adverse_events
                (ae_id, patient_sk, term_code, severity, onset_date, related_flag)
                VALUES (:ae_id, :patient_sk, :term_code, :severity, :onset_date, :related_flag)
                """
            ),
            aes,
        )


if __name__ == "__main__":
    seed_warehouse()
