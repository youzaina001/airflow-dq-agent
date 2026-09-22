from datetime import date, datetime

import polars as pl

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts import (
    CandidateAction,
    ExecutablePlanItem,
    Proposal,
    QualityEvidence,
    TargetSet,
)
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS, get_table_contract
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import get_check_spec


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


def _pk_column(table: str) -> str:
    return get_table_contract(table).primary_key[0]


def _failed_primary_keys(check_id: str, frames: dict[str, pl.DataFrame]) -> set[object]:
    spec = get_check_spec(check_id)
    pk = _pk_column(spec.table)
    from_rows = set(spec.failed_rows(frames)[pk].to_list())
    report = run_suite_on_frames(frames)
    check = report.get(check_id)
    assert check is not None
    from_report = {row[pk] for row in check.sample_failures}
    assert from_report == from_rows
    return from_rows


def _render(check_id: str, action_id: str):
    spec = get_check_spec(check_id)
    action = get_governed_action(action_id)
    return spec, action.render(
        table=spec.table, params=action.derive_params(spec), run_id="parity-run"
    )


def test_completeness_failed_pks_agree_with_shipped_target_predicate() -> None:
    null_order_id = 101
    frames = _contracted_frames()
    frames["fact_orders"] = pl.concat(
        [
            frames["fact_orders"],
            pl.DataFrame(
                {
                    "order_id": [null_order_id],
                    "customer_sk": [1],
                    "order_ts": [datetime(2025, 1, 2)],
                    "status": ["paid"],
                    "total_amount": pl.Series("total_amount", [None], dtype=pl.Float64),
                    "currency": ["USD"],
                }
            ),
        ]
    )
    check_id = "fact_orders.total_amount.completeness"
    assert _failed_primary_keys(check_id, frames) == {null_order_id}

    spec, rendered = _render(check_id, "quarantine_nulls")
    assert spec.quarantine_predicate is not None
    assert spec.column is not None
    assert f"{spec.column} IS NULL" in spec.sample_sql
    assert spec.quarantine_predicate in (rendered.target_sql or "")
    assert spec.quarantine_predicate in rendered.sql


def test_validity_failed_pks_agree_with_shipped_target_predicate() -> None:
    invalid_order_id = 201
    frames = _contracted_frames()
    frames["fact_orders"] = pl.concat(
        [
            frames["fact_orders"],
            pl.DataFrame(
                {
                    "order_id": [invalid_order_id],
                    "customer_sk": [1],
                    "order_ts": [datetime(2025, 1, 2)],
                    "status": ["SHIPPPED"],
                    "total_amount": [1.0],
                    "currency": ["USD"],
                }
            ),
        ]
    )
    check_id = "fact_orders.status.validity"
    assert _failed_primary_keys(check_id, frames) == {invalid_order_id}

    spec, rendered = _render(check_id, "quarantine_invalids")
    assert spec.quarantine_predicate is not None
    assert spec.column is not None
    assert f"{spec.column} NOT IN" in spec.sample_sql
    assert spec.quarantine_predicate in (rendered.target_sql or "")
    assert spec.quarantine_predicate in rendered.sql


def test_email_validity_boundary_failed_primary_keys() -> None:
    invalid_email_pk = 10
    valid_email_pk = 11
    frames = _contracted_frames()
    frames["dim_customer"] = pl.DataFrame(
        {
            "customer_sk": [invalid_email_pk, valid_email_pk],
            "customer_nk": ["C10", "C11"],
            "email": ["not-an-email", "user@example.test"],
            "country": ["US", "US"],
            "signup_date": [date(2025, 1, 1), date(2025, 1, 1)],
            "is_active": [True, True],
        }
    )
    check_id = "dim_customer.email.validity"
    failed = _failed_primary_keys(check_id, frames)
    assert failed == {invalid_email_pk}
    assert valid_email_pk not in failed

    spec, rendered = _render(check_id, "quarantine_invalids")
    assert spec.contains == "@"
    assert spec.quarantine_predicate is not None
    assert "NOT LIKE '%@%'" in spec.sample_sql
    assert spec.quarantine_predicate in (rendered.target_sql or "")


def test_referential_integrity_failed_pks_agree_with_orphan_target_sql() -> None:
    orphan_item_id = 301
    frames = _contracted_frames()
    frames["fact_order_items"] = pl.concat(
        [
            frames["fact_order_items"],
            pl.DataFrame(
                {
                    "order_item_id": [orphan_item_id],
                    "order_id": [1],
                    "product_sk": [999001],
                    "qty": [1],
                    "unit_price": [1.0],
                }
            ),
        ]
    )
    check_id = "fact_order_items.product_sk.referential_integrity"
    assert _failed_primary_keys(check_id, frames) == {orphan_item_id}

    spec, rendered = _render(check_id, "quarantine_orphans")
    target_sql = rendered.target_sql or ""
    assert "LEFT JOIN" in spec.sample_sql
    assert "IS NULL" in spec.sample_sql
    assert "LEFT JOIN" in target_sql
    assert "IS NULL" in target_sql
    assert spec.column is not None
    assert spec.column in spec.sample_sql
    assert spec.column in target_sql


def test_schema_drift_compares_column_names_and_ticket_is_a_noop() -> None:
    extra_column = "shadow_segment"
    frames = _contracted_frames()
    frames["dim_customer"] = frames["dim_customer"].with_columns(pl.lit("x").alias(extra_column))
    check_id = "dim_customer.schema_drift"
    spec = get_check_spec(check_id)
    failed = spec.failed_rows(frames)
    drifted = set(failed["column"].to_list())
    contract_columns = set(TABLE_CONTRACTS["dim_customer"].column_names)
    observed_columns = set(frames["dim_customer"].columns)
    assert drifted == {extra_column}
    assert drifted == observed_columns - contract_columns
    assert "customer_sk" not in failed.columns

    report = run_suite_on_frames(frames)
    check = report.get(check_id)
    assert check is not None
    assert {row["column"] for row in check.sample_failures} == {extra_column}

    rendered = get_governed_action("schema_drift_ticket").render(
        table=spec.table,
        params=get_governed_action("schema_drift_ticket").derive_params(spec),
        run_id="parity-run",
    )
    assert get_governed_action("schema_drift_ticket").mutates is False
    assert rendered.target_sql is None


def test_uniqueness_suite_failed_pks_are_not_the_remediation_target_set() -> None:
    """Suite fails every duplicate-group row; dedupe keeps MIN(pk) extras only."""
    duplicate_pks = {20, 21}
    unique_pk = 1
    shared_ts = datetime(2025, 6, 3, 12, 0, 0)
    frames = _contracted_frames()
    frames["fact_orders"] = pl.concat(
        [
            frames["fact_orders"],
            pl.DataFrame(
                {
                    "order_id": [20, 21],
                    "customer_sk": [1, 1],
                    "order_ts": [shared_ts, shared_ts],
                    "status": ["paid", "paid"],
                    "total_amount": [1.0, 1.0],
                    "currency": ["USD", "USD"],
                }
            ),
        ]
    )
    check_id = "fact_orders.order_nk.uniqueness"
    suite_failed_pks = _failed_primary_keys(check_id, frames)
    assert suite_failed_pks == duplicate_pks
    assert unique_pk not in suite_failed_pks

    spec, rendered = _render(check_id, "dedupe_keep_min_pk")
    target_sql = rendered.target_sql or ""
    assert "MIN(" in target_sql
    assert "NOT IN" in target_sql
    assert "GROUP BY" in target_sql
    assert "HAVING COUNT(*) > 1" in spec.sample_sql

    report = run_suite_on_frames(frames)
    failed = report.get(check_id)
    assert failed is not None
    scoped = report.model_copy(update={"checks": [failed]})

    class ExtraDuplicateTargets:
        def resolve(self, **_: object) -> TargetSet:
            return TargetSet(count=1, fingerprint="targets:dedupe-extra-1")

    plan = compile_remediation_plan(
        scoped,
        Proposal(
            summary="Quarantine extra duplicate grain rows.",
            root_cause_hypothesis="The source loaded the same business key twice.",
            candidate_actions=[
                CandidateAction(
                    action_id="dedupe_keep_min_pk",
                    evidence=[
                        QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                    ],
                    rationale="Keep MIN(pk) and quarantine the extra duplicate.",
                )
            ],
            confidence=0.9,
        ),
        target_sets=ExtraDuplicateTargets(),
    )
    assert plan.blocked is False
    item = plan.items[0]
    assert isinstance(item, ExecutablePlanItem)
    assert item.action_id == "dedupe_keep_min_pk"
    extra_duplicate_count = 1
    assert item.target_set.count == extra_duplicate_count
    assert len(suite_failed_pks) == 2
    assert item.target_set.count != len(suite_failed_pks)
