from datetime import date, datetime

import polars as pl
import pytest
from psycopg.errors import UndefinedTable, UniqueViolation
from sqlalchemy.exc import ProgrammingError

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts import CandidateAction, Proposal, QualityEvidence, TargetSet
from airflow_dq_agent.contracts.models import CheckStatus
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import CHECK_SPECS, CheckSpec, get_check_spec
from airflow_dq_agent.quality.suite import TABLES, load_frames


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
    assert "missing column total_amount on fact_orders" in completeness.message
    assert len(completeness.message) <= 200

    drift = report.get("fact_orders.schema_drift")
    assert drift is not None
    assert drift.status == CheckStatus.FAIL
    assert "total_amount" in drift.message
    assert any(
        row.get("kind") == "missing" and row.get("column") == "total_amount"
        for row in drift.sample_failures
    )
    for check_id in (
        "fact_orders.status.validity",
        "fact_orders.order_nk.uniqueness",
        "fact_orders.customer_sk.referential_integrity",
    ):
        sibling = report.get(check_id)
        assert sibling is not None
        assert sibling.status == CheckStatus.PASS
    assert report.check_ids == set(CHECK_SPECS)

    class _NoTargets:
        def resolve(self, **_: object) -> TargetSet:
            raise AssertionError("ERROR checks must not resolve a target set")

    plan = compile_remediation_plan(
        report.model_copy(update={"checks": [completeness]}),
        Proposal(
            summary="Quarantine rows for a check that could not be evaluated.",
            root_cause_hypothesis="ERROR is not failed-check Quality Evidence.",
            candidate_actions=[
                CandidateAction(
                    action_id="quarantine_nulls",
                    evidence=[
                        QualityEvidence(
                            check_id=completeness.check_id,
                            contract_id=completeness.contract_id,
                        )
                    ],
                    rationale="This check never produced failed-row evidence.",
                )
            ],
            confidence=0.1,
        ),
        target_sets=_NoTargets(),
    )
    assert plan.blocked is True
    assert plan.items[0].kind == "non_executable"
    assert not any(item.kind == "executable" for item in plan.items)


def test_missing_table_returns_a_report() -> None:
    frames = _contracted_frames()
    del frames["fact_orders"]

    report = run_suite_on_frames(frames)

    drift = report.get("fact_orders.schema_drift")
    assert drift is not None
    assert drift.status == CheckStatus.FAIL
    assert drift.failed
    assert "missing table fact_orders" in drift.message
    assert any(row.get("kind") == "missing_table" for row in drift.sample_failures)

    completeness = report.get("fact_orders.total_amount.completeness")
    assert completeness is not None
    assert completeness.status == CheckStatus.ERROR
    assert completeness.sample_failures == []
    assert "missing table fact_orders" in completeness.message
    assert report.check_ids == set(CHECK_SPECS)
    assert "fact_orders.schema_drift" in report.failing_check_ids


def test_extra_column_returns_a_report_with_schema_drift() -> None:
    frames = _contracted_frames()
    frames["fact_orders"] = frames["fact_orders"].with_columns(pl.lit(1).alias("unexpected_col"))

    report = run_suite_on_frames(frames)

    drift = report.get("fact_orders.schema_drift")
    assert drift is not None
    assert drift.status == CheckStatus.FAIL
    assert "unexpected_col" in drift.message
    assert any(
        row.get("kind") == "extra" and row.get("column") == "unexpected_col"
        for row in drift.sample_failures
    )
    completeness = report.get("fact_orders.total_amount.completeness")
    assert completeness is not None
    assert completeness.status == CheckStatus.PASS
    assert report.check_ids == set(CHECK_SPECS)


def test_missing_parent_table_names_parent_in_ri_error() -> None:
    frames = _contracted_frames()
    del frames["dim_customer"]

    report = run_suite_on_frames(frames)

    ri = report.get("fact_orders.customer_sk.referential_integrity")
    assert ri is not None
    assert ri.status == CheckStatus.ERROR
    assert ri.sample_failures == []
    assert "missing table dim_customer" in ri.message
    status = report.get("fact_orders.status.validity")
    assert status is not None
    assert status.status == CheckStatus.PASS


def test_dropped_pk_names_column_in_ri_error() -> None:
    frames = _contracted_frames()
    frames["fact_orders"] = frames["fact_orders"].drop("order_id")

    report = run_suite_on_frames(frames)

    ri = report.get("fact_orders.customer_sk.referential_integrity")
    assert ri is not None
    assert ri.status == CheckStatus.ERROR
    assert "missing column order_id on fact_orders" in ri.message
    drift = report.get("fact_orders.schema_drift")
    assert drift is not None
    assert drift.status == CheckStatus.FAIL


def test_type_error_in_check_logic_still_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(self: CheckSpec, frames: object) -> pl.DataFrame:
        raise TypeError("check logic bug")

    monkeypatch.setattr(CheckSpec, "failed_rows", boom)
    with pytest.raises(TypeError, match="check logic bug"):
        run_suite_on_frames(_contracted_frames())


class _StubConnection:
    def __enter__(self) -> "_StubConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _StubEngine:
    def connect(self) -> _StubConnection:
        return _StubConnection()


def test_load_frames_omits_undefined_warehouse_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    available = _contracted_frames()

    def fake_read(query: str, connection: object) -> pl.DataFrame:
        table = query.rsplit(".", 1)[-1]
        if table == "fact_orders":
            raise ProgrammingError(
                query,
                {},
                UndefinedTable('relation "warehouse.fact_orders" does not exist'),
            )
        return available[table]

    monkeypatch.setattr(pl, "read_database", fake_read)

    loaded = load_frames(_StubEngine())  # type: ignore[arg-type]
    assert "fact_orders" not in loaded
    assert set(loaded) == set(TABLES) - {"fact_orders"}

    report = run_suite_on_frames(loaded)
    drift = report.get("fact_orders.schema_drift")
    assert drift is not None
    assert drift.status == CheckStatus.FAIL
    assert "missing table fact_orders" in drift.message
    completeness = report.get("fact_orders.total_amount.completeness")
    assert completeness is not None
    assert completeness.status == CheckStatus.ERROR


def test_load_frames_still_raises_other_database_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_read(query: str, connection: object) -> pl.DataFrame:
        raise ProgrammingError(query, {}, UniqueViolation("duplicate key"))

    monkeypatch.setattr(pl, "read_database", fake_read)
    with pytest.raises(ProgrammingError, match="duplicate key"):
        load_frames(_StubEngine())  # type: ignore[arg-type]
