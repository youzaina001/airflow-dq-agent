"""Issue #4: suite evaluation, sample SQL, and target resolution on PostgreSQL.

Uniqueness keeps the 2026-09-13 split. The suite fails every row in a duplicate
group. The Remediation Target Set keeps MIN(pk) out and contains only the extras.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from tests.settings_isolation import use_settings_without_dotenv

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.agent.runner import sample_failing_rows
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import CheckStatus, Dimension
from airflow_dq_agent.contracts.tables import get_table_contract
from airflow_dq_agent.demo import seed_warehouse
from airflow_dq_agent.planning.targets import PostgresTargetSetResolver
from airflow_dq_agent.quality import run_quality_suite
from airflow_dq_agent.quality.registry import get_check_spec
from airflow_dq_agent.warehouse.db import make_engine

# Known seeded primary keys. These literals are the warehouse defects, not a
# recomputation of the Polars or SQL predicates.
_ROW_PARITY: tuple[tuple[str, str, set[int], set[int]], ...] = (
    (
        "fact_orders.total_amount.completeness",
        "order_id",
        {9001, 9002, 9003, 9004, 9005},
        {9001, 9002, 9003, 9004, 9005},
    ),
    (
        "fact_orders.status.validity",
        "order_id",
        {9101, 9102, 9103},
        {9101, 9102, 9103},
    ),
    (
        "fact_orders.order_nk.uniqueness",
        "order_id",
        {9201, 9202},
        {9202},
    ),
    (
        "fact_order_items.product_sk.referential_integrity",
        "order_item_id",
        {9301, 9302, 9303},
        {9301, 9302, 9303},
    ),
    (
        "dim_customer.email.validity",
        "customer_sk",
        {101, 102},
        {101, 102},
    ),
    (
        "dim_patient.sex.completeness",
        "patient_sk",
        {501},
        {501},
    ),
    (
        "dim_patient.subject_id.uniqueness",
        "patient_sk",
        {502, 503},
        {503},
    ),
    (
        "fact_visits.visit_date.validity",
        "visit_id",
        {9401, 9402, 9403, 9404},
        {9401, 9402, 9403, 9404},
    ),
    (
        "fact_visits.patient_sk.referential_integrity",
        "visit_id",
        {9501, 9502},
        {9501, 9502},
    ),
    (
        "fact_adverse_events.term_code.completeness",
        "ae_id",
        {9601, 9602, 9603},
        {9601, 9602, 9603},
    ),
    (
        "fact_adverse_events.severity.validity",
        "ae_id",
        {9701, 9702},
        {9701, 9702},
    ),
)

# Seeded uniqueness business keys. These literals are the warehouse defects,
# not a Polars group or a copy of the sample SQL GROUP BY.
_SEEDED_UNIQUENESS_KEYS: dict[str, dict[str, object]] = {
    "fact_orders.order_nk.uniqueness": {
        "customer_sk": 7,
        "order_ts": datetime(2025, 6, 3, 12, tzinfo=UTC),
    },
    "dim_patient.subject_id.uniqueness": {
        "subject_id": "SUBJ-DUPE",
    },
}


@pytest.fixture(autouse=True)
def _ignore_developer_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    use_settings_without_dotenv(monkeypatch)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.setenv("LLM_MODE", "stub")
    monkeypatch.setenv("APPLY_MODE", "off")


@pytest.fixture(scope="module")
def parity_warehouse_dsn() -> Iterator[str]:
    """PostgreSQL for issue #4. A skip would leave the parity claim unverified."""
    configured = os.getenv("TEST_WAREHOUSE_DSN")
    if configured:
        yield configured
        return
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:16")
        container.start()
    except Exception as exc:
        pytest.fail(
            f"PostgreSQL is required for check/SQL parity (issue #4); a skip is incomplete: {exc}"
        )
    try:
        yield container.get_connection_url().replace("postgresql+psycopg2", "postgresql+psycopg")
    finally:
        container.stop()


def _primary_keys(rows: list[dict[str, object]], column: str) -> set[int]:
    return {int(row[column]) for row in rows}


def _matches_seeded_literal(actual: object, expected: object) -> bool:
    """Compare a returned value to a seeded literal without dropping timezone."""
    if not isinstance(expected, datetime):
        return actual == expected
    observed = actual
    if isinstance(actual, str):
        try:
            observed = datetime.fromisoformat(actual)
        except ValueError:
            return False
    if not isinstance(observed, datetime):
        return False
    # Naive values are not UTC. Aware values compare as the same instant.
    if observed.tzinfo is None or observed.utcoffset() is None:
        return False
    return observed == expected


@pytest.mark.integration
def test_suite_sample_sql_and_target_resolution_agree_on_postgres(
    parity_warehouse_dsn: str,
) -> None:
    seed_warehouse(parity_warehouse_dsn)
    report = run_quality_suite(parity_warehouse_dsn)
    engine = make_engine(parity_warehouse_dsn)
    resolver = PostgresTargetSetResolver(engine=engine)

    for check_id, pk_column, suite_pks, target_pks in _ROW_PARITY:
        spec = get_check_spec(check_id)
        check = report.get(check_id)
        assert check is not None
        assert check.status is CheckStatus.FAIL
        assert check.n_failed == len(suite_pks)
        assert len(check.sample_failures) == check.n_failed
        assert _primary_keys(check.sample_failures, pk_column) == suite_pks

        samples = sample_failing_rows(check_id, limit=100, dsn=parity_warehouse_dsn)
        if spec.dimension is Dimension.UNIQUENESS:
            assert len(samples) == 1
            assert int(samples[0]["n"]) == len(suite_pks)
            expected_key = _SEEDED_UNIQUENESS_KEYS[check_id]
            sample = samples[0]
            assert set(sample) == {*expected_key, "n"}
            for column, expected in expected_key.items():
                assert _matches_seeded_literal(sample[column], expected), (
                    check_id,
                    column,
                    type(sample[column]).__name__,
                    sample[column],
                    sample,
                )
            for row in check.sample_failures:
                for column, expected in expected_key.items():
                    assert _matches_seeded_literal(row[column], expected), (
                        check_id,
                        column,
                        type(row[column]).__name__,
                        row[column],
                        row,
                    )
            assert target_pks == suite_pks - {min(suite_pks)}
            assert min(suite_pks) not in target_pks
        else:
            assert _primary_keys(samples, pk_column) == suite_pks
            assert target_pks == suite_pks

        action = get_governed_action(spec.policies[0].action_id)
        params = action.derive_params(spec)
        rendered = action.render(table=spec.table, params=params, run_id="parity")
        assert rendered.target_sql is not None
        with engine.connect() as connection:
            selected = {
                int(row[0])
                for row in connection.execute(text(rendered.target_sql), rendered.target_params)
            }
        assert selected == target_pks
        resolved = resolver.resolve(
            report_run_id=report.run_id,
            check_id=check_id,
            action_id=action.action_id,
            table=spec.table,
            params=params,
        )
        ordered = [(pk,) for pk in sorted(target_pks)]
        assert resolved.count == len(target_pks)
        assert resolved.fingerprint == canonical_fingerprint(
            {"table": spec.table, "primary_keys": ordered}
        )

    email = report.get("dim_customer.email.validity")
    assert email is not None
    failed_customers = _primary_keys(email.sample_failures, "customer_sk")
    assert failed_customers == {101, 102}
    with engine.connect() as connection:
        valid_email = connection.execute(
            text("SELECT email FROM warehouse.dim_customer WHERE customer_sk = 1")
        ).scalar_one()
    assert valid_email == "c001@example.test"
    assert 1 not in failed_customers

    drift = report.get("dim_customer.schema_drift")
    assert drift is not None
    assert drift.status is CheckStatus.FAIL
    assert {row["column"] for row in drift.sample_failures} == {"shadow_segment"}
    assert {row["kind"] for row in drift.sample_failures} == {"extra"}
    observed = {
        str(row["column_name"])
        for row in sample_failing_rows(
            "dim_customer.schema_drift", limit=100, dsn=parity_warehouse_dsn
        )
    }
    contract_columns = set(get_table_contract("dim_customer").column_names)
    assert observed - contract_columns == {"shadow_segment"}
    assert "customer_sk" in observed
    drift_spec = get_check_spec("dim_customer.schema_drift")
    drift_action = get_governed_action("schema_drift_ticket")
    assert drift_action.mutates is False
    drift_params = drift_action.derive_params(drift_spec)
    drift_rendered = drift_action.render(
        table=drift_spec.table, params=drift_params, run_id="parity"
    )
    assert drift_rendered.target_sql is None
    drift_resolved = resolver.resolve(
        report_run_id=report.run_id,
        check_id=drift_spec.check_id,
        action_id=drift_action.action_id,
        table=drift_spec.table,
        params=drift_params,
    )
    assert drift_resolved.count == 0
    assert drift_resolved.fingerprint == canonical_fingerprint(
        {"table": "dim_customer", "primary_keys": []}
    )
