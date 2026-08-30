import pytest

from airflow_dq_agent.apply import render_step
from airflow_dq_agent.apply.executor import _set_controlled_transaction_mode
from airflow_dq_agent.contracts.models import RemediationStep


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object) -> None:
        self.statements.append(str(statement))


def test_apply_uses_a_serializable_snapshot_before_target_locking() -> None:
    connection = _RecordingConnection()

    _set_controlled_transaction_mode(connection, dry_run=False)

    assert connection.statements == ["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"]


def test_dry_run_uses_the_same_serializable_snapshot_and_read_only_authority() -> None:
    connection = _RecordingConnection()

    _set_controlled_transaction_mode(connection, dry_run=True)

    assert connection.statements == [
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "SET TRANSACTION READ ONLY",
    ]


def test_renderer_uses_compound_key_and_ignores_claimed_row_count() -> None:
    rendered = render_step(
        RemediationStep(
            action_id="dedupe_keep_min_pk",
            table="fact_orders",
            params={"business_key": ["customer_sk", "order_ts"], "pk_column": "order_id"},
            estimated_rows=999999,
            rationale="test",
            sql_preview="safe preview",
        )
    )
    assert 'GROUP BY s."customer_sk", s."order_ts"' in rendered.sql
    assert "999999" not in rendered.sql


def test_quarantine_renderer_binds_json_primary_key_as_text() -> None:
    rendered = render_step(
        RemediationStep(
            action_id="quarantine_nulls",
            table="fact_orders",
            params={"column": "total_amount", "pk_column": "order_id"},
            rationale="test",
            sql_preview="safe preview",
        )
    )

    assert 'jsonb_build_object(CAST(:pk_key AS text), t."order_id")' in rendered.sql


def test_renderer_rejects_unknown_column_and_forbidden_preview() -> None:
    unknown = RemediationStep(
        action_id="null_fill",
        table="fact_orders",
        params={"column": "not_a_column", "fill_value": 0.0},
        rationale="test",
        sql_preview="safe preview",
    )
    with pytest.raises(ValueError, match="not_a_column"):
        render_step(unknown)
    forbidden = RemediationStep(
        action_id="no_op_alert",
        table="fact_orders",
        params={"check_id": "fact_orders.total_amount.completeness"},
        rationale="test",
        sql_preview="DROP TABLE warehouse.fact_orders",
    )
    with pytest.raises(ValueError, match="unsafe"):
        render_step(forbidden)


def test_null_fill_requires_contract_compatible_value() -> None:
    step = RemediationStep(
        action_id="null_fill",
        table="fact_orders",
        params={"column": "total_amount", "fill_value": "not-a-float"},
        rationale="test",
        sql_preview="safe preview",
    )
    with pytest.raises(ValueError, match="float64"):
        render_step(step)
