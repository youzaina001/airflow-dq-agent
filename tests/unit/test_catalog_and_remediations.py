import pytest

from airflow_dq_agent.action_definitions import GOVERNED_ACTIONS, get_action_definition
from airflow_dq_agent.catalog import (
    get_check,
    get_lineage,
    get_remediation,
    get_table_contract,
    list_tables,
)
from airflow_dq_agent.contracts.remediations import REMEDIATION_CATALOG, validate_step_params


def test_catalog_reads_are_contract_backed() -> None:
    assert len(list_tables()) == 8
    assert get_table_contract("warehouse.fact_orders")["primary_key"] == ["order_id"]
    assert get_check("fact_orders.status.validity")["table"] == "fact_orders"
    assert get_lineage("fact_orders")["upstream"]
    assert get_remediation("quarantine_invalids")["mutates"] is True


def test_compound_business_key_and_controlled_validity_params_are_valid() -> None:
    assert not validate_step_params(
        "dedupe_keep_min_pk",
        "fact_orders",
        {"business_key": ["customer_sk", "order_ts"], "pk_column": "order_id"},
    )
    assert not validate_step_params(
        "quarantine_invalids",
        "fact_orders",
        {
            "check_id": "fact_orders.status.validity",
            "column": "status",
            "pk_column": "order_id",
        },
    )
    assert validate_step_params(
        "quarantine_invalids",
        "fact_orders",
        {
            "check_id": "fact_orders.status.validity",
            "column": "currency",
            "pk_column": "order_id",
        },
    )


_ACTION_CASES = {
    "no_op_alert": ("fact_orders", {"check_id": "fact_orders.total_amount.completeness"}),
    "quarantine_nulls": (
        "fact_orders",
        {"column": "total_amount", "pk_column": "order_id"},
    ),
    "quarantine_invalids": (
        "fact_orders",
        {
            "check_id": "fact_orders.status.validity",
            "column": "status",
            "pk_column": "order_id",
        },
    ),
    "null_fill": ("fact_orders", {"column": "total_amount", "fill_value": 0.0}),
    "quarantine_orphans": (
        "fact_orders",
        {
            "fk_column": "customer_sk",
            "ref_table": "dim_customer",
            "ref_column": "customer_sk",
            "pk_column": "order_id",
        },
    ),
    "dedupe_keep_min_pk": (
        "fact_orders",
        {"business_key": ["customer_sk", "order_ts"], "pk_column": "order_id"},
    ),
    "schema_drift_ticket": ("fact_orders", {"check_id": "fact_orders.schema_drift"}),
}


@pytest.mark.parametrize(
    ("action_id", "table", "params"),
    [(action_id, table, params) for action_id, (table, params) in _ACTION_CASES.items()],
)
def test_every_catalogued_action_owns_validation_and_rendering(
    action_id: str, table: str, params: dict[str, object]
) -> None:
    action = get_action_definition(action_id)

    assert action.metadata == REMEDIATION_CATALOG[action_id]
    assert not action.validate_params(table, params)
    rendered = action.render(table=table, params=params, run_id="action-coverage")

    assert rendered.action_id == action_id
    assert rendered.table == table
    assert action.mutates is action.metadata.mutates


def test_action_registry_is_complete_and_has_no_extra_implementations() -> None:
    assert set(GOVERNED_ACTIONS) == set(REMEDIATION_CATALOG)
    assert set(_ACTION_CASES) == set(GOVERNED_ACTIONS)
