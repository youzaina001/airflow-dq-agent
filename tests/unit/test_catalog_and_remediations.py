import pytest

from airflow_dq_agent.action_definitions import (
    get_governed_action,
    list_remediation_actions,
)
from airflow_dq_agent.catalog import (
    get_check,
    get_lineage,
    get_remediation,
    get_table_contract,
    list_tables,
)
from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.quality.registry import CheckPolicy, CheckSpec, get_check_spec


def test_catalog_reads_are_contract_backed() -> None:
    assert len(list_tables()) == 8
    assert get_table_contract("warehouse.fact_orders")["primary_key"] == ["order_id"]
    assert get_check("fact_orders.status.validity")["table"] == "fact_orders"
    assert get_lineage("fact_orders")["upstream"]
    assert get_remediation("quarantine_invalids")["mutates"] is True


def test_governed_action_registry_is_the_single_source_of_catalogued_metadata() -> None:
    actions = list_remediation_actions()

    assert {action.action_id for action in actions} == {
        "dedupe_keep_min_pk",
        "no_op_alert",
        "null_fill",
        "quarantine_invalids",
        "quarantine_nulls",
        "quarantine_orphans",
        "schema_drift_ticket",
    }
    assert get_governed_action("quarantine_nulls").metadata in actions


def test_governed_action_validates_controlled_params() -> None:
    action = get_governed_action("quarantine_invalids")

    assert not action.validate_params(
        "fact_orders",
        {
            "check_id": "fact_orders.status.validity",
            "column": "status",
            "pk_column": "order_id",
        },
    )
    assert action.validate_params(
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


_DERIVATION_CASES = {
    "no_op_alert": (
        CheckSpec(
            check_id="fact_orders.total_amount.alert",
            table="fact_orders",
            column="total_amount",
            dimension=Dimension.COMPLETENESS,
            description="A reviewed alert policy records the failure.",
            policies=[CheckPolicy(action_id="no_op_alert")],
        ),
        {"check_id": "fact_orders.total_amount.alert"},
    ),
    "quarantine_nulls": (
        get_check_spec("fact_orders.total_amount.completeness"),
        {"column": "total_amount", "pk_column": "order_id"},
    ),
    "quarantine_invalids": (
        get_check_spec("fact_orders.status.validity"),
        {
            "check_id": "fact_orders.status.validity",
            "column": "status",
            "pk_column": "order_id",
        },
    ),
    "null_fill": (
        CheckSpec(
            check_id="fact_orders.total_amount.null_fill",
            table="fact_orders",
            column="total_amount",
            dimension=Dimension.COMPLETENESS,
            description="A reviewed fill policy supplies the controlled value.",
            policies=[CheckPolicy(action_id="null_fill", parameters={"fill_value": 0.0})],
        ),
        {"column": "total_amount", "fill_value": 0.0},
    ),
    "quarantine_orphans": (
        get_check_spec("fact_orders.customer_sk.referential_integrity"),
        {
            "fk_column": "customer_sk",
            "ref_table": "dim_customer",
            "ref_column": "customer_sk",
            "pk_column": "order_id",
        },
    ),
    "dedupe_keep_min_pk": (
        get_check_spec("fact_orders.order_nk.uniqueness"),
        {"business_key": ["customer_sk", "order_ts"], "pk_column": "order_id"},
    ),
    "schema_drift_ticket": (
        get_check_spec("fact_orders.schema_drift"),
        {"check_id": "fact_orders.schema_drift"},
    ),
}


@pytest.mark.parametrize(
    ("action_id", "spec", "expected_params"),
    [
        (action_id, spec, expected_params)
        for action_id, (spec, expected_params) in _DERIVATION_CASES.items()
    ],
)
def test_every_governed_action_derives_its_controlled_params(
    action_id: str, spec: CheckSpec, expected_params: dict[str, object]
) -> None:
    assert get_governed_action(action_id).derive_params(spec) == expected_params


@pytest.mark.parametrize(
    ("action_id", "table", "params"),
    [(action_id, table, params) for action_id, (table, params) in _ACTION_CASES.items()],
)
def test_every_catalogued_action_owns_validation_and_rendering(
    action_id: str, table: str, params: dict[str, object]
) -> None:
    action = get_governed_action(action_id)

    assert not action.validate_params(table, params)
    rendered = action.render(table=table, params=params, run_id="action-coverage")

    assert rendered.action_id == action_id
    assert rendered.table == table
    assert action.mutates == (action_id not in {"no_op_alert", "schema_drift_ticket"})


def test_action_coverage_cases_match_the_governed_action_registry() -> None:
    assert set(_ACTION_CASES) == {action.action_id for action in list_remediation_actions()}
    assert set(_DERIVATION_CASES) == {action.action_id for action in list_remediation_actions()}
