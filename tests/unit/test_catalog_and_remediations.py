from airflow_dq_agent.catalog import (
    get_check,
    get_lineage,
    get_remediation,
    get_table_contract,
    list_tables,
)
from airflow_dq_agent.contracts.remediations import validate_step_params


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
