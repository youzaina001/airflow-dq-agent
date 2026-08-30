"""Metadata shared by allow-listed governed remediation actions."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from airflow_dq_agent.contracts.models import DestructiveRank
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS, get_table_contract


class RemediationAction(BaseModel):
    action_id: str
    description: str
    mutates: bool
    destructive_rank: DestructiveRank
    reversible: bool
    required_params: list[str]
    optional_params: list[str] = Field(default_factory=list)
    allowed_tables: frozenset[str]
    preview_sql: str
    notes: str = ""


def validate_common_params(
    action: RemediationAction, table: str, params: dict[str, Any]
) -> list[str]:
    """Validate metadata shared by every governed remediation action."""
    errors: list[str] = []
    table_key = table.split(".")[-1]
    if table_key not in action.allowed_tables:
        errors.append(f"{action.action_id} is not allowed on {table_key}")
        return errors
    try:
        contract = get_table_contract(table_key)
    except KeyError as exc:
        errors.append(str(exc))
        return errors
    for req in action.required_params:
        if req not in params:
            errors.append(f"missing required param {req!r}")
    allowed_params = set(action.required_params) | set(action.optional_params)
    for key in sorted(set(params) - allowed_params):
        errors.append(f"unexpected param {key!r}")
    identifier_params = {
        "column",
        "pk_column",
        "fk_column",
        "ref_column",
        "business_key",
        "ref_table",
    }
    for key, value in params.items():
        if key not in identifier_params:
            continue
        if key == "business_key" and isinstance(value, list):
            if not value:
                errors.append("business_key must be a non-empty list of identifiers")
            for item in value:
                if not isinstance(item, str):
                    errors.append("business_key must contain only string identifiers")
                elif not contract.has_column(item):
                    errors.append(f"{item!r} is not a column on {contract.table}")
            continue
        if not isinstance(value, str):
            errors.append(f"{key} must be a string identifier")
            continue
        if key == "ref_table":
            if value.split(".")[-1] not in TABLE_CONTRACTS:
                errors.append(f"ref_table {value!r} is not a contracted table")
            continue
        if not contract.has_column(value) and key != "ref_column":
            errors.append(f"{value!r} is not a column on {contract.table}")
        if key == "ref_column":
            ref = str(params.get("ref_table", ""))
            if ref:
                try:
                    ref_contract = get_table_contract(ref)
                    if not ref_contract.has_column(value):
                        errors.append(f"{value!r} is not a column on {ref_contract.table}")
                except KeyError:
                    pass
    return errors
