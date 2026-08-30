"""A small, deterministic catalog interface.

This module is deliberately transport-free. FastMCP and the live agent use the
same functions, so tests can cross the same seam without starting a server.
"""

from __future__ import annotations

from typing import Any, cast

from airflow_dq_agent.action_definitions import get_governed_action, list_remediation_actions
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.contracts.tables import get_table_contract as contract_for
from airflow_dq_agent.lineage.graph import get_lineage as lineage_for
from airflow_dq_agent.quality.registry import CHECK_SPECS, get_check_spec


def _dump(value: Any) -> dict[str, Any]:
    """Serialize Pydantic catalog records into JSON-safe tool results."""
    return cast(dict[str, Any], value.model_dump(mode="json"))


def list_tables() -> list[dict[str, Any]]:
    """Return every contracted table, including its columns and grain."""
    return [_dump(contract) for contract in TABLE_CONTRACTS.values()]


def get_table_contract(table: str) -> dict[str, Any]:
    """Return the contract for one table or raise KeyError."""
    return _dump(contract_for(table))


def list_checks() -> list[dict[str, Any]]:
    """Return the allow-listed checks; callers never supply SQL."""
    return [_dump(spec) for spec in CHECK_SPECS.values()]


def get_check(check_id: str) -> dict[str, Any]:
    """Return one check definition, including its controlled sample query."""
    return _dump(get_check_spec(check_id))


def get_lineage(table: str) -> dict[str, list[str]]:
    """Return static upstream/downstream neighbors for a contracted table."""
    contract_for(table)
    return lineage_for(table)


def list_remediations() -> list[dict[str, Any]]:
    """Return allow-listed remediation metadata only."""
    return [_dump(action) for action in list_remediation_actions()]


def get_remediation(action_id: str) -> dict[str, Any]:
    """Return the metadata for one allow-listed action."""
    return _dump(get_governed_action(action_id).metadata)
