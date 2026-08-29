"""Read-only catalog interface shared by the agent and MCP adapter."""

from airflow_dq_agent.catalog.service import (
    get_check,
    get_lineage,
    get_remediation,
    get_table_contract,
    list_checks,
    list_remediations,
    list_tables,
)

__all__ = [
    "get_check",
    "get_lineage",
    "get_remediation",
    "get_table_contract",
    "list_checks",
    "list_remediations",
    "list_tables",
]
