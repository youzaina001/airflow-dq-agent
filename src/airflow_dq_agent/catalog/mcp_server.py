"""FastMCP adapter for the transport-free catalog module."""

from __future__ import annotations

from typing import Any

from airflow_dq_agent.catalog import service
from airflow_dq_agent.config import get_settings


def build_server() -> Any:
    """Build an MCP server without making catalog behavior depend on FastMCP."""
    from fastmcp import FastMCP

    mcp = FastMCP("airflow-dq-agent-catalog")
    mcp.tool()(service.list_tables)
    mcp.tool()(service.get_table_contract)
    mcp.tool()(service.list_checks)
    mcp.tool()(service.get_check)
    mcp.tool()(service.get_lineage)
    mcp.tool()(service.list_remediations)
    mcp.tool()(service.get_remediation)
    return mcp


def main() -> None:
    settings = get_settings()
    server = build_server()
    server.run(
        transport="http",
        host=settings.catalog_mcp_host,
        port=settings.catalog_mcp_port,
    )


if __name__ == "__main__":
    main()
