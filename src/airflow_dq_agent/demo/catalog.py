"""Bundled demo catalog MCP: register the synthetic warehouse, then serve it."""

from __future__ import annotations

from airflow_dq_agent.demo import register_demo


def prepare_demo_catalog() -> None:
    """Load the synthetic table contracts and check specs for the bundled catalog."""
    register_demo()


def main() -> None:
    prepare_demo_catalog()
    from airflow_dq_agent.catalog.mcp_server import main as run_catalog_server

    run_catalog_server()


if __name__ == "__main__":
    main()
