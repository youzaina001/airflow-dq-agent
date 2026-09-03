"""Proposal generation behind a small, structured interface."""

from airflow_dq_agent.agent.runner import AgentRun, build_read_only_toolset, run_proposal_agent
from airflow_dq_agent.agent.sanitize import safe_proposal_for_xcom

__all__ = [
    "AgentRun",
    "build_read_only_toolset",
    "run_proposal_agent",
    "safe_proposal_for_xcom",
]
