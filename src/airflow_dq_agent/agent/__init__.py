"""Proposal generation behind a small, structured interface."""

from airflow_dq_agent.agent.runner import AgentRun, build_read_only_toolset, run_proposal_agent

__all__ = ["AgentRun", "build_read_only_toolset", "run_proposal_agent"]
