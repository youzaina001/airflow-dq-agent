"""Eval-driven data-quality agent: detect → explain → propose → evaluate → (optional) apply."""

from airflow_dq_agent.contracts.tables import register_contract
from airflow_dq_agent.quality.registry import register_check

__version__ = "0.1.0"

__all__ = ["register_check", "register_contract"]
