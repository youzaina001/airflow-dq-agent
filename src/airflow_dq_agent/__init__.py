"""Eval-driven data-quality agent: detect → explain → propose → evaluate → (optional) apply."""

from airflow_dq_agent.contracts.tables import register_contract
from airflow_dq_agent.load import load_registry
from airflow_dq_agent.quality.registry import register_check

__version__ = "0.1.0"

__all__ = ["load_registry", "register_check", "register_contract"]
