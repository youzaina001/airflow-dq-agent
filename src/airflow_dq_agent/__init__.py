"""Eval-driven data-quality agent: detect → explain → propose → evaluate → (optional) apply."""

from airflow_dq_agent.contracts.tables import ColumnContract, TableContract, register_contract
from airflow_dq_agent.load import load_registry
from airflow_dq_agent.quality.registry import CheckPolicy, CheckSpec, register_check

__version__ = "0.2.0"

__all__ = [
    "CheckPolicy",
    "CheckSpec",
    "ColumnContract",
    "TableContract",
    "load_registry",
    "register_check",
    "register_contract",
]
