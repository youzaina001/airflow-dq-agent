"""Optional synthetic warehouse used by the CLI demo, DAG, and tests."""

from __future__ import annotations

from pathlib import Path

from airflow_dq_agent.demo.fixtures import green_report, seeded_failure_report
from airflow_dq_agent.demo.seed import seed_warehouse
from airflow_dq_agent.load import load_registry

_REGISTRY = Path(__file__).with_name("registry.yaml")
_loaded = False


def register_demo() -> None:
    """Load the synthetic table contracts and check specs once per process."""
    global _loaded
    if _loaded:
        return
    load_registry(_REGISTRY)
    _loaded = True


__all__ = ["green_report", "register_demo", "seed_warehouse", "seeded_failure_report"]
