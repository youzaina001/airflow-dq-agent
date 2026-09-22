"""The public traces seam must not expose unvalidated decision persistence."""

import importlib

import pytest


def test_traces_package_does_not_export_append_human_decision() -> None:
    traces = importlib.import_module("airflow_dq_agent.traces")
    assert not hasattr(traces, "append_human_decision")


def test_writer_all_does_not_advertise_append_human_decision() -> None:
    writer = importlib.import_module("airflow_dq_agent.traces.writer")
    assert "append_human_decision" not in writer.__all__


def test_importing_append_human_decision_from_traces_fails() -> None:
    traces = importlib.import_module("airflow_dq_agent.traces")
    with pytest.raises((ImportError, AttributeError)):
        _ = traces.append_human_decision
