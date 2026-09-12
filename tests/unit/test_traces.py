import json

import pytest

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.demo import seeded_failure_report
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.traces import __all__ as traces_exports
from airflow_dq_agent.traces import trace_agent_run
from airflow_dq_agent.traces import writer as traces_writer


def test_trace_appends_minimized_report_and_candidate_events(tmp_path) -> None:
    report = seeded_failure_report()
    agent_run = run_proposal_agent(report)
    trace = trace_agent_run(
        agent_run, report, evaluate_proposal(report, agent_run.proposal), directory=tmp_path
    )
    lines = (tmp_path / "agent-traces.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    events = [json.loads(line) for line in lines]
    assert [event["kind"] for event in events] == ["quality_report", "candidate_proposal"]
    assert events[-1]["event_id"] == trace.event_id


def test_public_traces_package_does_not_record_a_human_decision() -> None:
    assert "append_human_decision" not in traces_exports
    assert not hasattr(traces_writer, "append_human_decision")
    with pytest.raises(ImportError):
        from airflow_dq_agent.traces import append_human_decision  # noqa: F401
