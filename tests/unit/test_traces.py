import json

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.traces import trace_agent_run


def test_trace_appends_one_jsonl_record(tmp_path) -> None:
    report = seeded_failure_report()
    agent_run = run_proposal_agent(report)
    trace = trace_agent_run(
        agent_run, report, evaluate_proposal(report, agent_run.proposal), directory=tmp_path
    )
    lines = (tmp_path / "agent-traces.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["trace_id"] == trace.trace_id
