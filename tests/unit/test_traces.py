import json

import pytest

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.cli import command_demo
from airflow_dq_agent.demo import seeded_failure_report
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.traces import __all__ as traces_exports
from airflow_dq_agent.traces import candidate_proposal_event, quality_report_event, trace_agent_run
from airflow_dq_agent.traces import writer as traces_writer

# Seeded dim_customer.email.validity sample. A live proposer can echo it into
# Candidate Proposal identifiers; durable audit must not persist it.
SAMPLED_VALUE = "c101.invalid"


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


def test_cli_demo_trace_does_not_persist_sampled_proposal_identifiers(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = seeded_failure_report()
    agent_run = run_proposal_agent(report)
    poisoned = agent_run.proposal.model_copy(
        update={"proposal_id": SAMPLED_VALUE, "fingerprint": SAMPLED_VALUE}
    )
    poisoned_run = agent_run.model_copy(update={"proposal": poisoned})

    monkeypatch.setenv("TRACES_DIR", str(tmp_path))
    monkeypatch.setenv("APPLY_MODE", "off")
    monkeypatch.setenv("TRACE_POSTGRES", "false")
    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: report)
    monkeypatch.setattr("airflow_dq_agent.cli.run_proposal_agent", lambda _report: poisoned_run)

    command_demo(no_db=True)

    lines = (tmp_path / "agent-traces.jsonl").read_text(encoding="utf-8").splitlines()
    body = "\n".join(lines)
    assert SAMPLED_VALUE not in body
    events = [json.loads(line) for line in lines]
    candidate = next(event for event in events if event["kind"] == "candidate_proposal")
    assert candidate["proposal_id"] != SAMPLED_VALUE
    assert candidate["candidate_fingerprint"] != SAMPLED_VALUE
    assert candidate["candidate_fingerprint"].startswith("sha256:")


def test_candidate_proposal_event_ignores_proposer_supplied_fingerprint() -> None:
    report = seeded_failure_report()
    proposal = run_proposal_agent(report).proposal
    predecessor = quality_report_event(report)
    trusted = candidate_proposal_event(
        report, proposal.model_copy(update={"fingerprint": None}), predecessor
    )
    echoed = candidate_proposal_event(
        report, proposal.model_copy(update={"fingerprint": SAMPLED_VALUE}), predecessor
    )

    assert SAMPLED_VALUE not in json.dumps(echoed.model_dump(mode="json"))
    assert echoed.candidate_fingerprint == trusted.candidate_fingerprint
    assert echoed.candidate_fingerprint.startswith("sha256:")
