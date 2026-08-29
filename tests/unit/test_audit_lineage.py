import json

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.traces import candidate_proposal_event, quality_report_event


def test_lineage_events_link_to_the_quality_run_without_durable_prompt_or_samples() -> None:
    report = seeded_failure_report()
    agent_run = run_proposal_agent(report)

    report_event = quality_report_event(report)
    candidate_event = candidate_proposal_event(report, agent_run.proposal, report_event)
    body = json.dumps(candidate_event.model_dump(mode="json"))

    assert report_event.quality_run_id == report.run_id
    assert candidate_event.predecessor_ids == [report_event.event_id]
    assert candidate_event.candidate_fingerprint
    assert agent_run.prompt not in body
    assert "sample_failures" not in body
    assert "root_cause_hypothesis" not in body
