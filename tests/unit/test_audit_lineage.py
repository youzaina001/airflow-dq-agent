import json

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import ExecutablePlanItem, TargetSet
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.traces import candidate_proposal_event, quality_report_event
from airflow_dq_agent.traces.lineage import plan_event


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:lineage-v1")


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


def test_plan_event_retains_minimized_target_set_summary() -> None:
    report = seeded_failure_report()
    agent_run = run_proposal_agent(report)
    report_event = quality_report_event(report)
    candidate_event = candidate_proposal_event(report, agent_run.proposal, report_event)
    plan = compile_remediation_plan(report, agent_run.proposal, target_sets=_TargetSets())

    event = plan_event(plan, candidate_event)

    executable = [item for item in plan.items if isinstance(item, ExecutablePlanItem)]
    assert event.target_count == sum(item.target_set.count for item in executable)
    assert event.target_set_fingerprint == canonical_fingerprint(
        [item.target_set.fingerprint for item in executable]
    )
