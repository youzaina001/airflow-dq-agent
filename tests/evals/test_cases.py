from pathlib import Path

import pytest
import yaml

from airflow_dq_agent.agent import run_proposal_agent
from airflow_dq_agent.cli import _drop_table_proposal, _spurious_green_proposal
from airflow_dq_agent.evals import evaluate_proposal
from airflow_dq_agent.quality.fixtures import green_report, seeded_failure_report

CASES = sorted((Path(__file__).parents[2] / "evals/cases").glob("*.yaml"))


def _load_case(path: Path) -> dict[str, object]:
    result = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(result, dict)
    return result


@pytest.mark.eval
@pytest.mark.parametrize("case_path", CASES, ids=lambda path: path.stem)
def test_yaml_eval_cases(case_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    case = _load_case(case_path)
    report = green_report() if case["report"] == "green" else seeded_failure_report()
    kind = case["proposal"]
    if kind == "stub":
        monkeypatch.setenv("LLM_MODE", "stub")
        proposal = run_proposal_agent(report).proposal
    elif kind == "replay":
        replay = Path(__file__).parents[2] / "evals/fixtures/traces/replay-proposal.json"
        monkeypatch.setenv("LLM_MODE", "replay")
        monkeypatch.setenv("REPLAY_TRACE_PATH", str(replay))
        proposal = run_proposal_agent(report).proposal
    elif kind == "drop_table":
        proposal = _drop_table_proposal(report)
    else:
        proposal = _spurious_green_proposal()
    evaluation = evaluate_proposal(report, proposal)
    assert evaluation.passed is case["expected_pass"]
    for name in case.get("expected_failed_scores", []):
        assert evaluation.get(name).passed is False  # type: ignore[union-attr]
