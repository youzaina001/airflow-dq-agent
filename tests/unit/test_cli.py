from pathlib import Path

import pytest

from airflow_dq_agent.cli import build_parser, main
from airflow_dq_agent.contracts.models import CheckStatus, QualitySuiteReport
from airflow_dq_agent.demo import green_report, seeded_failure_report


def _error_report() -> QualitySuiteReport:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    errored = failed.model_copy(
        update={"status": CheckStatus.ERROR, "n_failed": 0, "sample_failures": []}
    )
    return QualitySuiteReport(checks=[errored], observed_columns={})


def test_suite_propose_eval_exits_distinguish_completed_and_incomplete(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    help_text = build_parser().format_help().lower()
    assert "exit 0" in help_text
    assert "exit 1" in help_text
    assert "exit 2" in help_text
    assert "incomplete" in help_text

    monkeypatch.setenv("LLM_MODE", "stub")

    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: green_report())
    assert main(["suite", "--no-db"]) == 0
    passed_out = capsys.readouterr().out.lower()
    assert "all checks passed" in passed_out
    assert "next:" in passed_out

    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: seeded_failure_report())
    assert main(["suite", "--no-db"]) == 1
    failed_out = capsys.readouterr().out.lower()
    assert "failed" in failed_out
    assert "next:" in failed_out

    empty = QualitySuiteReport(checks=[], observed_columns={})
    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: empty)
    assert main(["suite", "--no-db"]) == 2
    empty_out = capsys.readouterr().out.lower()
    assert "all checks passed" not in empty_out
    assert "0 passed" in empty_out
    assert "0 failed" in empty_out
    assert "0 error" in empty_out
    assert "next:" in empty_out

    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: _error_report())
    assert main(["propose", "--no-db"]) == 2
    propose_out = capsys.readouterr().out.lower()
    assert "all checks passed" not in propose_out
    assert "next:" in propose_out

    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: green_report())
    assert main(["propose", "--no-db"]) == 0
    capsys.readouterr()
    assert main(["eval", "--no-db"]) == 0
    capsys.readouterr()

    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: seeded_failure_report())
    assert main(["eval", "--no-db"]) == 1
    capsys.readouterr()
    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: empty)
    assert main(["eval", "--no-db"]) == 2
    eval_out = capsys.readouterr().out.lower()
    assert "all checks passed" not in eval_out

    mixed = seeded_failure_report()
    failed = mixed.get("fact_orders.total_amount.completeness")
    assert failed is not None
    mixed = mixed.model_copy(
        update={
            "checks": [
                failed,
                failed.model_copy(
                    update={"status": CheckStatus.ERROR, "n_failed": 0, "sample_failures": []}
                ),
            ]
        }
    )
    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: mixed)
    assert main(["suite", "--no-db"]) == 2
    mixed_out = capsys.readouterr().out.lower()
    assert "incomplete" in mixed_out
    assert "sample_failures" not in mixed_out
    assert "all checks passed" not in mixed_out

    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: seeded_failure_report())
    assert main(["suite", "--no-db"]) == 1
    suite_json = capsys.readouterr().out.lower()
    assert "sample_failures" not in suite_json

    def _boom(_no_db: bool) -> QualitySuiteReport:
        raise RuntimeError("warehouse unavailable")

    monkeypatch.setattr("airflow_dq_agent.cli._report", _boom)
    assert main(["suite", "--no-db"]) == 2
    boom_out = capsys.readouterr().out.lower()
    assert "next:" in boom_out
    assert "warehouse unavailable" not in boom_out
    assert "all checks passed" not in boom_out

    monkeypatch.setattr("airflow_dq_agent.cli._report", lambda _no_db: seeded_failure_report())
    assert main(["demo", "--no-db"]) == 0
    demo_out = capsys.readouterr().out.lower()
    assert "error" in demo_out
    assert "next:" in demo_out


@pytest.mark.parametrize("command", ["propose", "eval"])
def test_proposal_setup_error_returns_controlled_exit(
    command: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("LLM_MODE", "replay")
    monkeypatch.setenv("REPLAY_TRACE_PATH", str(tmp_path / "secret-source-missing.json"))

    assert main([command, "--no-db"]) == 2

    output = capsys.readouterr().out
    assert "setup or execution error" in output
    assert "secret-source" not in output
    assert "do not review a remediation plan" in output
