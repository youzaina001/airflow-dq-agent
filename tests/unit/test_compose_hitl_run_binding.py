"""Exercise the shipped proof helpers with interleaved DAG-run evidence."""

import json
import os
import re
import sqlite3
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/compose-hitl-reject-timeout.sh"
OWN_RUN = "1" * 32
OTHER_RUN = "2" * 32


def _proof_helper(
    tmp_path: Path, expression: str, *, value: object
) -> subprocess.CompletedProcess[str]:
    database = tmp_path / "traces.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE traces (seq INTEGER, kind TEXT, body TEXT)")
        connection.executemany(
            "INSERT INTO traces VALUES (?, ?, ?)",
            [
                (1, "quality_report", json.dumps({"quality_run_id": OWN_RUN})),
                (2, "quality_report", json.dumps({"quality_run_id": OTHER_RUN})),
                (3, "human_timed_out", json.dumps({"quality_run_id": OTHER_RUN})),
            ],
        )
    # Load function definitions without starting the Compose stack.
    functions = "\n".join(
        function
        for function in re.findall(r"^[a-z_]+\(\) \{\n.*?^\}", SCRIPT.read_text(), re.M | re.S)
        if function.split("()", 1)[0]
        in {"quality_trace_found", "quality_run_id_for", "timeout_trace_found"}
    )
    assert "quality_run_id_for()" in functions and "timeout_trace_found()" in functions
    harness = (
        functions
        + """
API=http://airflow.invalid
DAG_ID=dq_external_invoice
auth_header=unused-fixture
before_timeout_seq=0
timeout_qid=$CI_TEST_OWN_RUN
poll_until() { shift 3; "$@"; }
api_curl() {
  python3 -c 'import os,sys; assert "/dagRuns/reject-proof/" in sys.argv[-1]; print(os.environ["CI_TEST_RESPONSE"])' "$@"
}
psql_wh() {
  python3 -c 'import os,sqlite3,sys; db=sqlite3.connect(":memory:"); db.execute("ATTACH DATABASE ? AS dq", (os.environ["CI_TEST_DATABASE"],)); row=db.execute(sys.argv[1]).fetchone(); print(row[0] if row else "")' "$1"
}
"""
    )
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", harness + expression],
        env=os.environ
        | {
            "CI_TEST_DATABASE": str(database),
            "CI_TEST_OWN_RUN": OWN_RUN,
            "CI_TEST_RESPONSE": json.dumps({"value": value}),
        },
        capture_output=True,
        text=True,
        timeout=5,
    )


@pytest.mark.parametrize("encoded", [False, True])
def test_quality_identity_belongs_to_triggered_run_despite_newer_report(
    tmp_path: Path, encoded: bool
) -> None:
    report = {"run_id": OWN_RUN}
    result = _proof_helper(
        tmp_path,
        'quality_run_id_for "reject-proof"',
        value=json.dumps(report) if encoded else report,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == OWN_RUN


def test_another_dag_run_timeout_cannot_satisfy_this_proof(tmp_path: Path) -> None:
    result = _proof_helper(tmp_path, "timeout_trace_found", value={"run_id": OWN_RUN})

    assert result.returncode != 0, "an unrelated timeout satisfied the triggered run's proof"


@pytest.mark.parametrize("value", [{}, {"run_id": ""}, {"run_id": None}, {"run_id": "unsafe'"}])
def test_missing_or_invalid_triggered_report_never_falls_back_to_latest_trace(
    tmp_path: Path, value: object
) -> None:
    result = _proof_helper(tmp_path, 'quality_run_id_for "reject-proof"', value=value)

    assert result.returncode != 0
