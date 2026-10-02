"""The Compose proof must run both callers and refuse missing database evidence."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("database_evidence", ["t", "f"])
def test_shadow_proof_requires_database_evidence(tmp_path: Path, database_evidence: str) -> None:
    script = tmp_path / "scripts/compose-shadow.sh"
    script.parent.mkdir()
    shutil.copyfile(REPO / "scripts/compose-shadow.sh", script)
    commands = tmp_path / "commands.jsonl"
    docker = tmp_path / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "args = sys.argv[1:]\n"
        "with Path(os.environ['PROOF_COMMANDS']).open('a') as log:\n"
        "    log.write(json.dumps(args) + '\\n')\n"
        "if 'run' in args:\n"
        "    sys.stdin.read()\n"
        "    print('READ_DSN=postgresql+psycopg://reader:x@warehouse/proof')\n"
        "    print('AUDIT_DSN=postgresql+psycopg://auditor:x@warehouse/proof')\n"
        "elif 'psql' in args and 'SELECT' in args[-1]:\n"
        "    print(os.environ['PROOF_EVIDENCE'])\n"
        "elif 'airflow' in args and 'list' in args:\n"
        "    print('dq_shadow')\n"
        "elif 'airflow_dq_agent.cli' in args:\n"
        "    print('Shadow Review: billing.invoice; Exact target count: 1')\n"
        "    sys.exit(1)\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    completed = subprocess.run(
        ["bash", str(script)],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "PROOF_COMMANDS": str(commands),
            "PROOF_EVIDENCE": database_evidence,
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert completed.returncode == (0 if database_evidence == "t" else 1), completed.stderr
    calls = [json.loads(line) for line in commands.read_text().splitlines()]
    assert any("airflow_dq_agent.cli" in call and "shadow" in call for call in calls)
    assert any(call[-5:-1] == ["airflow", "dags", "test", "dq_shadow"] for call in calls)
    assert ("shadow proof passed" in completed.stdout) == (database_evidence == "t")
    assert not list((tmp_path / "config").glob("shadow-registry.*"))
