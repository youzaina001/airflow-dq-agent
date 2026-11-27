from __future__ import annotations

import re
import subprocess
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
_EXPECTED_LINT_DIRS = ("src", "tests", "dags", "examples", "evals", "scripts")
_FORBIDDEN_PUBLIC_PATTERNS = re.compile(
    r"(^|/)AGENTS\.md$|^docs/agents/|(^|/)pr[0-9]+-(handoff|review)\.md$|(^|/)review-follow-up\.md$|(^|/).*-handoff\.md$"
)


def test_ruff_selects_ruf100_and_traces_noqa_is_required() -> None:
    pyproject = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    lint_cfg = pyproject["tool"]["ruff"]["lint"]
    select = lint_cfg["select"]
    ignore = lint_cfg.get("ignore", [])
    per_file_ignores = lint_cfg.get("per-file-ignores", {})
    assert "RUF" in select or "RUF100" in select
    assert "RUF100" not in ignore
    assert not any("RUF100" in rules for rules in per_file_ignores.values())

    traces_test = (REPO / "tests/unit/test_traces.py").read_text(encoding="utf-8")
    assert re.search(
        r"from\s+airflow_dq_agent\.traces\s+import\s+append_human_decision\s+#\s*noqa:\s*F401\b",
        traces_test,
    )


def test_makefile_and_ci_cover_all_python_and_eval_trees() -> None:
    makefile = (REPO / "Makefile").read_text(encoding="utf-8")
    for target in ("lint", "fmt"):
        match = re.search(rf"(?ms)^{target}:.*?(?=^[a-zA-Z0-9_.-]+:|\Z)", makefile)
        assert match is not None, f"Missing Makefile target: {target}"
        ruff_lines = [line.strip() for line in match.group(0).splitlines() if "ruff " in line]
        assert len(ruff_lines) == 2, f"Expected check and format lines in {target}"
        for line in ruff_lines:
            tokens = line.split()
            for directory in _EXPECTED_LINT_DIRS:
                assert directory in tokens, f"{target} line {line!r} missing {directory}"

    ci_yml = (REPO / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    ci_ruff_lines = [
        line.strip()
        for line in ci_yml.splitlines()
        if line.strip().startswith(("ruff check ", "ruff format "))
    ]
    assert len(ci_ruff_lines) == 2
    for line in ci_ruff_lines:
        tokens = line.split()
        for directory in _EXPECTED_LINT_DIRS:
            assert directory in tokens, f"ci.yml line {line!r} missing {directory}"


def test_no_internal_agent_or_handoff_files_tracked() -> None:
    tracked = subprocess.run(
        ["git", "-C", str(REPO), "ls-files"],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.splitlines()
    forbidden = [path for path in tracked if _FORBIDDEN_PUBLIC_PATTERNS.search(path)]
    assert forbidden == [], f"Forbidden internal agent/handoff files tracked: {forbidden}"
