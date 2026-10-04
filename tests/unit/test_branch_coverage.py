"""The slice-1 gate is scoped branch coverage, not coverage.py's combined percent."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check_branch_coverage.py"
_SPEC = importlib.util.spec_from_file_location("check_branch_coverage", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_CHECKER = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_CHECKER)

BranchCoverageError = _CHECKER.BranchCoverageError
evaluate_branch_floor = _CHECKER.evaluate_branch_floor
expected_source_paths = _CHECKER.expected_source_paths
main = _CHECKER.main

_SCOPE_DIRS = ("planning", "apply", "contracts", "quality", "traces", "evals")


def _files(counts: dict[str, tuple[int, int]]) -> dict[str, object]:
    return {
        "files": {
            path: {
                "summary": {
                    "covered_branches": covered,
                    "num_branches": total,
                    "percent_covered": 100.0,
                    "percent_branches_covered": 100.0,
                }
            }
            for path, (covered, total) in counts.items()
        }
    }


def _write_scope(repo_root: Path) -> None:
    package = repo_root / "src" / "airflow_dq_agent"
    for directory in _SCOPE_DIRS:
        root = package / directory
        root.mkdir(parents=True)
        (root / "__init__.py").write_text("", encoding="utf-8")
        (root / "added.py").write_text("x = 1\n", encoding="utf-8")
    (package / "quality" / "pandera_schemas.py").write_text("x = 1\n", encoding="utf-8")
    (package / "cli.py").write_text("x = 1\n", encoding="utf-8")
    (package / "agent").mkdir()
    (package / "agent" / "sanitize.py").write_text("x = 1\n", encoding="utf-8")
    (package / "agent" / "runner.py").write_text("x = 1\n", encoding="utf-8")
    for name in ("action_definitions.py", "check_policy.py", "hitl.py", "airflow_hitl.py"):
        (package / name).write_text("x = 1\n", encoding="utf-8")


def test_below_floor_fails_even_when_percent_covered_is_high() -> None:
    report = _files({"src/airflow_dq_agent/planning/a.py": (1, 4)})

    with pytest.raises(BranchCoverageError, match="below"):
        evaluate_branch_floor(report, {"src/airflow_dq_agent/planning/a.py"})


def test_missing_expected_path_fails() -> None:
    report = _files({"src/airflow_dq_agent/planning/a.py": (3, 4)})

    with pytest.raises(BranchCoverageError, match="missing expected source coverage"):
        evaluate_branch_floor(
            report,
            {
                "src/airflow_dq_agent/planning/a.py",
                "src/airflow_dq_agent/planning/b.py",
            },
        )


def test_zero_num_branches_fails() -> None:
    report = _files({"src/airflow_dq_agent/planning/__init__.py": (0, 0)})

    with pytest.raises(BranchCoverageError, match="num_branches summed to 0"):
        evaluate_branch_floor(report, {"src/airflow_dq_agent/planning/__init__.py"})


def test_at_or_above_floor_returns_numerator_and_denominator() -> None:
    report = _files(
        {
            "src/airflow_dq_agent/planning/a.py": (2, 2),
            "src/airflow_dq_agent/apply/executor.py": (1, 2),
            "src/airflow_dq_agent/planning/__init__.py": (0, 0),
        }
    )

    assert evaluate_branch_floor(
        report,
        {
            "src/airflow_dq_agent/planning/a.py",
            "src/airflow_dq_agent/apply/executor.py",
            "src/airflow_dq_agent/planning/__init__.py",
        },
    ) == (3, 4)


def test_files_outside_the_expected_set_do_not_count() -> None:
    report = _files(
        {
            "src/airflow_dq_agent/planning/a.py": (3, 4),
            "src/airflow_dq_agent/cli.py": (0, 100),
        }
    )

    assert evaluate_branch_floor(report, {"src/airflow_dq_agent/planning/a.py"}) == (3, 4)


def test_absolute_report_paths_match_repo_relative_files() -> None:
    report = _files({"/tmp/repo/src/airflow_dq_agent/planning/a.py": (8, 10)})

    assert evaluate_branch_floor(report, {"src/airflow_dq_agent/planning/a.py"}) == (8, 10)


def test_report_without_branch_counts_is_unreadable() -> None:
    report = {"files": {"a.py": {"summary": {"percent_covered": 100.0}}}}

    with pytest.raises(BranchCoverageError, match="unreadable"):
        evaluate_branch_floor(report, {"a.py"})


def test_expected_paths_include_new_files_and_exclude_only_pandera(tmp_path: Path) -> None:
    _write_scope(tmp_path)

    paths = expected_source_paths(tmp_path)

    for directory in _SCOPE_DIRS:
        assert f"src/airflow_dq_agent/{directory}/added.py" in paths
        assert f"src/airflow_dq_agent/{directory}/__init__.py" in paths
    assert "src/airflow_dq_agent/quality/pandera_schemas.py" not in paths
    assert "src/airflow_dq_agent/cli.py" not in paths
    assert "src/airflow_dq_agent/agent/runner.py" not in paths
    assert "src/airflow_dq_agent/agent/sanitize.py" in paths
    for name in ("action_definitions.py", "check_policy.py", "hitl.py", "airflow_hitl.py"):
        assert f"src/airflow_dq_agent/{name}" in paths


def test_missing_report_exits_nonzero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main([str(tmp_path / "missing.json")])

    assert code == 1
    assert "missing or unreadable" in capsys.readouterr().err


def test_unreadable_report_exits_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    report = tmp_path / "coverage.json"
    report.write_text("{", encoding="utf-8")

    assert main([str(report)]) == 1
    assert "missing or unreadable" in capsys.readouterr().err


def test_main_prints_branch_numerator_and_denominator(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_scope(tmp_path)
    paths = expected_source_paths(tmp_path)
    report = _files({path: (3, 4) for path in paths})
    covered, total = evaluate_branch_floor(report, paths)
    report_path = tmp_path / "coverage.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    assert main([str(report_path), "--repo-root", str(tmp_path)]) == 0
    assert f"covered_branches/num_branches {covered}/{total}" in capsys.readouterr().out
