#!/usr/bin/env python3
"""Fail the slice-1 branch-coverage floor for a scoped coverage.py JSON report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Repo-relative paths. New modules under the scope directories count; pandera does not.
_SCOPE_DIRS = ("planning", "apply", "contracts", "quality", "traces", "evals")
_EXCLUDED = frozenset({"src/airflow_dq_agent/quality/pandera_schemas.py"})
_EXTRA_FILES = (
    "src/airflow_dq_agent/action_definitions.py",
    "src/airflow_dq_agent/check_policy.py",
    "src/airflow_dq_agent/hitl.py",
    "src/airflow_dq_agent/airflow_hitl.py",
    "src/airflow_dq_agent/agent/sanitize.py",
)


class BranchCoverageError(Exception):
    """Scoped branch coverage is missing, unreadable, or below the floor."""


def expected_source_paths(repo_root: Path) -> set[str]:
    """Return the repo-relative Python files that the branch floor measures."""
    package = repo_root / "src" / "airflow_dq_agent"
    found: set[str] = set()
    for directory in _SCOPE_DIRS:
        root = package / directory
        if not root.is_dir():
            raise BranchCoverageError(f"missing scope directory: {directory}")
        for path in root.rglob("*.py"):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            relative = path.relative_to(repo_root).as_posix()
            if relative not in _EXCLUDED:
                found.add(relative)
    for relative in _EXTRA_FILES:
        if not (repo_root / relative).is_file():
            raise BranchCoverageError(f"missing scope file: {relative}")
        found.add(relative)
    return found


def load_report(path: Path) -> dict:
    """Read a coverage.py JSON report, or fail if it is missing or unreadable."""
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BranchCoverageError(f"coverage JSON is missing or unreadable: {exc}") from exc
    if not isinstance(loaded, dict):
        raise BranchCoverageError("coverage JSON is missing or unreadable: expected an object")
    return loaded


def evaluate_branch_floor(
    report: dict,
    expected_paths: set[str],
    floor: float = 0.75,
) -> tuple[int, int]:
    """Return ``(covered_branches, num_branches)`` for the expected files.

    The ratio is ``sum(covered_branches) / sum(num_branches)`` for that scope.
    ``percent_covered`` is not consulted. A file with zero branches still has to
    be present. Missing files, an unreadable report, a zero denominator, or a
    ratio below ``floor`` fail.
    """
    files = report.get("files")
    if not isinstance(files, dict):
        raise BranchCoverageError("coverage JSON has no files object")

    expected = {_normalize(path) for path in expected_paths}
    matched: dict[str, tuple[int, int]] = {}
    for key, payload in files.items():
        if not isinstance(key, str):
            raise BranchCoverageError("unreadable coverage file path")
        path = _match_expected(key, expected)
        if path is None:
            continue
        if path in matched:
            raise BranchCoverageError(f"duplicate coverage entry for {path}")
        if not isinstance(payload, dict):
            raise BranchCoverageError(f"unreadable coverage summary for {path}")
        matched[path] = _branch_counts(path, payload.get("summary"))

    missing = expected - matched.keys()
    if missing:
        listed = ", ".join(sorted(missing))
        raise BranchCoverageError(f"missing expected source coverage: {listed}")

    covered = sum(counts[0] for counts in matched.values())
    total = sum(counts[1] for counts in matched.values())
    if total == 0:
        raise BranchCoverageError("num_branches summed to 0")
    # coverage.py percent_covered mixes statements and branches; the floor is branch-only.
    ratio = covered / total
    if ratio < floor:
        raise BranchCoverageError(
            f"branch coverage {covered}/{total} = {ratio:.8f} is below {floor}"
        )
    return covered, total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="coverage.py JSON report")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="repository root used to discover the coverage scope",
    )
    parser.add_argument("--floor", type=float, default=0.75)
    args = parser.parse_args(argv)
    try:
        report = load_report(args.report)
        expected = expected_source_paths(args.repo_root)
        covered, total = evaluate_branch_floor(report, expected, args.floor)
    except BranchCoverageError as exc:
        print(f"branch coverage check failed: {exc}", file=sys.stderr)
        return 1
    ratio = covered / total
    print(f"covered_branches/num_branches {covered}/{total}")
    print(f"ratio {ratio:.8f}")
    return 0


def _normalize(path: str) -> str:
    key = path.replace("\\", "/")
    while key.startswith("./"):
        key = key[2:]
    return key


def _match_expected(report_key: str, expected_paths: set[str]) -> str | None:
    """Map a coverage JSON path onto one repo-relative expected path."""
    key = _normalize(report_key)
    if key in expected_paths:
        return key
    # Absolute coverage paths still end with the repo-relative source path.
    prefixed = [path for path in expected_paths if key.endswith("/" + path)]
    if prefixed:
        return _single_expected(report_key, prefixed)
    # Some reports omit the src/ layout prefix.
    nested = [path for path in expected_paths if path.endswith("/" + key)]
    if nested:
        return _single_expected(report_key, nested)
    return None


def _single_expected(report_key: str, matches: list[str]) -> str:
    longest = max(matches, key=len)
    ambiguous = [
        match for match in matches if match != longest and not longest.endswith("/" + match)
    ]
    if ambiguous:
        raise BranchCoverageError(f"ambiguous coverage path: {report_key}")
    return longest


def _branch_counts(path: str, summary: object) -> tuple[int, int]:
    if not isinstance(summary, dict):
        raise BranchCoverageError(f"unreadable coverage summary for {path}")
    try:
        covered = summary["covered_branches"]
        total = summary["num_branches"]
    except KeyError as exc:
        raise BranchCoverageError(f"unreadable coverage summary for {path}") from exc
    if not _is_count(covered) or not _is_count(total) or covered > total:
        raise BranchCoverageError(f"unreadable branch counts for {path}")
    return covered, total


def _is_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


if __name__ == "__main__":
    sys.exit(main())
