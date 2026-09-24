from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_HOOK = Path(__file__).resolve().parents[2] / ".githooks" / "commit-msg"


def _run_hook(tmp_path: Path, message: str) -> subprocess.CompletedProcess[str]:
    path = tmp_path / "COMMIT_EDITMSG"
    path.write_text(message, encoding="utf-8")
    return subprocess.run(
        [str(_HOOK), str(path)],
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    "message",
    [
        "feat(hitl): add validated Human Decision recorder (#53)\n",
        "feat: add OpenAI integration\n",
        "fix: support Claude and Gemini models\n",
    ],
)
def test_commit_msg_hook_accepts_a_single_conventional_line(tmp_path: Path, message: str) -> None:
    result = _run_hook(tmp_path, message)

    assert result.returncode == 0, result.stderr


def test_commit_msg_hook_rejects_a_body_or_issue_trailer(tmp_path: Path) -> None:
    result = _run_hook(tmp_path, "feat(hitl): add recorder\n\nCloses #53\n")

    assert result.returncode != 0
    assert "one line" in result.stderr.lower()


@pytest.mark.parametrize(
    "message",
    [
        "feat: add recorder Co-Authored-By: Claude Code <noreply@anthropic.com>\n",
        "Generated with Claude Code\n",
        "fix: copilot suggested null check\n",
    ],
)
def test_commit_msg_hook_rejects_ai_agent_attribution(tmp_path: Path, message: str) -> None:
    result = _run_hook(tmp_path, message)

    assert result.returncode != 0
    assert "attribution" in result.stderr.lower()
