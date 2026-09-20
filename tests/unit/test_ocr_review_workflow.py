from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ocr-review.yml"

ALLOWED_MODELS = [
    "z-ai/glm-5.3-flash",
    "z-ai/glm-5.3-flashx",
    "google/gemini-3.8-flash",
    "qwen/qwen3.8-flash",
    "anthropic/claude-sonnet-5",
    "x-ai/grok-4.6",
    "openai/gpt-6-astra",
]


def _load_workflow() -> dict:
    # GitHub's unquoted `on:` is YAML 1.1 boolean true.
    text = WORKFLOW.read_text()
    text = text.replace("\non:", '\n"on":', 1)
    loaded = yaml.safe_load(text)
    assert isinstance(loaded, dict)
    return loaded


def test_ocr_review_workflow_is_manual_with_model_choice() -> None:
    workflow = _load_workflow()
    triggers = workflow["on"]
    assert "pull_request" not in triggers
    assert "pull_request_target" not in triggers
    assert "push" not in triggers

    dispatch = triggers["workflow_dispatch"]["inputs"]
    assert dispatch["pr_number"]["required"] is True
    model = dispatch["model"]
    assert model["type"] == "choice"
    assert model["default"] == "z-ai/glm-5.3-flash"
    assert model["options"] == ALLOWED_MODELS

    assert "issue_comment" in triggers
    job_if = workflow["jobs"]["code-review"]["if"]
    assert "@ocr" in job_if
    assert "workflow_dispatch" in job_if
