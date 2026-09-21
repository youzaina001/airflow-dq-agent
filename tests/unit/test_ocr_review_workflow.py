from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ocr-review.yml"

ALLOWED_MODELS = [
    "z-ai/glm-5.3-flash",
    "z-ai/glm-5.3-flashx",
    "deepseek/deepseek-v4.1-flash",
]

ALLOWED_REASONING_EFFORT = ["low", "high", "max"]


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

    effort = dispatch["reasoning_effort"]
    assert effort["type"] == "choice"
    assert effort["default"] == "low"
    assert effort["options"] == ALLOWED_REASONING_EFFORT

    assert "issue_comment" in triggers
    job_if = workflow["jobs"]["code-review"]["if"]
    assert "/ocreview" in job_if
    assert "@ocr" not in job_if
    assert "OCReview" not in job_if
    assert "workflow_dispatch" in job_if


def test_ocr_review_timeouts_cover_grouped_openrouter_reviews() -> None:
    workflow = _load_workflow()
    job = workflow["jobs"]["code-review"]
    assert job["timeout-minutes"] == 60
    step = next(s for s in job["steps"] if s.get("name") == "Run OpenCodeReview")
    assert str(step["with"]["review_task_timeout"]) == "30"
    assert (
        step["with"]["llm_reasoning_effort"] == "${{ steps.pr-context.outputs.reasoning_effort }}"
    )


def test_ocr_version_pin_cannot_be_replaced_by_the_background_updater() -> None:
    workflow = _load_workflow()
    job = workflow["jobs"]["code-review"]
    step = next(s for s in job["steps"] if s.get("name") == "Run OpenCodeReview")
    env = {**workflow.get("env", {}), **job.get("env", {}), **step.get("env", {})}
    assert env.get("OCR_NO_UPDATE") == "1"
    assert step["with"]["ocr_version"] == "1.12.7"
    assert step["uses"] == ("alibaba/open-code-review@85cecfe5f935da2b2aae8f91ce4fee8ed343a681")


def test_ocr_execution_failure_fails_the_check() -> None:
    job = _load_workflow()["jobs"]["code-review"]
    step = next(s for s in job["steps"] if s.get("name") == "Run OpenCodeReview")
    assert job.get("continue-on-error", False) is False
    assert step.get("continue-on-error", False) is False
