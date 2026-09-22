import json
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ocr-review.yml"

ALLOWED_MODELS = [
    "z-ai/glm-5.3-flash",
    "z-ai/glm-5.3-flashx",
    "deepseek/deepseek-v4.1-flash",
    "xiaomi/mimo-v2.6-pro",
    "xiaomi/mimo-v2.6-flash",
    "tencent/hy4-preview",
    "openai/gpt-6-luna",
    "meta/muse-spark-1.3-contributor",
]

ALLOWED_REASONING_EFFORT = ["low", "medium", "high", "max"]


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


@pytest.mark.parametrize("event", ["workflow_dispatch", "issue_comment"])
@pytest.mark.parametrize("model", [*ALLOWED_MODELS, "", "unknown/model"])
@pytest.mark.parametrize("effort", ALLOWED_REASONING_EFFORT)
def test_model_selection(event: str, model: str, effort: str) -> None:
    script = _load_workflow()["jobs"]["code-review"]["steps"][0]["with"]["script"]
    harness = (
        """
const outputs = {};
const core = {setOutput: (key, value) => outputs[key] = value,
              setFailed: message => {throw new Error(message)}};
const context = {eventName: EVENT, repo: {owner: 'test', repo: 'test'},
                 issue: {number: 80},
                 payload: {inputs: {pr_number: '80', model: MODEL,
                                    reasoning_effort: EFFORT},
                           comment: {body: '/ocreview ' + MODEL}}};
const github = {rest: {pulls: {get: async () => ({data: {
    base: {ref: 'master'}, head: {sha: 'test-head'}
}})}}};
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
new AsyncFunction('core', 'context', 'github', SCRIPT)(core, context, github)
    .then(() => console.log(JSON.stringify(outputs)));
""".replace("SCRIPT", json.dumps(script))
        .replace("EVENT", json.dumps(event))
        .replace("MODEL", json.dumps(model))
        .replace("EFFORT", json.dumps(effort))
    )
    result = subprocess.run(["node", "-e", harness], capture_output=True, text=True, check=True)
    outputs = json.loads(result.stdout)
    expected_efforts = {
        "xiaomi/mimo-v2.6-pro": ["", "", "", ""],
        "xiaomi/mimo-v2.6-flash": ["", "", "", ""],
        "tencent/hy4-preview": ["low", "high", "high", "high"],
    }.get(model, ALLOWED_REASONING_EFFORT)
    effort_index = ALLOWED_REASONING_EFFORT.index(effort) if event == "workflow_dispatch" else 0
    assert outputs["reasoning_effort"] == expected_efforts[effort_index]
    assert outputs["model"] == (model if model in ALLOWED_MODELS else "z-ai/glm-5.3-flash")
    assert outputs["pr_number"] == "80"
    assert outputs["head_sha"] == "test-head"
    step = _load_workflow()["jobs"]["code-review"]["steps"][1]
    assert step["with"]["llm_model"] == "${{ steps.pr-context.outputs.model }}"
