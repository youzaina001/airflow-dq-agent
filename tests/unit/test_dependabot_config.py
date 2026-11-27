from pathlib import Path

import yaml

CONFIG = Path(__file__).resolve().parents[2] / ".github" / "dependabot.yml"


def _load_config() -> dict:
    loaded = yaml.safe_load(CONFIG.read_text())
    assert isinstance(loaded, dict)
    return loaded


def test_dependabot_monthly_uv_and_github_actions() -> None:
    assert CONFIG.is_file()
    config = _load_config()
    assert config["version"] == 2
    assert "reviewers" not in config
    assert "npm" not in {entry["package-ecosystem"] for entry in config["updates"]}

    by_ecosystem = {entry["package-ecosystem"]: entry for entry in config["updates"]}
    assert set(by_ecosystem) == {"uv", "github-actions"}

    for entry in by_ecosystem.values():
        assert entry["directory"] == "/"
        assert entry["schedule"] == {"interval": "monthly"}
        assert "reviewers" not in entry

    uv_group = by_ecosystem["uv"]["groups"]["python-minor-and-patch"]
    assert uv_group["patterns"] == ["*"]
    assert uv_group["update-types"] == ["minor", "patch"]

    actions_groups = by_ecosystem["github-actions"]["groups"]
    assert actions_groups == {"github-actions-all": {"patterns": ["*"]}}

    uv_ignore = by_ecosystem["uv"].get("ignore", [])
    assert {"dependency-name": "sqlalchemy", "versions": [">= 2.1.0"]} in uv_ignore
    assert {"dependency-name": "opentelemetry-*"} in uv_ignore
