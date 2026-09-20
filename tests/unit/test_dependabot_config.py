from pathlib import Path

import yaml

CONFIG = Path(__file__).resolve().parents[2] / ".github" / "dependabot.yml"


def _load_config() -> dict:
    loaded = yaml.safe_load(CONFIG.read_text())
    assert isinstance(loaded, dict)
    return loaded


def test_dependabot_weekly_uv_and_github_actions() -> None:
    assert CONFIG.is_file()
    config = _load_config()
    assert config["version"] == 2
    assert "reviewers" not in config
    assert "npm" not in {entry["package-ecosystem"] for entry in config["updates"]}

    by_ecosystem = {entry["package-ecosystem"]: entry for entry in config["updates"]}
    assert set(by_ecosystem) == {"uv", "github-actions"}

    for ecosystem, group_name in (
        ("uv", "python-minor-and-patch"),
        ("github-actions", "github-actions-minor-and-patch"),
    ):
        entry = by_ecosystem[ecosystem]
        assert entry["directory"] == "/"
        assert entry["schedule"] == {"interval": "weekly"}
        assert "reviewers" not in entry
        group = entry["groups"][group_name]
        assert group["patterns"] == ["*"]
        assert group["update-types"] == ["minor", "patch"]
