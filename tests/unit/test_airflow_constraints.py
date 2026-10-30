import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
OVERLAY = ROOT / "constraints-airflow-overlay.txt"


def _merge_module() -> object:
    path = Path(__file__).resolve().parents[2] / "scripts/merge_airflow_constraints.py"
    spec = importlib.util.spec_from_file_location("merge_airflow_constraints", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_overlay_replaces_sqlalchemy_and_keeps_other_pins() -> None:
    module = _merge_module()
    merged = module.merge_constraints(  # type: ignore[attr-defined]
        "pydantic==2.12.5\nSQLAlchemy==1.4.54\nopentelemetry-api==1.27.0\nhttpx==0.28.1\n",
        ["SQLAlchemy==2.0.36", "opentelemetry-api==1.28.0"],
    )
    assert "SQLAlchemy==2.0.36" in merged
    assert "SQLAlchemy==1.4.54" not in merged
    assert "opentelemetry-api==1.28.0" in merged
    assert "opentelemetry-api==1.27.0" not in merged
    assert "pydantic==2.12.5" in merged
    assert "httpx==0.28.1" in merged


def test_overlay_normalizes_package_names_and_rejects_duplicate_pins() -> None:
    module = _merge_module()
    merged = module.merge_constraints(  # type: ignore[attr-defined]
        "opentelemetry_api==1.27.0\n",
        ["opentelemetry-api==1.28.0"],
    )
    assert merged == "opentelemetry-api==1.28.0\n"

    with pytest.raises(ValueError, match="duplicate overlay pin"):
        module.merge_constraints(  # type: ignore[attr-defined]
            "",
            ["opentelemetry-api==1.28.0", "opentelemetry_api==1.28.0"],
        )


def test_airflow_image_matches_package_pin() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text()
    base_image = re.search(r"^FROM apache/airflow:(\d+\.\d+\.\d+)-python3\.12$", dockerfile, re.M)
    build_arg = re.search(r"^ARG AIRFLOW_VERSION=(\d+\.\d+\.\d+)$", dockerfile, re.M)
    compose_tags = re.findall(
        r"^[ \t]*image: airflow-dq-agent:(\d+\.\d+\.\d+)[ \t]*$",
        (ROOT / "docker-compose.yaml").read_text(),
        re.M,
    )
    package = re.search(
        r'"apache-airflow==(\d+\.\d+\.\d+)"',
        (ROOT / "pyproject.toml").read_text(),
    )
    assert base_image is not None
    assert build_arg is not None
    assert package is not None
    assert compose_tags == [package.group(1), package.group(1)]
    assert base_image.group(1) == package.group(1)
    assert build_arg.group(1) == package.group(1)


def test_airflow_overlay_sqlalchemy_pin_stays_on_2_0() -> None:
    pins = [
        line.strip()
        for line in OVERLAY.read_text().splitlines()
        if line.lower().startswith("sqlalchemy==")
    ]
    assert len(pins) == 1
    version = pins[0].split("==", 1)[1]
    assert version.startswith("2.0.")
    assert version != "2.1.0"
