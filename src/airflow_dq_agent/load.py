"""Load table contracts and check specs from a YAML file."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from airflow_dq_agent.contracts.tables import TableContract, register_contract
from airflow_dq_agent.quality.registry import CheckSpec, register_check


class _RegistryDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tables: list[TableContract] = Field(default_factory=list)
    checks: list[dict[str, Any]] = Field(default_factory=list)


def _named(path: Path, exc: ValidationError, *, prefix: str = "") -> ValueError:
    lines: list[str] = []
    for err in exc.errors():
        loc_parts = [part for part in ((prefix,) if prefix else ()) + err["loc"] if part != ""]
        loc = ".".join(str(part) for part in loc_parts)
        label = f"{path}: {loc}" if loc else str(path)
        lines.append(f"{label}: {err['msg']}")
    return ValueError("\n".join(lines))


def load_registry(path: str | Path) -> None:
    """Register table contracts and check specs from one YAML document."""
    source = Path(path)
    try:
        raw = yaml.safe_load(source.read_text())
    except yaml.YAMLError as exc:
        raise ValueError(f"{source}: {exc}") from exc
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"{source}: document must be a mapping")
    try:
        document = _RegistryDocument.model_validate(raw)
    except ValidationError as exc:
        raise _named(source, exc) from exc
    for contract in document.tables:
        register_contract(contract)
    for index, raw_check in enumerate(document.checks):
        try:
            spec = CheckSpec.model_validate(raw_check)
        except ValidationError as exc:
            raise _named(source, exc, prefix=f"checks.{index}") from exc
        except (KeyError, ValueError) as exc:
            raise ValueError(f"{source}: checks.{index}: {exc}") from exc
        try:
            register_check(spec)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"{source}: checks.{index}: {exc}") from exc
