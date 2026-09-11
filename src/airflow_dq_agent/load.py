"""Load table contracts and check specs from a YAML file."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.tables import (
    TABLE_CONTRACTS,
    TableContract,
    get_table_contract,
    register_contract,
)
from airflow_dq_agent.quality.registry import CHECK_SPECS, CheckSpec, register_check


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
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    try:
        for index, contract in enumerate(document.tables):
            try:
                register_contract(contract)
            except ValueError as exc:
                raise ValueError(f"{source}: tables.{index}.table: {exc}") from exc
        for index, raw_check in enumerate(document.checks):
            table = raw_check.get("table")
            if isinstance(table, str):
                try:
                    get_table_contract(table)
                except KeyError as exc:
                    raise ValueError(f"{source}: checks.{index}.table: {exc}") from exc
            try:
                spec = CheckSpec.model_validate(raw_check)
            except ValidationError as exc:
                raise _named(source, exc, prefix=f"checks.{index}") from exc
            except (KeyError, ValueError) as exc:
                raise ValueError(f"{source}: checks.{index}: {exc}") from exc
            if spec.check_id in CHECK_SPECS:
                raise ValueError(
                    f"{source}: checks.{index}.check_id: check_id {spec.check_id!r} is already registered"
                )
            for policy_index, policy in enumerate(spec.policies):
                try:
                    get_governed_action(policy.action_id)
                except KeyError as exc:
                    raise ValueError(
                        f"{source}: checks.{index}.policies.{policy_index}.action_id: {exc}"
                    ) from exc
            try:
                register_check(spec)
            except (KeyError, ValueError) as exc:
                raise ValueError(f"{source}: checks.{index}: {exc}") from exc
    except Exception:
        TABLE_CONTRACTS.clear()
        TABLE_CONTRACTS.update(contracts)
        CHECK_SPECS.clear()
        CHECK_SPECS.update(checks)
        raise
