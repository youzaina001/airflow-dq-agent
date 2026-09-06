"""Allow-listed check catalog. Each CheckSpec owns suite, sample SQL, and apply predicates."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl
from pydantic import BaseModel, Field

from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.tables import get_table_contract
from airflow_dq_agent.quality.predicates import (
    failed_rows as rows_failing_spec,
)
from airflow_dq_agent.quality.predicates import (
    quarantine_predicate_for,
    sample_sql_for,
)


class CheckPolicy(BaseModel):
    """One controlled remediation rule declared by a quality check."""

    action_id: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class CheckSpec(BaseModel):
    check_id: str
    table: str
    column: str | None = None
    dimension: Dimension
    description: str
    contract_id: str = ""
    policies: list[CheckPolicy] = Field(default_factory=list)
    contains: str | None = None
    window_start_column: str | None = None
    window_end_column: str | None = None
    business_key: list[str] | None = None
    sample_sql: str = ""
    quarantine_predicate: str | None = None

    def model_post_init(self, __context: object) -> None:
        if not self.contract_id:
            self.contract_id = get_table_contract(self.table).contract_id
        self.sample_sql = sample_sql_for(self)
        self.quarantine_predicate = quarantine_predicate_for(self)
        contract = get_table_contract(self.table)
        if self.dimension is Dimension.COMPLETENESS and self.column is None:
            raise ValueError(f"{self.check_id} completeness check must name a column")
        if self.dimension is Dimension.VALIDITY:
            if self.column is None:
                raise ValueError(f"{self.check_id} validity check must name a column")
            has_rule = self.contains is not None or (
                self.window_start_column is not None and self.window_end_column is not None
            )
            if not has_rule and not contract.column(self.column).allowed_values:
                raise ValueError(f"{self.check_id} has no validity rule")
        if self.dimension is Dimension.UNIQUENESS and not self.business_key:
            raise ValueError(f"{self.check_id} uniqueness check must declare a business_key")
        if self.dimension is Dimension.REFERENTIAL_INTEGRITY and (
            self.column is None
            or not any(foreign_key[0] == self.column for foreign_key in contract.foreign_keys)
        ):
            raise ValueError(f"{self.check_id} column is not a contracted foreign key")

    def rule_for(self, action_id: str) -> CheckPolicy | None:
        return next((policy for policy in self.policies if policy.action_id == action_id), None)

    def failed_rows(self, frames: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
        return rows_failing_spec(self, frames)


CHECK_SPECS: dict[str, CheckSpec] = {}


def register_check(spec: CheckSpec) -> None:
    """Add one check spec to the process-wide catalog."""
    if spec.check_id in CHECK_SPECS:
        raise ValueError(f"check_id {spec.check_id!r} is already registered")
    CHECK_SPECS[spec.check_id] = spec


def get_check_spec(check_id: str) -> CheckSpec:
    if check_id not in CHECK_SPECS:
        raise KeyError(f"Unknown check_id {check_id!r}")
    return CHECK_SPECS[check_id]
