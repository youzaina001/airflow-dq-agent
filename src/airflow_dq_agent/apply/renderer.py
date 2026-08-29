"""Render controlled remediation templates; proposal SQL is never used as input SQL."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field

from airflow_dq_agent.contracts.models import (
    FORBIDDEN_SQL_TOKENS,
    DestructiveRank,
    ExecutablePlanItem,
    NonExecutablePlanItem,
    RemediationStep,
)
from airflow_dq_agent.contracts.remediations import get_action, validate_step_params
from airflow_dq_agent.contracts.tables import TableContract, get_table_contract
from airflow_dq_agent.quality.registry import get_check_spec

_PREVIEW_BLOCKED = (*FORBIDDEN_SQL_TOKENS, "DELETE")


class RenderedStep(BaseModel):
    """A bindable statement and controlled count query derived from one allowed step."""

    action_id: str
    table: str
    sql: str
    params: dict[str, Any] = Field(default_factory=dict)
    estimate_sql: str | None = None
    estimate_params: dict[str, Any] = Field(default_factory=dict)
    target_sql: str | None = None
    target_params: dict[str, Any] = Field(default_factory=dict)


def _quote(identifier: str) -> str:
    """Quote a previously contract-validated identifier."""
    return f'"{identifier}"'


def _qualified(contract: TableContract) -> str:
    return f"{_quote(contract.schema_name)}.{_quote(contract.table)}"


def _contract_for_step(step: RemediationStep) -> TableContract:
    return get_table_contract(step.table)


def _ensure_preview_is_safe(step: RemediationStep) -> None:
    preview = step.sql_preview.upper()
    bad = [token for token in _PREVIEW_BLOCKED if token in preview]
    if bad or step.destructive_rank == DestructiveRank.CRITICAL:
        raise ValueError(
            f"Refusing unsafe proposal preview for {step.action_id}: {bad or ['CRITICAL rank']}"
        )


def _check_params(step: RemediationStep) -> TableContract:
    _ensure_preview_is_safe(step)
    action = get_action(step.action_id)
    contract = _contract_for_step(step)
    errors = validate_step_params(action.action_id, contract.table, step.params)
    if errors:
        raise ValueError(f"Unrenderable remediation step: {errors}")
    allowed_keys = set(action.required_params) | set(action.optional_params)
    unexpected = sorted(set(step.params) - allowed_keys)
    if unexpected:
        raise ValueError(f"Unexpected remediation parameters: {unexpected}")
    return contract


def _coerce_fill_value(contract: TableContract, column: str, value: Any) -> Any:
    dtype = contract.column(column).dtype
    if dtype == "int64":
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{contract.table}.{column} requires an int64 fill_value")
        return value
    if dtype == "float64":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"{contract.table}.{column} requires a float64 fill_value")
        return float(value)
    if dtype == "utf8":
        if not isinstance(value, str):
            raise ValueError(f"{contract.table}.{column} requires a utf8 fill_value")
        return value
    if dtype == "bool":
        if not isinstance(value, bool):
            raise ValueError(f"{contract.table}.{column} requires a bool fill_value")
        return value
    if dtype == "date":
        if isinstance(value, datetime):
            raise ValueError(f"{contract.table}.{column} requires a date fill_value")
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(
                    f"{contract.table}.{column} requires an ISO date fill_value"
                ) from exc
        raise ValueError(f"{contract.table}.{column} requires a date fill_value")
    if dtype == "datetime":
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(
                    f"{contract.table}.{column} requires an ISO datetime fill_value"
                ) from exc
        raise ValueError(f"{contract.table}.{column} requires a datetime fill_value")
    raise ValueError(f"Unsupported contract dtype {dtype!r}")


def _invalid_predicate(check_id: str) -> str:
    spec = get_check_spec(check_id)
    if spec.quarantine_predicate is None:
        raise ValueError(f"No controlled quarantine predicate exists for {check_id!r}")
    return spec.quarantine_predicate


def _copy_sql(
    contract: TableContract,
    where_clause: str,
    *,
    join_clause: str = "",
) -> tuple[str, str, str]:
    table = _qualified(contract)
    pk = _quote(contract.primary_key[0])
    return (
        "INSERT INTO dq.quarantine_rows (run_id, table_name, pk_json, reason, payload)\n"
        "SELECT :run_id, :table_name, jsonb_build_object(:pk_key, t."
        + pk
        + "), :reason, to_jsonb(t)\n"
        + f"FROM {table} t{join_clause}\nWHERE {where_clause}",
        f"SELECT COUNT(*) FROM {table} t{join_clause} WHERE {where_clause}",
        f"SELECT t.{pk} FROM {table} t{join_clause} WHERE {where_clause} ORDER BY t.{pk}",
    )


def _render_no_op(step: RemediationStep, contract: TableContract) -> RenderedStep:
    return RenderedStep(
        action_id=step.action_id,
        table=contract.table,
        sql=f"-- {step.action_id}: no source mutation for {_quote(contract.table)}",
    )


def render_step(step: RemediationStep, *, run_id: str = "dry-run") -> RenderedStep:
    """Render one action entirely from controlled policy and contract data."""
    contract = _check_params(step)
    table = _qualified(contract)
    common = {
        "run_id": run_id,
        "table_name": contract.qualified,
        "pk_key": contract.primary_key[0],
        "reason": step.action_id,
    }
    if step.action_id in {"no_op_alert", "schema_drift_ticket"}:
        return _render_no_op(step, contract)
    if step.action_id == "quarantine_nulls":
        column = _quote(str(step.params["column"]))
        sql, estimate_sql, target_sql = _copy_sql(contract, f"t.{column} IS NULL")
        return RenderedStep(
            action_id=step.action_id,
            table=contract.table,
            sql=sql,
            params=common,
            estimate_sql=estimate_sql,
            estimate_params=common,
            target_sql=target_sql,
        )
    if step.action_id == "quarantine_invalids":
        check_id = str(step.params["check_id"])
        sql, estimate_sql, target_sql = _copy_sql(contract, _invalid_predicate(check_id))
        return RenderedStep(
            action_id=step.action_id,
            table=contract.table,
            sql=sql,
            params=common,
            estimate_sql=estimate_sql,
            estimate_params=common,
            target_sql=target_sql,
        )
    if step.action_id == "quarantine_orphans":
        ref = get_table_contract(str(step.params["ref_table"]))
        fk = _quote(str(step.params["fk_column"]))
        ref_column = _quote(str(step.params["ref_column"]))
        ref_table = _qualified(ref)
        join = f" LEFT JOIN {ref_table} r ON r.{ref_column} = t.{fk}"
        sql, estimate_sql, target_sql = _copy_sql(
            contract, f"r.{ref_column} IS NULL", join_clause=join
        )
        return RenderedStep(
            action_id=step.action_id,
            table=contract.table,
            sql=sql,
            params=common,
            estimate_sql=estimate_sql,
            estimate_params=common,
            target_sql=target_sql,
        )
    if step.action_id == "dedupe_keep_min_pk":
        business_key = step.params["business_key"]
        key_columns = business_key if isinstance(business_key, list) else [business_key]
        quoted_keys = ", ".join(f"s.{_quote(str(column))}" for column in key_columns)
        pk = _quote(str(step.params["pk_column"]))
        where = f"t.{pk} NOT IN (SELECT MIN(s.{pk}) FROM {table} s GROUP BY {quoted_keys})"
        sql, estimate_sql, target_sql = _copy_sql(contract, where)
        return RenderedStep(
            action_id=step.action_id,
            table=contract.table,
            sql=sql,
            params=common,
            estimate_sql=estimate_sql,
            estimate_params=common,
            target_sql=target_sql,
        )
    if step.action_id == "null_fill":
        column_name = str(step.params["column"])
        column = _quote(column_name)
        fill_value = _coerce_fill_value(contract, column_name, step.params["fill_value"])
        sql = f"UPDATE {table} SET {column} = :fill_value WHERE {column} IS NULL"
        estimate_sql = f"SELECT COUNT(*) FROM {table} t WHERE t.{column} IS NULL"
        pk = _quote(contract.primary_key[0])
        target_sql = f"SELECT t.{pk} FROM {table} t WHERE t.{column} IS NULL ORDER BY t.{pk}"
        params = {"fill_value": fill_value}
        return RenderedStep(
            action_id=step.action_id,
            table=contract.table,
            sql=sql,
            params=params,
            estimate_sql=estimate_sql,
            estimate_params={},
            target_sql=target_sql,
        )
    raise ValueError(f"No renderer exists for action {step.action_id!r}")


def render_plan_item(
    item: ExecutablePlanItem | NonExecutablePlanItem, *, run_id: str = "dry-run"
) -> RenderedStep:
    """Render one compiled item without accepting candidate text or SQL previews."""
    if not isinstance(item, ExecutablePlanItem):
        raise ValueError("Cannot render a non-executable remediation plan item")
    return render_controlled_action(
        action_id=item.action_id, table=item.table, params=item.params, run_id=run_id
    )


def render_controlled_action(
    *, action_id: str, table: str, params: dict[str, Any], run_id: str = "dry-run"
) -> RenderedStep:
    """Render policy-derived fields only; this is the target-set resolver's input."""
    action = get_action(action_id)
    controlled = RemediationStep(
        action_id=action_id,
        table=table,
        params=params,
        reversible=action.reversible,
        destructive_rank=action.destructive_rank,
        rationale="Compiled from check policy.",
    )
    return render_step(controlled, run_id=run_id)
