"""The governed remediation-action module.

Each registered action owns its metadata, policy parameter derivation, controlled
SQL rendering, validation, and mutation capability.  Compiler, renderer, target
resolution, and execution cross this one seam instead of switching on action IDs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field

from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.remediations import (
    REMEDIATION_CATALOG,
    RemediationAction,
    get_action,
    validate_common_params,
)
from airflow_dq_agent.contracts.tables import TableContract, get_table_contract
from airflow_dq_agent.quality.registry import CheckSpec, get_check_spec


class RenderedStep(BaseModel):
    """A bindable statement and controlled count query derived from one allowed action."""

    action_id: str
    table: str
    sql: str
    params: dict[str, Any] = Field(default_factory=dict)
    estimate_sql: str | None = None
    estimate_params: dict[str, Any] = Field(default_factory=dict)
    target_sql: str | None = None
    target_params: dict[str, Any] = Field(default_factory=dict)


DerivedParams = Callable[[CheckSpec, TableContract, dict[str, Any]], dict[str, Any]]
RenderAction = Callable[[RemediationAction, TableContract, dict[str, Any], str], RenderedStep]
ValidateAction = Callable[[TableContract, dict[str, Any]], list[str]]


@dataclass(frozen=True)
class GovernedAction:
    """A deep module for one allow-listed remediation action."""

    metadata: RemediationAction
    _derive_params: DerivedParams
    _render: RenderAction
    _validate: ValidateAction

    @property
    def action_id(self) -> str:
        return self.metadata.action_id

    @property
    def mutates(self) -> bool:
        return self.metadata.mutates

    def derive_params(self, spec: CheckSpec) -> dict[str, Any]:
        rule = spec.rule_for(self.action_id)
        if rule is None:
            raise ValueError("requested action is not declared by the check policy")
        contract = get_table_contract(spec.table)
        params = self._derive_params(spec, contract, dict(rule.parameters))
        errors = self.validate_params(contract.table, params)
        if errors:
            raise ValueError(
                f"check policy does not produce a bindable controlled action: {errors}"
            )
        return params

    def validate_params(self, table: str, params: dict[str, Any]) -> list[str]:
        errors = validate_common_params(self.metadata, table, params)
        if errors:
            return errors
        return self._validate(get_table_contract(table), params)

    def render(self, *, table: str, params: dict[str, Any], run_id: str) -> RenderedStep:
        errors = self.validate_params(table, params)
        if errors:
            raise ValueError(f"Unrenderable remediation step: {errors}")
        return self._render(self.metadata, get_table_contract(table), params, run_id)


def _quote(identifier: str) -> str:
    return f'"{identifier}"'


def _qualified(contract: TableContract) -> str:
    return f"{_quote(contract.schema_name)}.{_quote(contract.table)}"


def _require_column(spec: CheckSpec, message: str) -> str:
    if spec.column is None:
        raise ValueError(message)
    return spec.column


def _single_pk(contract: TableContract) -> str:
    if len(contract.primary_key) != 1:
        raise ValueError("controlled renderer does not support a composite primary key yet")
    return contract.primary_key[0]


def _validate_no_op(_: TableContract, __: dict[str, Any]) -> list[str]:
    return []


def _validate_primary_key(contract: TableContract, params: dict[str, Any]) -> list[str]:
    if params.get("pk_column") not in contract.primary_key:
        return ["pk_column must be a contracted primary-key column"]
    return []


def _validate_invalids(contract: TableContract, params: dict[str, Any]) -> list[str]:
    errors = _validate_primary_key(contract, params)
    check_id = params.get("check_id")
    if not isinstance(check_id, str):
        return [*errors, "check_id must name a declared check"]
    try:
        spec = get_check_spec(check_id)
    except KeyError:
        return [*errors, "check_id must name a declared check"]
    if spec.table != contract.table:
        errors.append(f"check_id {check_id!r} is not defined for {contract.table}")
    if spec.dimension is not Dimension.VALIDITY:
        errors.append(f"check_id {check_id!r} is not a validity check")
    if params.get("column") != spec.column:
        errors.append(f"column must match the declared check column {spec.column!r}")
    return errors


def _validate_orphans(contract: TableContract, params: dict[str, Any]) -> list[str]:
    errors = _validate_primary_key(contract, params)
    foreign_key = (params.get("fk_column"), params.get("ref_table"), params.get("ref_column"))
    if foreign_key not in contract.foreign_keys:
        errors.append("fk_column/ref_table/ref_column must match a contracted foreign key")
    return errors


def _derive_no_op(spec: CheckSpec, _: TableContract, params: dict[str, Any]) -> dict[str, Any]:
    return {**params, "check_id": spec.check_id}


def _derive_nulls(
    spec: CheckSpec, contract: TableContract, params: dict[str, Any]
) -> dict[str, Any]:
    return {
        **params,
        "column": _require_column(spec, "completeness policy does not name a target column"),
        "pk_column": _single_pk(contract),
    }


def _derive_invalids(
    spec: CheckSpec, contract: TableContract, params: dict[str, Any]
) -> dict[str, Any]:
    return {
        **params,
        "check_id": spec.check_id,
        "column": _require_column(spec, "validity policy does not name a target column"),
        "pk_column": _single_pk(contract),
    }


def _derive_orphans(
    spec: CheckSpec, contract: TableContract, params: dict[str, Any]
) -> dict[str, Any]:
    column = _require_column(spec, "referential policy does not name a foreign key")
    foreign_key = next((fk for fk in contract.foreign_keys if fk[0] == column), None)
    if foreign_key is None:
        raise ValueError("check column is not a contracted foreign key")
    return {
        **params,
        "fk_column": foreign_key[0],
        "ref_table": foreign_key[1],
        "ref_column": foreign_key[2],
        "pk_column": _single_pk(contract),
    }


def _derive_dedupe(_: CheckSpec, contract: TableContract, params: dict[str, Any]) -> dict[str, Any]:
    return {**params, "pk_column": _single_pk(contract)}


def _derive_fill(spec: CheckSpec, _: TableContract, params: dict[str, Any]) -> dict[str, Any]:
    return {
        **params,
        "column": _require_column(spec, "null-fill policy does not name a target column"),
    }


def _copy_sql(
    contract: TableContract, where_clause: str, *, join_clause: str = ""
) -> tuple[str, str, str]:
    table = _qualified(contract)
    pk = _quote(contract.primary_key[0])
    return (
        "INSERT INTO dq.quarantine_rows (run_id, table_name, pk_json, reason, payload)\n"
        "SELECT :run_id, :table_name, jsonb_build_object(CAST(:pk_key AS text), t."
        + pk
        + "), :reason, to_jsonb(t)\n"
        + f"FROM {table} t{join_clause}\nWHERE {where_clause}",
        f"SELECT COUNT(*) FROM {table} t{join_clause} WHERE {where_clause}",
        f"SELECT t.{pk} FROM {table} t{join_clause} WHERE {where_clause} ORDER BY t.{pk}",
    )


def _quarantine_step(
    action: RemediationAction,
    contract: TableContract,
    where_clause: str,
    *,
    run_id: str,
    join_clause: str = "",
) -> RenderedStep:
    sql, estimate_sql, target_sql = _copy_sql(contract, where_clause, join_clause=join_clause)
    common = {
        "run_id": run_id,
        "table_name": contract.qualified,
        "pk_key": contract.primary_key[0],
        "reason": action.action_id,
    }
    return RenderedStep(
        action_id=action.action_id,
        table=contract.table,
        sql=sql,
        params=common,
        estimate_sql=estimate_sql,
        estimate_params=common,
        target_sql=target_sql,
    )


def _render_no_op(
    action: RemediationAction, contract: TableContract, _: dict[str, Any], __: str
) -> RenderedStep:
    return RenderedStep(
        action_id=action.action_id,
        table=contract.table,
        sql=f"-- {action.action_id}: no source mutation for {_quote(contract.table)}",
    )


def _render_nulls(
    action: RemediationAction, contract: TableContract, params: dict[str, Any], run_id: str
) -> RenderedStep:
    return _quarantine_step(
        action, contract, f"t.{_quote(str(params['column']))} IS NULL", run_id=run_id
    )


def _render_invalids(
    action: RemediationAction, contract: TableContract, params: dict[str, Any], run_id: str
) -> RenderedStep:
    spec = get_check_spec(str(params["check_id"]))
    if spec.quarantine_predicate is None:
        raise ValueError(f"No controlled quarantine predicate exists for {spec.check_id!r}")
    return _quarantine_step(action, contract, spec.quarantine_predicate, run_id=run_id)


def _render_orphans(
    action: RemediationAction, contract: TableContract, params: dict[str, Any], run_id: str
) -> RenderedStep:
    ref = get_table_contract(str(params["ref_table"]))
    fk = _quote(str(params["fk_column"]))
    ref_column = _quote(str(params["ref_column"]))
    join = f" LEFT JOIN {_qualified(ref)} r ON r.{ref_column} = t.{fk}"
    return _quarantine_step(
        action, contract, f"r.{ref_column} IS NULL", run_id=run_id, join_clause=join
    )


def _render_dedupe(
    action: RemediationAction, contract: TableContract, params: dict[str, Any], run_id: str
) -> RenderedStep:
    business_key = params["business_key"]
    keys = business_key if isinstance(business_key, list) else [business_key]
    quoted_keys = ", ".join(f"s.{_quote(str(column))}" for column in keys)
    pk = _quote(str(params["pk_column"]))
    table = _qualified(contract)
    where = f"t.{pk} NOT IN (SELECT MIN(s.{pk}) FROM {table} s GROUP BY {quoted_keys})"
    return _quarantine_step(action, contract, where, run_id=run_id)


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


def _render_fill(
    action: RemediationAction, contract: TableContract, params: dict[str, Any], _: str
) -> RenderedStep:
    column_name = str(params["column"])
    column = _quote(column_name)
    fill_value = _coerce_fill_value(contract, column_name, params["fill_value"])
    table = _qualified(contract)
    pk = _quote(contract.primary_key[0])
    return RenderedStep(
        action_id=action.action_id,
        table=contract.table,
        sql=f"UPDATE {table} SET {column} = :fill_value WHERE {column} IS NULL",
        params={"fill_value": fill_value},
        estimate_sql=f"SELECT COUNT(*) FROM {table} t WHERE t.{column} IS NULL",
        target_sql=f"SELECT t.{pk} FROM {table} t WHERE t.{column} IS NULL ORDER BY t.{pk}",
    )


def _action(
    action_id: str,
    derive: DerivedParams,
    render: RenderAction,
    validate: ValidateAction = _validate_no_op,
) -> GovernedAction:
    return GovernedAction(get_action(action_id), derive, render, validate)


GOVERNED_ACTIONS: dict[str, GovernedAction] = {
    "no_op_alert": _action("no_op_alert", _derive_no_op, _render_no_op),
    "quarantine_nulls": _action(
        "quarantine_nulls", _derive_nulls, _render_nulls, _validate_primary_key
    ),
    "quarantine_invalids": _action(
        "quarantine_invalids", _derive_invalids, _render_invalids, _validate_invalids
    ),
    "null_fill": _action("null_fill", _derive_fill, _render_fill),
    "quarantine_orphans": _action(
        "quarantine_orphans", _derive_orphans, _render_orphans, _validate_orphans
    ),
    "dedupe_keep_min_pk": _action(
        "dedupe_keep_min_pk", _derive_dedupe, _render_dedupe, _validate_primary_key
    ),
    "schema_drift_ticket": _action("schema_drift_ticket", _derive_no_op, _render_no_op),
}

if set(GOVERNED_ACTIONS) != set(REMEDIATION_CATALOG):
    raise RuntimeError("Every catalogued remediation action must have exactly one implementation")


def get_action_definition(action_id: str) -> GovernedAction:
    if action_id not in GOVERNED_ACTIONS:
        raise KeyError(f"Unknown action_id {action_id!r}. Allow-list: {sorted(GOVERNED_ACTIONS)}")
    return GOVERNED_ACTIONS[action_id]


def derive_action_params(spec: CheckSpec, action_id: str) -> dict[str, Any]:
    return get_action_definition(action_id).derive_params(spec)
