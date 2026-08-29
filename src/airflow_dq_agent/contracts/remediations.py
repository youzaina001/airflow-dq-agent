"""Allow-listed remediations. The apply engine renders these templates; agent SQL is never executed."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from airflow_dq_agent.contracts.models import DestructiveRank
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS, get_table_contract
from airflow_dq_agent.quality.registry import CHECK_SPECS


class RemediationAction(BaseModel):
    action_id: str
    description: str
    mutates: bool
    destructive_rank: DestructiveRank
    reversible: bool
    required_params: list[str]
    optional_params: list[str] = Field(default_factory=list)
    allowed_tables: frozenset[str]
    preview_sql: str
    notes: str = ""


# Identifiers in preview_sql are placeholders; apply binds via quoted identifiers.
REMEDIATION_CATALOG: dict[str, RemediationAction] = {
    "no_op_alert": RemediationAction(
        action_id="no_op_alert",
        description="Record the failure. Do not mutate.",
        mutates=False,
        destructive_rank=DestructiveRank.NONE,
        reversible=True,
        required_params=["check_id"],
        allowed_tables=frozenset(TABLE_CONTRACTS),
        preview_sql="-- no-op: alert only for {check_id} on {table}",
    ),
    "quarantine_nulls": RemediationAction(
        action_id="quarantine_nulls",
        description="Copy rows where {column} IS NULL into dq.quarantine_rows; leave source intact until HITL.",
        mutates=True,
        destructive_rank=DestructiveRank.MEDIUM,
        reversible=True,
        required_params=["column", "pk_column"],
        allowed_tables=frozenset(TABLE_CONTRACTS),
        preview_sql=(
            "INSERT INTO dq.quarantine_rows (run_id, table_name, pk_json, reason, payload)\n"
            "SELECT :run_id, :table, jsonb_build_object(:pk_column, t.{pk_column}), :reason, to_jsonb(t)\n"
            "FROM warehouse.{table} t WHERE t.{column} IS NULL"
        ),
        notes="Apply never DELETEs from the source in v1; quarantine is copy-only.",
    ),
    "quarantine_invalids": RemediationAction(
        action_id="quarantine_invalids",
        description=(
            "Copy rows failing the controlled validity predicate into dq.quarantine_rows; "
            "leave source intact until HITL."
        ),
        mutates=True,
        destructive_rank=DestructiveRank.MEDIUM,
        reversible=True,
        required_params=["check_id", "column", "pk_column"],
        allowed_tables=frozenset(TABLE_CONTRACTS),
        preview_sql=(
            "INSERT INTO dq.quarantine_rows (...) SELECT ... FROM warehouse.{table} "
            "WHERE <controlled validity predicate for {check_id}>"
        ),
        notes=(
            "The predicate is looked up from CHECK_SPECS by check_id. It is never supplied by an agent."
        ),
    ),
    "null_fill": RemediationAction(
        action_id="null_fill",
        description="UPDATE {table} SET {column} = :fill_value WHERE {column} IS NULL.",
        mutates=True,
        destructive_rank=DestructiveRank.LOW,
        reversible=False,
        required_params=["column", "fill_value"],
        allowed_tables=frozenset({"fact_orders", "fact_visits", "dim_patient"}),
        preview_sql=("UPDATE warehouse.{table} SET {column} = :fill_value WHERE {column} IS NULL"),
        notes="fill_value is a bound parameter. Column must be in the table contract.",
    ),
    "quarantine_orphans": RemediationAction(
        action_id="quarantine_orphans",
        description="Copy rows whose FK does not resolve into dq.quarantine_rows.",
        mutates=True,
        destructive_rank=DestructiveRank.MEDIUM,
        reversible=True,
        required_params=["fk_column", "ref_table", "ref_column", "pk_column"],
        allowed_tables=frozenset(
            {"fact_order_items", "fact_orders", "fact_visits", "fact_adverse_events", "dim_patient"}
        ),
        preview_sql=(
            "INSERT INTO dq.quarantine_rows (run_id, table_name, pk_json, reason, payload)\n"
            "SELECT :run_id, :table, jsonb_build_object(:pk_column, t.{pk_column}), :reason, to_jsonb(t)\n"
            "FROM warehouse.{table} t\n"
            "LEFT JOIN warehouse.{ref_table} r ON r.{ref_column} = t.{fk_column}\n"
            "WHERE r.{ref_column} IS NULL"
        ),
    ),
    "dedupe_keep_min_pk": RemediationAction(
        action_id="dedupe_keep_min_pk",
        description="Copy duplicate business-key rows (keep min pk) into quarantine. Source is not deleted in v1.",
        mutates=True,
        destructive_rank=DestructiveRank.HIGH,
        reversible=True,
        required_params=["business_key", "pk_column"],
        allowed_tables=frozenset({"fact_orders", "dim_patient", "dim_customer"}),
        preview_sql=(
            "INSERT INTO dq.quarantine_rows (run_id, table_name, pk_json, reason, payload)\n"
            "SELECT :run_id, :table, jsonb_build_object(:pk_column, t.{pk_column}), :reason, to_jsonb(t)\n"
            "FROM warehouse.{table} t\n"
            "WHERE t.{pk_column} NOT IN (\n"
            "  SELECT MIN(s.{pk_column}) FROM warehouse.{table} s GROUP BY s.{business_key}\n"
            ")"
        ),
        notes="HIGH rank: HITL must approve. v1 copies dupes; it does not DELETE.",
    ),
    "schema_drift_ticket": RemediationAction(
        action_id="schema_drift_ticket",
        description="Do not auto-migrate. Open a contract change; humans update TABLE_CONTRACTS.",
        mutates=False,
        destructive_rank=DestructiveRank.NONE,
        reversible=True,
        required_params=["check_id"],
        allowed_tables=frozenset(TABLE_CONTRACTS),
        preview_sql="-- schema drift is a contract change, not a DML step",
    ),
}


def get_action(action_id: str) -> RemediationAction:
    if action_id not in REMEDIATION_CATALOG:
        raise KeyError(
            f"Unknown action_id {action_id!r}. Allow-list: {sorted(REMEDIATION_CATALOG)}"
        )
    return REMEDIATION_CATALOG[action_id]


def validate_step_params(action_id: str, table: str, params: dict[str, Any]) -> list[str]:
    """Return human-readable violations. Empty list means the step is bindable."""
    errors: list[str] = []
    try:
        action = get_action(action_id)
    except KeyError as exc:
        return [str(exc)]
    table_key = table.split(".")[-1]
    if table_key not in action.allowed_tables:
        errors.append(f"{action_id} is not allowed on {table_key}")
        return errors
    try:
        contract = get_table_contract(table_key)
    except KeyError as exc:
        errors.append(str(exc))
        return errors
    for req in action.required_params:
        if req not in params:
            errors.append(f"missing required param {req!r}")
    allowed_params = set(action.required_params) | set(action.optional_params)
    for key in sorted(set(params) - allowed_params):
        errors.append(f"unexpected param {key!r}")
    identifier_params = {
        "column",
        "pk_column",
        "fk_column",
        "ref_column",
        "business_key",
        "ref_table",
    }
    for key, value in params.items():
        if key not in identifier_params:
            continue
        if key == "business_key" and isinstance(value, list):
            if not value:
                errors.append("business_key must be a non-empty list of identifiers")
            for item in value:
                if not isinstance(item, str):
                    errors.append("business_key must contain only string identifiers")
                elif not contract.has_column(item):
                    errors.append(f"{item!r} is not a column on {contract.table}")
            continue
        if not isinstance(value, str):
            errors.append(f"{key} must be a string identifier")
            continue
        if key == "ref_table":
            if value.split(".")[-1] not in TABLE_CONTRACTS:
                errors.append(f"ref_table {value!r} is not a contracted table")
            continue
        if not contract.has_column(value) and key != "ref_column":
            errors.append(f"{value!r} is not a column on {contract.table}")
        if key == "ref_column":
            ref = str(params.get("ref_table", ""))
            if ref:
                try:
                    ref_contract = get_table_contract(ref)
                    if not ref_contract.has_column(value):
                        errors.append(f"{value!r} is not a column on {ref_contract.table}")
                except KeyError:
                    pass
    if action_id == "quarantine_invalids":
        check_id = params.get("check_id")
        if not isinstance(check_id, str) or check_id not in CHECK_SPECS:
            errors.append("check_id must name a declared check")
        else:
            spec = CHECK_SPECS[check_id]
            if spec.table != table_key:
                errors.append(f"check_id {check_id!r} is not defined for {table_key}")
            if spec.dimension.value != "validity":
                errors.append(f"check_id {check_id!r} is not a validity check")
            if params.get("column") != spec.column:
                errors.append(f"column must match the declared check column {spec.column!r}")
    if (
        action_id
        in {
            "quarantine_nulls",
            "quarantine_invalids",
            "quarantine_orphans",
            "dedupe_keep_min_pk",
        }
        and params.get("pk_column") not in contract.primary_key
    ):
        errors.append("pk_column must be a contracted primary-key column")
    if action_id == "quarantine_orphans":
        foreign_key = (params.get("fk_column"), params.get("ref_table"), params.get("ref_column"))
        if foreign_key not in contract.foreign_keys:
            errors.append("fk_column/ref_table/ref_column must match a contracted foreign key")
    return errors
