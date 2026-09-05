"""Exact target-set selection for controlled remediation plan items."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import ExecutablePlanItem, TargetSet
from airflow_dq_agent.planning.integrity import warehouse_environment_id
from airflow_dq_agent.warehouse.db import make_engine


def _fingerprint_target_rows(table: str, rows: list[tuple[Any, ...]]) -> str:
    """Fingerprint ordered primary-key tuples without retaining them in a plan or audit event."""
    return canonical_fingerprint({"table": table, "primary_keys": rows})


class PostgresTargetSetResolver:
    """Read controlled targets at compile time and lock/recheck them at apply time."""

    def __init__(self, *, engine: Engine | None = None, dsn: str | None = None) -> None:
        self._engine = engine or make_engine(dsn)

    @property
    def warehouse_environment_id(self) -> str:
        return warehouse_environment_id(str(self._engine.url))

    def resolve(
        self,
        *,
        report_run_id: str,
        check_id: str,
        action_id: str,
        table: str,
        params: dict[str, object],
    ) -> TargetSet:
        """Return the exact current controlled target summary for plan compilation."""
        del report_run_id, check_id
        rendered = get_governed_action(action_id).render(
            table=table, params=params, run_id="target-set"
        )
        with self._engine.connect() as connection:
            return self._select(connection, rendered.target_sql, rendered.target_params, table)

    def lock_and_resolve(self, connection: Connection, item: ExecutablePlanItem) -> TargetSet:
        """Select and lock precisely the rows whose fingerprint must match admission."""
        rendered = get_governed_action(item.action_id).render(
            table=item.table, params=item.params, run_id="apply"
        )
        target_sql = rendered.target_sql
        if target_sql is not None:
            target_sql = f"{target_sql} FOR UPDATE OF t"
        return self._select(connection, target_sql, rendered.target_params, item.table)

    def resolve_item(self, connection: Connection, item: ExecutablePlanItem) -> TargetSet:
        """Recompute a target summary in a read-only dry-run transaction."""
        rendered = get_governed_action(item.action_id).render(
            table=item.table, params=item.params, run_id="dry-run"
        )
        return self._select(connection, rendered.target_sql, rendered.target_params, item.table)

    @staticmethod
    def _select(
        connection: Connection,
        target_sql: str | None,
        params: dict[str, Any],
        table: str,
    ) -> TargetSet:
        if target_sql is None:
            return TargetSet(count=0, fingerprint=_fingerprint_target_rows(table, []))
        rows = [tuple(row) for row in connection.execute(text(target_sql), params)]
        return TargetSet(count=len(rows), fingerprint=_fingerprint_target_rows(table, rows))
