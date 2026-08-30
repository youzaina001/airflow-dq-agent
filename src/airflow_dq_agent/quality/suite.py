"""Deterministic quality suite. Polars + SQL. The agent does not run this."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import uuid4

import polars as pl
from sqlalchemy.engine import Engine

from airflow_dq_agent.contracts.models import (
    CheckResult,
    CheckStatus,
    Dimension,
    QualitySuiteReport,
)
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS, get_table_contract
from airflow_dq_agent.quality.registry import CHECK_SPECS, CheckSpec
from airflow_dq_agent.warehouse.db import make_engine

SAMPLE = 20
TABLES = tuple(TABLE_CONTRACTS)


def _result(
    spec: CheckSpec,
    *,
    failed: pl.DataFrame | list[dict[str, Any]],
    n_total: int,
    message: str,
    status: CheckStatus | None = None,
) -> CheckResult:
    if isinstance(failed, pl.DataFrame):
        rows = failed.head(SAMPLE).to_dicts()
        n_failed = failed.height
    else:
        rows = failed[:SAMPLE]
        n_failed = len(failed)
    resolved = status or (CheckStatus.FAIL if n_failed else CheckStatus.PASS)
    return CheckResult(
        check_id=spec.check_id,
        table=spec.table,
        column=spec.column,
        dimension=spec.dimension,
        status=resolved,
        n_failed=n_failed,
        n_total=n_total,
        sample_failures=rows,
        message=message,
        contract_id=spec.contract_id,
        predicate=spec.description,
    )


def _jsonable(df: pl.DataFrame) -> pl.DataFrame:
    """Dates/datetimes become strings so CheckResult can serialize."""
    casts: list[pl.Expr] = []
    for name, dtype in zip(df.columns, df.dtypes, strict=True):
        if dtype in (pl.Date, pl.Datetime, pl.Time, pl.Duration):
            casts.append(pl.col(name).cast(pl.String).alias(name))
    return df.with_columns(casts) if casts else df


def load_frames(engine: Engine) -> dict[str, pl.DataFrame]:
    frames: dict[str, pl.DataFrame] = {}
    with engine.connect() as conn:
        for table in TABLES:
            frames[table] = pl.read_database(
                f"SELECT * FROM warehouse.{table}",
                connection=conn,
            )
    return frames


def observed_columns(frames: Mapping[str, pl.DataFrame]) -> dict[str, list[str]]:
    return {name: list(df.columns) for name, df in frames.items()}


def _n_total(spec: CheckSpec, frames: Mapping[str, pl.DataFrame]) -> int:
    if spec.dimension is Dimension.SCHEMA_DRIFT:
        expected = set(get_table_contract(spec.table).column_names)
        observed = set(frames[spec.table].columns)
        return len(expected | observed)
    return frames[spec.table].height


def _message(spec: CheckSpec, failed: pl.DataFrame, n_total: int) -> str:
    n_failed = failed.height
    if spec.dimension is Dimension.COMPLETENESS:
        return f"{n_failed}/{n_total} null {spec.column} on {spec.table}"
    if spec.dimension is Dimension.VALIDITY:
        if spec.contains == "@":
            return f"{n_failed} emails missing @"
        if spec.window_start_column:
            return f"{n_failed} visits outside window"
        return f"{n_failed} illegal {spec.column} values"
    if spec.dimension is Dimension.UNIQUENESS:
        keys = ", ".join(spec.business_key or [])
        return f"{n_failed} rows share ({keys})"
    if spec.dimension is Dimension.REFERENTIAL_INTEGRITY:
        return f"{n_failed} orphan {spec.column} on {spec.table}"
    if spec.dimension is Dimension.SCHEMA_DRIFT:
        if n_failed:
            extra = [row["column"] for row in failed.to_dicts() if row["kind"] == "extra"]
            missing = [row["column"] for row in failed.to_dicts() if row["kind"] == "missing"]
            return f"drift on {spec.table}: extra={extra} missing={missing}"
        contract = get_table_contract(spec.table)
        return f"{spec.table} matches contract ({len(contract.column_names)} columns)"
    return spec.description


def run_suite_on_frames(frames: Mapping[str, pl.DataFrame]) -> QualitySuiteReport:
    checks: list[CheckResult] = []
    for spec in CHECK_SPECS.values():
        failed = _jsonable(spec.failed_rows(frames))
        n_total = _n_total(spec, frames)
        checks.append(
            _result(spec, failed=failed, n_total=n_total, message=_message(spec, failed, n_total))
        )
    return QualitySuiteReport(
        run_id=uuid4().hex,
        checks=checks,
        observed_columns=observed_columns(frames),
    )


def run_quality_suite(dsn: str | None = None) -> QualitySuiteReport:
    engine = make_engine(dsn)
    frames = load_frames(engine)
    report = run_suite_on_frames(frames)
    # In HITL mode this is a required Postgres audit write; shadow mode retains the
    # supplementary JSONL event only.  Either way per-check samples never leave the
    # quality process for durable audit.
    from airflow_dq_agent.config import get_settings
    from airflow_dq_agent.traces import record_quality_report

    event = record_quality_report(report, dsn=get_settings().audit_dsn)
    return report.model_copy(update={"audit_event_id": event.event_id})
