"""Deterministic quality suite. Polars + SQL. The agent does not run this."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import uuid4

import polars as pl
from psycopg.errors import UndefinedTable
from sqlalchemy.engine import Engine

from airflow_dq_agent.contracts.fingerprints import report_payload_fingerprint
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


def _is_undefined_table(exc: BaseException) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, UndefinedTable):
            return True
        orig = getattr(current, "orig", None)
        if isinstance(orig, BaseException) and orig is not current:
            current = orig
            continue
        current = current.__cause__
    return False


def load_frames(engine: Engine) -> dict[str, pl.DataFrame]:
    frames: dict[str, pl.DataFrame] = {}
    for table in TABLES:
        with engine.connect() as conn:
            try:
                frames[table] = pl.read_database(
                    f"SELECT * FROM warehouse.{table}",
                    connection=conn,
                )
            except Exception as exc:
                if _is_undefined_table(exc):
                    continue
                raise
    return frames


def observed_columns(frames: Mapping[str, pl.DataFrame]) -> dict[str, list[str]]:
    return {name: list(df.columns) for name, df in frames.items()}


def _n_total(spec: CheckSpec, frames: Mapping[str, pl.DataFrame]) -> int:
    if spec.dimension is Dimension.SCHEMA_DRIFT:
        expected = set(get_table_contract(spec.table).column_names)
        observed = set(frames[spec.table].columns) if spec.table in frames else set()
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
            rows = failed.to_dicts()
            if any(row["kind"] == "missing_table" for row in rows):
                return f"drift on {spec.table}: missing table {spec.table}"
            extra = [row["column"] for row in rows if row["kind"] == "extra"]
            missing = [row["column"] for row in rows if row["kind"] == "missing"]
            return f"drift on {spec.table}: extra={extra} missing={missing}"
        contract = get_table_contract(spec.table)
        return f"{spec.table} matches contract ({len(contract.column_names)} columns)"
    return spec.description


def _needed_columns(spec: CheckSpec) -> list[str]:
    names: list[str] = []
    if spec.column:
        names.append(spec.column)
    if spec.business_key:
        names.extend(spec.business_key)
    if spec.window_start_column:
        names.append(spec.window_start_column)
    if spec.window_end_column:
        names.append(spec.window_end_column)
    if spec.dimension is Dimension.REFERENTIAL_INTEGRITY:
        names.extend(get_table_contract(spec.table).primary_key)
    return list(dict.fromkeys(names))


def _structural_error_message(spec: CheckSpec, frames: Mapping[str, pl.DataFrame]) -> str | None:
    if spec.table not in frames:
        return f"cannot evaluate {spec.check_id}: missing table {spec.table}"[:200]
    observed = set(frames[spec.table].columns)
    missing = next((name for name in _needed_columns(spec) if name not in observed), None)
    if missing:
        return f"cannot evaluate {spec.check_id}: missing column {missing} on {spec.table}"[:200]
    if spec.dimension is Dimension.REFERENTIAL_INTEGRITY and spec.column:
        for fk_col, ref_table, ref_column in get_table_contract(spec.table).foreign_keys:
            if fk_col != spec.column:
                continue
            if ref_table not in frames:
                return f"cannot evaluate {spec.check_id}: missing table {ref_table}"[:200]
            if ref_column not in frames[ref_table].columns:
                return (
                    f"cannot evaluate {spec.check_id}: missing column {ref_column} on {ref_table}"
                )[:200]
    return None


def run_suite_on_frames(frames: Mapping[str, pl.DataFrame]) -> QualitySuiteReport:
    checks: list[CheckResult] = []
    for spec in CHECK_SPECS.values():
        if spec.dimension is not Dimension.SCHEMA_DRIFT:
            reason = _structural_error_message(spec, frames)
            if reason:
                checks.append(
                    _result(
                        spec,
                        failed=[],
                        n_total=0,
                        message=reason,
                        status=CheckStatus.ERROR,
                    )
                )
                continue
        try:
            evaluated = spec.failed_rows(frames)
        except (KeyError, pl.exceptions.ColumnNotFoundError) as exc:
            checks.append(
                _result(
                    spec,
                    failed=[],
                    n_total=0,
                    message=f"cannot evaluate {spec.check_id}: {type(exc).__name__}"[:200],
                    status=CheckStatus.ERROR,
                )
            )
            continue
        failed = _jsonable(evaluated)
        n_total = _n_total(spec, frames)
        checks.append(
            _result(spec, failed=failed, n_total=n_total, message=_message(spec, failed, n_total))
        )
    report = QualitySuiteReport(
        run_id=uuid4().hex,
        checks=checks,
        observed_columns=observed_columns(frames),
    )
    return report.model_copy(update={"fingerprint": report_payload_fingerprint(report)})


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
