"""Deterministic quality suite. Polars + Pandera + SQL. The agent does not run this."""

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
from airflow_dq_agent.quality.pandera_schemas import PANDERA_MODELS
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


def check_schema_drift(frames: Mapping[str, pl.DataFrame]) -> list[CheckResult]:
    out: list[CheckResult] = []
    for table, contract in TABLE_CONTRACTS.items():
        df = frames[table]
        expected = set(contract.column_names)
        observed = set(df.columns)
        extra = sorted(observed - expected)
        missing = sorted(expected - observed)
        spec = CHECK_SPECS.get(
            f"{table}.schema_drift",
            CheckSpec(
                check_id=f"{table}.schema_drift",
                table=table,
                column=None,
                dimension=Dimension.SCHEMA_DRIFT,
                description="observed columns must match TABLE_CONTRACTS",
                sample_sql="",
                contract_id=contract.contract_id,
            ),
        )
        failed_rows = [{"kind": "extra", "column": c} for c in extra] + [
            {"kind": "missing", "column": c} for c in missing
        ]
        n_total = len(expected | observed)
        if failed_rows:
            msg = f"drift on {table}: extra={extra} missing={missing}"
        else:
            msg = f"{table} matches contract ({len(expected)} columns)"
        out.append(_result(spec, failed=failed_rows, n_total=n_total, message=msg))
    return out


def check_completeness(frames: Mapping[str, pl.DataFrame]) -> list[CheckResult]:
    targets = [
        ("fact_orders", "total_amount"),
        ("dim_patient", "sex"),
        ("fact_adverse_events", "term_code"),
    ]
    out: list[CheckResult] = []
    for table, column in targets:
        spec = CHECK_SPECS[f"{table}.{column}.completeness"]
        df = frames[table]
        failed = _jsonable(df.filter(pl.col(column).is_null()))
        out.append(
            _result(
                spec,
                failed=failed,
                n_total=df.height,
                message=f"{failed.height}/{df.height} null {column} on {table}",
            )
        )
    return out


def check_validity(frames: Mapping[str, pl.DataFrame]) -> list[CheckResult]:
    out: list[CheckResult] = []

    orders = frames["fact_orders"]
    spec = CHECK_SPECS["fact_orders.status.validity"]
    allowed = set(get_table_contract("fact_orders").column("status").allowed_values or [])
    failed = _jsonable(orders.filter(~pl.col("status").is_in(sorted(allowed))))
    out.append(
        _result(
            spec, failed=failed, n_total=orders.height, message=f"{failed.height} illegal statuses"
        )
    )

    customers = frames["dim_customer"]
    spec = CHECK_SPECS["dim_customer.email.validity"]
    failed = _jsonable(customers.filter(~pl.col("email").str.contains("@").fill_null(False)))
    out.append(
        _result(
            spec,
            failed=failed,
            n_total=customers.height,
            message=f"{failed.height} emails missing @",
        )
    )

    visits = frames["fact_visits"]
    spec = CHECK_SPECS["fact_visits.visit_date.validity"]
    failed = _jsonable(
        visits.filter(
            pl.col("visit_date").is_not_null()
            & (
                (pl.col("visit_date") < pl.col("window_start"))
                | (pl.col("visit_date") > pl.col("window_end"))
            )
        )
    )
    out.append(
        _result(
            spec,
            failed=failed,
            n_total=visits.height,
            message=f"{failed.height} visits outside window",
        )
    )

    aes = frames["fact_adverse_events"]
    spec = CHECK_SPECS["fact_adverse_events.severity.validity"]
    allowed_sev = set(
        get_table_contract("fact_adverse_events").column("severity").allowed_values or []
    )
    failed = _jsonable(aes.filter(~pl.col("severity").is_in(sorted(allowed_sev))))
    out.append(
        _result(
            spec, failed=failed, n_total=aes.height, message=f"{failed.height} illegal severities"
        )
    )
    return out


def check_uniqueness(frames: Mapping[str, pl.DataFrame]) -> list[CheckResult]:
    out: list[CheckResult] = []

    orders = frames["fact_orders"]
    spec = CHECK_SPECS["fact_orders.order_nk.uniqueness"]
    dup_keys = (
        orders.group_by(["customer_sk", "order_ts"])
        .len()
        .filter(pl.col("len") > 1)
        .select(["customer_sk", "order_ts"])
    )
    failed = _jsonable(orders.join(dup_keys, on=["customer_sk", "order_ts"], how="inner"))
    out.append(
        _result(
            spec,
            failed=failed,
            n_total=orders.height,
            message=f"{failed.height} rows share (customer_sk, order_ts)",
        )
    )

    patients = frames["dim_patient"]
    spec = CHECK_SPECS["dim_patient.subject_id.uniqueness"]
    dup_ids = patients.group_by("subject_id").len().filter(pl.col("len") > 1).select("subject_id")
    failed = _jsonable(patients.join(dup_ids, on="subject_id", how="inner"))
    out.append(
        _result(
            spec,
            failed=failed,
            n_total=patients.height,
            message=f"{failed.height} rows share a subject_id",
        )
    )

    products = frames["dim_product"]
    spec = CHECK_SPECS["dim_product.sku.uniqueness"]
    dup_sku = products.group_by("sku").len().filter(pl.col("len") > 1).select("sku")
    failed = _jsonable(products.join(dup_sku, on="sku", how="inner"))
    out.append(
        _result(
            spec, failed=failed, n_total=products.height, message=f"{failed.height} sku collisions"
        )
    )
    return out


def check_referential(frames: Mapping[str, pl.DataFrame]) -> list[CheckResult]:
    pairs = [
        ("fact_order_items", "product_sk", "dim_product", "product_sk", "order_item_id"),
        ("fact_orders", "customer_sk", "dim_customer", "customer_sk", "order_id"),
        ("fact_visits", "patient_sk", "dim_patient", "patient_sk", "visit_id"),
        ("dim_patient", "site_sk", "dim_site", "site_sk", "patient_sk"),
        ("fact_adverse_events", "patient_sk", "dim_patient", "patient_sk", "ae_id"),
    ]
    out: list[CheckResult] = []
    for table, fk, ref_table, ref_col, pk in pairs:
        spec = CHECK_SPECS[f"{table}.{fk}.referential_integrity"]
        left = frames[table]
        right = frames[ref_table].select(pl.col(ref_col).alias("_ref"))
        failed = _jsonable(
            left.join(right, left_on=fk, right_on="_ref", how="anti").select([pk, fk])
        )
        out.append(
            _result(
                spec,
                failed=failed,
                n_total=left.height,
                message=f"{failed.height} orphan {fk} on {table}",
            )
        )
    return out


def check_pandera(frames: Mapping[str, pl.DataFrame]) -> list[CheckResult]:
    """Run DataFrameModel.validate as a contract-level parse. Failures become error/fail rows."""
    out: list[CheckResult] = []
    for table, model in PANDERA_MODELS.items():
        df = frames[table]
        spec = CheckSpec(
            check_id=f"{table}.pandera_schema",
            table=table,
            column=None,
            dimension=Dimension.VALIDITY,
            description="Pandera DataFrameModel.parse",
            sample_sql="",
            contract_id=get_table_contract(table).contract_id,
        )
        try:
            model.validate(df, lazy=True)
            out.append(
                _result(spec, failed=[], n_total=df.height, message=f"Pandera accepted {table}")
            )
        except Exception as exc:  # pandera SchemaError variants differ by version
            out.append(
                _result(
                    spec,
                    failed=[{"error": str(exc)[:500]}],
                    n_total=df.height,
                    message=f"Pandera rejected {table}: {exc.__class__.__name__}",
                    status=CheckStatus.FAIL,
                )
            )
    return out


def run_suite_on_frames(frames: Mapping[str, pl.DataFrame]) -> QualitySuiteReport:
    checks: list[CheckResult] = []
    checks.extend(check_completeness(frames))
    checks.extend(check_validity(frames))
    checks.extend(check_uniqueness(frames))
    checks.extend(check_referential(frames))
    checks.extend(check_schema_drift(frames))
    checks.extend(check_pandera(frames))
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
