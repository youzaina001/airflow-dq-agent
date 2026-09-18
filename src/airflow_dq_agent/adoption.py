"""Packaged adopter example: one external invoice table and restricted logins."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url

from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.tables import (
    TABLE_CONTRACTS,
    ColumnContract,
    TableContract,
    register_contract,
)
from airflow_dq_agent.quality.registry import CHECK_SPECS, CheckPolicy, CheckSpec, register_check
from airflow_dq_agent.warehouse.db import apply_ddl, make_engine

EXTERNAL_INVOICE_TABLE = "ext_invoice"
EXTERNAL_INVOICE_CHECK_ID = "ext_invoice.amount.completeness"
COMPLETE_INVOICE_IDS = (101, 103)
MISSING_INVOICE_IDS = (102, 104)

_DDL_PATH = Path(__file__).with_name("demo") / "ddl.sql"

READ_LOGIN = "dq_read_login"
AUDIT_LOGIN = "dq_audit_login"
APPLY_LOGIN = "dq_apply_login"


@dataclass(frozen=True)
class RestrictedCredentials:
    owner_dsn: str
    read_dsn: str
    audit_dsn: str
    apply_dsn: str


def login_dsn(admin_dsn: str, *, user: str, password: str) -> str:
    """Rewrite a DSN to a login role without changing host/database identity."""
    return (
        make_url(admin_dsn)
        .set(username=user, password=password)
        .render_as_string(hide_password=False)
    )


def register_external_invoice() -> None:
    """Register the example invoice contract and completeness Check Policy."""
    if EXTERNAL_INVOICE_TABLE not in TABLE_CONTRACTS:
        register_contract(
            TableContract(
                table=EXTERNAL_INVOICE_TABLE,
                grain="one row per invoice_id",
                description="Adopter-owned invoice header.",
                primary_key=["invoice_id"],
                columns=[
                    ColumnContract(name="invoice_id", dtype="int64", unique=True),
                    ColumnContract(name="amount", dtype="float64", nullable=True),
                ],
            )
        )
    if EXTERNAL_INVOICE_CHECK_ID not in CHECK_SPECS:
        register_check(
            CheckSpec(
                check_id=EXTERNAL_INVOICE_CHECK_ID,
                table=EXTERNAL_INVOICE_TABLE,
                column="amount",
                dimension=Dimension.COMPLETENESS,
                description="amount must be present",
                policies=[CheckPolicy(action_id="quarantine_nulls")],
            )
        )


def apply_governance_schema(dsn: str) -> None:
    """Install dq schema, quarantine storage, and capability roles from the packaged DDL."""
    apply_ddl(make_engine(dsn), _DDL_PATH.read_text(encoding="utf-8"))


def seed_external_invoice(dsn: str) -> None:
    """Create the example table with complete and missing amounts; source stays as seeded."""
    engine = make_engine(dsn)
    rows: list[dict[str, Any]] = [
        {"invoice_id": 101, "amount": 10.0},
        {"invoice_id": 102, "amount": None},
        {"invoice_id": 103, "amount": 20.0},
        {"invoice_id": 104, "amount": None},
    ]
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS warehouse.ext_invoice (
                    invoice_id BIGINT PRIMARY KEY,
                    amount DOUBLE PRECISION
                )
                """
            )
        )
        connection.execute(text("TRUNCATE warehouse.ext_invoice"))
        connection.execute(
            text(
                "INSERT INTO warehouse.ext_invoice (invoice_id, amount) "
                "VALUES (:invoice_id, :amount)"
            ),
            rows,
        )
        connection.execute(text("GRANT SELECT ON warehouse.ext_invoice TO dq_read, dq_apply"))
        connection.execute(
            text("REVOKE INSERT, UPDATE, DELETE ON warehouse.ext_invoice FROM dq_read, dq_apply")
        )


def provision_restricted_logins(admin_dsn: str) -> RestrictedCredentials:
    """Create LOGIN roles that inherit the packaged read, audit, and apply grants."""
    engine = make_engine(admin_dsn)
    database = make_url(admin_dsn).database or "warehouse"
    passwords = {
        READ_LOGIN: secrets.token_hex(8),
        AUDIT_LOGIN: secrets.token_hex(8),
        APPLY_LOGIN: secrets.token_hex(8),
    }
    memberships = {
        READ_LOGIN: "dq_read",
        AUDIT_LOGIN: "dq_audit",
        APPLY_LOGIN: "dq_apply",
    }
    with engine.begin() as connection:
        for login, capability in memberships.items():
            password = passwords[login]
            connection.execute(
                text(
                    f"""
                    DO $login$
                    BEGIN
                      IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{login}') THEN
                        CREATE ROLE {login} LOGIN PASSWORD '{password}' INHERIT;
                      ELSE
                        ALTER ROLE {login} WITH LOGIN PASSWORD '{password}';
                      END IF;
                    END
                    $login$;
                    """
                )
            )
            connection.execute(text(f"GRANT {capability} TO {login}"))
            connection.execute(text(f'GRANT CONNECT ON DATABASE "{database}" TO {login}'))
    return RestrictedCredentials(
        owner_dsn=admin_dsn,
        read_dsn=login_dsn(admin_dsn, user=READ_LOGIN, password=passwords[READ_LOGIN]),
        audit_dsn=login_dsn(admin_dsn, user=AUDIT_LOGIN, password=passwords[AUDIT_LOGIN]),
        apply_dsn=login_dsn(admin_dsn, user=APPLY_LOGIN, password=passwords[APPLY_LOGIN]),
    )
