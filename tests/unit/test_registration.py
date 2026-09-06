"""Registration seam: adopters add table contracts and check specs without forking."""

from __future__ import annotations

import re

import polars as pl
import pytest

from airflow_dq_agent import register_check, register_contract
from airflow_dq_agent.contracts import CandidateAction, Proposal, QualityEvidence, TargetSet
from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.tables import (
    TABLE_CONTRACTS,
    ColumnContract,
    TableContract,
    get_table_contract,
)
from airflow_dq_agent.evals import evaluate_plan, evaluate_proposal
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import (
    CHECK_SPECS,
    CheckPolicy,
    CheckSpec,
    get_check_spec,
)


@pytest.fixture(autouse=True)
def restore_registries() -> None:
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    yield
    TABLE_CONTRACTS.clear()
    TABLE_CONTRACTS.update(contracts)
    CHECK_SPECS.clear()
    CHECK_SPECS.update(checks)


def _invoice_contract() -> TableContract:
    return TableContract(
        table="ext_invoice",
        grain="one row per invoice_id",
        description="Adopter-owned invoice header.",
        primary_key=["invoice_id"],
        columns=[
            ColumnContract(name="invoice_id", dtype="int64", unique=True),
            ColumnContract(name="amount", dtype="float64", nullable=True),
        ],
    )


def test_register_contract_is_retrievable() -> None:
    with pytest.raises(KeyError, match="ext_invoice"):
        get_table_contract("ext_invoice")

    register_contract(_invoice_contract())

    loaded = get_table_contract("ext_invoice")
    assert loaded.table == "ext_invoice"
    assert loaded.primary_key == ["invoice_id"]
    assert loaded.column("amount").dtype == "float64"


def test_register_contract_rejects_duplicate_table() -> None:
    register_contract(_invoice_contract())
    with pytest.raises(ValueError, match="ext_invoice"):
        register_contract(_invoice_contract())


def _invoice_completeness() -> CheckSpec:
    return CheckSpec(
        check_id="ext_invoice.amount.completeness",
        table="ext_invoice",
        column="amount",
        dimension=Dimension.COMPLETENESS,
        description="amount must be present",
        policies=[CheckPolicy(action_id="quarantine_nulls")],
    )


def test_register_check_is_retrievable() -> None:
    register_contract(_invoice_contract())
    with pytest.raises(KeyError, match=re.escape("ext_invoice.amount.completeness")):
        get_check_spec("ext_invoice.amount.completeness")

    register_check(_invoice_completeness())

    loaded = get_check_spec("ext_invoice.amount.completeness")
    assert loaded.table == "ext_invoice"
    assert loaded.column == "amount"
    assert loaded.dimension is Dimension.COMPLETENESS
    assert loaded.rule_for("quarantine_nulls") is not None


def test_register_check_rejects_duplicate_check_id() -> None:
    register_contract(_invoice_contract())
    register_check(_invoice_completeness())
    with pytest.raises(ValueError, match=re.escape("ext_invoice.amount.completeness")):
        register_check(_invoice_completeness())


def _invoice_frames() -> dict[str, pl.DataFrame]:
    return {
        "ext_invoice": pl.DataFrame(
            {
                "invoice_id": [1, 2],
                "amount": [10.0, None],
            }
        )
    }


def test_suite_evaluates_a_registered_check() -> None:
    register_contract(_invoice_contract())
    register_check(_invoice_completeness())

    report = run_suite_on_frames(_invoice_frames())
    check = report.get("ext_invoice.amount.completeness")
    assert check is not None
    assert check.failed
    assert check.n_failed == 1
    assert check.contract_id == "warehouse.ext_invoice"


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=1, fingerprint="targets:ext-invoice-null-v1")


def test_registered_check_compiles_and_evaluates() -> None:
    register_contract(_invoice_contract())
    register_check(_invoice_completeness())
    report = run_suite_on_frames(_invoice_frames())
    failed = report.get("ext_invoice.amount.completeness")
    assert failed is not None
    scoped = report.model_copy(update={"checks": [failed]})
    candidate = Proposal(
        summary="Quarantine invoices with a missing amount.",
        root_cause_hypothesis="The source omitted a required amount.",
        candidate_actions=[
            CandidateAction(
                action_id="quarantine_nulls",
                evidence=[
                    QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                ],
                rationale="Preserve source rows while routing the failed target set for review.",
            )
        ],
        confidence=0.8,
    )

    assert evaluate_proposal(scoped, candidate).passed
    plan = compile_remediation_plan(scoped, candidate, target_sets=_TargetSets())
    assert plan.blocked is False
    item = plan.items[0]
    assert item.kind == "executable"
    assert item.action_id == "quarantine_nulls"
    assert item.table == "ext_invoice"
    assert item.params == {"column": "amount", "pk_column": "invoice_id"}
    assert evaluate_plan(plan).passed
