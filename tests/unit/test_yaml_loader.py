"""YAML registry loader: pydantic models validate adopter files without a fork."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

from airflow_dq_agent import load_registry
from airflow_dq_agent.contracts import CandidateAction, Proposal, QualityEvidence, TargetSet
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS, get_table_contract
from airflow_dq_agent.evals import evaluate_plan, evaluate_proposal
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.quality import run_suite_on_frames
from airflow_dq_agent.quality.registry import CHECK_SPECS, get_check_spec

_INVOICE_YAML = """\
tables:
  - table: ext_invoice
    grain: one row per invoice_id
    description: Adopter-owned invoice header.
    primary_key: [invoice_id]
    columns:
      - name: invoice_id
        dtype: int64
        unique: true
      - name: amount
        dtype: float64
        nullable: true
checks:
  - check_id: ext_invoice.amount.completeness
    table: ext_invoice
    column: amount
    dimension: completeness
    description: amount must be present
    policies:
      - action_id: quarantine_nulls
"""


@pytest.fixture(autouse=True)
def restore_registries() -> None:
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    yield
    TABLE_CONTRACTS.clear()
    TABLE_CONTRACTS.update(contracts)
    CHECK_SPECS.clear()
    CHECK_SPECS.update(checks)


def test_examples_warehouse_yaml_loads() -> None:
    load_registry(Path(__file__).resolve().parents[2] / "examples" / "my_warehouse.yaml")
    assert get_table_contract("ext_invoice").primary_key == ["invoice_id"]
    assert get_check_spec("ext_invoice.amount.completeness").rule_for("quarantine_nulls")


def test_load_registry_registers_tables_and_checks(tmp_path: Path) -> None:
    path = tmp_path / "registry.yaml"
    path.write_text(_INVOICE_YAML)

    load_registry(path)

    contract = get_table_contract("ext_invoice")
    assert contract.primary_key == ["invoice_id"]
    assert contract.column("amount").nullable is True
    spec = get_check_spec("ext_invoice.amount.completeness")
    assert spec.table == "ext_invoice"
    assert spec.rule_for("quarantine_nulls") is not None


def test_load_registry_names_the_offending_field(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text(
        """\
tables:
  - table: ext_invoice
    grain: one row per invoice_id
    description: broken dtype
    primary_key: [invoice_id]
    columns:
      - name: invoice_id
        dtype: not_a_type
"""
    )

    with pytest.raises(ValueError, match=r"bad\.yaml: tables\.0\.columns\.0\.dtype:") as caught:
        load_registry(path)
    assert "not_a_type" in str(caught.value) or "dtype" in str(caught.value)


def test_load_registry_names_a_missing_check_field(tmp_path: Path) -> None:
    path = tmp_path / "bad-check.yaml"
    path.write_text(
        """\
tables:
  - table: ext_invoice
    grain: one row per invoice_id
    description: invoice
    primary_key: [invoice_id]
    columns:
      - name: invoice_id
        dtype: int64
checks:
  - table: ext_invoice
    column: invoice_id
    dimension: completeness
    description: missing check_id
"""
    )

    with pytest.raises(ValueError, match=r"bad-check\.yaml: checks\.0\.check_id:"):
        load_registry(path)


@pytest.mark.parametrize(("section", "field"), [("tables", "table"), ("checks", "check_id")])
def test_load_registry_names_duplicate_registration_field(
    tmp_path: Path, section: str, field: str
) -> None:
    path = tmp_path / "duplicate.yaml"
    document = yaml.safe_load(_INVOICE_YAML)
    document[section].append(document[section][0])
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(ValueError, match=rf"duplicate\.yaml: {section}\.1\.{field}:"):
        load_registry(path)


def test_load_registry_names_unknown_action_field(tmp_path: Path) -> None:
    path = tmp_path / "unknown-action.yaml"
    document = yaml.safe_load(_INVOICE_YAML)
    document["checks"][0]["policies"].append({"action_id": "unknown_action"})
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(
        ValueError, match=r"unknown-action\.yaml: checks\.0\.policies\.1\.action_id:"
    ):
        load_registry(path)


def test_load_registry_names_unknown_table_field(tmp_path: Path) -> None:
    path = tmp_path / "unknown-table.yaml"
    document = yaml.safe_load(_INVOICE_YAML)
    document["checks"][0]["table"] = "missing_table"
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(ValueError, match=r"unknown-table\.yaml: checks\.0\.table:"):
        load_registry(path)


@pytest.mark.parametrize("failure", ["duplicate_table", "missing_check_id", "unknown_action"])
def test_failed_load_preserves_registries_and_allows_corrected_retry(
    tmp_path: Path, failure: str
) -> None:
    path = tmp_path / "retry.yaml"
    contracts = dict(TABLE_CONTRACTS)
    checks = dict(CHECK_SPECS)
    document = yaml.safe_load(_INVOICE_YAML)
    if failure == "duplicate_table":
        document["tables"].append(document["tables"][0])
    else:
        broken_check = dict(document["checks"][0])
        if failure == "missing_check_id":
            del broken_check["check_id"]
        else:
            broken_check["check_id"] = "ext_invoice.amount.invalid_action"
            broken_check["policies"] = [{"action_id": "unknown_action"}]
        document["checks"].append(broken_check)
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(ValueError):
        load_registry(path)

    assert contracts == TABLE_CONTRACTS
    assert checks == CHECK_SPECS
    path.write_text(_INVOICE_YAML)
    load_registry(path)
    assert get_table_contract("ext_invoice").primary_key == ["invoice_id"]
    assert get_check_spec("ext_invoice.amount.completeness").table == "ext_invoice"


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=1, fingerprint="targets:ext-invoice-yaml-v1")


def test_yaml_registry_compiles_and_evaluates(tmp_path: Path) -> None:
    path = tmp_path / "registry.yaml"
    path.write_text(_INVOICE_YAML)
    load_registry(path)

    frames = {"ext_invoice": pl.DataFrame({"invoice_id": [1, 2], "amount": [10.0, None]})}
    report = run_suite_on_frames(frames)
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
    assert plan.items[0].kind == "executable"
    assert plan.items[0].table == "ext_invoice"
    assert evaluate_plan(plan).passed
