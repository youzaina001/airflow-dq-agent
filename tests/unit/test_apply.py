from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import airflow_dq_agent.action_definitions as action_definitions
from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.apply.executor import _set_controlled_transaction_mode, apply_plan
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    CandidateAction,
    EvalReport,
    ExecutablePlanItem,
    Proposal,
    QualityEvidence,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.evals import evaluate_plan
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.quality.fixtures import seeded_failure_report


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object) -> None:
        self.statements.append(str(statement))


class _RecordingTransaction:
    def __init__(self) -> None:
        self.connection = _RecordingConnection()

    def __enter__(self) -> _RecordingConnection:
        return self.connection

    def __exit__(self, *_: object) -> None:
        return None


class _RecordingEngine:
    def __init__(self) -> None:
        self.transaction = _RecordingTransaction()

    def begin(self) -> _RecordingTransaction:
        return self.transaction


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:orders-null-v1")


class _MatchingTargetResolver:
    def __init__(self, **_: object) -> None:
        pass

    def resolve_item(self, _: object, item: object) -> TargetSet:
        return item.target_set  # type: ignore[union-attr]

    def lock_and_resolve(self, _: object, item: object) -> TargetSet:
        return item.target_set  # type: ignore[union-attr]


class _MutationRecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object, *_: object) -> object:
        self.statements.append(str(statement))
        return type("Result", (), {"rowcount": 1})()


class _MutationRecordingTransaction:
    def __init__(self) -> None:
        self.connection = _MutationRecordingConnection()

    def __enter__(self) -> _MutationRecordingConnection:
        return self.connection

    def __exit__(self, *_: object) -> None:
        return None


class _MutationRecordingEngine:
    def __init__(self) -> None:
        self.transaction = _MutationRecordingTransaction()

    def begin(self) -> _MutationRecordingTransaction:
        return self.transaction


class _NoopAuditSink:
    def append(self, _: object) -> None:
        pass


def test_dry_run_retains_applied_steps_on_the_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped_report = report.model_copy(update={"checks": [failed]})
    candidate = Proposal(
        summary="Quarantine rows with missing totals.",
        root_cause_hypothesis="The source omitted a required value.",
        candidate_actions=[
            CandidateAction(
                action_id="quarantine_nulls",
                evidence=[
                    QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                ],
                rationale="Preserve source rows for review.",
            )
        ],
        confidence=0.9,
    )
    plan = compile_remediation_plan(scoped_report, candidate, target_sets=_TargetSets())
    evaluation = evaluate_plan(plan)
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver", _MatchingTargetResolver
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))

    result = apply_plan(
        plan,
        evaluation,
        dry_run=True,
        engine=_RecordingEngine(),  # type: ignore[arg-type]
        run_id="unit-dry-run",
    )

    assert len(result.steps) == 1
    assert result.steps[0].estimated_rows == 5


@pytest.mark.parametrize(
    ("action_id", "table", "params", "mutates"),
    [
        (
            "no_op_alert",
            "fact_orders",
            {"check_id": "fact_orders.total_amount.completeness"},
            False,
        ),
        (
            "quarantine_nulls",
            "fact_orders",
            {"column": "total_amount", "pk_column": "order_id"},
            True,
        ),
        (
            "quarantine_invalids",
            "fact_orders",
            {
                "check_id": "fact_orders.status.validity",
                "column": "status",
                "pk_column": "order_id",
            },
            True,
        ),
        ("null_fill", "fact_orders", {"column": "total_amount", "fill_value": 0.0}, True),
        (
            "quarantine_orphans",
            "fact_orders",
            {
                "fk_column": "customer_sk",
                "ref_table": "dim_customer",
                "ref_column": "customer_sk",
                "pk_column": "order_id",
            },
            True,
        ),
        (
            "dedupe_keep_min_pk",
            "fact_orders",
            {"business_key": ["customer_sk", "order_ts"], "pk_column": "order_id"},
            True,
        ),
        ("schema_drift_ticket", "fact_orders", {"check_id": "fact_orders.schema_drift"}, False),
    ],
)
def test_apply_uses_each_governed_action_mutation_capability(
    monkeypatch: pytest.MonkeyPatch,
    action_id: str,
    table: str,
    params: dict[str, object],
    mutates: bool,
) -> None:
    now = datetime(2026, 8, 30, tzinfo=UTC)
    target_set = TargetSet(count=0, fingerprint=f"targets:{action_id}")
    item = ExecutablePlanItem(
        item_id=f"item:{action_id}",
        action_id=action_id,
        table=table,
        params=params,
        evidence=[
            QualityEvidence(
                check_id="fact_orders.total_amount.completeness",
                contract_id="warehouse.fact_orders",
            )
        ],
        target_set=target_set,
        policy_fingerprint=f"policy:{action_id}",
    )
    plan = RemediationPlan(
        plan_id=f"plan:{action_id}",
        quality_run_id="quality-run",
        candidate_fingerprint="candidate",
        policy_fingerprint=f"policy:{action_id}",
        items=[item],
        blocked=False,
        fingerprint=f"plan-fingerprint:{action_id}",
    )
    evaluation = EvalReport(
        evaluation_id=f"evaluation:{action_id}",
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        policy_fingerprint=plan.policy_fingerprint,
        fingerprint=f"evaluation-fingerprint:{action_id}",
        passed=True,
        scores=[],
    )
    admission = ApplyAdmission(
        quality_run_id=plan.quality_run_id,
        plan_id=plan.plan_id,
        plan_fingerprint=plan.fingerprint,
        evaluation_id=evaluation.evaluation_id,
        evaluation_fingerprint=evaluation.fingerprint,
        decision_id="decision",
        decision_event_id="decision-event",
        policy_fingerprint=plan.policy_fingerprint,
        expires_at=now + timedelta(hours=1),
        fingerprint=f"admission:{action_id}",
    )
    engine = _MutationRecordingEngine()
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver", _MatchingTargetResolver
    )
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.current_policy_fingerprint",
        lambda _: plan.policy_fingerprint,
    )
    monkeypatch.setattr("airflow_dq_agent.apply.executor.JsonlAuditSink", _NoopAuditSink)

    result = apply_plan(
        plan,
        evaluation,
        admission,
        dry_run=False,
        engine=engine,  # type: ignore[arg-type]
        now=now,
    )

    executed_mutations = [
        statement
        for statement in engine.transaction.connection.statements
        if statement.lstrip().startswith(("INSERT", "UPDATE"))
    ]
    assert len(executed_mutations) == int(mutates)
    assert result.steps[0].rowcount == (1 if mutates else 0)


def test_apply_uses_a_serializable_snapshot_before_target_locking() -> None:
    connection = _RecordingConnection()

    _set_controlled_transaction_mode(connection, dry_run=False)

    assert connection.statements == ["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"]


def test_dry_run_uses_the_same_serializable_snapshot_and_read_only_authority() -> None:
    connection = _RecordingConnection()

    _set_controlled_transaction_mode(connection, dry_run=True)

    assert connection.statements == [
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "SET TRANSACTION READ ONLY",
    ]


def test_renderer_uses_compound_key_and_ignores_claimed_row_count() -> None:
    rendered = get_governed_action("dedupe_keep_min_pk").render(
        table="fact_orders",
        params={"business_key": ["customer_sk", "order_ts"], "pk_column": "order_id"},
        run_id="test-run",
    )
    assert 'GROUP BY s."customer_sk", s."order_ts"' in rendered.sql


def test_quarantine_renderer_binds_json_primary_key_as_text() -> None:
    rendered = get_governed_action("quarantine_nulls").render(
        table="fact_orders",
        params={"column": "total_amount", "pk_column": "order_id"},
        run_id="test-run",
    )

    assert 'jsonb_build_object(CAST(:pk_key AS text), t."order_id")' in rendered.sql


def test_rendering_rejects_composite_primary_key_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    composite_contract = TABLE_CONTRACTS["fact_orders"].model_copy(
        update={"primary_key": ["order_id", "customer_sk"]}
    )
    monkeypatch.setattr(action_definitions, "get_table_contract", lambda _: composite_contract)

    with pytest.raises(ValueError, match="composite primary key"):
        get_governed_action("quarantine_nulls").render(
            table="fact_orders",
            params={"column": "total_amount", "pk_column": "order_id"},
            run_id="test-run",
        )


def test_governed_action_rejects_unknown_column() -> None:
    with pytest.raises(ValueError, match="not_a_column"):
        get_governed_action("null_fill").render(
            table="fact_orders",
            params={"column": "not_a_column", "fill_value": 0.0},
            run_id="test-run",
        )


def test_null_fill_requires_contract_compatible_value() -> None:
    with pytest.raises(ValueError, match="float64"):
        get_governed_action("null_fill").render(
            table="fact_orders",
            params={"column": "total_amount", "fill_value": "not-a-float"},
            run_id="test-run",
        )
