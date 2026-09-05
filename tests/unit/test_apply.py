import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

import airflow_dq_agent.action_definitions as action_definitions
from airflow_dq_agent.action_definitions import get_governed_action
from airflow_dq_agent.apply.executor import _set_controlled_transaction_mode, apply_plan
from airflow_dq_agent.contracts.fingerprints import (
    canonical_fingerprint,
    report_payload_fingerprint,
)
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    CandidateAction,
    EvalReport,
    HumanDecision,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.contracts.tables import TABLE_CONTRACTS
from airflow_dq_agent.evals import evaluate_plan
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.planning.admission import create_apply_admission
from airflow_dq_agent.planning.integrity import decision_payload_fingerprint
from airflow_dq_agent.quality.fixtures import seeded_failure_report
from airflow_dq_agent.traces.lineage import apply_result_event
from airflow_dq_agent.warehouse.db import DDL_PATH


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
    def __init__(self, dsn: str = "postgresql+psycopg://dq:dq@localhost:5433/warehouse") -> None:
        self.transaction = _RecordingTransaction()
        self.url = make_url(dsn)

    def begin(self) -> _RecordingTransaction:
        return self.transaction


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:orders-null-v1")


def _audited_approval() -> HumanDecision:
    decision = HumanDecision(
        decision="Approve",
        actor="approver-1",
        note="Reviewed target set.",
        audit_event_id="decision-event-1",
    )
    return decision.model_copy(
        update={
            "fingerprint": decision_payload_fingerprint(
                decision_id=decision.decision_id,
                decision=decision.decision,
                actor=decision.actor,
                note=decision.note,
                decided_at=decision.decided_at,
            )
        }
    )


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
        return type(
            "Result",
            (),
            {"rowcount": 1, "scalar": lambda self: None, "first": lambda self: None},
        )()


class _MutationRecordingTransaction:
    def __init__(self) -> None:
        self.connection = _MutationRecordingConnection()

    def __enter__(self) -> _MutationRecordingConnection:
        return self.connection

    def __exit__(self, *_: object) -> None:
        return None


class _MutationRecordingEngine:
    def __init__(self, dsn: str = "postgresql+psycopg://dq:dq@localhost:5433/warehouse") -> None:
        self.transaction = _MutationRecordingTransaction()
        self.url = make_url(dsn)

    def begin(self) -> _MutationRecordingTransaction:
        return self.transaction


class _NoopAuditSink:
    def append(self, _: object) -> None:
        pass


class _ParamRecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    def execute(self, statement: object, params: dict[str, object] | None = None) -> object:
        self.calls.append((str(statement), params))
        return type(
            "Result",
            (),
            {"rowcount": 1, "scalar": lambda self: None, "first": lambda self: None},
        )()


class _CommitTrackingTransaction:
    def __init__(self) -> None:
        self.connection = _ParamRecordingConnection()
        self.committed = False
        self.rolled_back = False

    def __enter__(self) -> _ParamRecordingConnection:
        return self.connection

    def __exit__(self, exc_type: type[BaseException] | None, *_: object) -> None:
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True


class _CommitTrackingEngine:
    def __init__(self, dsn: str = "postgresql+psycopg://dq:dq@localhost:5433/warehouse") -> None:
        self.transaction = _CommitTrackingTransaction()
        self.url = make_url(dsn)

    def begin(self) -> _CommitTrackingTransaction:
        return self.transaction


class _JsonlFaultAfterCommit:
    """Raises at the real JSONL append seam, but only once the warehouse txn has committed."""

    def __init__(self, transaction: _CommitTrackingTransaction) -> None:
        self._transaction = transaction
        self.events: list[object] = []
        self.append_after_commit = False

    def append(self, event: object) -> None:
        self.append_after_commit = self._transaction.committed
        self.events.append(event)
        raise OSError("jsonl export fault")


class _LockBoomResolver:
    def __init__(self, **_: object) -> None:
        pass

    def resolve_item(self, _: object, item: object) -> TargetSet:
        del item
        raise AssertionError("mutation apply must lock, not resolve")

    def lock_and_resolve(self, _: object, item: object) -> TargetSet:
        del item
        raise RuntimeError("lock failed before mutation")


def _approved_quarantine_plan(
    now: datetime,
) -> tuple[RemediationPlan, EvalReport, ApplyAdmission, QualitySuiteReport]:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped = report.model_copy(update={"checks": [failed]})
    scoped = scoped.model_copy(update={"fingerprint": report_payload_fingerprint(scoped)})
    plan = compile_remediation_plan(
        scoped,
        Proposal(
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
        ),
        target_sets=_TargetSets(),
    )
    evaluation = evaluate_plan(plan)
    admission = create_apply_admission(
        plan,
        evaluation,
        _audited_approval(),
        report=scoped,
        now=now,
    )
    return plan, evaluation, admission, scoped


def test_dry_run_retains_applied_steps_on_the_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    report = seeded_failure_report()
    failed = report.get("fact_orders.total_amount.completeness")
    assert failed is not None
    scoped_report = report.model_copy(update={"checks": [failed]})
    scoped_report = scoped_report.model_copy(
        update={"fingerprint": report_payload_fingerprint(scoped_report)}
    )
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
        report=scoped_report,
        dry_run=True,
        engine=_RecordingEngine(),  # type: ignore[arg-type]
        run_id="unit-dry-run",
    )

    assert len(result.steps) == 1
    assert result.steps[0].estimated_rows == 5


@pytest.mark.parametrize(
    ("check_id", "action_id", "mutates"),
    [
        ("fact_orders.total_amount.completeness", "quarantine_nulls", True),
        ("fact_orders.status.validity", "quarantine_invalids", True),
        ("fact_orders.order_nk.uniqueness", "dedupe_keep_min_pk", True),
        ("fact_order_items.product_sk.referential_integrity", "quarantine_orphans", True),
        ("dim_customer.schema_drift", "schema_drift_ticket", False),
    ],
)
def test_apply_uses_each_governed_action_mutation_capability(
    monkeypatch: pytest.MonkeyPatch,
    check_id: str,
    action_id: str,
    mutates: bool,
) -> None:
    now = datetime(2026, 8, 30, tzinfo=UTC)
    report = seeded_failure_report()
    failed = report.get(check_id)
    assert failed is not None
    scoped_report = report.model_copy(update={"checks": [failed]})
    scoped_report = scoped_report.model_copy(
        update={"fingerprint": report_payload_fingerprint(scoped_report)}
    )
    plan = compile_remediation_plan(
        scoped_report,
        Proposal(
            summary="Apply one governed action.",
            root_cause_hypothesis="A declared check failed.",
            candidate_actions=[
                CandidateAction(
                    action_id=action_id,
                    evidence=[
                        QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                    ],
                    rationale="Request the reviewed action declared by this check.",
                )
            ],
            confidence=0.9,
        ),
        target_sets=_TargetSets(),
    )
    evaluation = evaluate_plan(plan)
    admission = create_apply_admission(
        plan,
        evaluation,
        _audited_approval(),
        report=scoped_report,
        now=now,
    )
    engine = _MutationRecordingEngine()
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver", _MatchingTargetResolver
    )
    monkeypatch.setattr("airflow_dq_agent.apply.executor.JsonlAuditSink", _NoopAuditSink)

    result = apply_plan(
        plan,
        evaluation,
        admission,
        report=scoped_report,
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


def test_jsonl_fault_after_commit_keeps_terminal_apply_success(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    now = datetime(2026, 8, 30, tzinfo=UTC)
    plan, evaluation, admission, report = _approved_quarantine_plan(now)
    engine = _CommitTrackingEngine()
    sink = _JsonlFaultAfterCommit(engine.transaction)
    lineage: list[object] = []
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver", _MatchingTargetResolver
    )
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.append_event",
        lambda event, **_: lineage.append(event),
    )

    with caplog.at_level(logging.WARNING, logger="airflow_dq_agent.apply.executor"):
        result = apply_plan(
            plan,
            evaluation,
            admission,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=now,
            run_id="unit-jsonl-fault",
            audit_sink=sink,
        )

    statements = [sql for sql, _ in engine.transaction.connection.calls]
    apply_records = [params for _, params in engine.transaction.connection.calls if params]
    recorded_kinds = [str(params["kind"]) for params in apply_records if "kind" in params]
    bodies = [json.loads(str(params["body"])) for params in apply_records if "body" in params]
    sink_kinds = [getattr(event, "kind", None) for event in sink.events]
    messages = caplog.text + json.dumps(bodies) + json.dumps(sink_kinds)

    assert engine.transaction.committed
    assert not engine.transaction.rolled_back
    assert sink.append_after_commit
    assert any("record_apply_result" in sql for sql in statements)
    assert any(sql.lstrip().startswith(("INSERT", "UPDATE")) for sql in statements)
    assert result.dry_run is False
    assert result.fingerprint
    assert result.audit_event_id
    assert recorded_kinds == ["apply_succeeded"]
    assert sink_kinds == ["apply_succeeded"]
    assert lineage == []
    assert "apply_failed" not in recorded_kinds
    assert "apply_failed" not in sink_kinds
    assert "export" in caplog.text.lower()
    assert "OSError" in caplog.text
    assert "jsonl export fault" not in caplog.text
    assert "sample_failures" not in messages
    assert "root_cause_hypothesis" not in messages


@pytest.mark.parametrize("fault_type", [OSError, ValueError])
def test_default_jsonl_sink_fault_after_commit_keeps_apply_success(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    fault_type: type[Exception],
) -> None:
    now = datetime(2026, 8, 30, tzinfo=UTC)
    plan, evaluation, admission, report = _approved_quarantine_plan(now)
    engine = _CommitTrackingEngine()
    lineage: list[object] = []
    appended_after_commit: list[bool] = []
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver", _MatchingTargetResolver
    )
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.append_event",
        lambda event, **_: lineage.append(event),
    )

    def _boom(self: object, event: object) -> None:
        del event
        appended_after_commit.append(engine.transaction.committed)
        raise fault_type("jsonl export fault")

    monkeypatch.setattr("airflow_dq_agent.apply.executor.JsonlAuditSink.append", _boom)

    with caplog.at_level(logging.WARNING, logger="airflow_dq_agent.apply.executor"):
        result = apply_plan(
            plan,
            evaluation,
            admission,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=now,
            run_id="unit-jsonl-default-sink-fault",
        )

    assert result.dry_run is False
    assert result.fingerprint
    assert engine.transaction.committed
    assert appended_after_commit == [True]
    assert lineage == []
    assert fault_type.__name__ in caplog.text
    assert "jsonl export fault" not in caplog.text
    assert "sample_failures" not in caplog.text


class _OneShotConnection:
    def __init__(self, consumed: set[str]) -> None:
        self.consumed = consumed
        self.statements: list[str] = []
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    def execute(self, statement: object, params: dict[str, object] | None = None) -> object:
        sql = str(statement)
        self.statements.append(sql)
        self.calls.append((sql, params))
        admission_id = str(params["admission_id"]) if params and "admission_id" in params else None
        if admission_id is not None and "admission_consumed" in sql:
            consumed = admission_id in self.consumed

            class _ConsumedResult:
                def scalar(self) -> bool:
                    return consumed

                def first(self) -> tuple[bool] | None:
                    return (consumed,)

            return _ConsumedResult()
        if admission_id is not None and "record_apply_result" in sql:
            self.consumed.add(admission_id)
        return type(
            "Result",
            (),
            {"rowcount": 1, "scalar": lambda self: None, "first": lambda self: None},
        )()


class _OneShotTransaction:
    def __init__(self, consumed: set[str]) -> None:
        self.connection = _OneShotConnection(consumed)

    def __enter__(self) -> _OneShotConnection:
        return self.connection

    def __exit__(self, *_: object) -> None:
        return None


class _OneShotEngine:
    def __init__(self) -> None:
        self.consumed: set[str] = set()
        self.transaction = _OneShotTransaction(self.consumed)

    def begin(self) -> _OneShotTransaction:
        self.transaction = _OneShotTransaction(self.consumed)
        return self.transaction


def test_apply_refuses_to_consume_the_same_admission_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 30, tzinfo=UTC)
    plan, evaluation, admission, report = _approved_quarantine_plan(now)
    engine = _OneShotEngine()
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver", _MatchingTargetResolver
    )
    monkeypatch.setattr("airflow_dq_agent.apply.executor.JsonlAuditSink", _NoopAuditSink)

    first = apply_plan(
        plan,
        evaluation,
        admission,
        report=report,
        dry_run=False,
        engine=engine,  # type: ignore[arg-type]
        now=now,
        run_id="unit-one-shot-first",
    )
    assert first.steps
    first_mutations = [
        sql for sql in engine.transaction.connection.statements if sql.lstrip().startswith("INSERT")
    ]

    with pytest.raises(PermissionError, match="already been consumed"):
        apply_plan(
            plan,
            evaluation,
            admission,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=now,
            run_id="unit-one-shot-second",
        )

    second_mutations = [
        sql for sql in engine.transaction.connection.statements if sql.lstrip().startswith("INSERT")
    ]
    assert first_mutations
    assert second_mutations == []


def test_pre_commit_apply_failure_still_emits_apply_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 30, tzinfo=UTC)
    plan, evaluation, admission, report = _approved_quarantine_plan(now)
    engine = _CommitTrackingEngine()
    lineage: list[object] = []
    factory_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver", _LockBoomResolver
    )
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.append_event",
        lambda event, **_: lineage.append(event),
    )

    def _spy(*args: object, **kwargs: object) -> object:
        factory_calls.append((args, kwargs))
        return apply_result_event(*args, **kwargs)

    monkeypatch.setattr("airflow_dq_agent.apply.executor.apply_result_event", _spy)

    with pytest.raises(RuntimeError, match="lock failed before mutation"):
        apply_plan(
            plan,
            evaluation,
            admission,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=now,
            audit_sink=_NoopAuditSink(),
        )

    apply_records = [
        params
        for sql, params in engine.transaction.connection.calls
        if params and "record_apply_result" in sql
    ]
    lineage_kinds = [getattr(event, "kind", None) for event in lineage]
    assert engine.transaction.rolled_back
    assert not engine.transaction.committed
    assert apply_records == []
    assert lineage_kinds == ["apply_failed"]
    assert len(factory_calls) == 1
    _, kwargs = factory_calls[0]
    assert kwargs["dry_run"] is False
    assert kwargs["failed"] is True
    failure = lineage[0]
    body = json.dumps(failure.model_dump(mode="json"))  # type: ignore[attr-defined]
    assert "sample_failures" not in body
    assert getattr(failure, "reasons", []) == [
        "controlled apply failed before a result could be admitted"
    ]
    assert failure.fingerprint == canonical_fingerprint(  # type: ignore[attr-defined]
        failure.model_dump(mode="json", exclude={"fingerprint"})  # type: ignore[attr-defined]
    )


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


def test_ddl_does_not_grant_dq_audit_select_on_traces() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    assert "GRANT SELECT ON dq.traces TO dq_audit" not in ddl
    assert "GRANT INSERT ON dq.traces, dq.check_runs TO dq_audit" in ddl
