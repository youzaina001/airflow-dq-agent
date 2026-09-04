from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from airflow_dq_agent.apply.executor import apply_plan
from airflow_dq_agent.contracts.fingerprints import canonical_fingerprint
from airflow_dq_agent.contracts.models import (
    ApplyAdmission,
    CandidateAction,
    EvalReport,
    ExecutablePlanItem,
    HumanDecision,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
    RemediationPlan,
    TargetSet,
)
from airflow_dq_agent.evals import evaluate_plan
from airflow_dq_agent.planning import compile_remediation_plan, current_policy_fingerprint
from airflow_dq_agent.planning.admission import create_apply_admission
from airflow_dq_agent.planning.integrity import (
    admission_payload_fingerprint,
    evaluation_payload_fingerprint,
    plan_payload_fingerprint,
)
from airflow_dq_agent.quality.fixtures import seeded_failure_report

NOW = datetime(2026, 8, 30, tzinfo=UTC)


class _TargetSets:
    def resolve(self, **_: object) -> TargetSet:
        return TargetSet(count=5, fingerprint="targets:orders-null-v1")


def _target_set_from_received_params(params: dict[str, object]) -> TargetSet:
    column = str(params.get("column", "unknown"))
    return TargetSet(count=len(column), fingerprint=f"targets:from-params:{column}")


class _ParamsDerivedTargetResolver:
    """Re-resolve from received params so echoing item.target_set cannot hide F1."""

    def __init__(self, **_: object) -> None:
        pass

    def _resolve(self, item: object) -> TargetSet:
        params = getattr(item, "params", {})
        if not isinstance(params, dict):
            params = {}
        return _target_set_from_received_params(params)

    def resolve_item(self, _: object, item: object) -> TargetSet:
        return self._resolve(item)

    def lock_and_resolve(self, _: object, item: object) -> TargetSet:
        return self._resolve(item)


class _RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object, *_: object) -> object:
        self.statements.append(str(statement))
        return type("Result", (), {"rowcount": 1})()


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


def _decision() -> HumanDecision:
    return HumanDecision(
        decision="Approve",
        actor="approver-1",
        note="Reviewed target set.",
        audit_event_id="decision-event-1",
    )


def _compile_evaluated(
    *check_actions: tuple[str, str],
) -> tuple[RemediationPlan, EvalReport, QualitySuiteReport]:
    report = seeded_failure_report()
    checks = []
    actions = []
    for check_id, action_id in check_actions:
        failed = report.get(check_id)
        assert failed is not None
        checks.append(failed)
        actions.append(
            CandidateAction(
                action_id=action_id,
                evidence=[
                    QualityEvidence(check_id=failed.check_id, contract_id=failed.contract_id)
                ],
                rationale="Preserve source rows for review.",
            )
        )
    scoped = report.model_copy(update={"checks": checks})
    plan = compile_remediation_plan(
        scoped,
        Proposal(
            summary="Quarantine failed rows.",
            root_cause_hypothesis="A required value was omitted.",
            candidate_actions=actions,
            confidence=0.9,
        ),
        target_sets=_TargetSets(),
    )
    evaluation = evaluate_plan(plan)
    assert evaluation.passed
    return plan, evaluation, scoped


def _first_item(plan: RemediationPlan) -> ExecutablePlanItem:
    item = plan.items[0]
    assert isinstance(item, ExecutablePlanItem)
    return item


def _replace_first_item(plan: RemediationPlan, **updates: Any) -> RemediationPlan:
    replaced = _first_item(plan).model_copy(update=updates)
    return plan.model_copy(update={"items": [replaced, *plan.items[1:]]})


def _assert_refusal_is_safe(exc: BaseException) -> None:
    message = str(exc)
    lowered = message.lower()
    assert "sample" not in lowered
    assert "prompt" not in lowered
    assert "9001" not in message
    assert "secret" not in lowered


def test_evaluate_plan_refuses_mismatched_plan_fingerprint() -> None:
    plan, _evaluation, _report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    item = _first_item(plan)
    tampered = _replace_first_item(plan, params={**item.params, "column": "status"})
    assert tampered.fingerprint == plan.fingerprint
    with pytest.raises(PermissionError, match="received payload") as refused:
        evaluate_plan(tampered)
    _assert_refusal_is_safe(refused.value)


def test_admission_and_apply_refuse_column_and_target_set_tamper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    admission = create_apply_admission(plan, evaluation, _decision(), now=NOW, report=report)
    item = _first_item(plan)
    tampered_params = {**item.params, "column": "status"}
    tampered = _replace_first_item(
        plan,
        params=tampered_params,
        target_set=_target_set_from_received_params(tampered_params),
    )
    assert tampered.fingerprint == plan.fingerprint
    assert _first_item(tampered).params["column"] == "status"

    with pytest.raises(PermissionError, match="received payload") as admitted:
        create_apply_admission(tampered, evaluation, _decision(), now=NOW, report=report)
    _assert_refusal_is_safe(admitted.value)

    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver",
        _ParamsDerivedTargetResolver,
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))
    engine = _RecordingEngine()
    with pytest.raises(PermissionError, match="received payload") as applied:
        apply_plan(
            tampered,
            evaluation,
            admission,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=NOW,
            run_id="unit-tamper",
        )
    _assert_refusal_is_safe(applied.value)
    rendered = " ".join(engine.transaction.connection.statements)
    assert 't."status" IS NULL' not in rendered


def test_admission_and_apply_refuse_rehashed_plan_retargeting_catalogued_check(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    origin_plan, _origin_evaluation, origin_report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    other_plan, _other_evaluation, _other_report = _compile_evaluated(
        ("dim_customer.email.validity", "quarantine_invalids")
    )
    stolen = _first_item(other_plan)
    retargeted = origin_plan.model_copy(update={"items": [stolen]})
    retargeted = retargeted.model_copy(
        update={"policy_fingerprint": current_policy_fingerprint(retargeted)}
    )
    retargeted = retargeted.model_copy(
        update={
            "fingerprint": plan_payload_fingerprint(
                plan_id=retargeted.plan_id,
                quality_run_id=retargeted.quality_run_id,
                candidate_fingerprint=retargeted.candidate_fingerprint,
                policy_fingerprint=retargeted.policy_fingerprint,
                items=retargeted.items,
            )
        }
    )
    evaluation = evaluate_plan(retargeted)
    assert evaluation.passed
    assert stolen.table == "dim_customer"
    assert stolen.evidence[0].check_id == "dim_customer.email.validity"
    assert stolen.params["column"] == "email"
    assert retargeted.quality_run_id == origin_plan.quality_run_id
    assert retargeted.fingerprint != origin_plan.fingerprint

    with pytest.raises(PermissionError, match="quality") as admitted:
        create_apply_admission(retargeted, evaluation, _decision(), now=NOW, report=origin_report)
    _assert_refusal_is_safe(admitted.value)

    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver",
        _ParamsDerivedTargetResolver,
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))
    engine = _RecordingEngine()
    with pytest.raises(PermissionError, match="quality") as applied:
        apply_plan(
            retargeted,
            evaluation,
            report=origin_report,
            dry_run=True,
            engine=engine,  # type: ignore[arg-type]
            now=NOW,
            run_id="unit-rehash-retarget",
        )
    _assert_refusal_is_safe(applied.value)
    rendered = " ".join(engine.transaction.connection.statements)
    assert 't."email"' not in rendered


def test_apply_refuses_params_not_derived_from_check_policy_even_after_rehash(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    item = _first_item(plan)
    tampered = _replace_first_item(plan, params={**item.params, "column": "status"})
    tampered = tampered.model_copy(
        update={
            "fingerprint": plan_payload_fingerprint(
                plan_id=tampered.plan_id,
                quality_run_id=tampered.quality_run_id,
                candidate_fingerprint=tampered.candidate_fingerprint,
                policy_fingerprint=tampered.policy_fingerprint,
                items=tampered.items,
            )
        }
    )
    evaluation = evaluation.model_copy(update={"plan_fingerprint": tampered.fingerprint})
    evaluation = evaluation.model_copy(
        update={
            "fingerprint": evaluation_payload_fingerprint(
                evaluation_id=evaluation.evaluation_id,
                plan_id=evaluation.plan_id,
                plan_fingerprint=evaluation.plan_fingerprint,
                passed=evaluation.passed,
                scores=evaluation.scores,
                blocked_reasons=evaluation.blocked_reasons,
            )
        }
    )
    with pytest.raises(PermissionError, match="Check Policy") as admitted:
        create_apply_admission(tampered, evaluation, _decision(), now=NOW, report=report)
    _assert_refusal_is_safe(admitted.value)

    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver",
        _ParamsDerivedTargetResolver,
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))
    engine = _RecordingEngine()
    with pytest.raises(PermissionError, match="Check Policy") as applied:
        apply_plan(
            tampered,
            evaluation,
            report=report,
            dry_run=True,
            engine=engine,  # type: ignore[arg-type]
            now=NOW,
        )
    _assert_refusal_is_safe(applied.value)
    assert 't."status" IS NULL' not in " ".join(engine.transaction.connection.statements)


def _plan_eval_tampers() -> list[tuple[str, Any]]:
    def item_order(
        plan: RemediationPlan, evaluation: EvalReport
    ) -> tuple[RemediationPlan, EvalReport]:
        return plan.model_copy(update={"items": [plan.items[1], plan.items[0]]}), evaluation

    def action_id(
        plan: RemediationPlan, evaluation: EvalReport
    ) -> tuple[RemediationPlan, EvalReport]:
        return _replace_first_item(plan, action_id="quarantine_invalids"), evaluation

    def evidence(
        plan: RemediationPlan, evaluation: EvalReport
    ) -> tuple[RemediationPlan, EvalReport]:
        return (
            _replace_first_item(
                plan,
                evidence=[
                    QualityEvidence(
                        check_id="fact_orders.status.validity",
                        contract_id="warehouse.fact_orders",
                    )
                ],
            ),
            evaluation,
        )

    def target_count(
        plan: RemediationPlan, evaluation: EvalReport
    ) -> tuple[RemediationPlan, EvalReport]:
        item = _first_item(plan)
        return (
            _replace_first_item(
                plan,
                target_set=item.target_set.model_copy(update={"count": item.target_set.count + 1}),
            ),
            evaluation,
        )

    def target_fingerprint(
        plan: RemediationPlan, evaluation: EvalReport
    ) -> tuple[RemediationPlan, EvalReport]:
        item = _first_item(plan)
        return (
            _replace_first_item(
                plan,
                target_set=item.target_set.model_copy(update={"fingerprint": "targets:tampered"}),
            ),
            evaluation,
        )

    def policy_fingerprint(
        plan: RemediationPlan, evaluation: EvalReport
    ) -> tuple[RemediationPlan, EvalReport]:
        return plan.model_copy(update={"policy_fingerprint": "sha256:tampered-policy"}), evaluation

    def evaluation_result(
        plan: RemediationPlan, evaluation: EvalReport
    ) -> tuple[RemediationPlan, EvalReport]:
        score = evaluation.scores[0].model_copy(update={"rationale": "tampered evaluation result"})
        return plan, evaluation.model_copy(update={"scores": [score, *evaluation.scores[1:]]})

    return [
        ("item_order", item_order),
        ("action_id", action_id),
        ("quality_evidence", evidence),
        ("target_count", target_count),
        ("target_fingerprint", target_fingerprint),
        ("policy_fingerprint", policy_fingerprint),
        ("evaluation_result", evaluation_result),
    ]


_PLAN_EVAL_TAMPERS = _plan_eval_tampers()


@pytest.mark.parametrize(
    "tamper",
    [fn for _, fn in _PLAN_EVAL_TAMPERS],
    ids=[name for name, _ in _PLAN_EVAL_TAMPERS],
)
def test_admission_refuses_plan_and_evaluation_tampers(tamper: Any) -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls"),
        ("fact_orders.status.validity", "quarantine_invalids"),
    )
    tampered_plan, tampered_evaluation = tamper(plan, evaluation)
    with pytest.raises(PermissionError, match="received payload") as refused:
        create_apply_admission(
            tampered_plan, tampered_evaluation, _decision(), now=NOW, report=report
        )
    _assert_refusal_is_safe(refused.value)


@pytest.mark.parametrize(
    "tamper",
    [fn for _, fn in _PLAN_EVAL_TAMPERS],
    ids=[name for name, _ in _PLAN_EVAL_TAMPERS],
)
def test_apply_refuses_plan_and_evaluation_tampers(
    tamper: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls"),
        ("fact_orders.status.validity", "quarantine_invalids"),
    )
    admission = create_apply_admission(plan, evaluation, _decision(), now=NOW, report=report)
    tampered_plan, tampered_evaluation = tamper(plan, evaluation)
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver",
        _ParamsDerivedTargetResolver,
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))
    engine = _RecordingEngine()
    with pytest.raises(PermissionError, match="received payload") as refused:
        apply_plan(
            tampered_plan,
            tampered_evaluation,
            admission,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=NOW,
        )
    _assert_refusal_is_safe(refused.value)
    assert 't."status" IS NULL' not in " ".join(engine.transaction.connection.statements)


def test_apply_refuses_decision_link_and_expiry_tampers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    admission = create_apply_admission(plan, evaluation, _decision(), now=NOW, report=report)
    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver",
        _ParamsDerivedTargetResolver,
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))

    for tampered in (
        admission.model_copy(update={"decision_event_id": "decision-event-forged"}),
        admission.model_copy(update={"decision_id": "forged-decision"}),
        admission.model_copy(update={"expires_at": admission.expires_at + timedelta(days=7)}),
    ):
        assert tampered.fingerprint == admission.fingerprint
        engine = _RecordingEngine()
        with pytest.raises(PermissionError, match="received payload") as refused:
            apply_plan(
                plan,
                evaluation,
                tampered,
                report=report,
                dry_run=False,
                engine=engine,  # type: ignore[arg-type]
                now=NOW,
            )
        _assert_refusal_is_safe(refused.value)
        assert 't."status" IS NULL' not in " ".join(engine.transaction.connection.statements)


def test_apply_refuses_rewritten_admission_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    admission = create_apply_admission(plan, evaluation, _decision(), now=NOW, report=report)
    forged_id = "forged-admission-id"
    tampered = admission.model_copy(update={"admission_id": forged_id})
    assert tampered.fingerprint == admission.fingerprint

    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver",
        _ParamsDerivedTargetResolver,
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))
    engine = _RecordingEngine()
    with pytest.raises(PermissionError, match="received payload") as refused:
        apply_plan(
            plan,
            evaluation,
            tampered,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=NOW,
            run_id="unit-forged-admission-id",
        )
    _assert_refusal_is_safe(refused.value)
    rendered = " ".join(engine.transaction.connection.statements)
    assert forged_id not in rendered
    assert engine.transaction.connection.statements == []


def test_admission_and_apply_refuse_rewritten_evaluation_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    admission = create_apply_admission(plan, evaluation, _decision(), now=NOW, report=report)
    tampered = evaluation.model_copy(update={"evaluation_id": "forged-evaluation-id"})
    assert tampered.fingerprint == evaluation.fingerprint

    with pytest.raises(PermissionError, match="received payload") as admitted:
        create_apply_admission(plan, tampered, _decision(), now=NOW, report=report)
    _assert_refusal_is_safe(admitted.value)

    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver",
        _ParamsDerivedTargetResolver,
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))
    engine = _RecordingEngine()
    with pytest.raises(PermissionError, match="received payload") as applied:
        apply_plan(
            plan,
            tampered,
            admission,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=NOW,
            run_id="unit-forged-evaluation-id",
        )
    _assert_refusal_is_safe(applied.value)
    assert engine.transaction.connection.statements == []


def test_admission_and_apply_refuse_rewritten_plan_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    admission = create_apply_admission(plan, evaluation, _decision(), now=NOW, report=report)
    tampered = plan.model_copy(update={"plan_id": "forged-plan-id"})
    assert tampered.fingerprint == plan.fingerprint

    with pytest.raises(PermissionError, match="received payload") as admitted:
        create_apply_admission(tampered, evaluation, _decision(), now=NOW, report=report)
    _assert_refusal_is_safe(admitted.value)

    monkeypatch.setattr(
        "airflow_dq_agent.apply.executor.PostgresTargetSetResolver",
        _ParamsDerivedTargetResolver,
    )
    monkeypatch.setenv("TRACES_DIR", str(tmp_path))
    engine = _RecordingEngine()
    with pytest.raises(PermissionError, match="received payload") as applied:
        apply_plan(
            tampered,
            evaluation,
            admission,
            report=report,
            dry_run=False,
            engine=engine,  # type: ignore[arg-type]
            now=NOW,
            run_id="unit-forged-plan-id",
        )
    _assert_refusal_is_safe(applied.value)
    assert engine.transaction.connection.statements == []


def test_plan_evaluation_and_admission_reject_unexpected_fields() -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    admission = create_apply_admission(plan, evaluation, _decision(), now=NOW, report=report)
    for model, payload in (
        (RemediationPlan, plan.model_dump(mode="json")),
        (EvalReport, evaluation.model_dump(mode="json")),
        (ApplyAdmission, admission.model_dump(mode="json")),
        (
            Proposal,
            Proposal(summary="s", root_cause_hypothesis="h", confidence=0.1).model_dump(
                mode="json"
            ),
        ),
    ):
        with pytest.raises(ValidationError):
            model.model_validate({**payload, "unexpected_authority": "forged"})


def test_payload_fingerprint_helpers_match_stored_honest_artifacts() -> None:
    plan, evaluation, report = _compile_evaluated(
        ("fact_orders.total_amount.completeness", "quarantine_nulls")
    )
    admission = create_apply_admission(plan, evaluation, _decision(), now=NOW, report=report)
    assert plan.fingerprint == plan_payload_fingerprint(
        plan_id=plan.plan_id,
        quality_run_id=plan.quality_run_id,
        candidate_fingerprint=plan.candidate_fingerprint,
        policy_fingerprint=plan.policy_fingerprint,
        items=plan.items,
    )
    assert evaluation.fingerprint == evaluation_payload_fingerprint(
        evaluation_id=evaluation.evaluation_id,
        plan_id=evaluation.plan_id,
        plan_fingerprint=evaluation.plan_fingerprint,
        passed=evaluation.passed,
        scores=evaluation.scores,
        blocked_reasons=evaluation.blocked_reasons,
    )
    assert admission.fingerprint == admission_payload_fingerprint(
        admission_id=admission.admission_id,
        quality_run_id=admission.quality_run_id,
        plan_id=admission.plan_id,
        plan_fingerprint=admission.plan_fingerprint,
        evaluation_id=admission.evaluation_id,
        evaluation_fingerprint=admission.evaluation_fingerprint,
        decision_id=admission.decision_id,
        decision_event_id=admission.decision_event_id,
        policy_fingerprint=admission.policy_fingerprint,
        issued_at=admission.issued_at,
        expires_at=admission.expires_at,
    )
    assert plan.fingerprint == canonical_fingerprint(
        {
            "plan_id": plan.plan_id,
            "quality_run_id": plan.quality_run_id,
            "candidate_fingerprint": plan.candidate_fingerprint,
            "policy_fingerprint": plan.policy_fingerprint,
            "items": [item.model_dump(mode="json") for item in plan.items],
        }
    )
