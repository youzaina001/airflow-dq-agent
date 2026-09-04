"""The XCom boundary is an allow-list: named Quality Evidence fields only."""

from airflow_dq_agent.contracts.models import QualitySuiteReport
from airflow_dq_agent.quality import (
    project_report_for_xcom,
    sample_free_report,
    seeded_failure_report,
)
from airflow_dq_agent.quality.sanitize import XCOM_CHECK_FIELDS, XCOM_REPORT_FIELDS


def _assert_no_sample_fields(node: object) -> None:
    if isinstance(node, dict):
        assert "sample_failures" not in node
        for value in node.values():
            _assert_no_sample_fields(value)
    elif isinstance(node, list):
        for item in node:
            _assert_no_sample_fields(item)


def test_seeded_fixture_report_carries_samples() -> None:
    report = seeded_failure_report()
    assert any(check.sample_failures for check in report.checks)


def test_sample_free_report_omits_every_sample_failures_field() -> None:
    report = seeded_failure_report()
    payload = sample_free_report(report)
    _assert_no_sample_fields(payload)


def test_sample_free_report_preserves_downstream_fields() -> None:
    report = seeded_failure_report()
    report = report.model_copy(update={"audit_event_id": "audit-root-1", "fingerprint": "fp-1"})
    payload = sample_free_report(report)

    assert payload["run_id"] == report.run_id
    assert payload["report_id"] == report.report_id
    assert payload["audit_event_id"] == "audit-root-1"
    assert payload["fingerprint"] == "fp-1"
    assert payload["observed_columns"] == report.observed_columns

    original = {check.check_id: check for check in report.checks}
    assert {check_data["check_id"] for check_data in payload["checks"]} == set(original)
    for check_data in payload["checks"]:
        check = original[check_data["check_id"]]
        assert check_data["table"] == check.table
        assert check_data["column"] == check.column
        assert check_data["dimension"] == check.dimension.value
        assert check_data["status"] == check.status.value
        assert check_data["n_failed"] == check.n_failed
        assert check_data["n_total"] == check.n_total
        assert check_data["message"] == check.message
        assert check_data["contract_id"] == check.contract_id
        assert check_data["predicate"] == check.predicate


def test_sanitized_payload_round_trips_for_downstream_tasks() -> None:
    report = seeded_failure_report()
    revived = QualitySuiteReport.model_validate(sample_free_report(report))

    assert revived.generated_at == report.generated_at
    assert revived.failed_count == report.failed_count
    assert revived.failing_check_ids == report.failing_check_ids
    assert revived.observed_columns == report.observed_columns
    assert all(check.sample_failures == [] for check in revived.checks)


def test_xcom_projector_omits_unknown_fields() -> None:
    dumped = seeded_failure_report().model_dump(mode="json")
    dumped["secret_rows"] = [{"email": "leaked@example.invalid"}]
    dumped["prompt"] = "do not persist this prompt"
    dumped["raw_values"] = [{"email": "leaked@example.invalid"}]
    dumped["checks"][0]["secret_rows"] = [{"email": "leaked@example.invalid"}]
    dumped["checks"][0]["prompt"] = "do not persist this prompt"
    dumped["checks"][0]["raw_values"] = [{"email": "leaked@example.invalid"}]

    payload = project_report_for_xcom(dumped)

    assert set(payload) <= set(XCOM_REPORT_FIELDS)
    assert "secret_rows" not in payload
    assert "prompt" not in payload
    assert "sample_failures" not in payload
    for check in payload["checks"]:
        assert set(check) <= set(XCOM_CHECK_FIELDS)
        assert "secret_rows" not in check
        assert "prompt" not in check
        assert "sample_failures" not in check
    revived = QualitySuiteReport.model_validate(payload)
    assert revived.failing_check_ids
    assert all(check.sample_failures == [] for check in revived.checks)
