"""Sample-free serialization boundary for the Airflow XCom channel.

XCom is durable storage: Airflow persists task return values. The quality
report therefore crosses the task boundary through one explicit allow-list,
matching the privacy guarantees already enforced for JSONL and Postgres audit
lineage: named IDs, statuses, counts, messages, and schema metadata move; row
samples and unknown fields never do.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from airflow_dq_agent.contracts.models import QualitySuiteReport

# Membership is the privacy classification; do not add a name without reviewing
# row-sample / prompt / secret content.
XCOM_REPORT_FIELDS: tuple[str, ...] = (
    "run_id",
    "report_id",
    "fingerprint",
    "audit_event_id",
    "generated_at",
    "checks",
    "observed_columns",
)

XCOM_CHECK_FIELDS: tuple[str, ...] = (
    "check_id",
    "table",
    "column",
    "dimension",
    "status",
    "n_failed",
    "n_total",
    "message",
    "contract_id",
    "predicate",
)


def _allowed_fields(node: Mapping[str, Any], allowed: tuple[str, ...]) -> dict[str, Any]:
    return {key: node[key] for key in allowed if key in node}


def project_report_for_xcom(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct a quality-report XCom payload from named fields only.

    Unknown keys are omitted so a future sensitive field cannot cross XCom by
    default. Nested check dicts are projected independently.
    """
    projected = {
        key: payload[key] for key in XCOM_REPORT_FIELDS if key in payload and key != "checks"
    }
    checks = payload.get("checks")
    if isinstance(checks, list):
        projected["checks"] = [
            _allowed_fields(check, XCOM_CHECK_FIELDS) for check in checks if isinstance(check, dict)
        ]
    return projected


def sample_free_report(report: QualitySuiteReport) -> dict[str, Any]:
    """Serialize one quality report for XCom from the named-field allow-list.

    The payload keeps everything downstream tasks need—run, report, and audit
    lineage IDs; check IDs; contract IDs; dimension; status; failure counts;
    messages; predicates; and observed columns—but no row samples. Downstream
    tasks revalidate the payload with ``QualitySuiteReport.model_validate``.
    """
    return project_report_for_xcom(report.model_dump(mode="json"))
