"""Sample-free serialization boundary for the Airflow XCom channel.

XCom is durable storage: Airflow persists task return values. The quality
report therefore crosses the task boundary through one explicit sanitizer,
matching the privacy guarantees already enforced for JSONL and Postgres audit
lineage: IDs, statuses, counts, messages, and schema metadata move; row
samples never do.
"""

from __future__ import annotations

from typing import Any, cast

from airflow_dq_agent.contracts.models import QualitySuiteReport

SAMPLE_FAILURES_FIELD = "sample_failures"


def _omit_sample_fields(node: Any) -> Any:
    """Recursively drop every sample_failures field from a JSON-safe structure."""
    if isinstance(node, dict):
        return {
            key: _omit_sample_fields(value)
            for key, value in node.items()
            if key != SAMPLE_FAILURES_FIELD
        }
    if isinstance(node, list):
        return [_omit_sample_fields(item) for item in node]
    return node


def sample_free_report(report: QualitySuiteReport) -> dict[str, Any]:
    """Serialize one quality report for XCom with every sample_failures field omitted.

    The payload keeps everything downstream tasks need—run, report, and audit
    lineage IDs; check IDs; contract IDs; dimension; status; failure counts;
    messages; predicates; and observed columns—but no row samples. Downstream
    tasks revalidate the payload with ``QualitySuiteReport.model_validate``.
    """
    return cast(dict[str, Any], _omit_sample_fields(report.model_dump(mode="json")))
