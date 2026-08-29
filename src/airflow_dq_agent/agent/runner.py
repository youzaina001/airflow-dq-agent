"""Governed proposal generation.

The proposal agent may inspect catalog metadata, but it cannot execute arbitrary
SQL or apply a remediation.  The deterministic stub is the default used by CI
and the portfolio demo; live mode deliberately fails closed on setup or model
errors.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import inspect, text

from airflow_dq_agent.catalog import service as catalog
from airflow_dq_agent.config import Settings, get_settings
from airflow_dq_agent.contracts.models import (
    CheckResult,
    Citation,
    Dimension,
    Proposal,
    QualitySuiteReport,
    RemediationStep,
    ToolCallRecord,
)
from airflow_dq_agent.contracts.remediations import get_action, validate_step_params
from airflow_dq_agent.contracts.tables import get_table_contract
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.warehouse.db import make_engine

SYSTEM_PROMPT = """You are a governed data-quality proposal agent.
Return only the Proposal contract. Cite every failing check, use only catalog
allow-listed actions, and never propose a mutation if all checks are green.
sql_preview is informational for a human and is never executed. You have
read-only catalog, controlled check sampling, and observed-schema tools only.
"""


class AgentRun(BaseModel):
    """The structured output and audit context from a single proposal attempt."""

    proposal: Proposal
    prompt: str
    tool_calls: list[ToolCallRecord] = Field(default_factory=list)
    llm_mode: str


def _table_key(table: str) -> str:
    return table.split(".")[-1]


def _primary_key(table: str) -> str:
    contract = get_table_contract(table)
    if len(contract.primary_key) != 1:
        raise ValueError(f"{contract.table} must have one primary key for v1 remediation")
    return contract.primary_key[0]


def _business_key(check: CheckResult) -> str | list[str]:
    """Return the contracted business key for known uniqueness checks."""
    if check.check_id == "fact_orders.order_nk.uniqueness":
        return ["customer_sk", "order_ts"]
    if check.check_id == "dim_patient.subject_id.uniqueness":
        return "subject_id"
    if check.check_id == "dim_customer.customer_nk.uniqueness":
        return "customer_nk"
    if check.column:
        return check.column
    raise ValueError(f"No declared business key for {check.check_id}")


def _step_for_failure(check: CheckResult) -> RemediationStep:
    """Map a real failed check to a bindable, allow-listed policy action."""
    table = _table_key(check.table)
    params: dict[str, Any]
    action_id: str
    rationale: str

    if check.dimension == Dimension.COMPLETENESS and check.column:
        action_id = "quarantine_nulls"
        params = {"column": check.column, "pk_column": _primary_key(table)}
        rationale = f"Copy rows with NULL {check.column} for human review; leave source unchanged."
    elif check.dimension == Dimension.VALIDITY and check.check_id in CHECK_SPECS and check.column:
        action_id = "quarantine_invalids"
        params = {
            "check_id": check.check_id,
            "column": check.column,
            "pk_column": _primary_key(table),
        }
        rationale = "Copy rows matching the check registry's controlled validity predicate."
    elif check.dimension == Dimension.UNIQUENESS:
        try:
            business_key = _business_key(check)
        except ValueError:
            action_id = "no_op_alert"
            params = {"check_id": check.check_id}
            rationale = "Alert only: no contracted business key exists for a safe duplicate policy."
        else:
            action_id = "dedupe_keep_min_pk"
            params = {"business_key": business_key, "pk_column": _primary_key(table)}
            rationale = "Copy non-canonical duplicate rows, retaining the minimum surrogate key."
    elif check.dimension == Dimension.REFERENTIAL_INTEGRITY and check.column:
        contract = get_table_contract(table)
        foreign_key = next((fk for fk in contract.foreign_keys if fk[0] == check.column), None)
        if foreign_key is None:
            action_id = "no_op_alert"
            params = {"check_id": check.check_id}
            rationale = "Alert only: the failed foreign key is not in the table contract."
        else:
            _, ref_table, ref_column = foreign_key
            action_id = "quarantine_orphans"
            params = {
                "fk_column": check.column,
                "ref_table": ref_table,
                "ref_column": ref_column,
                "pk_column": _primary_key(table),
            }
            rationale = "Copy unresolved foreign-key rows for review; do not delete source rows."
    elif check.dimension == Dimension.SCHEMA_DRIFT:
        action_id = "schema_drift_ticket"
        params = {"check_id": check.check_id}
        rationale = "Open a contract-change ticket; schema changes are never applied automatically."
    else:
        action_id = "no_op_alert"
        params = {"check_id": check.check_id}
        rationale = "Alert only: this failure does not have a deterministic safe remediation."

    violations = validate_step_params(action_id, table, params)
    if violations:
        raise ValueError(
            f"Internal remediation policy is invalid for {check.check_id}: {violations}"
        )
    action = get_action(action_id)
    return RemediationStep(
        action_id=action_id,
        table=table,
        params=params,
        estimated_rows=check.n_failed,
        reversible=action.reversible,
        destructive_rank=action.destructive_rank,
        rationale=rationale,
        sql_preview=action.preview_sql.format(table=table, **params),
    )


def build_prompt(report: QualitySuiteReport) -> str:
    """Make the agent's evidence boundary explicit in a compact deterministic prompt."""
    failures = [
        {
            "check_id": check.check_id,
            "table": check.table,
            "column": check.column,
            "dimension": check.dimension.value,
            "n_failed": check.n_failed,
            "message": check.message,
        }
        for check in report.failed_checks
    ]
    return f"{SYSTEM_PROMPT}\nQuality report failures (authoritative):\n{json.dumps(failures, indent=2)}"


def _stub_proposal(report: QualitySuiteReport) -> Proposal:
    failures = report.failed_checks
    if not failures:
        return Proposal(
            summary="All declared quality checks passed; no remediation is proposed.",
            failing_check_ids=[],
            root_cause_hypothesis="No failed check evidence was provided.",
            citations=[],
            steps=[],
            do_not_apply_reasons=["all checks passed"],
            confidence=1.0,
        )
    return Proposal(
        summary=f"{len(failures)} failed checks have deterministic, allow-listed proposals.",
        failing_check_ids=[check.check_id for check in failures],
        root_cause_hypothesis="Seeded or observed rows violate declared table contracts.",
        citations=[
            Citation(
                check_id=check.check_id,
                contract_id=check.contract_id,
                evidence=f"{check.n_failed}/{check.n_total}: {check.message}",
            )
            for check in failures
        ],
        steps=[_step_for_failure(check) for check in failures],
        do_not_apply_reasons=["requires evaluation and, for mutations, proposal-level HITL"],
        confidence=0.9,
    )


def _load_replay(path: Path) -> Proposal:
    """Load a Proposal or a trace envelope from JSON or JSONL and validate it again."""
    if not path.is_file():
        raise RuntimeError(f"Replay trace does not exist: {path}")
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise RuntimeError(f"Replay trace is empty: {path}")
    try:
        record: Any = json.loads(raw)
    except json.JSONDecodeError:
        records = [json.loads(line) for line in raw.splitlines() if line.strip()]
        matching = [item for item in records if isinstance(item, dict) and item.get("proposal")]
        if not matching:
            raise RuntimeError(f"Replay JSONL has no proposal envelope: {path}") from None
        record = matching[-1]
    if isinstance(record, dict) and "proposal" in record:
        record = record["proposal"]
    return Proposal.model_validate(record)


def sample_failing_rows(
    check_id: str, limit: int = 20, *, dsn: str | None = None
) -> list[dict[str, Any]]:
    """Run one check registry sample query, never caller-provided SQL."""
    if check_id not in CHECK_SPECS:
        raise KeyError(f"Unknown allow-listed check_id {check_id!r}")
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    spec = CHECK_SPECS[check_id]
    if not spec.sample_sql:
        return []
    engine = make_engine(dsn)
    with engine.connect() as connection:
        result = connection.execute(text(spec.sample_sql), {"limit": limit})
        return [dict(row) for row in result.mappings()]


def get_observed_schema(table: str, *, dsn: str | None = None) -> dict[str, str]:
    """Return observed column names/types for a contracted table."""
    contract = get_table_contract(table)
    inspector = inspect(make_engine(dsn))
    return {
        str(column["name"]): str(column["type"])
        for column in inspector.get_columns(contract.table, schema=contract.schema_name)
    }


def _sample_failing_rows_tool(check_id: str, limit: int = 20) -> list[dict[str, Any]]:
    """Read rows from a declared failing-check sample using the configured warehouse only."""
    return sample_failing_rows(check_id, limit)


def _get_observed_schema_tool(table: str) -> dict[str, str]:
    """Read the configured warehouse schema for one contracted table only."""
    return get_observed_schema(table)


def _tool_functions() -> list[Callable[..., Any]]:
    return [
        catalog.list_tables,
        catalog.get_table_contract,
        catalog.list_checks,
        catalog.get_check,
        catalog.get_lineage,
        catalog.list_remediations,
        catalog.get_remediation,
        _sample_failing_rows_tool,
        _get_observed_schema_tool,
    ]


def build_read_only_toolset() -> Any:
    """Create PydanticAI's FunctionToolset using only deterministic read functions."""
    try:
        from pydantic_ai.toolsets import FunctionToolset
    except ImportError as exc:  # pragma: no cover - exercised only by live deployments
        raise RuntimeError("Live mode requires pydantic-ai") from exc

    toolset = FunctionToolset()
    for function in _tool_functions():
        tool_name = {
            "_sample_failing_rows_tool": "sample_failing_rows",
            "_get_observed_schema_tool": "get_observed_schema",
        }.get(function.__name__, function.__name__)
        toolset.add_function(function, name=tool_name)
    return toolset


def _run_live(report: QualitySuiteReport, settings: Settings) -> AgentRun:
    """Run the optional live model; every setup, transport, and output error is fatal."""
    if not settings.openai_api_key:
        raise RuntimeError("LLM_MODE=live requires OPENAI_API_KEY; refusing to fall back")
    try:
        from pydantic_ai import Agent
        from pydantic_ai.models.openai import OpenAIChatModel
        from pydantic_ai.providers.openai import OpenAIProvider
    except ImportError as exc:  # pragma: no cover - requires optional live dependencies
        raise RuntimeError("LLM_MODE=live requires pydantic-ai OpenAI dependencies") from exc

    model_name = settings.llm_model.removeprefix("openai:")
    provider = OpenAIProvider(api_key=settings.openai_api_key, base_url=settings.openai_base_url)
    model = OpenAIChatModel(model_name, provider=provider)
    prompt = build_prompt(report)
    try:
        agent = Agent(
            model,
            output_type=Proposal,
            system_prompt=SYSTEM_PROMPT,
            toolsets=[build_read_only_toolset()],
        )
        result = agent.run_sync(prompt)
        proposal = Proposal.model_validate(result.output)
    except Exception as exc:  # pragma: no cover - network/model behavior is deployment-specific
        raise RuntimeError("Live proposal failed closed; no proposal was produced") from exc
    return AgentRun(proposal=proposal, prompt=prompt, tool_calls=[], llm_mode="live")


def run_proposal_agent(report: QualitySuiteReport) -> AgentRun:
    """Create one structured proposal in the configured mode."""
    settings = get_settings()
    prompt = build_prompt(report)
    if settings.llm_mode == "stub":
        return AgentRun(
            proposal=_stub_proposal(report), prompt=prompt, tool_calls=[], llm_mode="stub"
        )
    if settings.llm_mode == "replay":
        if settings.replay_trace_path is None:
            raise RuntimeError("LLM_MODE=replay requires REPLAY_TRACE_PATH")
        return AgentRun(
            proposal=_load_replay(settings.replay_trace_path),
            prompt=prompt,
            tool_calls=[
                ToolCallRecord(name="load_replay", args={"path": str(settings.replay_trace_path)})
            ],
            llm_mode="replay",
        )
    return _run_live(report, settings)
