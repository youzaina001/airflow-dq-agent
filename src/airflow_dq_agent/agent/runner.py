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
    CandidateAction,
    CheckResult,
    Proposal,
    QualityEvidence,
    QualitySuiteReport,
    ToolCallRecord,
)
from airflow_dq_agent.contracts.tables import get_table_contract
from airflow_dq_agent.quality.registry import CHECK_SPECS
from airflow_dq_agent.warehouse.db import make_engine

SYSTEM_PROMPT = """You are a governed data-quality proposal agent.
Return only the Candidate Proposal contract. Each requested action must cite one
or more failed checks with their contract IDs. You may choose only an action ID;
the deterministic compiler derives tables, values, target rows, and SQL. You have
read-only catalog, controlled check sampling, and observed-schema tools only.
"""


class AgentRun(BaseModel):
    """The structured output and audit context from a single proposal attempt."""

    proposal: Proposal
    prompt: str
    tool_calls: list[ToolCallRecord] = Field(default_factory=list)
    llm_mode: str


def _candidate_action_for_failure(check: CheckResult) -> CandidateAction:
    """Ask only for the first declared policy action; compilation owns all parameters."""
    spec = CHECK_SPECS.get(check.check_id)
    if spec is None or not spec.policies:
        action_id = "no_op_alert"
        rationale = "The check has no reviewed executable policy; record an alert only."
    else:
        action_id = spec.policies[0].action_id
        rationale = "Request the reviewed action declared by this failed check's policy."
    return CandidateAction(
        action_id=action_id,
        evidence=[QualityEvidence(check_id=check.check_id, contract_id=check.contract_id)],
        rationale=rationale,
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
            root_cause_hypothesis="No failed check evidence was provided.",
            candidate_actions=[],
            do_not_apply_reasons=["all checks passed"],
            confidence=1.0,
        )
    return Proposal(
        summary=f"{len(failures)} failed checks have deterministic, allow-listed proposals.",
        root_cause_hypothesis="Seeded or observed rows violate declared table contracts.",
        candidate_actions=[_candidate_action_for_failure(check) for check in failures],
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
