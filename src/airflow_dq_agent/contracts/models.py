"""Pydantic contracts. The agent returns a Proposal; evals score it; apply never sees free SQL."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class Dimension(StrEnum):
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    SCHEMA_DRIFT = "schema_drift"
    REFERENTIAL_INTEGRITY = "referential_integrity"


class CheckStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"


class DestructiveRank(StrEnum):
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


RANK_WEIGHT: dict[DestructiveRank, float] = {
    DestructiveRank.NONE: 0.0,
    DestructiveRank.LOW: 0.15,
    DestructiveRank.MEDIUM: 0.4,
    DestructiveRank.HIGH: 0.75,
    DestructiveRank.CRITICAL: 1.0,
}

FORBIDDEN_SQL_TOKENS = (
    "DROP",
    "TRUNCATE",
    "ALTER",
    "GRANT",
    "REVOKE",
    "COPY",
    "EXECUTE",
    "CALL",
    "DO ",
    "VACUUM",
    "CLUSTER",
    "REINDEX",
    "CREATE USER",
    "CREATE ROLE",
    "LOAD ",
    "LO_IMPORT",
    "PG_READ_FILE",
    "PG_WRITE_FILE",
    "DBLINK",
)


class CheckResult(BaseModel):
    check_id: str
    table: str
    column: str | None = None
    dimension: Dimension
    status: CheckStatus
    n_failed: int = 0
    n_total: int = 0
    sample_failures: list[dict[str, Any]] = Field(default_factory=list)
    message: str
    contract_id: str
    predicate: str | None = None

    @property
    def failed(self) -> bool:
        return self.status == CheckStatus.FAIL


class QualitySuiteReport(BaseModel):
    run_id: str = Field(default_factory=lambda: uuid4().hex)
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    checks: list[CheckResult]
    observed_columns: dict[str, list[str]] = Field(default_factory=dict)

    @property
    def failed_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if c.failed]

    @property
    def failed_count(self) -> int:
        return len(self.failed_checks)

    @property
    def passed_count(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.PASS)

    @property
    def check_ids(self) -> set[str]:
        return {c.check_id for c in self.checks}

    @property
    def failing_check_ids(self) -> list[str]:
        return [c.check_id for c in self.failed_checks]

    def get(self, check_id: str) -> CheckResult | None:
        for check in self.checks:
            if check.check_id == check_id:
                return check
        return None


class Citation(BaseModel):
    """A proposal must point at a real check + contract, not a vibe."""

    check_id: str
    contract_id: str
    evidence: str = Field(min_length=1)


class RemediationStep(BaseModel):
    """One allow-listed action. `sql_preview` is for humans; apply re-renders from action_id."""

    action_id: str
    table: str
    params: dict[str, Any] = Field(default_factory=dict)
    estimated_rows: int | None = None
    reversible: bool = False
    destructive_rank: DestructiveRank = DestructiveRank.LOW
    rationale: str
    sql_preview: str = Field(
        default="",
        description="Human-readable preview. Never executed. Apply binds the template.",
    )

    @field_validator("action_id")
    @classmethod
    def action_id_slug(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned or any(ch.isspace() for ch in cleaned):
            raise ValueError("action_id must be a non-empty slug")
        return cleaned


class QualityEvidence(BaseModel):
    """A failed-check reference scoped by the remediation plan's quality run."""

    check_id: str = Field(min_length=1)
    contract_id: str = Field(min_length=1)


class CandidateAction(BaseModel):
    """An untrusted request for one action; it has no SQL parameters or authority."""

    action_id: str = Field(min_length=1)
    evidence: list[QualityEvidence] = Field(min_length=1)
    rationale: str = Field(min_length=1)


class TargetSet(BaseModel):
    """Durable summary of a controlled target set; never carries its raw keys."""

    count: int = Field(ge=0)
    fingerprint: str = Field(min_length=1)


class ExecutablePlanItem(BaseModel):
    kind: Literal["executable"] = "executable"
    item_id: str = Field(min_length=1)
    action_id: str = Field(min_length=1)
    table: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    evidence: list[QualityEvidence] = Field(min_length=1)
    target_set: TargetSet
    policy_fingerprint: str = Field(min_length=1)


class NonExecutablePlanItem(BaseModel):
    kind: Literal["non_executable"] = "non_executable"
    item_id: str = Field(min_length=1)
    evidence: list[QualityEvidence] = Field(min_length=1)
    reason: str = Field(min_length=1)


PlanItem = Annotated[
    ExecutablePlanItem | NonExecutablePlanItem,
    Field(discriminator="kind"),
]


class RemediationPlan(BaseModel):
    """A deterministic, complete collection of executable or blocked plan items."""

    plan_id: str = Field(default_factory=lambda: uuid4().hex)
    quality_run_id: str = Field(min_length=1)
    candidate_fingerprint: str = Field(min_length=1)
    policy_fingerprint: str = Field(min_length=1)
    items: list[PlanItem]
    blocked: bool
    blocked_reasons: list[str] = Field(default_factory=list)
    fingerprint: str = Field(min_length=1)
    compiled_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Proposal(BaseModel):
    """Structured agent output. Free text is not a contract."""

    summary: str
    failing_check_ids: list[str] = Field(default_factory=list)
    root_cause_hypothesis: str
    citations: list[Citation] = Field(default_factory=list)
    steps: list[RemediationStep] = Field(default_factory=list)
    candidate_actions: list[CandidateAction] = Field(default_factory=list)
    do_not_apply_reasons: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def green_means_no_steps(self) -> Proposal:
        if not self.failing_check_ids and self.steps:
            self.do_not_apply_reasons = [
                *self.do_not_apply_reasons,
                "proposal_has_steps_but_no_failing_checks",
            ]
        return self

    @property
    def max_destructive_rank(self) -> DestructiveRank:
        if not self.steps:
            return DestructiveRank.NONE
        return max(self.steps, key=lambda s: RANK_WEIGHT[s.destructive_rank]).destructive_rank

    def cited_check_ids(self) -> set[str]:
        return {c.check_id for c in self.citations}


class EvalScore(BaseModel):
    name: str
    score: float = Field(ge=0.0, le=1.0)
    passed: bool
    rationale: str
    details: dict[str, Any] = Field(default_factory=dict)


class EvalReport(BaseModel):
    passed: bool
    scores: list[EvalScore]
    blocked_reasons: list[str] = Field(default_factory=list)
    summary_markdown: str = ""

    def score_map(self) -> dict[str, EvalScore]:
        return {s.name: s for s in self.scores}

    def get(self, name: str) -> EvalScore | None:
        return self.score_map().get(name)


class HumanDecision(BaseModel):
    decided_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    decision: Literal["Approve", "Reject", "shadow_skip"]
    actor: str = "airflow-hitl"
    note: str | None = None


class ToolCallRecord(BaseModel):
    name: str
    args: dict[str, Any] = Field(default_factory=dict)
    result: Any = None
    error: str | None = None


class TraceRecord(BaseModel):
    """Append-only. One object per event; never updated in place."""

    trace_id: str = Field(default_factory=lambda: uuid4().hex)
    parent_trace_id: str | None = None
    kind: Literal["agent_run", "human_decision"] = "agent_run"
    ts: datetime = Field(default_factory=lambda: datetime.now(UTC))
    dag_id: str | None = None
    run_id: str | None = None
    llm_mode: str
    apply_mode: str
    llm_model: str | None = None
    prompt: str = ""
    retrieved_context: dict[str, Any] = Field(default_factory=dict)
    tool_calls: list[ToolCallRecord] = Field(default_factory=list)
    proposal: Proposal | None = None
    eval_scores: EvalReport | None = None
    human_decision: HumanDecision | None = None
    quality_run_id: str | None = None
