# Governed Airflow DQ Agent

A data-quality remediation workflow for Apache Airflow 3. Deterministic checks identify
failures; a stub, replay, or optional live LLM proposes allow-listed action IDs; and
deterministic policy decides what may run. The model never supplies executable SQL,
target keys, table names, or values. The package includes the `dq-agent` CLI and the
`dq_daily` Airflow DAG.

## End-to-end workflow

```mermaid
flowchart TD
  warehouse[(Warehouse)] --> suite[Deterministic quality suite]
  suite --> report[Quality Suite Report]
  report --> proposer[Stub, replay, or read-only live proposer]
  proposer --> candidate[Candidate Proposal]
  candidate --> candidate_eval[Candidate evaluation]
  candidate_eval -->|fail| stopped[Audit and stop]
  candidate_eval -->|pass| compiler[Deterministic plan compiler]
  compiler --> plan[Remediation Plan with target fingerprints]
  plan --> plan_eval[Plan evaluation]
  plan_eval -->|blocked or fail| stopped
  plan_eval -->|pass and APPLY_MODE=off| shadow[Audit and stop in shadow mode]
  plan_eval -->|pass and APPLY_MODE=hitl| hitl[Audited Airflow approval]
  hitl -->|reject or timeout| stopped
  hitl -->|approve| admission[Time-bounded whole-plan admission]
  admission --> apply[Recheck policy, lock targets, and compare fingerprints]
  apply --> execute[Render and execute controlled steps in one transaction]
  execute --> result[Audit apply result]

  report -.-> lineage[(Immutable audit lineage)]
  candidate -.-> lineage
  plan -.-> lineage
  plan_eval -.-> lineage
  hitl -.-> lineage
  result -.-> lineage
```

The safe defaults are `LLM_MODE=stub` and `APPLY_MODE=off`: no live model call, no
approval request, and no mutation. A Candidate Proposal is untrusted input. It becomes
an executable Remediation Plan only when it covers the failed checks and every requested
action is allowed by the corresponding check policy.

## Governed action boundary

Every remediation action crosses one registered `GovernedAction` seam:

```mermaid
flowchart LR
  policy[Reviewed Check Policy] --> action[Registered GovernedAction]
  contracts[Table and check contracts] --> action
  action -->|derive and validate parameters| compiler[Plan compiler]
  action -->|render controlled target query| resolver[Target-set resolver]
  action -->|render SQL and expose mutation flag| executor[Transactional executor]
```

One registration owns the action's metadata, policy-parameter derivation, validation,
controlled rendering, and mutation capability. The compiler, target resolver, and
executor delegate through that registration instead of maintaining parallel action-ID
switches. Registry-synchronized tests require every registered action to support the
same governed lifecycle.

| Capability | Authority | Guardrail |
| --- | --- | --- |
| Read catalog, samples, and observed schema | Available to the proposer | Fixed registries and bounded samples; no ad-hoc SQL |
| Propose | Untrusted | Action IDs and report-scoped evidence only |
| Compile | Deterministic | Check Policy supplies reviewed rules; the action derives and validates inputs |
| Apply | Disabled by default | Passing eval, audited approval, admission, policy check, target lock, and transaction |

## Modes

| Setting | Behavior | Can mutate? |
| --- | --- | --- |
| `LLM_MODE=stub` | Deterministic policy mapper; local and CI default | No |
| `LLM_MODE=replay` | Revalidates a recorded proposal | No |
| `LLM_MODE=live` | Optional model with catalog, bounded-sample, and schema reads | No authority by itself |
| `APPLY_MODE=off` | Evaluates and audits, then stops | No |
| `APPLY_MODE=hitl` | Enables audited approval and admission for a passing plan | Only through controlled apply |

See [`.env.example`](.env.example) for the complete local configuration. Production
deployments should supply separate least-privilege read, audit, and apply DSNs.

## Use against your warehouse

Install the package (PyPI publishing is not part of this release):

```bash
pip install "airflow-dq-agent @ git+https://github.com/youzaina001/airflow-dq-agent.git"
# or from a checkout:
pip install -e ".[dev]"
```

Safe defaults remain `LLM_MODE=stub` and `APPLY_MODE=off`. Register your tables and
checks in process — the governance core starts with empty catalogs:

```python
from airflow_dq_agent import (
    CheckPolicy,
    CheckSpec,
    ColumnContract,
    TableContract,
    load_registry,
    register_check,
    register_contract,
)
from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.evals import evaluate_plan, evaluate_proposal
from airflow_dq_agent.planning import compile_remediation_plan
from airflow_dq_agent.quality import run_quality_suite

register_contract(
    TableContract(
        table="ext_invoice",
        grain="one row per invoice_id",
        description="Invoice header",
        primary_key=["invoice_id"],
        columns=[
            ColumnContract(name="invoice_id", dtype="int64", unique=True),
            ColumnContract(name="amount", dtype="float64", nullable=True),
        ],
    )
)
register_check(
    CheckSpec(
        check_id="ext_invoice.amount.completeness",
        table="ext_invoice",
        column="amount",
        dimension=Dimension.COMPLETENESS,
        description="amount must be present",
        policies=[CheckPolicy(action_id="quarantine_nulls")],
    )
)
# equivalent: load_registry("examples/my_warehouse.yaml")

report = run_quality_suite(dsn)  # your read DSN
# propose (stub/replay/live) → evaluate_proposal → compile_remediation_plan
# → evaluate_plan → HITL / apply. The model still never supplies SQL.
```

`dags/dq_daily.py` is the synthetic demo (`register_demo()`). Human Decision and
apply against an adopter table are proven by
[`examples/dq_external_invoice.py`](examples/dq_external_invoice.py), not by
swapping that registration into `dq_daily`. Keep `APPLY_MODE=off` until a passing
plan should request approval.

## External invoice example (restricted credentials)

The packaged example registers one external PostgreSQL table (`ext_invoice`) with a
single-column primary key and one completeness Check Policy that permits
`quarantine_nulls`. It uses `LLM_MODE=stub`: no model credentials and no language-model
calls. Approval copies the authorized Remediation Target Set into
`dq.quarantine_rows`. Source rows remain unchanged; success is a quarantine copy, not
source repair.

1. Install as above (`pip install -e ".[dev]"` from a checkout).
2. Apply the packaged governance schema (dq traces, quarantine table, capability roles)
   and seed the example table. YAML equivalent: `examples/my_warehouse.yaml`.

```python
from airflow_dq_agent.adoption import (
    apply_governance_schema,
    provision_restricted_logins,
    register_external_invoice,
    seed_external_invoice,
)

register_external_invoice()
apply_governance_schema(owner_dsn)
seed_external_invoice(owner_dsn)
credentials = provision_restricted_logins(owner_dsn)
```

3. Create distinct LOGIN roles (or call `provision_restricted_logins`) so `READ_DSN`
   inherits `dq_read`, `AUDIT_DSN` inherits `dq_audit`, and `APPLY_DSN` inherits
   `dq_apply`. The reader cannot write; the apply account cannot UPDATE/DELETE source
   rows or rewrite `dq.traces`.
4. Copy [`examples/dq_external_invoice.py`](examples/dq_external_invoice.py) into
   `dags/`. Do not treat editing `register_demo()` in `dags/dq_daily.py` as the
   HITL/apply path. The example DAG binds the suite and compiler to `READ_DSN`,
   HITL/Audit Lineage to `AUDIT_DSN`, and apply to `APPLY_DSN`.
5. Keep `LLM_MODE=stub`. Leave `APPLY_MODE=off` until you intend a Human Decision.
   Then set `APPLY_MODE=hitl`, `TRACE_POSTGRES=true`, and an allow-listed
   `HITL_APPROVER_IDS` identity. Unpause `dq_external_invoice`.
6. Approve, reject, or let the HITL task time out. Inspect PostgreSQL after each
   outcome — do not treat a skipped Postgres or Airflow run as acceptance.

   | Outcome | How to exercise | `dq.quarantine_rows` | Source `warehouse.ext_invoice` | Audit Lineage |
   | --- | --- | --- | --- | --- |
   | Approve | Required Actions: choose Approve and a non-empty note | Copies missing-amount ids 102 and 104 | Unchanged (101/103 keep amounts; 102/104 stay NULL) | `human_approved` then `apply_succeeded` |
   | Reject | Required Actions: choose Reject | Empty for that run | Unchanged | `human_rejected`; no `apply_succeeded` |
   | Timeout | Do not respond before `DQ_HITL_TIMEOUT_SECONDS` (default 86400). Pinned `apache-airflow-providers-standard==1.12.1` uses `execution_timeout`, not `response_timeout`. | Empty for that run | Unchanged | `human_timed_out` with actor `airflow-timeout`; not an approval |

   `scripts/compose-hitl-reject-timeout.sh` runs Reject and Timeout against real Airflow and asserts these outcomes. Timeout cannot be interpreted as Approve.

## Quick start

The database-free path requires Python 3.12 or later and Make:

```bash
make install
make test
make eval
make demo
```

The demo runs deterministic fixtures. It shows a safe proposal passing, destructive and
ungrounded proposals failing, and apply being skipped.

Docker Engine or Docker Desktop with the Compose plugin is required for Airflow and
Postgres:

```bash
make up
make seed
```

Open `http://localhost:8080` and sign in with `airflow` / `airflow`. The `dq_daily` DAG
is paused when created. `make integration` exercises the Postgres path, and
`make compose-smoke` runs the deterministic Compose verification.

## AI code review

Local review runs through OpenCodeReview (`ocr`) with OpenRouter
(`z-ai/glm-5.3-flash`). On GitHub, start a PR review from Actions (*Run
workflow*, pick the PR and model) or by commenting `@ocr` on the pull request.
Findings are advisory: the deterministic loop and human review remain the
authority.

```bash
make review-preview   # files OCR would review (no LLM call)
make review           # review the workspace diff
make review-branch    # review the current branch against origin/master
```

Project rules live in `.opencodereview/rule.json`, and the PR workflow is
`.github/workflows/ocr-review.yml`. See
[docs/ocr-code-review.md](docs/ocr-code-review.md) for setup, the
`OPENROUTER_API_KEY` secret, and cost controls.

## Deterministic evaluation cases

- A failed uniqueness check does not authorize `DROP TABLE fact_orders`. The evaluator
  assigns zero to destructive-risk and allow-list compliance, so nothing is applied.
- A green quality report does not authorize `null_fill`. Groundedness fails because a
  report without failures must not produce remediation steps.

These cases test the authority boundary independently of model quality or SQL syntax.

## Safety boundary

There is no `SQLToolset.query`. Live mode exposes only catalog reads, fixed check
sampling with a bounded limit, and observed-schema reads. Missing credentials, transport
errors, replay errors, malformed output, failed evaluations, rejected approvals, and
expired admissions fail closed.

The v1 remediation catalog is deliberately small. Quarantine actions copy affected rows
into `dq.quarantine_rows`; they do not delete source rows. `dq.quarantine_rows.payload`
copies source rows and is not covered by sample-free XCom or Audit Lineage guarantees.
`null_fill` is registered but unavailable until a reviewed Check Policy supplies both a
target rule and fill value.
Every executable plan item records the exact primary-key target count and fingerprint.
Apply recomputes and locks the same set in its transaction; target drift, policy drift,
or an expired admission aborts the operation.

Audit lineage uses `quality_run_id` as its root and immutable IDs for the report,
candidate, plan, evaluation, decision, admission, and apply result. Postgres is required
for HITL audit writes; JSONL is supplementary. Durable payloads contain IDs,
fingerprints, counts, and sanitized reasons—not prompts or row samples.

### Privacy boundaries

Row samples exist only inside the quality process and in bounded, read-only proposer
sampling. Raw live-model output remains transient in the proposal task: before that
task returns, the proposal is reconstructed from canonical governed-action and
report-scoped evidence identifiers with controlled narrative text. Three durable
channels persist data, and none carries row samples or model-authored narrative:

| Channel | Written by | Contents |
| --- | --- | --- |
| Airflow XCom | `run_suite_task` via `sample_free_report`; `propose_task` via `safe_proposal_for_xcom` | Allow-listed report fields and bounded proposal authority identifiers—never `sample_failures`, sampled values, unknown fields, or model-authored text |
| JSONL traces | `JsonlAuditSink` | Minimized `AuditEvent` lineage bodies (IDs, fingerprints, counts, reasons) |
| Postgres audit | `PostgresAuditSink` | The same `AuditEvent` bodies plus per-check `dq.check_runs` rows with counts only |

A DAG-level integration test (`tests/integration/test_dag_xcom_privacy.py`) executes the
real `dq_daily` task bodies and asserts that no XCom payload contains `sample_failures`
or seeded row values while proposal, compilation, and evaluation still pass.

## Repository layout

```text
src/airflow_dq_agent/
  contracts/             # table, check-result, proposal, and remediation contracts
  quality/               # deterministic Polars/Pandera quality suite
  catalog/               # transport-free catalog plus empty FastMCP adapter
  agent/                 # stub, replay, and opt-in read-only live proposal paths
  evals/                 # deterministic proposal scorers and gates
  action_definitions.py  # governed action ownership and registration
  planning/              # plan compiler, target-set resolver, and apply admission
  apply/                 # transactional executor
  traces/                # append-only JSONL and optional Postgres mirror
  warehouse/             # DSN/engine helpers (no demo schema)
  demo/                  # optional synthetic warehouse, seed, fixtures, and catalog MCP
  load.py                # YAML registry loader
dags/dq_daily.py         # Airflow TaskFlow orchestration and HITL boundary
evals/cases/             # portable deterministic evaluation cases
examples/                # adopter YAML, external-invoice DAG, and registration example
```

The scope is detection, typed proposals, deterministic evaluation, audited approval,
and controlled remediation. The authority boundary is explicit and testable at every
stage.

## Runtime verification

`make compose-smoke` builds the pinned Airflow 3.1.5/Python 3.12 image, starts Compose
in stub/shadow mode, seeds the warehouse, runs `dq_daily`, and verifies persisted lineage
and zero quarantine rows.

Before a release or demo, manually verify approval and rejection with
`APPLY_MODE=hitl`, `TRACE_POSTGRES=true`, and an allow-listed `HITL_APPROVER_IDS` user.
An approval requires a non-empty note. Rejection and timeout must create distinct audit
outcomes, and an approval must create a fresh whole-plan admission that cannot authorize
a different plan.

For an opt-in live-model smoke test, use sanitized seeded data with `LLM_MODE=live` and
`APPLY_MODE=off`. The run may propose, compile, evaluate, and audit, but cannot create an
admission or mutate data.
