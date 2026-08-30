# Hands-on guide: Governed Airflow DQ Agent

This guide walks through the package as it exists today. Start in shadow mode:
the default configuration uses a deterministic stub and never mutates the
warehouse. Move to HITL only after you have seen the shadow flow end to end.

## What you will exercise

| Path | What it proves | Can it mutate data? |
| --- | --- | --- |
| Fixture CLI | Checks, proposal, proposal evals, audit trace | No |
| Replay CLI | Recorded proposal is revalidated | No |
| Compose shadow DAG | Postgres checks, Airflow orchestration, plan and lineage | No |
| Compose HITL | Whole-plan approval, target lock, controlled apply | Yes: quarantine copies only |
| Live-model smoke | Read-only model proposal path | No, when `APPLY_MODE=off` |

The v1 mutation actions write copies to `dq.quarantine_rows`; they do not delete
or update source rows. `null_fill` is catalogued but has no reviewed check policy,
so it cannot compile into an executable plan.

## 1. Prepare the repository

Run all commands from the repository root.

```bash
cd airflow-dq-agent
```

You need Python 3.12 or later. Docker Desktop / Docker Engine with the Compose
plugin is needed only for the Postgres and Airflow sections.

With `uv` installed, create the development environment:

```bash
uv sync --extra dev
```

Without `uv`, create a virtual environment and install the development extras:

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

Set a helper for the CLI examples:

```bash
PYTHON=.venv/bin/python
```

Confirm the local loop is available:

```bash
make help
```

## 2. Start with the safe, database-free flow

This is the fastest way to understand the product boundary. It uses deterministic
in-memory fixtures, the stub proposal agent, and `APPLY_MODE=off`.

```bash
make test
make eval
make demo
```

Expected observations:

- The demo reports a mix of failed and passed checks.
- The stub creates one candidate action per failed check.
- The safe proposal evaluation passes.
- The deliberate `drop_table` proposal fails destructive-risk and allow-list checks.
- The deliberate green-report `null_fill` proposal fails groundedness.
- The demo prints `apply skipped (APPLY_MODE=off)`.

Run the stages individually to inspect their JSON contracts:

```bash
$PYTHON -m airflow_dq_agent.cli suite --no-db
$PYTHON -m airflow_dq_agent.cli propose --no-db
$PYTHON -m airflow_dq_agent.cli eval --no-db
```

The commands append minimized JSONL lineage under `traces/` by default. Inspect
the latest event without expecting prompts or sample rows to be persisted:

```bash
tail -n 1 traces/agent-traces.jsonl
```

## 3. Exercise replay mode

Replay is useful for testing proposal parsing and evaluation without a live model.
The fixture is deliberately recorded in the repository.

```bash
LLM_MODE=replay \
REPLAY_TRACE_PATH=evals/fixtures/traces/replay-proposal.json \
$PYTHON -m airflow_dq_agent.cli propose --no-db
```

Then run the same mode through evaluation:

```bash
LLM_MODE=replay \
REPLAY_TRACE_PATH=evals/fixtures/traces/replay-proposal.json \
$PYTHON -m airflow_dq_agent.cli eval --no-db
```

Missing, malformed, or invalid replay data fails closed; it does not fall back to
the stub or a live model.

## 4. Bring up the local warehouse and Airflow in shadow mode

Copy the local environment template once, keep its safe defaults, then start the
stack and load deterministic defects.

```bash
cp .env.example .env
make up
make seed
docker compose ps
```

The local warehouse is exposed on `localhost:5433`. Explore the actual quality
report and proposal against Postgres:

```bash
$PYTHON -m airflow_dq_agent.cli suite
$PYTHON -m airflow_dq_agent.cli propose
$PYTHON -m airflow_dq_agent.cli eval
```

To inspect the seeded source and its quarantine table directly:

```bash
docker compose exec warehouse psql -U dq -d warehouse -c \
  "SELECT COUNT(*) AS quarantined_rows FROM dq.quarantine_rows;"

docker compose exec warehouse psql -U dq -d warehouse -c \
  "SELECT order_id, total_amount FROM warehouse.fact_orders WHERE total_amount IS NULL;"
```

At this point `dq.quarantine_rows` should contain zero rows. Running `suite`,
`propose`, or `eval` from the CLI does not apply a remediation.

## 5. Run the Airflow shadow DAG

Open [http://localhost:8080](http://localhost:8080) and sign in with
`airflow` / `airflow`.

1. Find `dq_daily` and unpause it. It is intentionally paused on creation.
2. Click **Trigger DAG**.
3. Open the run and inspect the task logs.

With the `.env` defaults (`LLM_MODE=stub`, `APPLY_MODE=off`), the run exercises:

```text
run_suite_task
  → propose_stub_task
  → audit_candidate_task
  → compile_plan_task
  → evaluate_plan_task
```

There is no approval or apply task in shadow mode. Confirm that by checking the
quarantine count again:

```bash
docker compose exec warehouse psql -U dq -d warehouse -c \
  "SELECT COUNT(*) AS quarantined_rows FROM dq.quarantine_rows;"
```

For durable Postgres lineage, set `TRACE_POSTGRES=true` in `.env` before starting
the stack (or recreate the services after changing it):

```bash
docker compose up -d --force-recreate
```

After another DAG run, inspect the lineage kinds:

```bash
docker compose exec warehouse psql -U dq -d warehouse -c \
  "SELECT kind, COUNT(*) FROM dq.traces GROUP BY kind ORDER BY kind;"
```

The deterministic Compose verification performs the same shadow-only exercise:

```bash
make compose-smoke
```

It builds the image, seeds the warehouse, runs `dq_daily`, checks persisted
lineage, and asserts that no quarantine rows were written.

## 6. Exercise the audited HITL path

This is the only section that can write rows. It remains bounded: approval applies
the entire evaluated plan, recomputes and locks the approved target set, and writes
quarantine copies rather than deleting source records.

First, edit `.env` to use the local test identity and durable audit store:

```dotenv
LLM_MODE=stub
APPLY_MODE=hitl
TRACE_POSTGRES=true
HITL_APPROVER_IDS=airflow
```

Recreate the services so the Airflow DAG is reparsed with the HITL branch, then
reseed the deterministic warehouse:

```bash
docker compose up -d --force-recreate
make seed
```

In the Airflow UI, trigger `dq_daily`. When `approve_remediation_plan` is ready:

1. Review the upstream plan/evaluation task output.
2. Approve it as `airflow`.
3. Enter a non-empty approval note, for example `Reviewed exact target counts.`

The remaining tasks create an apply admission and run the controlled transaction.
Verify both lineage and the copy-only outcome:

```bash
docker compose exec warehouse psql -U dq -d warehouse -c \
  "SELECT kind, COUNT(*) FROM dq.traces GROUP BY kind ORDER BY kind;"

docker compose exec warehouse psql -U dq -d warehouse -c \
  "SELECT table_name, action_id, target_count, rowcount FROM dq.apply_log ORDER BY applied_at DESC;"

docker compose exec warehouse psql -U dq -d warehouse -c \
  "SELECT COUNT(*) AS quarantined_rows FROM dq.quarantine_rows;"
```

For the rejection test, record the current quarantine count, trigger a new DAG
run, reject the approval request, and confirm the count is unchanged. The trace
history should include a `human_rejected` event; an approved run records
`human_approved` and `apply_succeeded`.

If the plan is blocked or its evaluation fails, Airflow skips the approval path.
That is expected: approval cannot repair a failed deterministic gate.

## 7. Optional: live-model smoke, still non-mutating

Use only the synthetic seeded warehouse. Configure a valid OpenAI-compatible
credential in `.env`, retain the safe apply mode, then recreate the stack:

```dotenv
LLM_MODE=live
APPLY_MODE=off
OPENAI_API_KEY=your-key
# Optional for an OpenAI-compatible endpoint:
# OPENAI_BASE_URL=https://example.internal/v1
```

```bash
docker compose up -d --force-recreate
make seed
```

Trigger `dq_daily` in Airflow and inspect the live proposal task. The model can
read only the catalog, fixed-size check samples, and observed schema. It cannot
submit arbitrary warehouse SQL, create an admission, or mutate data while
`APPLY_MODE=off`.

Afterward, verify the guardrail one more time:

```bash
docker compose exec warehouse psql -U dq -d warehouse -c \
  "SELECT COUNT(*) AS quarantined_rows FROM dq.quarantine_rows;"
```

## 8. Reset safely

Return `.env` to the safe defaults before further experimentation:

```dotenv
LLM_MODE=stub
APPLY_MODE=off
TRACE_POSTGRES=false
```

Recreate the services after changing those values:

```bash
docker compose up -d --force-recreate
```

Use the following when you are done. It stops the containers and retains Docker
volumes, so your local data is not deleted:

```bash
make down
```

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `make seed` cannot connect | Wait for `docker compose ps` to show the warehouse healthy, then retry. |
| `dq_daily` is missing | Check `docker compose logs airflow-dag-processor` and ensure the stack was recreated after changing `.env`. |
| No approval task appears | Confirm `APPLY_MODE=hitl`, `TRACE_POSTGRES=true`, and a non-empty `HITL_APPROVER_IDS`, then recreate the stack. |
| Live mode fails | Confirm `OPENAI_API_KEY` and the Airflow `pydanticai_default` connection created by Compose; live mode intentionally refuses to fall back. |
| Source rows seem unchanged after approval | That is the v1 design: successful remediation writes quarantine copies only. Query `dq.quarantine_rows` and `dq.apply_log`. |

For a clean verification pass at any time, run:

```bash
make ci
make demo
```
