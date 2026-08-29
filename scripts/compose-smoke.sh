#!/usr/bin/env bash
set -euo pipefail

# Deterministic PR path: stub candidate, shadow-only DAG, no mutation authority.
export LLM_MODE=stub
export APPLY_MODE=off
export TRACE_POSTGRES=true

docker compose up -d --build
docker compose exec -T airflow-scheduler python -m airflow_dq_agent.cli seed
docker compose exec -T airflow-scheduler airflow dags list | grep -F 'dq_daily'
docker compose exec -T airflow-scheduler airflow dags test dq_daily 2026-08-30

trace_count="$(docker compose exec -T warehouse psql -U dq -d warehouse -Atc \
  "SELECT count(*) FROM dq.traces WHERE kind IN ('quality_report', 'candidate_proposal', 'plan_compiled', 'evaluation')")"
test "$trace_count" -ge 4
mutation_count="$(docker compose exec -T warehouse psql -U dq -d warehouse -Atc \
  "SELECT count(*) FROM dq.quarantine_rows")"
test "$mutation_count" -eq 0
