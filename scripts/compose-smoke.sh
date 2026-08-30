#!/usr/bin/env bash
set -euo pipefail

# Deterministic PR path: stub candidate, shadow-only DAG, no mutation authority.
export LLM_MODE=stub
export APPLY_MODE=off
export TRACE_POSTGRES=true

docker compose up -d --build
docker compose exec -T airflow-scheduler python -m airflow_dq_agent.cli seed

dag_ready_timeout_seconds="${DAG_READY_TIMEOUT_SECONDS:-120}"
dag_ready_deadline=$((SECONDS + dag_ready_timeout_seconds))
until docker compose exec -T airflow-scheduler airflow dags list | grep -F 'dq_daily'; do
  if ((SECONDS >= dag_ready_deadline)); then
    echo "dq_daily was not available after ${dag_ready_timeout_seconds}s" >&2
    docker compose ps >&2 || true
    docker compose logs --no-color --tail=100 airflow-dag-processor airflow-scheduler >&2 || true
    exit 1
  fi
  echo "Waiting for the DAG processor to register dq_daily..." >&2
  sleep 5
done

docker compose exec -T airflow-scheduler airflow dags test dq_daily 2026-08-30

trace_count="$(docker compose exec -T warehouse psql -U dq -d warehouse -Atc \
  "SELECT count(*) FROM dq.traces WHERE kind IN ('quality_report', 'candidate_proposal', 'plan_compiled', 'evaluation')")"
test "$trace_count" -ge 4
mutation_count="$(docker compose exec -T warehouse psql -U dq -d warehouse -Atc \
  "SELECT count(*) FROM dq.quarantine_rows")"
test "$mutation_count" -eq 0
