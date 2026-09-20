#!/usr/bin/env bash
# Actual Airflow proof for issue #42: crash after commit, retry recovers one copy.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export LLM_MODE=stub
export APPLY_MODE=hitl
export TRACE_POSTGRES=true
export HITL_APPROVER_IDS=airflow
export DQ_HITL_TIMEOUT_SECONDS="${DQ_HITL_TIMEOUT_SECONDS:-90}"
export AIRFLOW_PYTHONPATH="/opt/airflow/src:/opt/airflow/scripts/compose-hitl-crash"
export DQ_COMPOSE_CRASH_MARKER="/opt/airflow/traces/.apply-crash-once"

DAG_FILE="dags/dq_external_invoice.py"
DAILY_DAG="dags/dq_daily.py"
DAILY_BAK=""
API="http://localhost:8080"
HITL_TASK="approve_remediation_plan"
APPLY_TASK="apply_after_admission_task"
DAG_ID="dq_external_invoice"
MARKER_HOST="traces/.apply-crash-once"

cleanup() {
  if [[ -n "$DAILY_BAK" && -f "$DAILY_BAK" ]]; then
    mv -f "$DAILY_BAK" "$DAILY_DAG" || true
  fi
  rm -f "$DAG_FILE"
}
trap cleanup EXIT

mkdir -p dags logs plugins config traces
rm -f "$MARKER_HOST"
cp examples/dq_external_invoice.py "$DAG_FILE"
if [[ -f "$DAILY_DAG" ]]; then
  DAILY_BAK="$(mktemp "$ROOT/dags/.dq_daily.py.bak.XXXXXX")"
  mv "$DAILY_DAG" "$DAILY_BAK"
fi

poll_until() {
  local timeout_seconds="$1" interval="$2" label="$3"
  shift 3
  local deadline=$((SECONDS + timeout_seconds))
  until "$@"; do
    if ((SECONDS >= deadline)); then
      echo "$label was not ready after ${timeout_seconds}s" >&2
      return 1
    fi
    sleep "$interval"
  done
}

docker compose up -d --build warehouse
poll_until 120 2 "warehouse" docker compose exec -T warehouse pg_isready -U dq -d warehouse \
  || { docker compose logs --no-color --tail=80 warehouse >&2 || true; exit 1; }

docker compose build airflow-scheduler
provision_out="$(
  docker compose run -T --rm --no-deps --entrypoint python airflow-scheduler - <<'PY'
from airflow_dq_agent.adoption import (
    apply_governance_schema,
    provision_restricted_logins,
    seed_external_invoice,
)

owner = "postgresql+psycopg://dq:dq@warehouse:5432/warehouse"
apply_governance_schema(owner)
seed_external_invoice(owner)
creds = provision_restricted_logins(owner)
print(f"READ_DSN={creds.read_dsn}")
print(f"AUDIT_DSN={creds.audit_dsn}")
print(f"APPLY_DSN={creds.apply_dsn}")
PY
)"
eval "$(printf '%s\n' "$provision_out" | grep -E '^(READ_DSN|AUDIT_DSN|APPLY_DSN)=')"
export READ_DSN AUDIT_DSN APPLY_DSN

# Recreate Airflow processes so PYTHONPATH picks up the compose-only crash hook.
docker compose up -d --build --force-recreate

dag_registered() {
  docker compose exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -F "$DAG_ID" >/dev/null
}

dag_ready_timeout_seconds="${DAG_READY_TIMEOUT_SECONDS:-180}"
poll_until "$dag_ready_timeout_seconds" 5 "DAG $DAG_ID" dag_registered \
  || {
    docker compose ps >&2 || true
    docker compose logs --no-color --tail=120 airflow-dag-processor airflow-scheduler >&2 || true
    exit 1
  }

api_curl() {
  docker compose exec -T airflow-apiserver curl -sS "$@"
}

api_curl_code() {
  api_curl -w '\n%{http_code}' "$@"
}

split_body_code() {
  local payload="$1"
  HTTP_CODE="$(printf '%s' "$payload" | tail -n1)"
  HTTP_BODY="$(printf '%s' "$payload" | sed '$d')"
}

api_ready() {
  api_curl -f -o /dev/null "$API/api/v2/version"
}

poll_until 180 3 "Airflow API" api_ready \
  || { docker compose logs --no-color --tail=80 airflow-apiserver >&2 || true; exit 1; }

token=""
fetch_token() {
  split_body_code "$(
    api_curl_code -X POST "$API/auth/token" \
      -H 'Content-Type: application/json' \
      -d '{"username":"airflow","password":"airflow"}' || true
  )"
  if [[ "$HTTP_CODE" == "201" || "$HTTP_CODE" == "200" ]]; then
    token="$(printf '%s' "$HTTP_BODY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')" \
      || return 1
    return 0
  fi
  echo "Waiting for FAB /auth/token (HTTP ${HTTP_CODE:-none})" >&2
  printf '%s\n' "$HTTP_BODY" >&2 || true
  return 1
}

poll_until 180 3 "Airflow JWT for user airflow" fetch_token \
  || { docker compose logs --no-color --tail=80 airflow-apiserver >&2 || true; exit 1; }
auth_header="Authorization: Bearer $token"

psql_wh() {
  docker compose exec -T warehouse psql -U dq -d warehouse -Atc "$1"
}

docker compose exec -T warehouse psql -U dq -d warehouse -Atc \
  "TRUNCATE dq.quarantine_rows RESTART IDENTITY; TRUNCATE dq.apply_log RESTART IDENTITY;" >/dev/null

hitl_details_ready() {
  local run_id="$1"
  split_body_code "$(
    api_curl_code -H "$auth_header" \
      "$API/api/v2/dags/${DAG_ID}/dagRuns/${run_id}/taskInstances/${HITL_TASK}/-1/hitlDetails" || true
  )"
  [[ "$HTTP_CODE" == "200" ]]
}

wait_for_hitl() {
  local run_id="$1"
  poll_until 180 3 "HITL detail for $run_id" hitl_details_ready "$run_id" \
    || {
      printf '%s\n' "${HTTP_BODY:-}" >&2 || true
      docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$run_id" >&2 || true
      exit 1
    }
}

dag_run_success() {
  local run_id="$1"
  DAG_STATE="$(
    api_curl -H "$auth_header" "$API/api/v2/dags/${DAG_ID}/dagRuns/${run_id}" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' || true
  )"
  [[ "$DAG_STATE" == "success" ]]
}

wait_dag_success() {
  local run_id="$1"
  poll_until 240 3 "DAG run $run_id success after crash/retry" dag_run_success "$run_id" \
    || {
      echo "DAG run $run_id ended in state ${DAG_STATE:-unknown}; task states:" >&2
      docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$run_id" >&2 || true
      psql_wh "SELECT seq, kind, body ->> 'quality_run_id' FROM dq.traces ORDER BY seq DESC LIMIT 20" >&2 || true
      exit 1
    }
}

docker compose exec -T airflow-scheduler airflow dags unpause "$DAG_ID"

before_seq="$(psql_wh "SELECT COALESCE(max(seq),0) FROM dq.traces")"
crash_run="crash-retry-proof"
docker compose exec -T airflow-scheduler airflow dags trigger "$DAG_ID" --run-id "$crash_run"
wait_for_hitl "$crash_run"

quality_after_trigger() {
  QID="$(
    psql_wh "SELECT body ->> 'quality_run_id' FROM dq.traces WHERE kind = 'quality_report' AND seq > ${before_seq} ORDER BY seq DESC LIMIT 1"
  )"
  [[ -n "$QID" ]]
}

QID=""
poll_until 60 2 "quality_report after $crash_run" quality_after_trigger || exit 1
crash_qid="$QID"

split_body_code "$(
  api_curl_code -X PATCH \
    -H "$auth_header" \
    -H 'Content-Type: application/json' \
    -d '{"chosen_options":["Approve"],"params_input":{"approval_note":"Copy the authorized missing-amount invoices into quarantine."}}' \
    "$API/api/v2/dags/${DAG_ID}/dagRuns/${crash_run}/taskInstances/${HITL_TASK}/-1/hitlDetails" || true
)"
if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "201" ]]; then
  echo "HITL Approve PATCH failed HTTP ${HTTP_CODE:-none}" >&2
  printf '%s\n' "$HTTP_BODY" >&2 || true
  exit 1
fi

wait_dag_success "$crash_run"

test -f "$MARKER_HOST"

apply_try_number="$(
  api_curl -H "$auth_header" \
    "$API/api/v2/dags/${DAG_ID}/dagRuns/${crash_run}/taskInstances" \
    | python3 -c "
import json, sys
payload = json.load(sys.stdin)
rows = payload.get('task_instances') or payload.get('task_instances', payload)
if isinstance(payload, list):
    rows = payload
elif isinstance(payload, dict):
    rows = payload.get('task_instances') or payload.get('content') or []
for row in rows:
    if row.get('task_id') == '${APPLY_TASK}':
        print(row.get('try_number') or row.get('tryNumber') or 0)
        break
else:
    print(0)
"
)"
if [[ "${apply_try_number:-0}" -lt 2 ]]; then
  echo "apply task try_number=${apply_try_number:-missing}; expected a retry after the crash" >&2
  api_curl -H "$auth_header" \
    "$API/api/v2/dags/${DAG_ID}/dagRuns/${crash_run}/taskInstances" >&2 || true
  docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$crash_run" >&2 || true
  exit 1
fi

test "$(psql_wh "SELECT amount = 10 FROM warehouse.ext_invoice WHERE invoice_id = 101")" = "t"
test "$(psql_wh "SELECT amount IS NULL FROM warehouse.ext_invoice WHERE invoice_id = 102")" = "t"
test "$(psql_wh "SELECT amount = 20 FROM warehouse.ext_invoice WHERE invoice_id = 103")" = "t"
test "$(psql_wh "SELECT amount IS NULL FROM warehouse.ext_invoice WHERE invoice_id = 104")" = "t"
test "$(
  psql_wh "SELECT count(*) FROM dq.quarantine_rows WHERE table_name = 'warehouse.ext_invoice'"
)" -eq 2
test "$(
  psql_wh "SELECT string_agg(pk_json ->> 'invoice_id', ',' ORDER BY pk_json ->> 'invoice_id') FROM dq.quarantine_rows WHERE table_name = 'warehouse.ext_invoice'"
)" = "102,104"
test "$(
  psql_wh "SELECT count(*) FROM dq.traces WHERE kind = 'human_approved' AND body ->> 'quality_run_id' = '$crash_qid'"
)" -ge 1
test "$(
  psql_wh "SELECT count(*) FROM dq.traces WHERE kind = 'apply_succeeded' AND body ->> 'quality_run_id' = '$crash_qid'"
)" -eq 1
test "$(
  psql_wh "SELECT count(*) FROM dq.traces WHERE kind = 'apply_failed' AND body ->> 'quality_run_id' = '$crash_qid'"
)" -eq 0
test "$(psql_wh "SELECT count(*) FROM dq.apply_log")" -eq 1

committed_id="$(
  psql_wh "SELECT body ->> 'apply_result_id' FROM dq.traces WHERE kind = 'apply_succeeded' AND body ->> 'quality_run_id' = '$crash_qid' ORDER BY seq DESC LIMIT 1"
)"
marker_id="$(cat "$MARKER_HOST")"
test -n "$committed_id"
test "$marker_id" = "$committed_id"

docker compose exec -T airflow-scheduler python - <<PY
import os

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from airflow_dq_agent.warehouse.db import make_engine

read_engine = make_engine(os.environ["READ_DSN"])
apply_engine = make_engine(os.environ["APPLY_DSN"])

def must_fail(engine, sql: str) -> None:
    try:
        with engine.begin() as connection:
            connection.execute(text(sql))
    except DBAPIError:
        return
    raise SystemExit(f"restricted login unexpectedly succeeded: {sql}")

must_fail(read_engine, "INSERT INTO warehouse.ext_invoice (invoice_id, amount) VALUES (999, 1.0)")
must_fail(apply_engine, "UPDATE warehouse.ext_invoice SET amount = 0 WHERE invoice_id = 101")
must_fail(apply_engine, "DELETE FROM warehouse.ext_invoice WHERE invoice_id = 101")
must_fail(apply_engine, "UPDATE dq.traces SET body = '{}'::jsonb")
print("restricted credentials still cannot write source or rewrite audit")
PY

echo "issue-42 compose proof: crash after commit retried the same admission with one quarantine copy"
