#!/usr/bin/env bash
# Actual Airflow proof for issue #41: Reject and Timeout write zero quarantine rows.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export LLM_MODE=stub
export APPLY_MODE=hitl
export TRACE_POSTGRES=true
export HITL_APPROVER_IDS=airflow
export DQ_HITL_TIMEOUT_SECONDS="${DQ_HITL_TIMEOUT_SECONDS:-90}"

DAG_FILE="dags/dq_external_invoice.py"
DAILY_DAG="dags/dq_daily.py"
DAILY_BAK=""
API="http://localhost:8080"
HITL_TASK="approve_remediation_plan"
DAG_ID="dq_external_invoice"

cleanup() {
  if [[ -n "$DAILY_BAK" && -f "$DAILY_BAK" ]]; then
    mv -f "$DAILY_BAK" "$DAILY_DAG" || true
  fi
  rm -f "$DAG_FILE"
}
trap cleanup EXIT

mkdir -p dags logs plugins config traces
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

docker compose up -d --build

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

assert_non_mutating_outcome() {
  local run_id="$1"
  local expected_kind="$2"
  local expected_actor="$3"
  test "$(
    psql_wh "SELECT count(*) FROM dq.quarantine_rows WHERE table_name = 'warehouse.ext_invoice' AND run_id = '$run_id'"
  )" -eq 0
  test "$(psql_wh "SELECT amount = 10 FROM warehouse.ext_invoice WHERE invoice_id = 101")" = "t"
  test "$(psql_wh "SELECT amount IS NULL FROM warehouse.ext_invoice WHERE invoice_id = 102")" = "t"
  test "$(psql_wh "SELECT amount = 20 FROM warehouse.ext_invoice WHERE invoice_id = 103")" = "t"
  test "$(psql_wh "SELECT amount IS NULL FROM warehouse.ext_invoice WHERE invoice_id = 104")" = "t"
  test "$(
    psql_wh "SELECT count(*) FROM dq.traces WHERE kind = 'apply_succeeded' AND body ->> 'quality_run_id' = '$run_id'"
  )" -eq 0
  test "$(
    psql_wh "SELECT count(*) FROM dq.traces WHERE kind = 'human_approved' AND body ->> 'quality_run_id' = '$run_id'"
  )" -eq 0
  test "$(
    psql_wh "SELECT count(*) FROM dq.traces WHERE kind = '$expected_kind' AND body ->> 'quality_run_id' = '$run_id'"
  )" -ge 1
  if [[ -n "$expected_actor" ]]; then
    test "$(
      psql_wh "SELECT body ->> 'decision_actor' FROM dq.traces WHERE kind = '$expected_kind' AND body ->> 'quality_run_id' = '$run_id' ORDER BY seq DESC LIMIT 1"
    )" = "$expected_actor"
  fi
}

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

quality_trace_found() {
  QID="$(psql_wh "SELECT body ->> 'quality_run_id' FROM dq.traces WHERE kind = 'quality_report' ORDER BY seq DESC LIMIT 1")"
  [[ -n "$QID" ]]
}

quality_run_id_for() {
  local dag_run_id="$1"
  QID=""
  poll_until 60 2 "quality_report trace after $dag_run_id" quality_trace_found || exit 1
  printf '%s' "$QID"
}

timeout_trace_found() {
  QID="$(psql_wh "SELECT body ->> 'quality_run_id' FROM dq.traces WHERE kind = 'human_timed_out' AND seq > ${before_timeout_seq} ORDER BY seq DESC LIMIT 1")"
  [[ -n "$QID" ]]
}

dag_run_terminal() {
  local run_id="$1"
  DAG_STATE="$(
    api_curl -H "$auth_header" "$API/api/v2/dags/${DAG_ID}/dagRuns/${run_id}" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' || true
  )"
  case "$DAG_STATE" in
    success|skipped) return 0 ;;
    failed)
      echo "DAG run $run_id failed; task states:" >&2
      docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$run_id" >&2 || true
      exit 1
      ;;
    *) return 1 ;;
  esac
}

wait_dag_terminal() {
  local run_id="$1"
  poll_until 180 3 "DAG run $run_id to finish" dag_run_terminal "$run_id"
}

docker compose exec -T airflow-scheduler airflow dags unpause "$DAG_ID"

# --- Reject ---
reject_run="reject-proof"
docker compose exec -T airflow-scheduler airflow dags trigger "$DAG_ID" --run-id "$reject_run"
wait_for_hitl "$reject_run"
reject_qid="$(quality_run_id_for "$reject_run")"
split_body_code "$(
  api_curl_code -X PATCH \
    -H "$auth_header" \
    -H 'Content-Type: application/json' \
    -d '{"chosen_options":["Reject"],"params_input":{"approval_note":"Do not copy invoices into quarantine."}}' \
    "$API/api/v2/dags/${DAG_ID}/dagRuns/${reject_run}/taskInstances/${HITL_TASK}/-1/hitlDetails" || true
)"
if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "201" ]]; then
  echo "HITL Reject PATCH failed HTTP ${HTTP_CODE:-none}" >&2
  printf '%s\n' "$HTTP_BODY" >&2 || true
  exit 1
fi
wait_dag_terminal "$reject_run"
assert_non_mutating_outcome "$reject_qid" "human_rejected" ""

# --- Timeout (do not respond; HITLTrigger emits timedout=True) ---
timeout_run="timeout-proof"
before_timeout_seq="$(psql_wh "SELECT COALESCE(max(seq),0) FROM dq.traces")"
docker compose exec -T airflow-scheduler airflow dags trigger "$DAG_ID" --run-id "$timeout_run"
wait_for_hitl "$timeout_run"
timeout_wait=$((DQ_HITL_TIMEOUT_SECONDS + 60))
QID=""
poll_until "$timeout_wait" 5 "human_timed_out trace" timeout_trace_found \
  || {
    psql_wh "SELECT seq, kind, body ->> 'quality_run_id' FROM dq.traces ORDER BY seq DESC LIMIT 20" >&2 || true
    docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$timeout_run" >&2 || true
    exit 1
  }
timeout_qid="$QID"
wait_dag_terminal "$timeout_run"
assert_non_mutating_outcome "$timeout_qid" "human_timed_out" "airflow-timeout"

echo "issue-41 compose proof: reject and timeout wrote zero quarantine rows"
