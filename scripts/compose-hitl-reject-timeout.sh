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
  DAILY_BAK="$(mktemp "$ROOT/dags/.dq_daily.py.bak.XXXX")"
  mv "$DAILY_DAG" "$DAILY_BAK"
fi

docker compose up -d --build warehouse
warehouse_ready_deadline=$((SECONDS + 120))
until docker compose exec -T warehouse pg_isready -U dq -d warehouse >/dev/null 2>&1; do
  if ((SECONDS >= warehouse_ready_deadline)); then
    echo "warehouse was not ready" >&2
    docker compose logs --no-color --tail=80 warehouse >&2 || true
    exit 1
  fi
  sleep 2
done

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

dag_ready_timeout_seconds="${DAG_READY_TIMEOUT_SECONDS:-180}"
dag_ready_deadline=$((SECONDS + dag_ready_timeout_seconds))
until docker compose exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -F "$DAG_ID"; do
  if ((SECONDS >= dag_ready_deadline)); then
    echo "$DAG_ID was not available after ${dag_ready_timeout_seconds}s" >&2
    docker compose ps >&2 || true
    docker compose logs --no-color --tail=120 airflow-dag-processor airflow-scheduler >&2 || true
    exit 1
  fi
  echo "Waiting for the DAG processor to register $DAG_ID..." >&2
  sleep 5
done

api_ready_deadline=$((SECONDS + 180))
until curl -sf "$API/api/v2/version" >/dev/null; do
  if ((SECONDS >= api_ready_deadline)); then
    echo "Airflow API was not ready" >&2
    docker compose logs --no-color --tail=80 airflow-apiserver >&2 || true
    exit 1
  fi
  sleep 3
done

token="$(
  curl -sf -X POST "$API/auth/token" \
    -H 'Content-Type: application/json' \
    -d '{"username":"airflow","password":"airflow"}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
)"
auth_header="Authorization: Bearer $token"

psql_wh() {
  docker compose exec -T warehouse psql -U dq -d warehouse -Atc "$1"
}

assert_zero_writes() {
  local run_id="$1"
  local expected_kind="$2"
  local expected_actor="$3"
  local copied
  copied="$(psql_wh "SELECT count(*) FROM dq.quarantine_rows WHERE table_name = 'warehouse.ext_invoice'")"
  test "$copied" -eq 0
  test "$(psql_wh "SELECT amount FROM warehouse.ext_invoice WHERE invoice_id = 101")" = "10"
  test "$(psql_wh "SELECT amount FROM warehouse.ext_invoice WHERE invoice_id = 102")" = ""
  test "$(psql_wh "SELECT amount FROM warehouse.ext_invoice WHERE invoice_id = 103")" = "20"
  test "$(psql_wh "SELECT amount FROM warehouse.ext_invoice WHERE invoice_id = 104")" = ""
  test "$(psql_wh "SELECT count(*) FROM dq.traces WHERE kind = 'apply_succeeded' AND body ->> 'quality_run_id' = '$run_id'")" -eq 0
  test "$(psql_wh "SELECT count(*) FROM dq.traces WHERE kind = 'human_approved' AND body ->> 'quality_run_id' = '$run_id'")" -eq 0
  test "$(psql_wh "SELECT count(*) FROM dq.traces WHERE kind = '$expected_kind' AND body ->> 'quality_run_id' = '$run_id'")" -ge 1
  if [[ -n "$expected_actor" ]]; then
    test "$(psql_wh "SELECT body ->> 'decision_actor' FROM dq.traces WHERE kind = '$expected_kind' AND body ->> 'quality_run_id' = '$run_id' ORDER BY seq DESC LIMIT 1")" = "$expected_actor"
  fi
}

wait_for_hitl() {
  local run_id="$1"
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    code="$(
      curl -sS -o /tmp/hitl-detail.json -w '%{http_code}' \
        -H "$auth_header" \
        "$API/api/v2/dags/${DAG_ID}/dagRuns/${run_id}/taskInstances/${HITL_TASK}/-1/hitlDetails" || true
    )"
    if [[ "$code" == "200" ]]; then
      return 0
    fi
    sleep 3
  done
  echo "HITL detail was not ready for $run_id" >&2
  cat /tmp/hitl-detail.json >&2 || true
  docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$run_id" >&2 || true
  exit 1
}

quality_run_id_for() {
  local dag_run_id="$1"
  local deadline=$((SECONDS + 60))
  local qid=""
  while ((SECONDS < deadline)); do
    qid="$(psql_wh "SELECT body ->> 'quality_run_id' FROM dq.traces WHERE kind = 'quality_report' ORDER BY seq DESC LIMIT 1")"
    if [[ -n "$qid" ]]; then
      printf '%s' "$qid"
      return 0
    fi
    sleep 2
  done
  echo "No quality_report trace after $dag_run_id" >&2
  exit 1
}

wait_dag_terminal() {
  local run_id="$1"
  local deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    state="$(
      curl -sf -H "$auth_header" \
        "$API/api/v2/dags/${DAG_ID}/dagRuns/${run_id}" \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' || true
    )"
    case "$state" in
      success|failed|skipped) return 0 ;;
    esac
    sleep 3
  done
  echo "DAG run $run_id did not finish (state=$state)" >&2
  docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$run_id" >&2 || true
  exit 1
}

docker compose exec -T airflow-scheduler airflow dags unpause "$DAG_ID"

# --- Reject ---
reject_run="reject-proof"
docker compose exec -T airflow-scheduler airflow dags trigger "$DAG_ID" --run-id "$reject_run"
wait_for_hitl "$reject_run"
reject_qid="$(quality_run_id_for "$reject_run")"
curl -sf -X PATCH \
  -H "$auth_header" \
  -H 'Content-Type: application/json' \
  -d '{"chosen_options":["Reject"],"params_input":{"approval_note":"Do not copy invoices into quarantine."}}' \
  "$API/api/v2/dags/${DAG_ID}/dagRuns/${reject_run}/taskInstances/${HITL_TASK}/-1/hitlDetails" >/dev/null
wait_dag_terminal "$reject_run"
assert_zero_writes "$reject_qid" "human_rejected" ""

# --- Timeout (do not respond; HITLTrigger emits timedout=True) ---
timeout_run="timeout-proof"
before_timeout_seq="$(psql_wh "SELECT COALESCE(max(seq),0) FROM dq.traces")"
docker compose exec -T airflow-scheduler airflow dags trigger "$DAG_ID" --run-id "$timeout_run"
wait_for_hitl "$timeout_run"
timeout_wait=$((DQ_HITL_TIMEOUT_SECONDS + 60))
timeout_deadline=$((SECONDS + timeout_wait))
timeout_qid=""
while ((SECONDS < timeout_deadline)); do
  timeout_qid="$(psql_wh "SELECT body ->> 'quality_run_id' FROM dq.traces WHERE kind = 'human_timed_out' AND seq > ${before_timeout_seq} ORDER BY seq DESC LIMIT 1")"
  if [[ -n "$timeout_qid" ]]; then
    break
  fi
  sleep 5
done
if [[ -z "$timeout_qid" ]]; then
  echo "human_timed_out was not recorded within ${timeout_wait}s" >&2
  psql_wh "SELECT seq, kind, body ->> 'quality_run_id' FROM dq.traces ORDER BY seq DESC LIMIT 20" >&2 || true
  docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run "$DAG_ID" "$timeout_run" >&2 || true
  exit 1
fi
wait_dag_terminal "$timeout_run"
assert_zero_writes "$timeout_qid" "human_timed_out" "airflow-timeout"

echo "issue-41 compose proof: reject and timeout wrote zero quarantine rows"
