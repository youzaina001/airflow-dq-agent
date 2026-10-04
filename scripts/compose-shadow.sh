#!/usr/bin/env bash
# Issue #69: real CLI and Airflow Shadow Review with restricted logins.
set -euo pipefail

proof_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$proof_root"
export LLM_MODE=stub APPLY_MODE=off TRACE_POSTGRES=true
export OPENAI_API_KEY= OPENAI_BASE_URL=
export APPLY_DSN=postgresql+psycopg://disabled:disabled@127.0.0.1:1/disabled
export COMPOSE_PROJECT_NAME="dq-shadow-proof-$$"
mkdir -p config dags logs plugins traces
registry_file="$(mktemp "$proof_root/config/shadow-registry.XXXXXX")"
trap 'rm -f "$registry_file"' EXIT
export REGISTRY_PATH="/opt/airflow/config/$(basename "$registry_file")"
cat > "$registry_file" <<'YAML'
tables:
  - table: invoice
    schema_name: billing
    grain: one row per invoice_id
    description: Disposable shadow proof invoice.
    primary_key: [invoice_id]
    columns:
      - name: invoice_id
        dtype: int64
        unique: true
      - name: amount
        dtype: float64
        nullable: true
checks:
  - check_id: invoice.amount.completeness
    table: invoice
    column: amount
    dimension: completeness
    description: amount must be present
    policies:
      - action_id: quarantine_nulls
YAML

compose() { docker compose --env-file /dev/null "$@"; }
compose up -d --wait --wait-timeout 180 warehouse
proof_database="shadow_proof_$(date +%s)_$$"
compose exec -T warehouse psql -U dq -d warehouse -v ON_ERROR_STOP=1 \
  -c "CREATE DATABASE $proof_database"
compose build airflow-scheduler
provision_out="$(compose run -T --rm --no-deps --entrypoint python \
  -e "SHADOW_OWNER_DSN=postgresql+psycopg://dq:dq@warehouse:5432/$proof_database" \
  airflow-scheduler - <<'PY'
import os
from sqlalchemy import text
from airflow_dq_agent.adoption import apply_governance_schema, provision_restricted_logins
from airflow_dq_agent.warehouse.db import make_engine

owner = os.environ["SHADOW_OWNER_DSN"]
apply_governance_schema(owner)
credentials = provision_restricted_logins(owner)
with make_engine(owner).begin() as connection:
    connection.execute(text("CREATE SCHEMA billing"))
    connection.execute(text("CREATE TABLE billing.invoice (invoice_id BIGINT PRIMARY KEY, amount DOUBLE PRECISION)"))
    connection.execute(text("INSERT INTO billing.invoice VALUES (1, 10), (2, NULL)"))
    connection.execute(text("GRANT USAGE ON SCHEMA billing TO dq_read"))
    connection.execute(text("GRANT SELECT ON billing.invoice TO dq_read"))
print(f"READ_DSN={credentials.read_dsn}")
print(f"AUDIT_DSN={credentials.audit_dsn}")
PY
)"
while IFS='=' read -r name value; do
  case "$name" in
    READ_DSN) export READ_DSN="$value" ;;
    AUDIT_DSN) export AUDIT_DSN="$value" ;;
  esac
done <<< "$provision_out"
test -n "${READ_DSN:-}" && test -n "${AUDIT_DSN:-}"

compose up -d --build --wait --wait-timeout 180
deadline=$((SECONDS + 180))
until compose exec -T airflow-scheduler airflow dags list | grep -F 'dq_shadow'; do
  if ((SECONDS >= deadline)); then
    echo "dq_shadow was not registered after 180 seconds" >&2
    exit 1
  fi
  sleep 5
done
if compose exec -T airflow-scheduler python -m airflow_dq_agent.cli shadow --registry "$REGISTRY_PATH"; then
  cli_status=0
else
  cli_status=$?
fi
# A completed review of the seeded quality failure returns 1, not setup error 2.
test "$cli_status" -eq 1
compose exec -T airflow-scheduler airflow dags test dq_shadow 2026-09-26

evidence="$(compose exec -T warehouse psql -U dq -d "$proof_database" -Atc "
SELECT
  (SELECT count(*) FROM billing.invoice) = 2
  AND (SELECT amount = 10 FROM billing.invoice WHERE invoice_id = 1)
  AND (SELECT amount IS NULL FROM billing.invoice WHERE invoice_id = 2)
  AND NOT EXISTS (SELECT 1 FROM dq.quarantine_rows)
  AND NOT EXISTS (SELECT 1 FROM dq.apply_log)
  AND (SELECT count(*) FROM dq.check_runs) = 2
  AND NOT EXISTS (SELECT 1 FROM dq.check_runs
    WHERE check_id <> 'invoice.amount.completeness' OR status <> 'fail'
      OR n_failed <> 1 OR n_total <> 2)
  AND (SELECT count(*) FROM (
    SELECT body ->> 'quality_run_id' FROM dq.traces
    GROUP BY body ->> 'quality_run_id'
    HAVING array_agg(kind ORDER BY seq) = ARRAY[
      'quality_report', 'candidate_proposal', 'plan_compiled', 'evaluation', 'approval_review'
    ]::text[]
  ) AS complete_runs) = 2
  AND NOT EXISTS (SELECT 1 FROM dq.traces child
    WHERE child.kind <> 'quality_report' AND NOT EXISTS (
      SELECT 1 FROM dq.traces parent
      WHERE parent.trace_id = child.body -> 'predecessor_ids' ->> 0
        AND parent.body ->> 'quality_run_id' = child.body ->> 'quality_run_id'
        AND parent.kind = CASE child.kind
          WHEN 'candidate_proposal' THEN 'quality_report'
          WHEN 'plan_compiled' THEN 'candidate_proposal'
          WHEN 'evaluation' THEN 'plan_compiled'
          WHEN 'approval_review' THEN 'evaluation' END
    ))
  AND (SELECT count(*) FROM dq.traces) = 10
  AND NOT EXISTS (SELECT 1 FROM dq.traces WHERE kind = 'approval_review' AND (
    jsonb_array_length(body -> 'review_payload' -> 'items') <> 1
    OR body -> 'review_payload' -> 'items' -> 0 ->> 'table' <> 'invoice'
    OR body -> 'review_payload' -> 'items' -> 0 ->> 'target_count' <> '1'
    OR body -> 'review_payload' ->> 'evaluation_passed' <> 'true'
  ));")"
if [[ "$evidence" != t ]]; then
  echo "shadow proof failed: source, no-mutation, or PostgreSQL lineage evidence disagrees" >&2
  exit 1
fi
echo "shadow proof passed: CLI and real Airflow recorded two restricted, non-mutating reviews"
