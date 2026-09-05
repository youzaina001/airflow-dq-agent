-- Synthetic e-commerce + clinical-trial-ish warehouse.
-- No real PII. pgvector is available on the image but unused in v1.

CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_sk     BIGINT PRIMARY KEY,
    customer_nk     TEXT NOT NULL,
    email           TEXT NOT NULL,
    country         TEXT NOT NULL,
    signup_date     DATE NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_sk      BIGINT PRIMARY KEY,
    sku             TEXT NOT NULL,
    category        TEXT NOT NULL,
    unit_price      DOUBLE PRECISION NOT NULL,
    active_flag     BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_id        BIGINT PRIMARY KEY,
    customer_sk     BIGINT NOT NULL,
    order_ts        TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL,
    total_amount    DOUBLE PRECISION,
    currency        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    order_item_id   BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL,
    product_sk      BIGINT NOT NULL,
    qty             INTEGER NOT NULL,
    unit_price      DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_site (
    site_sk         BIGINT PRIMARY KEY,
    site_id         TEXT NOT NULL,
    country         TEXT NOT NULL,
    region          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_patient (
    patient_sk      BIGINT PRIMARY KEY,
    subject_id      TEXT NOT NULL,
    site_sk         BIGINT NOT NULL,
    sex             TEXT,
    birth_year      INTEGER NOT NULL,
    enrolled_on     DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_visits (
    visit_id        BIGINT PRIMARY KEY,
    patient_sk      BIGINT NOT NULL,
    visit_code      TEXT NOT NULL,
    window_start    DATE NOT NULL,
    window_end      DATE NOT NULL,
    visit_date      DATE,
    status          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_adverse_events (
    ae_id           BIGINT PRIMARY KEY,
    patient_sk      BIGINT NOT NULL,
    term_code       TEXT,
    severity        TEXT NOT NULL,
    onset_date      DATE NOT NULL,
    related_flag    BOOLEAN NOT NULL
);

-- Schema-drift fixture: a column the contract does not know about.
ALTER TABLE warehouse.dim_customer
    ADD COLUMN IF NOT EXISTS shadow_segment TEXT;

CREATE TABLE IF NOT EXISTS dq.quarantine_rows (
    quarantine_id   BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    pk_json         JSONB NOT NULL,
    reason          TEXT NOT NULL,
    payload         JSONB NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dq.traces (
    seq             BIGSERIAL PRIMARY KEY,
    trace_id        TEXT NOT NULL,
    kind            TEXT NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    body            JSONB NOT NULL
);

ALTER TABLE dq.traces
    ADD COLUMN IF NOT EXISTS trace_id TEXT;

CREATE TABLE IF NOT EXISTS dq.check_runs (
    seq             BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    check_id        TEXT NOT NULL,
    status          TEXT NOT NULL,
    n_failed        INTEGER NOT NULL,
    n_total         INTEGER NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    body            JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS dq.apply_log (
    seq             BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    action_id       TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    plan_id         TEXT,
    admission_id    TEXT,
    item_id         TEXT,
    target_count    INTEGER,
    target_fingerprint TEXT,
    rowcount        INTEGER,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE dq.apply_log
    ADD COLUMN IF NOT EXISTS plan_id TEXT,
    ADD COLUMN IF NOT EXISTS admission_id TEXT,
    ADD COLUMN IF NOT EXISTS item_id TEXT,
    ADD COLUMN IF NOT EXISTS target_count INTEGER,
    ADD COLUMN IF NOT EXISTS target_fingerprint TEXT;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'dq' AND table_name = 'apply_log' AND column_name = 'sql_text'
    ) THEN
        ALTER TABLE dq.apply_log ALTER COLUMN sql_text DROP NOT NULL;
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS traces_trace_id_unique_idx ON dq.traces (trace_id);
CREATE INDEX IF NOT EXISTS check_runs_run_check_idx ON dq.check_runs (run_id, check_id);
CREATE INDEX IF NOT EXISTS quarantine_run_idx ON dq.quarantine_rows (run_id);
CREATE INDEX IF NOT EXISTS apply_log_plan_idx ON dq.apply_log (plan_id, run_id);
CREATE UNIQUE INDEX IF NOT EXISTS apply_log_admission_id_unique_idx
    ON dq.apply_log (admission_id)
    WHERE admission_id IS NOT NULL AND admission_id <> '';

-- The only audit write authority granted to dq_apply.  Its caller supplies no SQL:
-- controlled statements are rendered by the trusted local executor before this
-- function atomically records the minimized apply-result evidence.
CREATE OR REPLACE FUNCTION dq.record_apply_result(
    p_event_id TEXT,
    p_kind TEXT,
    p_event_body JSONB,
    p_run_id TEXT,
    p_plan_id TEXT,
    p_admission_id TEXT,
    p_item_id TEXT,
    p_action_id TEXT,
    p_table_name TEXT,
    p_target_count INTEGER,
    p_target_fingerprint TEXT,
    p_rowcount INTEGER
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = dq, pg_temp
AS $$
BEGIN
    INSERT INTO dq.apply_log (
        run_id, plan_id, admission_id, item_id, action_id, table_name,
        target_count, target_fingerprint, rowcount
    ) VALUES (
        p_run_id, p_plan_id, p_admission_id, p_item_id, p_action_id, p_table_name,
        p_target_count, p_target_fingerprint, p_rowcount
    );
    INSERT INTO dq.traces (trace_id, kind, body)
    VALUES (p_event_id, p_kind, p_event_body);
END;
$$;

-- Role adapters. Production deploys distinct login roles/DSNs that inherit these
-- capabilities; the local `dq` bootstrap role is intentionally only a convenience.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dq_read') THEN
        CREATE ROLE dq_read NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dq_audit') THEN
        CREATE ROLE dq_audit NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dq_apply') THEN
        CREATE ROLE dq_apply NOLOGIN;
    END IF;
END;
$$;

GRANT USAGE ON SCHEMA warehouse, dq TO dq_read, dq_audit, dq_apply;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO dq_read, dq_apply;
GRANT INSERT ON dq.traces, dq.check_runs TO dq_audit;
GRANT SELECT ON dq.traces TO dq_audit;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA dq TO dq_audit;
GRANT INSERT ON dq.quarantine_rows TO dq_apply;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dq TO dq_apply;
REVOKE ALL ON dq.traces, dq.check_runs, dq.apply_log FROM dq_apply;
REVOKE ALL ON dq.traces, dq.check_runs, dq.apply_log FROM dq_read;
REVOKE UPDATE, DELETE ON dq.traces, dq.check_runs FROM dq_audit;
REVOKE ALL ON FUNCTION dq.record_apply_result(
    TEXT, TEXT, JSONB, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INTEGER, TEXT, INTEGER
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION dq.record_apply_result(
    TEXT, TEXT, JSONB, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INTEGER, TEXT, INTEGER
) TO dq_apply;

CREATE OR REPLACE FUNCTION dq.admission_consumed(p_admission_id TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = dq, pg_temp
AS $$
BEGIN
    IF p_admission_id IS NULL OR btrim(p_admission_id) = '' THEN
        RETURN FALSE;
    END IF;
    RETURN EXISTS (
        SELECT 1 FROM dq.apply_log WHERE admission_id = p_admission_id
    );
END;
$$;

REVOKE ALL ON FUNCTION dq.admission_consumed(TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION dq.admission_consumed(TEXT) TO dq_apply;
