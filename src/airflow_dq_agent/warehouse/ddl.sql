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
    sql_text        TEXT NOT NULL,
    rowcount        INTEGER,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS traces_trace_id_idx ON dq.traces (trace_id);
CREATE INDEX IF NOT EXISTS quarantine_run_idx ON dq.quarantine_rows (run_id);
