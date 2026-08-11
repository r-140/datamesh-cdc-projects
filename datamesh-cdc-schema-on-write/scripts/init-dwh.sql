CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders_cdc (
    id              BIGINT PRIMARY KEY,
    customer_id     BIGINT,
    total_amount    NUMERIC(12,2) NOT NULL,  -- ← NOT NULL для демо
    status          TEXT,
    created_at      BIGINT,
    updated_at      BIGINT,
    __deleted       TEXT,
    __op            TEXT,
    __source_ts_ms  BIGINT,
    __kafka_partition INT,
    __kafka_offset  BIGINT
);

CREATE TABLE IF NOT EXISTS raw.customers_cdc (
    id              BIGINT PRIMARY KEY,
    name            TEXT,
    full_name       TEXT,
    email           TEXT NOT NULL,
    country         TEXT,
    created_at      BIGINT,
    updated_at      BIGINT,
    __deleted       TEXT,
    __op            TEXT,
    __source_ts_ms  BIGINT,
    __kafka_partition INT,
    __kafka_offset  BIGINT
);