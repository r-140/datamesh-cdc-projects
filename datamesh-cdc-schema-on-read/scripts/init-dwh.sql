CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders_cdc (
    id              BIGINT PRIMARY KEY,
    payload         JSONB NOT NULL,
    __op            TEXT,
    __source_ts_ms  BIGINT,
    __kafka_partition INT,
    __kafka_offset  BIGINT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.customers_cdc (
    id              BIGINT PRIMARY KEY,
    payload         JSONB NOT NULL,
    __op            TEXT,
    __source_ts_ms  BIGINT,
    __kafka_partition INT,
    __kafka_offset  BIGINT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);
