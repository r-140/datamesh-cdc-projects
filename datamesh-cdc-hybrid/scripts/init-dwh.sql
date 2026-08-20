CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS governance;
CREATE TABLE IF NOT EXISTS bronze.cdc_events (
 topic TEXT NOT NULL, kafka_partition INTEGER NOT NULL, kafka_offset BIGINT NOT NULL,
 record_key TEXT, source_table TEXT NOT NULL, operation TEXT, payload JSONB NOT NULL,
 source_ts_ms BIGINT, ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY (topic, kafka_partition, kafka_offset));
CREATE TABLE IF NOT EXISTS silver.orders (
 id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, total_amount NUMERIC(12,2) NOT NULL,
 status TEXT NOT NULL, bronze_topic TEXT NOT NULL, bronze_partition INTEGER NOT NULL,
 bronze_offset BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS silver.customers (
 id BIGINT PRIMARY KEY, email TEXT NOT NULL, full_name TEXT NOT NULL, country TEXT NOT NULL,
 bronze_topic TEXT NOT NULL, bronze_partition INTEGER NOT NULL, bronze_offset BIGINT NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS governance.projection_failures (
 topic TEXT NOT NULL, kafka_partition INTEGER NOT NULL, kafka_offset BIGINT NOT NULL,
 source_table TEXT NOT NULL, payload JSONB NOT NULL, error TEXT NOT NULL,
 failed_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(topic,kafka_partition,kafka_offset));
CREATE TABLE IF NOT EXISTS governance.observed_schemas (
 source_table TEXT NOT NULL, fingerprint TEXT NOT NULL, fields JSONB NOT NULL,
 first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 event_count BIGINT NOT NULL DEFAULT 1, PRIMARY KEY(source_table,fingerprint));
