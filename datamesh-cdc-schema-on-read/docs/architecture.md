# Project Architecture

## Overview

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  postgres-orders│     │             │     │     Kafka       │     │  postgres-dwh   │
│  (source) :5432 │────▶│  Debezium   │────▶│  (Avro topics)  │────▶│  (DWH) :5434    │
│                 │     │  Connector  │     │                 │     │  raw.*_cdc      │
└─────────────────┘     └─────────────┘     └─────────────────┘     └─────────────────┘
       │                                                                         │
       │                              ┌─────────────────┐                        │
       │                              │  Python CDC     │                        │
       │                              │  Consumer       │                        │
       │                              │  (Avro → JSONB) │                        │
       │                              └─────────────────┘                        │
       │                                                                         │
       │                                                                         ▼
       │                                                              ┌─────────────────┐
       │                                                              │   dbt models    │
       │                                                              │  bronze/silver/ │
       │                                                              │  gold           │
       │                                                              └─────────────────┘
       │
┌─────────────────┐
│postgres-customers│
│  (source) :5433 │
│                 │
└─────────────────┘
```

## Components

### Source Layer

- **postgres-orders** (:5432) — orders domain, WAL enabled for Debezium
- **postgres-customers** (:5433) — customers domain

### Streaming Layer

- **Kafka (KRaft)** (:9092) — message broker without Zookeeper
- **Schema Registry** (:8081) — Avro schemas, BACKWARD compatibility
- **Kafka Connect** (:8083) — Debezium Source Connectors

### Consumer Layer

- **Python CDC Consumer** — reads Avro from Kafka, writes JSONB to DWH
  - Handles schema evolution gracefully (missing fields = NULL in JSONB)
  - Upserts on `id` with conflict resolution
  - Tracks audit metadata: `__op`, `__source_ts_ms`, `__kafka_partition`, `__kafka_offset`

### Warehouse Layer

- **postgres-dwh** (:5434) — target DB for CDC consumer and dbt

  - `raw` — JSONB CDC tables (`orders_cdc`, `customers_cdc`)
  - `bronze` — pass-through views of raw JSONB
  - `silver` — typed extraction with explicit CAST
  - `gold` — business aggregates

### Transformations

- **dbt** — bronze → silver → gold models
- **dbt-postgres** — adapter for Postgres DWH

### Monitoring

- **Prometheus** (:9090) — metrics from JMX Exporter and postgres-exporter
- **Grafana** (:3000) — dashboards with CDC, DWH and consumer monitoring

## Data Flow

1. Application writes to `postgres-orders` / `postgres-customers`
2. Debezium reads WAL and publishes events to Kafka (Avro)
3. Schema Registry validates schemas (BACKWARD compat)
4. Python CDC Consumer reads Avro messages from Kafka topics
5. Consumer writes entire payload as `JSONB` to `raw.orders_cdc` / `raw.customers_cdc`
6. dbt builds bronze/silver/gold layers on top of JSONB data

## Schema-on-Read Approach

Unlike Schema-on-Write, this pipeline uses **JSONB bronze layer**:

| Aspect | Schema-on-Write | Schema-on-Read (this project) |
|--------|----------------|-------------------------------|
| Bronze storage | Typed columns | `JSONB` payload |
| Schema change handling | Connector crash / DLQ | Pipeline continues, NULL in JSONB |
| Failure detection | Kafka Connect runtime | dbt tests (batch) |
| Data loss | Possible (DLQ) | None — all data in JSONB |
| Flexibility | Low | High |

## Key Design Decisions

1. **No JDBC Sink Connector** — Python consumer provides more control over JSONB insertion and conflict resolution
2. **JSONB bronze layer** — Schema changes never break ingestion
3. **Explicit CAST in Silver** — Schema is enforced at read time, not write time
4. **Upsert semantics** — `ON CONFLICT (id) DO UPDATE` handles CDC updates and deletes
