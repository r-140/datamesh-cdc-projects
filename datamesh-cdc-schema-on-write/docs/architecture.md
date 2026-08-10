# Project Architecture

## Overview

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐
│  postgres-orders│     │             │     │  postgres-dwh   │
│  (source) :5432 │────▶│  Debezium   │────▶│  (DWH) :5434    │
│                 │     │  Connector  │     │                 │
└─────────────────┘     └──────┬──────┘     └─────────────────┘
                               │
┌─────────────────┐           │           ┌─────────────────┐
│ postgres-customers│         │           │  dbt models     │
│  (source) :5433 │───────────┘           │  bronze/silver/ │
│                 │                       │  gold           │
└─────────────────┘                       └─────────────────┘
                               │
                        ┌──────┴──────┐
                        │    Kafka    │
                        │  (KRaft)    │
                        │   :9092     │
                        └──────┬──────┘
                               │
                        ┌──────┴──────┐
                        │Schema Registry│
                        │   :8081     │
                        └─────────────┘
```

## Components

### Source Layer
- **postgres-orders** (:5432) — orders domain, WAL enabled for Debezium
- **postgres-customers** (:5433) — customers domain

### Streaming Layer
- **Kafka (KRaft)** (:9092) — message broker without Zookeeper
- **Schema Registry** (:8081) — Avro schemas, BACKWARD compatibility
- **Kafka Connect** (:8083) — Debezium Source Connectors

### Warehouse Layer
- **postgres-dwh** (:5434) — target DB for dbt
  - `raw` — seed data
  - `raw_bronze` — views (bronze)
  - `raw_silver` — cleaned tables
  - `raw_gold` — aggregates

### Transformations
- **dbt** — bronze → silver → gold models
- **dbt-postgres** — adapter for Postgres DWH

### Monitoring
- **Prometheus** (:9090) — metrics
- **Grafana** (:3000) — dashboards

## Data Flow

1. Application writes to `postgres-orders` / `postgres-customers`
2. Debezium reads WAL and publishes events to Kafka (Avro)
3. Schema Registry validates schemas (BACKWARD compat)
4. Data available in DWH via seed / CDC sink (optional)
5. dbt builds bronze/silver/gold layers
