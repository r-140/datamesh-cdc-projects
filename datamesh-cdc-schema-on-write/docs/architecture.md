# Architecture — Schema-on-Write (Strict)

## Overview

This project implements a **strict schema-on-write** CDC pipeline. Every event written to Kafka must conform to a registered Avro schema. The Schema Registry enforces backward compatibility, and breaking changes **pause** the pipeline.

## Data Flow

```
PostgreSQL (Domain) 
    → Debezium CDC 
    → Kafka (Avro with Schema Registry)
    → Schema Evolution Service (validates compatibility)
    → Iceberg Sink (typed tables)
    → Trino (query engine)
```

## Components

| Component | Technology | Role |
|-----------|-----------|------|
| Source DB | PostgreSQL | Domain-owned operational database |
| CDC | Debezium | Reads WAL, produces Avro events |
| Schema Registry | Confluent | Stores Avro schemas, enforces compatibility |
| Streaming | Kafka | Event bus between domains |
| Evolution Service | Python | Validates schemas, pauses on breaking changes |
| Storage | Iceberg on MinIO | Typed lakehouse tables |
| Query | Trino | SQL engine over Iceberg |

## Schema Enforcement Points

1. **Kafka Connect** — serializes with Avro, registers schema
2. **Schema Registry** — rejects incompatible schemas
3. **Evolution Service** — pauses pipeline, alerts owner
4. **Iceberg Sink** — applies DDL (ADD COLUMN, TYPE WIDENING)

## When to Use

- Streaming microservices consuming typed events
- Financial/compliance data requiring strict audit
- Data Mesh with strong contracts between domains
