# Hybrid Project Architecture

## Purpose

The hybrid project separates two concerns that the pure approaches combine:

- Bronze optimizes for capture and recovery. It accepts every decodable CDC payload as JSONB.
- Silver optimizes for consumers. It accepts only records satisfying an explicit typed contract.

PostgreSQL is used as the warehouse to keep the example focused on schema evolution rather than storage-engine differences.

## Data Flow

```text
PostgreSQL sources
    -> logical WAL
    -> Debezium source connectors
    -> Kafka topics encoded as Avro
    -> Schema Registry
    -> hybrid-consumer
         -> bronze.cdc_events
         -> governance.observed_schemas
         -> silver.orders / silver.customers
         `-> governance.projection_failures
```

## Components

| Component | Responsibility |
| --- | --- |
| `postgres-orders`, `postgres-customers` | Domain-owned operational sources. |
| Debezium/Kafka Connect | Captures inserts, updates and deletes from PostgreSQL WAL. |
| Kafka | Durable transport and replay boundary. |
| Schema Registry | Stores wire-level Avro schemas used to decode Kafka records. |
| `hybrid-consumer` | Writes lossless Bronze records, audits shapes and applies Silver contracts. |
| `postgres-dwh` | Contains Bronze, Silver and governance schemas. |
| dbt | Consumes stable Silver tables for Gold models and governance metadata for schema-evolution visibility. |

## Transaction Boundary

For one Kafka message, the consumer performs the following work in one PostgreSQL transaction:

1. Insert the immutable Bronze event using topic, partition and offset as the idempotency key.
2. Record or update the observed field-set fingerprint.
3. Attempt the typed Silver projection.
4. Upsert Silver on success or insert a projection failure on contract violation.
5. Commit PostgreSQL.
6. Commit the Kafka offset synchronously.

If the process stops before step 5, PostgreSQL rolls back and Kafka redelivers the event. If it stops after step 5 but before step 6, redelivery is harmless because the Bronze primary key makes processing idempotent.

## Warehouse Schemas

### Bronze

`bronze.cdc_events` stores source payload JSONB plus Kafka coordinates, source table, operation and timestamps. It is append-only per Kafka offset.

### Silver

`silver.orders` and `silver.customers` contain explicit PostgreSQL types and primary keys. Extra source fields are not exposed automatically.

### Governance

- `governance.observed_schemas` fingerprints each distinct field set and counts its occurrences.
- `governance.projection_failures` keeps incompatible payloads and their validation errors.

## Delivery Guarantees

The demo provides at-least-once Kafka delivery with idempotent warehouse processing. It is not a distributed exactly-once transaction, but the ordering of the PostgreSQL and Kafka commits prevents acknowledged-but-unwritten events.

## Downstream dbt Boundary

dbt does not re-parse Bronze JSON into the main business models. It reads typed Silver tables, so a breaking source event cannot break Gold SQL. Separate governance models read the failure and observed-schema tables, preventing this stability from becoming silent data loss.
