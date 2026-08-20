# Hybrid CDC Consumer

## Responsibility

`src/datamesh_cdc/consumer.py` is the runtime bridge between Kafka and the warehouse. It owns:

- Avro deserialization;
- topic-to-domain routing;
- Bronze persistence;
- schema-shape fingerprinting;
- Silver projection and upsert;
- projection quarantine;
- PostgreSQL and Kafka commit ordering.
- Prometheus outcome counters and warehouse-backed governance gauges.

It does not evolve Silver DDL automatically.

## Configuration

| Variable | Default inside Compose |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |
| `DWH_DSN` | `postgresql://dwh:dwh@postgres-dwh/datamesh_dwh` |
| `METRICS_PORT` | `8000` |

## Message Outcomes

- `promoted`: Bronze and governance writes succeed and the row is written to Silver.
- `bronze_only`: Bronze succeeds but contract validation fails; a governance failure is written.
- `duplicate`: the Kafka coordinate already exists, so no second copy is created.

## Adding a Domain

To add a new `products` domain:

1. Add the Debezium connector and topic.
2. Add the topic/table mapping to `TOPICS`.
3. Add a projection contract in `hybrid_projection.py`.
4. Add `silver.products` through DWH migration SQL.
5. Add its Silver upsert strategy.
6. Add unit tests and an end-to-end scenario.

For more domains, replace the current `if table == ...` branch with a registry of projector/writer strategies. The current explicit branch is kept small for demo readability.

## Operational Notes

The consumer disables Kafka auto-commit. PostgreSQL commits before the synchronous offset commit. Unexpected database or deserialization errors stop successful offset advancement and therefore allow retry.

Projection errors are different: they are expected data-contract outcomes, so the consumer records them, commits Bronze, and advances the Kafka offset.
