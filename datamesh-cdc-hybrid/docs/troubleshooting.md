# Troubleshooting

## Quick Diagnostics

```bash
docker compose ps
curl -fsS http://localhost:8083/connectors
curl -fsS http://localhost:8083/connectors/orders-cdc-connector/status
curl -fsS http://localhost:8081/subjects
docker compose logs --tail=200 kafka-connect hybrid-consumer
```

## `make demo` Cannot Connect

Run `make up`, then wait for PostgreSQL, Kafka Connect and the hybrid consumer to become healthy/running. Confirm ports 5432 and 5434 are reachable from the host.

## Bronze Is Empty

Check connector registration and task status. Then insert a source row and inspect Kafka Connect logs. If topics or connector offsets are stale after configuration changes, use `make reset` to recreate the demo environment.

## Bronze Has Data but Silver Does Not

First check whether this is expected governance behavior:

```sql
SELECT payload, error, failed_at
FROM governance.projection_failures
ORDER BY failed_at DESC;
```

If no failure exists, inspect `hybrid-consumer` logs for database or deserialization errors.

## Repeated Projection Failures

Fixing the contract without fixing the payload does not make an invalid historical event valid. Choose one explicit recovery policy:

- correct the source and emit a new event;
- repair and replay the Bronze payload;
- introduce a documented default;
- make the field optional if business semantics permit it.

## Duplicate Kafka Delivery

Duplicate delivery is expected after some failures. `bronze.cdc_events` uses `(topic, kafka_partition, kafka_offset)` as its primary key, so redelivery should produce the `duplicate` outcome without duplicating warehouse data.

## Demo Stopped After Dropping `total_amount`

The demo attempts restoration automatically. Verify it with:

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema='public' AND table_name='orders';
```

If needed:

```sql
ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount DECIMAL(12,2);
```

## Reset Everything

```bash
make reset
make up
```

This removes all project Docker volumes and data.
