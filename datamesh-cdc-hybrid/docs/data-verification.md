# Data Verification Queries

## Connections

```bash
psql postgresql://postgres:postgres@localhost:5432/orders_db
psql postgresql://postgres:postgres@localhost:5433/customers_db
psql postgresql://dwh:dwh@localhost:5434/datamesh_dwh
```

## Bronze

```sql
SELECT source_table, count(*)
FROM bronze.cdc_events
GROUP BY source_table;

SELECT topic, kafka_partition, kafka_offset, operation, payload, ingested_at
FROM bronze.cdc_events
ORDER BY ingested_at DESC
LIMIT 20;
```

## Silver

```sql
SELECT * FROM silver.orders ORDER BY updated_at DESC LIMIT 20;
SELECT * FROM silver.customers ORDER BY updated_at DESC LIMIT 20;
```

## Schema Evolution Audit

```sql
SELECT source_table, fields, event_count, first_seen_at, last_seen_at
FROM governance.observed_schemas
ORDER BY last_seen_at DESC;
```

## Projection Failures

```sql
SELECT source_table, payload, error, failed_at
FROM governance.projection_failures
ORDER BY failed_at DESC;
```

## Bronze Versus Silver Coverage

```sql
WITH latest_orders AS (
    SELECT DISTINCT ON ((payload->>'id')::bigint)
           (payload->>'id')::bigint AS id,
           payload,
           ingested_at
    FROM bronze.cdc_events
    WHERE source_table = 'orders'
    ORDER BY (payload->>'id')::bigint, kafka_offset DESC
)
SELECT b.id,
       b.payload,
       s.id IS NOT NULL AS promoted_to_silver
FROM latest_orders b
LEFT JOIN silver.orders s USING (id)
ORDER BY b.id;
```

## Kafka Lineage

Every Silver row contains `bronze_topic`, `bronze_partition` and `bronze_offset`. Use them to retrieve the exact source event:

```sql
SELECT s.id, s.status, b.payload, b.ingested_at
FROM silver.orders s
JOIN bronze.cdc_events b
  ON b.topic = s.bronze_topic
 AND b.kafka_partition = s.bronze_partition
 AND b.kafka_offset = s.bronze_offset;
```
