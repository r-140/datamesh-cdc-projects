# Dead Letter Queue (DLQ) — Production Error Handling

## Problem

Without DLQ, a single bad message (schema mismatch, deserialization error, null in NOT NULL column) crashes
the JDBC Sink connector. The connector enters a crash-loop:

```
Kafka message: {id: 10, customer_id: 1, status: "completed"}  ← missing total_amount!
JDBC Sink: INSERT INTO raw.orders_cdc (...) VALUES (...)  ← fails
Connector: FAILED → RESTART → FAILED → RESTART (infinite loop)
```

Result: **no new data reaches the DWH**, and the connector status flaps.

## Solution: DLQ (Dead Letter Queue)

Kafka Connect supports quarantining bad messages instead of crashing:

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dlq-orders-jdbc-sink",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true"
}
```

> **Note:** `errors.deadletterqueue.topic.replication.factor` must match your Kafka cluster. Use `1` for single-broker Docker setups, `3` for production clusters.

### Behavior with DLQ

```
Kafka message: {id: 10, customer_id: 1, status: "completed"}  ← missing total_amount
JDBC Sink: tries INSERT → fails with PSQLException
DLQ: message sent to dlq-orders-jdbc-sink with error headers (__connect.errors.*)
Connector: stays RUNNING, continues with next message
DWH: no new data (alert fires after 2 min stall)
```

## Why Restart Doesn't Help

| Without DLQ | With DLQ |
|-------------|----------|
| Connector restarts, re-reads bad message, crashes again | Bad message skipped, sent to DLQ |
| Infinite crash loop | Pipeline continues |
| DWH gets nothing | DWH gets good messages, bad ones quarantined |
| Manual intervention: delete topic / skip offset | Automatic: ops fixes schema, replays DLQ later |

## Inspecting DLQ (Reliable Methods)

### Check DLQ offset (most reliable)

```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell   --bootstrap-server localhost:29092   --topic dlq-orders-jdbc-sink

# Output: dlq-orders-jdbc-sink:0:1  ← 1 message in DLQ
```

### Read DLQ with headers (no consumer group cache issues)

```bash
# ❌ DON'T use --from-beginning alone — consumer group may skip messages
docker exec kafka kafka-console-consumer   --bootstrap-server localhost:29092   --topic dlq-orders-jdbc-sink   --partition 0   --offset 0   --max-messages 1   --property print.headers=true
```

> **Why `--partition 0 --offset 0`?** `--from-beginning` creates a random consumer group that caches offsets. If the group already exists, it may skip messages. Explicit partition/offset reads from the beginning every time.

### Example DLQ message with error headers

```
__connect.errors.topic:orders-server.public.orders,
__connect.errors.partition:0,
__connect.errors.offset:9,
__connect.errors.connector.name:orders-jdbc-sink,
__connect.errors.exception.class.name:java.sql.SQLException,
__connect.errors.exception.message:Exception chain:
  java.sql.BatchUpdateException: ... ERROR: null value in column "total_amount" ...
```

## Production Recovery Playbook

### 1. Alert fires: `CDC_SinkStall`

Check connector status:
```bash
curl -s http://localhost:8083/connectors/orders-jdbc-sink/status | python3 -m json.tool
```

### 2. Inspect DLQ depth and contents

```bash
# Check DLQ depth (offset)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell   --bootstrap-server localhost:29092   --topic dlq-orders-jdbc-sink

# Read DLQ with error details
docker exec kafka kafka-console-consumer   --bootstrap-server localhost:29092   --topic dlq-orders-jdbc-sink   --partition 0 --offset 0   --max-messages 1   --property print.headers=true
```

### 3. Fix the root cause

**Option A: Restore the column (if dropped by mistake)**
```sql
ALTER TABLE orders ADD COLUMN total_amount DECIMAL(12,2);
```

**Option B: Update downstream schema (if change was intentional)**
```bash
# Evolve Avro schema in Schema Registry
curl -X POST http://localhost:8081/subjects/orders-server.public.orders-value/versions   -H "Content-Type: application/vnd.schemaregistry.v1+json"   -d '{"schema": "..."}'
```

**Option C: Replay DLQ after fix**
```bash
# Consume DLQ, fix data, produce back to source topic
# (requires custom script or Kafka Streams)
```

### 4. Verify recovery

```bash
# Generate test data
python scripts/data_generator.py --mode batch --count 5

# Verify DWH receives new rows
python scripts/data_generator.py --mode verify
```

## DLQ Configuration Reference

### Sink Connector Config (Production-Ready)

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "tasks.max": "1",
  "topics": "orders-server.public.orders",
  "connection.url": "jdbc:postgresql://postgres-dwh:5432/datamesh_dwh",
  "connection.user": "dwh",
  "connection.password": "dwh",

  "auto.create": "false",
  "auto.evolve": "false",
  "insert.mode": "upsert",
  "pk.mode": "record_key",
  "pk.fields": "id",

  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dlq-orders-jdbc-sink",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.retry.delay.max.ms": "60000",
  "errors.retry.timeout": "300000"
}
```

### Important Notes

- **DLQ topic must exist** before connector starts (create explicitly with `kafka-topics --create`)
- **DLQ is per connector**, not per pipeline
- **Headers contain error details**: `__connect.errors.exception.class`, `__connect.errors.exception.message`, `__connect.errors.exception.stacktrace`
- **DLQ messages are not auto-replayed** — you need a separate consumer or manual process
- **DLQ can fill up** — monitor its size and set retention (`retention.ms=604800000` for 7 days)

## Common DLQ Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `column "total_amount" is of type numeric but expression is of type character varying` | `decimal.handling.mode=string` in Debezium source | Remove `decimal.handling.mode` (default `precise`) |
| `column "created_at" is of type timestamp but expression is of type bigint` | `time.precision.mode=adaptive` (default) sends microseconds as INT64 | Set `time.precision.mode=connect` OR use `BIGINT` in DWH |
| `Table is missing fields ([country]) and auto-evolution is disabled` | DWH table missing a column present in source | Add column to `init-dwh.sql`, recreate table |
| `Unable to replicate the partition 3 time(s)` | DLQ RF=3 but only 1 broker | `errors.deadletterqueue.topic.replication.factor=1` |
| `null value in column violates not-null constraint` | Bad message without required field | Expected behavior — DLQ catches it, DWH stays clean |

## Monitoring DLQ

### Prometheus Alert

```yaml
- alert: DLQ_Messages_Available
  expr: kafka_log_log_size{topic=~"dlq-.*"} > 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "DLQ has messages — pipeline encountered errors"

- alert: CDC_SinkStall
  expr: |
    (
      rate(kafka_consumer_records_consumed_rate{topic=~".*cdc.*"}[5m]) == 0
      and
      kafka_consumer_records_consumed_rate{topic=~".*cdc.*"}[5m] > 0
    )
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "CDC Sink stalled — check DLQ for bad messages"
```

### Grafana Panel

Query to show DLQ depth over time:
```promql
kafka_log_log_size{topic=~"dlq-.*"}
```

## Approaches to Breaking Changes in Schema-on-Write

| Approach | When to Use | How | Trade-off |
|----------|-------------|-----|-----------|
| **1. Dead Letter Queue (DLQ)** | Always. Baseline protection. | `errors.tolerance=all` + `errors.deadletterqueue.topic.name=dlq-orders`. Bad messages go to DLQ, connector continues. | Data in DLQ, not DWH. Needs DLQ monitoring. |
| **2. Schema Registry + Compatibility** | Schema-on-Write with quality gates | `BACKWARD` compatibility. On `DROP COLUMN`, Schema Registry rejects new schema → producer fails **before** writing to Kafka. | Message never reaches Kafka. Need producer fallback. |
| **3. Replay / Skip offset** | Message already in Kafka | Manually shift consumer offset past bad message: `kafka-consumer-groups --reset-offsets --to-offset N`. Or delete via `kafka-delete-records`. | **Data loss** (skip) or manual work (replay). |
| **4. Fix & Reprocess** | Need to preserve data | Fix schema (add column back), restart connector, process DLQ separately. | Longest path, but preserves all data. |
