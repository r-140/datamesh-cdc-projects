# Dead Letter Queue (DLQ) — Production Error Handling

## Problem

Without DLQ, a single bad message (schema mismatch, deserialization error) crashes
the JDBC Sink connector. The connector enters a crash-loop:

```
Kafka message: {id: 1, customer_id: 5, status: "completed"}  ← missing total_amount!
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
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true"
}
```

### Behavior with DLQ

```
Kafka message: {id: 1, customer_id: 5, status: "completed"}  ← missing total_amount
JDBC Sink: tries INSERT → fails
DLQ: message sent to dlq-orders-jdbc-sink with error headers
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

## Production Recovery Playbook

### 1. Alert fires: `CDC_SinkStall`

Check connector status:
```bash
curl http://localhost:8083/connectors/orders-jdbc-sink/status
```

### 2. Inspect DLQ

```bash
# Read DLQ messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic dlq-orders-jdbc-sink \
  --from-beginning \
  --max-messages 5

# Check DLQ depth (lag)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 \
  --topic dlq-orders-jdbc-sink --time -1
```

### 3. Fix the root cause

**Option A: Restore the column (if dropped by mistake)**
```sql
ALTER TABLE orders ADD COLUMN total_amount DECIMAL(12,2);
```

**Option B: Update downstream schema (if change was intentional)**
```bash
# Evolve Avro schema in Schema Registry
curl -X POST http://localhost:8081/subjects/orders-server.public.orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}'
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

### Sink Connector Config

```json
{
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dlq-orders-jdbc-sink",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.retry.delay.max.ms": "60000",
  "errors.retry.timeout": "300000"
}
```

### Important Notes

- **DLQ topic must exist** before connector starts (or Kafka `auto.create.topics.enable=true`)
- **DLQ is per connector**, not per pipeline
- **Headers contain error details**: `__connect.errors.exception.class`, `__connect.errors.exception.message`
- **DLQ messages are not auto-replayed** — you need a separate consumer or manual process
- **DLQ can fill up** — monitor its size and set retention

## Monitoring DLQ

### Prometheus Alert

```yaml
- alert: DLQ_Messages_Available
  expr: kafka_consumer_records_consumed_rate{topic=~"dlq-.*"} > 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "DLQ has messages — pipeline encountered errors"
```

### Grafana Panel

Query to show DLQ depth over time:
```promql
kafka_log_log_size{topic=~"dlq-.*"}
```
