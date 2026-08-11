# Breaking Changes in Schema-on-Write CDC Pipeline

> Practical guide based on debugging a Debezium → Kafka → JDBC Sink pipeline with DLQ protection.

---

## 1. What is a "Breaking Change" in CDC

In a **schema-on-write** pipeline, data is replicated from a source DB (PostgreSQL) to a DWH via Kafka Connect. If the source schema changes incompatibly (e.g., `DROP COLUMN`), the JDBC Sink cannot insert the message into the DWH — and without protection, the **connector enters a restart loop**.

**Classic scenario:**

```sql
-- Source DB
ALTER TABLE orders DROP COLUMN total_amount;
INSERT INTO orders (customer_id, status) VALUES (1, 'completed');
```

```text
Kafka message: {id: 10, customer_id: 1, status: "completed"}
                                    ↑
                                    └── total_amount is missing

JDBC Sink → INSERT INTO raw.orders_cdc (...) VALUES (...)
         → ERROR: null value in column "total_amount" violates not-null constraint
         → Connector FAILED → restart → FAILED → restart ...
```

---

## 2. Comparison of Approaches

| Approach | When to Use | How | Trade-off |
|----------|-------------|-----|-----------|
| **1. Dead Letter Queue (DLQ)** | **Always. Baseline protection.** | `errors.tolerance=all` + `errors.deadletterqueue.topic.name=dlq-orders`. Bad messages go to the DLQ topic, the connector continues processing the rest. | Data is in the DLQ, not the DWH. DLQ offset monitoring is required. |
| **2. Schema Registry + Compatibility** | Schema-on-Write with quality gates | `BACKWARD` compatibility. On `DROP COLUMN`, Schema Registry rejects the new schema → producer fails **before** writing to Kafka. | The message never reaches Kafka. Producer retry / fallback is needed. |
| **3. Replay / Skip offset** | Message is already in Kafka, need a quick fix | Manually shift consumer offset past the bad message: `kafka-consumer-groups --reset-offsets --to-offset N`. Or delete via `kafka-delete-records`. | **Data loss** (skip) or manual work (replay). |
| **4. Fix & Reprocess** | Need to preserve data from the bad message | Fix the schema (add the column back), restart the connector, process the DLQ separately. | Longest path, but preserves all data. |

---

## 3. Approach 1: DLQ (Dead Letter Queue)

### 3.1 JDBC Sink Configuration

```json
{
  "name": "orders-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
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
    "errors.log.include.messages": "true"
  }
}
```

> ⚠️ **Important:** `errors.deadletterqueue.topic.replication.factor=1` is for Docker with 1 broker. In production with 3 brokers, use `3`.

### 3.2 Create the DLQ Topic in Advance

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:29092 \
  --create --if-not-exists \
  --topic dlq-orders-jdbc-sink \
  --partitions 1 \
  --replication-factor 1
```

### 3.3 Reading the DLQ (Reliable Method)

```bash
# Check offset (most reliable)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:29092 \
  --topic dlq-orders-jdbc-sink

# Read with error headers (no consumer group cache issues)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic dlq-orders-jdbc-sink \
  --partition 0 --offset 0 \
  --max-messages 1 \
  --property print.headers=true
```

> **Why `--partition 0 --offset 0` instead of `--from-beginning`?** `--from-beginning` creates a random consumer group that caches offsets. If the group already exists, it may skip messages. Explicit partition/offset reads from the beginning every time.

### 3.4 Production Behavior

| Without DLQ | With DLQ |
|-------------|----------|
| Connector crashes, restarts, crashes again | Connector `RUNNING`, skips the bad message |
| DWH receives **nothing** | DWH receives **good messages** |
| Manual `make reset` required | Ops fixes the schema, DLQ can be replayed later |
| Data is lost | Data is in the DLQ, **not lost** |

---

## 4. Approach 2: Schema Registry + Compatibility

### 4.1 How It Works

Schema Registry stores Avro schema versions. With `BACKWARD` compatibility, a new schema must be readable by consumers using the old schema.

```
Producer → Schema Registry: "My new schema without total_amount"
Schema Registry → "No, this is not BACKWARD compatible" → 409 Conflict
Producer → fails with SchemaViolationException
```

The message **never reaches Kafka**.

### 4.2 Configuration

```json
{
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

### 4.3 Trade-off

- ✅ Bad message never reaches Kafka
- ❌ The source DB write **already happened** (WAL commit), but Kafka has nothing → **DWH data loss**
- ❌ Producer fallback logic is required

---

## 5. Approach 3: Replay / Skip Offset

### 5.1 Skip (Data Loss)

```bash
# Shift offset past the bad message
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group connect-orders-jdbc-sink \
  --topic orders-server.public.orders \
  --reset-offsets \
  --to-offset 42 \
  --execute
```

### 5.2 Replay (Preserve Data)

```bash
# 1. Read the bad message from DLQ
# 2. Fix the data manually
# 3. Write the corrected message back to the source topic
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders-server.public.orders

# Insert corrected JSON/Avro
```

### 5.3 When to Use

- **Skip:** Emergency to unblock the pipeline. Accept the loss of 1 message.
- **Replay:** If the data is critical and can be recovered from the DLQ.

---

## 6. Approach 4: Fix & Reprocess

### 6.1 Recovery Flow

```
1. Alert: CDC_SinkStall firing (DWH has not grown for 2+ minutes)
   ↓
2. Ops checks DLQ: kafka-console-consumer --topic dlq-orders-jdbc-sink
   ↓
3. Sees error: "null value in column total_amount"
   ↓
4. Fixes source schema: ALTER TABLE orders ADD COLUMN total_amount DECIMAL(12,2);
   ↓
5. New good messages start flowing into the DWH
   ↓
6. (Optional) Processes DLQ manually or via a replay job
```

### 6.2 Example from Our Project

```sql
-- Step 4: Breaking change
ALTER TABLE orders DROP COLUMN total_amount;
INSERT INTO orders (customer_id, status) VALUES (1, 'completed');

-- DWH: 9 rows (did not grow)
-- DLQ: 1 message (quarantined)

-- Step 9: Recovery
ALTER TABLE orders ADD COLUMN total_amount DECIMAL(12,2);
INSERT INTO orders (customer_id, total_amount, status) VALUES (1, 999.99, 'completed');

-- DWH: 10 rows (recovered)
```

---

## 7. Production Checklist

### 7.1 Mandatory

- [ ] `errors.tolerance=all` on all sink connectors
- [ ] `errors.deadletterqueue.topic.name` + `errors.deadletterqueue.topic.replication.factor`
- [ ] DLQ topics are created in advance (do not rely on auto-create)
- [ ] DLQ offset monitoring (alert if > 0)
- [ ] `auto.create=false`, `auto.evolve=false` in JDBC Sink (explicit schema management)

### 7.2 Recommended

- [ ] `errors.deadletterqueue.context.headers.enable=true` — headers with stack trace
- [ ] `errors.log.enable=true` — error logs in connector stdout
- [ ] Grafana dashboard: DLQ offset, DWH row count, connector status
- [ ] Alert `CDC_SinkStall`: DWH count has not grown for 2+ minutes
- [ ] Runbook: "How to read the DLQ and recover the pipeline"

### 7.3 Do Not Do

- [ ] Do not use `auto.create=true` + `auto.evolve=true` in production — the connector will create tables with unpredictable types
- [ ] Do not leave `decimal.handling.mode=string` if the JDBC Sink writes to Postgres `NUMERIC`
- [ ] Do not ignore the DLQ — a growing offset means data is being lost for the DWH

---

## 8. Typical Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `column "total_amount" is of type numeric but expression is of type character varying` | `decimal.handling.mode=string` in Debezium source | Remove `decimal.handling.mode` (default is `precise`) |
| `column "created_at" is of type timestamp but expression is of type bigint` | `time.precision.mode=adaptive` (default) sends microseconds as INT64 | Set `time.precision.mode=connect` OR use `BIGINT` in the DWH |
| `Table is missing fields ([country]) and auto-evolution is disabled` | DWH table is missing a column that exists in the source | Add the column to `init-dwh.sql` |
| `Unable to replicate the partition 3 time(s)` | DLQ RF=3, but only 1 broker | `errors.deadletterqueue.topic.replication.factor=1` |
| `null value in column violates not-null constraint` | Bad message without a required field | Expected behavior — DLQ catches it, DWH stays clean |

---

## 9. Final Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────────┐
│  Source DB      │────▶│   Debezium   │────▶│   Kafka Topic    │
│  (PostgreSQL)   │ WAL │   Source     │     │ orders-server... │
└─────────────────┘     └──────────────┘     └────────┬─────────┘
                                                      │
                              ┌───────────────────────┘
                              ▼
                    ┌──────────────────┐
                    │  JDBC Sink       │
                    │  orders-jdbc-sink│
                    └────────┬─────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌─────────────────┐           ┌──────────────────┐
    │  DWH            │           │  DLQ             │
    │  raw.orders_cdc │           │  dlq-orders-...  │
    └─────────────────┘           └──────────────────┘
```

**Golden rule:** The DLQ is not a "trash can" — it is a **quarantine**. Every message in the DLQ is an incident that must be investigated.

---

*Generated based on practical debugging of a CDC pipeline with Debezium 2.5, Kafka Connect 7.5.3, PostgreSQL 15.*
