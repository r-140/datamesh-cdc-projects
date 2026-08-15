# CDC Consumer (Python)

## Overview

The `scripts/cdc_consumer.py` service is a Python application that reads CDC events from Kafka (Avro) and writes them as JSONB to the PostgreSQL DWH.

## Architecture

```
Kafka Topic (Avro)
    │
    ▼
┌─────────────────┐
│  Avro           │
│  Deserializer   │ ← Schema Registry
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Python CDC     │
│  Consumer       │
│                 │
│  • Parse payload│
│  • Add metadata │
│  • Upsert JSONB │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  postgres-dwh   │
│  raw.orders_cdc │
│  (JSONB)        │
└─────────────────┘
```

## Consumer Behavior

### Message Processing

1. **Deserialize Avro** — Uses Schema Registry to decode Avro messages
2. **Extract payload** — Gets the `after` field from Debezium envelope
3. **Add metadata** — Appends audit fields:
   - `__op` — Operation type (c=create, u=update, d=delete, r=read)
   - `__source_ts_ms` — Source timestamp
   - `__kafka_partition` — Kafka partition number
   - `__kafka_offset` — Kafka offset
4. **Upsert JSONB** — Inserts or updates the DWH table:
   ```sql
   INSERT INTO raw.orders_cdc (id, payload, __op, __source_ts_ms, __kafka_partition, __kafka_offset, ingested_at)
   VALUES (%s, %s, %s, %s, %s, %s, NOW())
   ON CONFLICT (id) DO UPDATE SET
       payload = EXCLUDED.payload,
       __op = EXCLUDED.__op,
       __source_ts_ms = EXCLUDED.__source_ts_ms,
       __kafka_partition = EXCLUDED.__kafka_partition,
       __kafka_offset = EXCLUDED.__kafka_offset,
       ingested_at = NOW()
   ```

### Graceful Shutdown

- Handles `SIGINT` and `SIGTERM`
- Commits current offsets before exiting
- Logs shutdown event

### Error Handling

| Error | Behavior |
|-------|----------|
| Deserialization error | Log and skip message |
| DWH connection lost | Retry with exponential backoff |
| DWH write error | Log error, do not commit offset (will retry) |
| Schema Registry unavailable | Retry with exponential backoff |

## Configuration

Environment variables (set in `docker-compose.yml` or `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL |
| `DWH_HOST` | `localhost` | DWH PostgreSQL host |
| `DWH_PORT` | `5434` | DWH PostgreSQL port |
| `DWH_DB` | `datamesh_dwh` | DWH database name |
| `DWH_USER` | `dwh` | DWH username |
| `DWH_PASSWORD` | `dwh` | DWH password |
| `CONSUMER_GROUP_ID` | `cdc-consumer-group` | Kafka consumer group |
| `TOPICS` | `orders-server.public.orders,customers-server.public.customers` | Comma-separated topics |

## Running the Consumer

### Via Makefile (recommended)

```bash
# Start consumer in foreground
make consumer

# View logs
tail -f logs/consumer.log

# Start as part of full stack
make up
```

### Manual

```bash
# Install dependencies
pip install -e ".[dev]"

# Run consumer
python scripts/cdc_consumer.py
```

### Background Service

```bash
# Start in background
nohup python scripts/cdc_consumer.py > logs/consumer.log 2>&1 &

# Stop
pkill -f cdc_consumer.py
```

## Monitoring Consumer Health

### Logs

```bash
# Real-time logs
tail -f logs/consumer.log

# Search for errors
grep ERROR logs/consumer.log

# Check last activity
tail -n 20 logs/consumer.log
```

### Metrics (if prometheus-client enabled)

```bash
# Consumer metrics
curl http://localhost:8000/metrics
```

Key metrics:
- `cdc_consumer_messages_total` — Total messages processed
- `cdc_consumer_messages_per_second` — Processing rate
- `cdc_consumer_lag` — Consumer lag per partition
- `cdc_consumer_errors_total` — Total errors
- `cdc_consumer_dwh_upserts_total` — Total DWH upserts

## Consumer Lag

Lag is calculated as:
```
lag = kafka_high_watermark - last_committed_offset
```

High lag indicates:
- Consumer is slow (check CPU/memory)
- DWH writes are bottlenecking (check PostgreSQL performance)
- Network issues between consumer and Kafka/DWH

## Schema Evolution Handling

The consumer is **schema-agnostic** — it writes whatever payload it receives as JSONB:

| Scenario | Consumer Behavior |
|----------|-------------------|
| New field added | JSONB contains new field ✓ |
| Field removed | JSONB lacks the field (NULL on read) ✓ |
| Field renamed | JSONB contains new field name, old name absent ✓ |
| Type changed | Stored as-is in JSONB, CAST handles it in Silver ✓ |

This is the key advantage of Schema-on-Read: the consumer never breaks.

## Testing

```bash
# Run consumer tests
pytest tests/test_consumer.py -v

# Test with live data
python scripts/data_generator.py --mode batch --count 10
# Check DWH
psql -h localhost -p 5434 -U dwh -d datamesh_dwh -c "SELECT COUNT(*) FROM raw.orders_cdc;"
```
