# CDC Data Mesh — Step-by-Step Demo Guide

End-to-end walkthrough: PostgreSQL → Debezium → Kafka → Schema Registry.

---

## Prerequisites

```bash
# Infrastructure must be running
make up

# Connectors must be registered
./scripts/setup-connectors.sh

# Verify all services are healthy
docker ps
```

---

## Step 1: Insert Test Data into PostgreSQL

### Orders
```bash
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "INSERT INTO orders (customer_id, total_amount, status) VALUES (1, 150.00, 'confirmed');"
```

### Customers
```bash
docker exec postgres-customers psql -U postgres -d customers_db \
  -c "INSERT INTO customers (email, segment) VALUES ('alice@example.com', 'premium');"
```

---

## Step 2: Verify Topics Exist in Kafka

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
```

Expected output:
```
connect_configs
connect_offsets
connect_statuses
customers-server.public.customers
orders-server.public.orders
```

---

## Step 3: Read the Avro Message from Kafka

### Orders
```bash
docker exec kafka kafka-avro-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-server.public.orders \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081 \
  --max-messages 1
```

Expected output:
```json
{
  "before": null,
  "after": {
    "id": 1,
    "customer_id": 1,
    "total_amount": "150.00",
    "status": "confirmed",
    "created_at": 1690000000000000,
    "updated_at": 1690000000000000,
    "__deleted": null
  },
  "source": {
    "version": "2.5.0.Final",
    "connector": "postgresql",
    "name": "orders-server",
    "ts_ms": 1690000000000,
    "db": "orders_db",
    "schema": "public",
    "table": "orders"
  },
  "op": "c",
  "ts_ms": 1690000000000
}
```

> **Note:** `total_amount` is a `string` because Debezium connector config uses `decimal.handling.mode: string` to avoid precision loss.

### Customers
```bash
docker exec kafka kafka-avro-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic customers-server.public.customers \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081 \
  --max-messages 1
```

---

## Step 4: Inspect the Schema in Schema Registry

```bash
curl -s http://localhost:8081/subjects/orders-server.public.orders-value/versions/latest | python -m json.tool
```

Expected (simplified):
```json
{
  "subject": "orders-server.public.orders-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"Value\",\"namespace\":\"orders-server.public.orders\",...}"
}
```

Check all subjects:
```bash
curl -s http://localhost:8081/subjects | python -m json.tool
```

---

## Step 5: Add a New Column (Compatible Change)

```bash
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "ALTER TABLE orders ADD COLUMN promo_code VARCHAR(50) DEFAULT NULL;"
```

---

## Step 6: Insert a Record with the New Column

```bash
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "INSERT INTO orders (customer_id, total_amount, status, promo_code) VALUES (2, 99.99, 'pending', 'SUMMER2024');"
```

---

## Step 7: Read the New Message — New Field Appears

```bash
docker exec kafka kafka-avro-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-server.public.orders \
  --property schema.registry.url=http://schema-registry:8081 \
  --max-messages 1
```

Expected output:
```json
{
  "before": null,
  "after": {
    "id": 2,
    "customer_id": 2,
    "total_amount": "99.99",
    "status": "pending",
    "promo_code": "SUMMER2024",
    "created_at": 1690000001000000,
    "updated_at": 1690000001000000,
    "__deleted": null
  },
  "source": { ... },
  "op": "c",
  "ts_ms": 1690000001000
}
```

---

## Step 8: Verify Schema Registry Created Version 2

```bash
# List all versions
curl -s http://localhost:8081/subjects/orders-server.public.orders-value/versions | python -m json.tool
# [1, 2]

# Inspect version 2
curl -s http://localhost:8081/subjects/orders-server.public.orders-value/versions/2 | python -m json.tool
```

In schema v2 you will see `promo_code` field with `["null", "string"]` and `default: null`. Debezium automatically registered a **backward-compatible** schema.

---

## Step 9: Attempt a Breaking Change — Drop a Column

```bash
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "ALTER TABLE orders DROP COLUMN total_amount;"
```

This is **breaking** for BACKWARD compatibility because old consumers expect `total_amount` to exist.

---

## Step 10: Try to Insert a New Record

```bash
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "INSERT INTO orders (customer_id, status, promo_code) VALUES (3, 'shipped', 'WINTER2024');"
```

---

## Step 11: Check Kafka Connect Logs — See the Failure

```bash
docker logs kafka-connect --tail 30
```

You will see something like:
```
Schema being registered is incompatible with an earlier schema ...
ERROR ... Invalid schema ...
```

The connector enters a retry loop and eventually fails because Schema Registry returns **409 Conflict**.

---

## Step 12: Check Connector Status

```bash
curl -s http://localhost:8083/connectors/orders-cdc-connector/status | python -m json.tool
```

Expected:
```json
{
  "name": "orders-cdc-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "FAILED",
      "trace": "... SchemaRegistryException ... 409 ..."
    }
  ],
  "type": "source"
}
```

The task is `FAILED` because the new schema (without `total_amount`) is incompatible with the existing schema v2 under BACKWARD policy.

---

## Step 13: Fix the Breaking Change

### Option A — Restore the column (recommended for strict environments)

```bash
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "ALTER TABLE orders ADD COLUMN total_amount NUMERIC(10,2) DEFAULT 0;"
```

Then restart the connector task:
```bash
curl -X POST http://localhost:8083/connectors/orders-cdc-connector/tasks/0/restart
```

Verify:
```bash
curl -s http://localhost:8083/connectors/orders-cdc-connector/status | python -m json.tool
```

The task should return to `RUNNING`.

### Option B — Temporarily disable strict compatibility (NOT for production)

```bash
curl -X PUT http://localhost:8081/config/orders-server.public.orders-value \
  -H "Content-Type: application/json" \
  --data '{"compatibility": "NONE"}'
```

Then restart the connector:
```bash
curl -X POST http://localhost:8083/connectors/orders-cdc-connector/restart
```

> ⚠️ This allows any schema change and bypasses consumer protection.

### Option C — Start fresh with a new topic prefix

Delete and recreate the connector with a different `topic.prefix` (loses history but cleanest).

---

## Step 14: Verify the Fix

Insert another record after restoring the column:
```bash
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "INSERT INTO orders (customer_id, total_amount, status, promo_code) VALUES (4, 49.99, 'delivered', 'FALL2024');"
```

Read from Kafka:
```bash
docker exec kafka kafka-avro-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-server.public.orders \
  --property schema.registry.url=http://schema-registry:8081 \
  --max-messages 1
```

Check connector status one more time:
```bash
curl -s http://localhost:8083/connectors/orders-cdc-connector/status | python -m json.tool
```

---

## Quick Reference: Useful Commands

| Command | Purpose |
|---------|---------|
| `docker ps` | Check container health |
| `docker logs kafka-connect --tail 20` | View connector errors |
| `curl http://localhost:8083/connectors` | List connectors |
| `curl http://localhost:8081/subjects` | List schemas |
| `curl http://localhost:8081/subjects/orders-server.public.orders-value/versions` | List schema versions |
| `docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list` | List Kafka topics |
| `docker exec kafka kafka-avro-console-consumer ...` | Read Avro messages |

---

## Summary

| Action | Schema Registry | Connector | Result |
|--------|----------------|-----------|--------|
| `INSERT` | Registers v1 | RUNNING | ✅ Message in Kafka |
| `ADD COLUMN` (nullable) | Registers v2 | RUNNING | ✅ Compatible evolution |
| `DROP COLUMN` | Rejects (409) | FAILED | ⏸️ Breaking change blocked |
| Restore column + restart | — | RUNNING | ✅ Pipeline resumed |
