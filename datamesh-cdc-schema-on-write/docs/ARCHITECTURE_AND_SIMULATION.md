# CDC Data Mesh — Architecture, Simulation & Deep Dive

> Complete guide to the Schema-on-Write (Strict) CDC pipeline: Debezium → Kafka → Schema Registry → Schema Evolution Service.

---

## Table of Contents

1. [End-to-End Architecture](#1-end-to-end-architecture)
2. [Simulation Stages Step-by-Step](#2-simulation-stages-step-by-step)
3. [How Debezium Works (Deep Dive)](#3-how-debezium-works-deep-dive)
4. [How Kafka Connect Works](#4-how-kafka-connect-works)
5. [How Schema Registry Works](#5-how-schema-registry-works)
6. [Where to Watch Schema Evolution](#6-where-to-watch-schema-evolution)
7. [Advanced Simulation Scenarios](#7-advanced-simulation-scenarios)
8. [Useful Commands](#8-useful-commands)

---

## 1. End-to-End Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐
│  PostgreSQL     │────▶│  Debezium   │────▶│     Kafka       │
│  (customers,    │ CDC │  (Kafka     │     │  (KRaft mode)   │
│   orders)       │     │   Connect)  │     │                 │
└─────────────────┘     └─────────────┘     └────────┬────────┘
                                                     │
                              ┌──────────────────────┘
                              ▼
                     ┌─────────────────┐
                     │ Schema Registry │◄── Avro schemas
                     │   (Confluent)   │    (BACKWARD compat)
                     └────────┬────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │  Sim / Real     │
                     │  Schema Change  │
                     └────────┬────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │  opt-in      │  │  opt-out     │  │  Iceberg     │
    │  pipeline    │  │  pipeline    │  │  Sink        │
    │  (PROPAGATE) │  │  (PAUSE if   │  │  (typed      │
    │              │  │   breaking)  │  │   tables)    │
    └──────────────┘  └──────────────┘  └──────────────┘
```

### Components

| Component | Role | Image |
|-----------|------|-------|
| **PostgreSQL** | Source of truth (domain databases) | `postgres:15` |
| **Debezium** | CDC connector — reads WAL and publishes to Kafka | Custom build on `cp-kafka-connect:7.5.3` |
| **Kafka** | Event bus (KRaft mode, no ZooKeeper) | `confluentinc/cp-kafka:7.5.3` |
| **Schema Registry** | Avro schema store & compatibility gatekeeper | `confluentinc/cp-schema-registry:7.5.3` |
| **Schema Evolution Service** | Python daemon that validates changes and pauses pipelines | Local Python app |
| **Iceberg REST** | Table catalog for typed Iceberg tables | `tabulario/iceberg-rest:latest` |
| **Trino** | Query engine over Iceberg | `trinodb/trino:431` |
| **MinIO** | S3-compatible object storage for Iceberg | `minio/minio` |

---

## 2. Simulation Stages Step-by-Step

### Stage 0: `make up` — Infrastructure Bootstrap

| Step | What happens | Details |
|------|-------------|---------|
| `docker compose down -v --remove-orphans` | Destroy old containers, networks, volumes | Ensures clean state |
| `docker rm -f ...` | Force-remove named containers | Catches leftovers from other projects (e.g. `quantum-sim-minio-1`) |
| `docker compose up -d --build` | Build & start | Builds `kafka-connect/Dockerfile` (Confluent Connect + Debezium plugin) |
| `sleep 25` | Wait for warm-up | Kafka, Schema Registry and Connect need time to elect leaders and create internal topics |
| `./scripts/setup-connectors.sh` | Register Debezium connectors | POSTs JSON configs to Kafka Connect REST API (`:8083`) |

### Stage 1: Connector Registration (`setup-connectors.sh`)

```bash
curl -X POST http://localhost:8083/connectors \
  --data @debezium/connectors/customers-connector.json
```

**Inside Kafka Connect:**

1. **Worker** receives config and persists it to the internal topic `connect_configs` (in Kafka).
2. **Distributed Herder** assigns the task to available workers (we run a single worker).
3. **Task** starts — it is a `PostgresConnector` from the Debezium plugin.
4. Debezium connects to PostgreSQL, creates a **logical replication slot** `debezium` (if missing) and a **publication** `dbz_publication`.
5. **Initial snapshot** begins — reads the entire table and writes every row to Kafka.
6. After snapshot finishes, switches to **streaming mode** — reads WAL (Write-Ahead Log) via `pgoutput`.

### Stage 2: First Schema in Schema Registry

When Debezium writes the first message to `customers-server.public.customers`, it **automatically** registers the Avro schema in Schema Registry.

```bash
curl http://localhost:8081/subjects/customers-server.public.customers-value/versions/latest
```

Example response (simplified):
```json
{
  "schema": "{\"type\":\"record\",\"name\":\"Value\",\"namespace\":\"customers-server.public.customers\",...}",
  "version": 1,
  "id": 1
}
```

> **Important:** The default compatibility level for a new subject is `BACKWARD`. This means: a new schema must be readable by old consumers.

### Stage 3: `make simulate` — Launch the Simulation

```bash
python -m src.datamesh_cdc.schema_evolution_service --simulate
```

**Inside the Python process:**

1. `PipelineManager` loads `/tmp/datamesh_simulate.json` (if it exists) or creates pipelines from scratch.
2. Three demo pipelines are created:
   - `orders-to-analytics` (**opt-in**) — accepts any change.
   - `orders-to-reporting` (**opt-out**) — protects `total_amount`.
   - `customers-to-analytics` (**opt-in**).
3. **Scenario 1:** Registers `schema_v2` (adds `promo_code` with `default: None`).
4. **Scenario 2:** Registers `schema_v3` (removes `total_amount`).

### Stage 4: Schema Evolution Service Validates Compatibility

**For `orders-to-analytics` (opt-in):**
- Service calls `SchemaRegistryClient.register_schema("sim.orders-value", schema_v2)`.
- Schema Registry checks BACKWARD compatibility.
- `promo_code` is optional with a default → **compatible** change.
- Schema is registered → `schema_id: 6`.
- `PipelineManager` marks action: `"PROPAGATED"`.

**For `orders-to-reporting` (opt-out):**
- Service checks: does the change affect `consumed_fields`?
- `schema_v3` removed `total_amount`, which is listed in `consumed_fields`.
- **Breaking change detected!**
- `PipelineManager` sets status `PAUSED` and sends an alert.

---

## 3. How Debezium Works (Deep Dive)

### Logical Decoding in PostgreSQL

PostgreSQL writes every change to the **WAL** (Write-Ahead Log). Debezium uses **logical decoding** via the built-in `pgoutput` plugin (available since PostgreSQL 10):

```sql
-- Debezium creates these on first connection:
CREATE PUBLICATION dbz_publication FOR TABLE public.customers, public.orders;
SELECT pg_create_logical_replication_slot('debezium', 'pgoutput');
```

**What lands in WAL:**

| Operation | WAL content |
|-----------|-------------|
| `INSERT` | Full record |
| `UPDATE` | `before` + `after` (if `REPLICA IDENTITY FULL`) |
| `DELETE` | Key + `__deleted: true` |

### Debezium → Kafka Message Structure

```json
{
  "before": null,
  "after": {
    "id": 1,
    "customer_id": 42,
    "total_amount": "150.00",
    "status": "confirmed",
    "created_at": 1690000000000,
    "updated_at": 1690000000000
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

The **Avro schema** of this message is automatically registered in Schema Registry. The key is `orders-server.public.orders.Key` (based on the primary key), the value is `orders-server.public.orders.Value`.

### Why `total_amount` is a `string` in Debezium schemas

Debezium uses `decimal.handling.mode: string` in our connector config. This converts PostgreSQL `NUMERIC` / `DECIMAL` columns to strings in Avro, avoiding precision loss. If you change it to `double`, you get `double` in the schema but risk rounding errors.

---

## 4. How Kafka Connect Works

### Distributed Mode

Our Kafka Connect runs in **distributed mode** (not standalone). This means:

- Configs are stored in Kafka topics (`connect_configs`, `connect_offsets`, `connect_statuses`).
- You can scale by adding more worker nodes.
- REST API is available on port `8083`.

### Connector REST API

```bash
# List connectors
curl http://localhost:8083/connectors
# ["customers-cdc-connector", "orders-cdc-connector"]

# Status of a specific connector
curl http://localhost:8083/connectors/customers-cdc-connector/status
# {"name":"customers-cdc-connector","connector":{"state":"RUNNING",...},"tasks":[{"state":"RUNNING",...}]}

# Restart a task
curl -X POST http://localhost:8083/connectors/customers-cdc-connector/tasks/0/restart

# Delete a connector
curl -X DELETE http://localhost:8083/connectors/customers-cdc-connector
```

### Converter: Avro + Schema Registry

In the connector config:
```json
"key.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter.schema.registry.url": "http://schema-registry:8081"
```

This means: Kafka Connect **serializes** messages to Avro and **automatically** registers schemas in Schema Registry on first write.

---

## 5. How Schema Registry Works

### Schema Storage by Subject

Schema Registry stores schemas by **subject** (topic name + suffix `-key` or `-value`):

```bash
curl http://localhost:8081/subjects
# [
#   "customers-server.public.customers-key",
#   "customers-server.public.customers-value",
#   "orders-server.public.orders-key",
#   "orders-server.public.orders-value"
# ]
```

### Compatibility Levels

| Level | Rule | Example |
|-------|------|---------|
| `BACKWARD` (default) | New schema readable by old readers | Add optional field with default |
| `FORWARD` | Old writer can write to new schema | Remove optional field |
| `FULL` | Both BACKWARD and FORWARD | Only add/remove optional fields |
| `NONE` | No checks | Any change allowed |

### Compatibility Check

When you `POST /subjects/{subject}/versions`, SR validates:

```bash
curl -X POST http://localhost:8081/subjects/orders-server.public.orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\":\"record\",\"name\":\"Value\",...}"}'
```

If the schema is incompatible → **409 Conflict**:
```json
{
  "error_code": 409,
  "message": "Schema being registered is incompatible..."
}
```

### Version Evolution

```bash
# All versions of a schema
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions
# [1, 2, 3]

# Specific version
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions/2

# Test compatibility of a new schema against latest
curl http://localhost:8081/compatibility/subjects/orders-server.public.orders-value/versions/latest \
  -X POST -H "Content-Type: application/json" \
  --data '{"schema": "..."}'
```

---

## 6. Where to Watch Schema Evolution

### A. REST API (Schema Registry)

```bash
# All subjects
curl -s http://localhost:8081/subjects | python -m json.tool

# Latest schema version
curl -s http://localhost:8081/subjects/orders-server.public.orders-value/versions/latest | python -m json.tool

# All versions
curl -s http://localhost:8081/subjects/orders-server.public.orders-value/versions | python -m json.tool

# Current compatibility setting
curl -s http://localhost:8081/config/orders-server.public.orders-value
# {"compatibilityLevel":"BACKWARD"}
```

### B. Kafka Avro Console Consumer

```bash
# Read messages with automatic Avro deserialization
docker exec kafka kafka-avro-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-server.public.orders \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081 \
  --max-messages 5
```

### C. Schema Registry UI (Optional)

Add to `docker-compose.yml`:

```yaml
  schema-registry-ui:
    image: landoop/schema-registry-ui:latest
    ports:
      - "8000:8000"
    environment:
      SCHEMAREGISTRY_URL: http://schema-registry:8081
      PROXY: "true"
    networks:
      - datamesh
```

Open `http://localhost:8000` for a visual schema history browser.

### D. Python (confluent-kafka)

```python
from confluent_kafka.schema_registry import SchemaRegistryClient

sr = SchemaRegistryClient({'url': 'http://localhost:8081'})

# Get all versions
versions = sr.get_versions('orders-server.public.orders-value')
for v in versions:
    schema = sr.get_version('orders-server.public.orders-value', v)
    print(f"Version {v}: {schema.schema.schema_str[:100]}...")
```

### E. Prometheus + Grafana

The stack includes Prometheus (scrapes Schema Registry metrics) and Grafana. You can build dashboards for:
- Schema registration rate
- Compatibility check failures (409 errors)
- Subject count over time

---

## 7. Advanced Simulation Scenarios

### Scenario A: Field Type Change (Breaking)

```python
schema_v1 = {
    "type": "record", "name": "Order", "namespace": "sim.orders",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "total_amount", "type": "double"}  # was double
    ]
}

schema_v2 = {
    "type": "record", "name": "Order", "namespace": "sim.orders",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "total_amount", "type": "string"}  # became string
    ]
}
```

**Expected:** Schema Registry rejects with `TYPE_MISMATCH` → pipeline PAUSED.

### Scenario B: Field Rename (Breaking)

```python
schema_v2 = {
    "type": "record", "name": "Order", "namespace": "sim.orders",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "new_amount", "type": "double"}  # was total_amount
    ]
}
```

**Expected:** `NAME_MISMATCH` or `FIELD_REMOVED` → PAUSED for opt-out.

### Scenario C: Adding a Required Field (Breaking)

```python
schema_v2 = {
    "type": "record", "name": "Order", "namespace": "sim.orders",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "total_amount", "type": "double"},
        {"name": "priority", "type": "string"}  # no default!
    ]
}
```

**Expected:** BACKWARD violated (old readers don't know about `priority`) → 409 Conflict.

### Scenario D: Union Types (null + type)

```python
schema_v2 = {
    "type": "record", "name": "Order", "namespace": "sim.orders",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "total_amount", "type": "double"},
        {"name": "discount", "type": ["null", "double"], "default": None}
    ]
}
```

**Expected:** ✅ Accepted, because `["null", "double"]` with `default: None` is an optional field.

### Scenario E: Nested Records

```python
schema_v2 = {
    "type": "record", "name": "Order", "namespace": "sim.orders",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "customer", "type": {
            "type": "record", "name": "Customer",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "email", "type": "string"}
            ]
        }}
    ]
}
```

**Expected:** Nested record registers as a separate schema (if named) or inline.

### Scenario F: Enum Types

```python
schema_v2 = {
    "type": "record", "name": "Order", "namespace": "sim.orders",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "status", "type": {
            "type": "enum", "name": "OrderStatus",
            "symbols": ["PENDING", "CONFIRMED", "SHIPPED", "CANCELLED"]
        }}
    ]
}
```

**Expected:** Enum registers. Adding a symbol to an enum is **compatible** for FORWARD but **breaking** for BACKWARD (old reader doesn't know the new symbol).

### Scenario G: Multiple Pipelines on One Topic

```python
# Add 3 pipelines with different consumed_fields
manager.create_pipeline(
    "orders-to-ml", "sim.orders",
    consumed_fields=["id", "status"], mode=PipelineMode.OPT_OUT
)
manager.create_pipeline(
    "orders-to-bi", "sim.orders",
    consumed_fields=["total_amount"], mode=PipelineMode.OPT_OUT
)
manager.create_pipeline(
    "orders-to-archive", "sim.orders",
    consumed_fields=[], mode=PipelineMode.OPT_IN
)
```

**Action:** Remove `status` field.
**Result:** `orders-to-ml` PAUSED, others RUNNING.

### Scenario H: Compatibility Level Switch

```python
# Change from BACKWARD to FULL
sr.set_compatibility("sim.orders-value", "FULL")
```

Now only changes that are both BACKWARD and FORWARD are allowed. Try adding a required field → rejected even for opt-in.

---

## 8. Useful Commands

### Kafka

```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Describe topic
docker exec kafka kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic orders-server.public.orders

# Message count
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 --topic orders-server.public.orders

# Read raw messages (no Avro)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-server.public.orders \
  --from-beginning
```

### Schema Registry

```bash
# All subjects
curl http://localhost:8081/subjects

# Delete subject permanently (careful!)
curl -X DELETE "http://localhost:8081/subjects/orders-server.public.orders-value?permanent=true"

# Change compatibility
curl -X PUT http://localhost:8081/config/orders-server.public.orders-value \
  -H "Content-Type: application/json" \
  --data '{"compatibility": "FULL"}'

# Global compatibility
curl -X PUT http://localhost:8081/config \
  -H "Content-Type: application/json" \
  --data '{"compatibility": "FULL"}'
```

### Kafka Connect

```bash
# Connector logs
docker logs -f kafka-connect

# Restart connector
curl -X POST http://localhost:8083/connectors/orders-cdc-connector/restart

# Pause connector
curl -X PUT http://localhost:8083/connectors/orders-cdc-connector/pause

# Resume connector
curl -X PUT http://localhost:8083/connectors/orders-cdc-connector/resume

# Connector config
curl http://localhost:8083/connectors/orders-cdc-connector/config
```

### PostgreSQL

```bash
# View replication slots
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "SELECT * FROM pg_replication_slots;"

# View publication
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "SELECT * FROM pg_publication;"

# Insert test row
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "INSERT INTO orders (customer_id, total_amount, status) \
      VALUES (1, 99.99, 'confirmed');"

# Check tables
docker exec postgres-orders psql -U postgres -d orders_db \
  -c "\\dt"
```

### Debezium

```bash
# Check if connector is streaming WAL
docker logs kafka-connect | grep "Streaming changes from"

# Check snapshot progress
docker logs kafka-connect | grep "Snapshot -"
```

### Schema Evolution Service

```bash
# Run simulation
make simulate

# View simulation state
cat /tmp/datamesh_simulate.json | python -m json.tool

# View pipeline state (real daemon)
cat state.json | python -m json.tool
```

---

*Generated for the CDC Data Mesh Schema-on-Write project.*
