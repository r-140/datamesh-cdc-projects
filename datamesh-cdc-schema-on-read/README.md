# Data Mesh CDC Platform — Schema-on-Read (Flexible)

> **Approach**: Schema-on-Read with JSONB bronze layer. Schema is applied at read time in Silver/Gold. The pipeline **never breaks** on source schema changes.

## Architecture

```
PostgreSQL (Domain)
    -> Debezium CDC (Source)
    -> Kafka + Schema Registry (Avro)
    -> Python CDC Consumer
    -> PostgreSQL DWH (raw.orders_cdc JSONB, raw.customers_cdc JSONB)
    -> dbt (bronze/silver/gold)
    -> [Optional] Iceberg + Trino for advanced analytics
```

## Quick Start

```bash
# 1. Install Python dependencies
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# 2. Start all infrastructure (connectors auto-register via Docker Compose)
make up

# 3. Verify connectors are active
make connectors          # idempotent — safe to run multiple times

# 4. Check CDC consumer is running
tail -f logs/consumer.log

# 5. Run end-to-end demo
python scripts/run_demo.py

# 6. Generate live CDC data and watch it flow to DWH
python scripts/data_generator.py --mode batch --count 20

# 7. Run schema evolution simulation
make simulate

# 8. Build dbt models
make dbt-setup
make dbt-run
make dbt-test
```

> **Note on persistence:**
>
> - `make up` — starts containers and **preserves** existing volumes (Kafka data, Postgres data).
> - `make down` — stops containers but **keeps** volumes intact.
> - `make reset` — **destroys** all volumes (`down -v`) and recreates everything from scratch. Use this when you want a clean slate.

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka Connect REST API | http://localhost:8083 | — |
| Schema Registry | http://localhost:8081 | — |
| Kafka (bootstrap) | `localhost:9092` | — |
| PostgreSQL Orders | `localhost:5432` | `postgres` / `postgres` |
| PostgreSQL Customers | `localhost:5433` | `postgres` / `postgres` |
| PostgreSQL DWH | `localhost:5434` | `dwh` / `dwh` |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |

> **Note**: Trino (`:8080`), Iceberg REST Catalog (`:8181`) and MinIO (`:9000/:9001`) are available when running the extended stack.

## Grafana Dashboard

Pre-built dashboard with CDC, DWH and consumer monitoring:

```
# Dashboard is auto-imported via provisioning, or manually:
# Grafana → Dashboards → Import → upload grafana/dashboards/datamesh-cdc.json
```

**Dashboard includes:**

- **Overview**: Connector state, event totals, DWH row counts, consumer lag
- **CDC Source**: Events/sec, replication lag, snapshot status, poll rate
- **CDC Consumer**: Consume rate, processed records, lag
- **JVM**: Heap memory, GC pressure
- **PostgreSQL DWH**: Table sizes, transactions/sec, cache hit ratio, deadlocks
- **Alerts**: Firing Prometheus alerts in real-time

## Data Flow

```
postgres-orders     -> Debezium Source -> Kafka (Avro) -> CDC Consumer -> postgres-dwh.raw.orders_cdc (JSONB)
postgres-customers  -> Debezium Source -> Kafka (Avro) -> CDC Consumer -> postgres-dwh.raw.customers_cdc (JSONB)
```

- **Source connectors** read PostgreSQL WAL via logical replication (`pgoutput`)
- **CDC Consumer** (Python) reads Kafka topics, deserializes Avro, writes JSONB to DWH
- **Bronze layer** stores raw JSONB payloads — schema changes do not break ingestion
- **Silver layer** (dbt) extracts typed columns from JSONB with explicit `CAST`
- **Gold layer** (dbt) builds business aggregates on top of Silver

## Connector Auto-Registration

Connectors are registered automatically:

1. **Docker Compose** — the `setup-connectors` service waits until Kafka Connect is healthy and then runs `scripts/setup-connectors.sh` inside a transient container.
2. **Manual / Makefile** — run `make connectors` at any time. The script is **idempotent**: existing connectors are skipped, missing ones are created.

> **Note**: Only **source connectors** (Debezium) are registered. There is no JDBC Sink — the Python CDC Consumer handles ingestion.

```bash
# Re-register connectors manually (e.g. after changing connector JSON configs)
make connectors

# Check connector status
make connectors  # shows active connectors at the end
```

## CDC Consumer (Python)

The `scripts/cdc_consumer.py` service runs as a background process, reading Avro messages from Kafka and writing JSONB to the DWH.

```bash
# Start consumer in foreground
make consumer

# View logs
tail -f logs/consumer.log
```

**Consumer behavior:**

- Deserializes Avro using Schema Registry
- Writes entire payload as `JSONB` to `raw.orders_cdc` / `raw.customers_cdc`
- Upserts on `id` (conflict resolution)
- Tracks `__op`, `__source_ts_ms`, `__kafka_partition`, `__kafka_offset` for audit
- Graceful shutdown on SIGINT/SIGTERM

## Monitoring & Alerting

### How Metrics Flow to Prometheus

```
┌─────────────────┐     JMX      ┌──────────────────┐     HTTP     ┌─────────────┐
│  Kafka Connect  │ ───────────> │  JMX Exporter    │ ───────────> │  Prometheus │
│  (Debezium)     │  (port 9999) │  (port 7071)     │  /metrics    │  (port 9090)│
└─────────────────┘              └──────────────────┘              └─────────────┘
                                                                         │
┌─────────────────┐     HTTP     ┌──────────────────┐                  │
│  postgres-dwh   │ ───────────> │ postgres-exporter│ ────────────────┘
│                 │  (port 9187) │  (port 9187)     │   /metrics
└─────────────────┘              └──────────────────┘
```

**Components:**

1. **JMX Exporter** — Java agent attached to Kafka Connect JVM. Exposes Debezium metrics (events/sec, lag, snapshot status) on port `7071`.
2. **postgres-exporter** — Sidecar container that scrapes PostgreSQL DWH metrics on port `9187`.
3. **Prometheus** — Scrapes both exporters every 15s.
4. **Grafana** — Reads Prometheus as datasource.

### Metrics Available

| Metric | Source | Description |
|--------|--------|-------------|
| `debezium_events_total` | JMX | Total CDC events captured |
| `debezium_lag_ms` | JMX | Milliseconds since last event (replication lag) |
| `debezium_snapshot_running` | JMX | Is initial snapshot in progress (0/1) |
| `kafka_connect_connector_task_state` | JMX | Task state: RUNNING=1, FAILED=0 |
| `pg_stat_database_xact_commit` | postgres-exporter | DWH transactions committed |

### Alert Rules

Defined in `prometheus/alerts.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `CDC_Connector_Down` | Connector task not RUNNING for 30s | **critical** |
| `CDC_High_Lag` | Lag > 60 seconds for 2 minutes | warning |
| `CDC_Pipeline_Stall` | No events for 5 minutes (not snapshotting) | warning |
| `CDC_Consumer_Down` | Python consumer process not running | **critical** |

## Live Data Generator

The `scripts/data_generator.py` tool generates INSERT / UPDATE / DELETE operations on source databases and monitors CDC propagation in real-time.

```bash
# Batch insert (default: 10 orders + 10 customers)
python scripts/data_generator.py --mode batch --count 20

# Continuous streaming (Ctrl+C to stop)
python scripts/data_generator.py --mode continuous --interval 3

# Target specific table
python scripts/data_generator.py --mode insert --table orders --count 5
python scripts/data_generator.py --mode insert --table customers --count 5

# Updates and deletes (tombstones visible in Kafka!)
python scripts/data_generator.py --mode update --table orders --count 3
python scripts/data_generator.py --mode delete --table customers --count 2

# Mixed random operations
python scripts/data_generator.py --mode mixed --count 15

# Verify current state: Source → Kafka → DWH
python scripts/data_generator.py --mode verify
```

**Output example:**

```
======================================================================
                  VERIFICATION: Source → Kafka → DWH
======================================================================

Source Databases:
  orders       orders       → 24 rows
  customers    customers    → 21 rows

Kafka Offsets (latest):
  orders-server.public.orders              → offset 27
  customers-server.public.customers        → offset 27

Connector Status:
  orders-cdc-connector           → RUNNING
  customers-cdc-connector        → RUNNING

DWH (raw layer):
  raw.orders_cdc            → 24 rows
  raw.customers_cdc         → 21 rows
```

## Breaking Change Demo

Simulate a **breaking schema change** (e.g., `DROP COLUMN`) and watch the CDC pipeline **continue running** while downstream dbt tests catch the drift.

```bash
# Drop total_amount column from orders — pipeline keeps running
python scripts/breaking_change_demo.py --table orders --column total_amount

# Drop email column from customers
python scripts/breaking_change_demo.py --table customers --column email
```

**What the demo does:**

1. **Baseline** — Shows all connectors RUNNING, consumer active
2. **Schema** — Displays current table columns
3. **Insert** — Generates test data to establish flow
4. **💥 BREAKING CHANGE** — Executes `ALTER TABLE ... DROP COLUMN`
5. **Insert after drop** — Next insert succeeds, message lands in DWH as JSONB (missing field = NULL)
6. **Monitor** — Polls DWH count, shows it **continues growing**
7. **Inspect JSONB** — Shows the missing field is NULL in JSONB payload
8. **Simulate Silver** — Shows that `dbt test` / Silver model would fail (`not_null` constraint)
9. **Recovery** — Restores column, new messages are complete

**Expected output after breaking change:**

```
Connector Status:
  orders-cdc-connector         → RUNNING
    task-0 → RUNNING

DWH rows: 10 (delta: 1)  ← Pipeline NEVER stops!

JSONB field 'total_amount': NULL / MISSING

🚨 SILVER MODEL WOULD FAIL:
   null value in column "total_amount" violates not-null constraint
```

**Key difference from Schema-on-Write:**

| | Schema-on-Write | Schema-on-Read |
|---|---|---|
| **After DROP COLUMN** | Connector crashes, DWH stops | Pipeline runs, DWH grows |
| **Where failure surfaces** | Kafka Connect (runtime) | dbt tests (batch) |
| **Data loss** | Bad message in DLQ | No loss — message in JSONB |

## dbt Project

Located in `dbt_datamesh/`:

```
models/
  bronze/          -- Raw JSONB views (pass-through)
  silver/          -- Typed extraction (payload->>'field')::type
  gold/            -- Business aggregates
```

**Bronze** — Transparent pass-through of JSONB:
```sql
SELECT id, payload, __op, __source_ts_ms, ingested_at
FROM raw.orders_cdc
```

**Silver** — Explicit schema extraction with CAST:
```sql
SELECT
    id,
    (payload->>'customer_id')::bigint as customer_id,
    (payload->>'total_amount')::numeric(12,2) as total_amount,
    payload->>'status' as status,
    to_timestamp((payload->>'created_at')::bigint / 1000000.0) as created_at
FROM {{ ref('orders') }}
```

**Gold** — Business aggregates:
```sql
-- Daily orders summary
SELECT DATE(created_at) as order_date, status,
       COUNT(*) as order_count, SUM(total_amount) as total_revenue
FROM {{ ref('orders') }}
GROUP BY 1, 2
```

**Key tests** that catch breaking changes:

- `not_null(total_amount)` — FAILS after DROP COLUMN (detected in Silver)
- `accepted_values(status)` — Validates enums
- `relationships(customer_id)` — Referential integrity

```bash
cd dbt_datamesh
dbt run   # Build models
dbt test  # Run tests (catches schema breaks in Silver!)
```

## Schema Evolution Behavior

### Scenario 1: Add optional field

```sql
ALTER TABLE orders ADD COLUMN promo_code VARCHAR(50) DEFAULT NULL;
```

| Layer | Behavior |
|-------|----------|
| Bronze | ✅ Appends as JSONB — no schema change needed |
| Silver | `promo_code` becomes NULL until added to VIEW DDL |
| Gold | Unaffected until business decides to use the field |

### Scenario 2: Remove consumed field

```sql
ALTER TABLE orders DROP COLUMN total_amount;
```

| Layer | Behavior |
|-------|----------|
| Bronze | ✅ Continues — JSONB just lacks the key |
| Silver | `total_amount` = NULL → `dbt test not_null` **FAILS** |
| Gold | May fail if aggregate depends on `total_amount` |

## Useful Commands

```bash
# ── Lifecycle ──
make up                 # Start stack, wait for Connect, register connectors, start consumer
make down               # Stop stack (preserve volumes) + stop consumer
make reset              # FULL RESET: destroy volumes + recreate
make connectors         # Re-run connector registration (idempotent)
make consumer           # Start CDC consumer in foreground
make consumer-log       # Tail consumer logs
make clean              # Down + remove containers + temp files

# ── Kafka / Connect CLI ──
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/orders-cdc-connector/status
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions/latest

# Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Read CDC messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-server.public.orders \
  --from-beginning

# ── DWH JSONB queries ──
# List tables
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c "\\dt raw.*"

# Inspect raw JSONB
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh \
  -c "SELECT id, payload->>'status' as status, payload->>'total_amount' as amount FROM raw.orders_cdc LIMIT 5;"

# Pretty-print JSONB
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh \
  -c "SELECT id, jsonb_pretty(payload) FROM raw.orders_cdc LIMIT 1;"

# ── Data & demos ──
python scripts/data_generator.py --mode batch --count 20
python scripts/data_generator.py --mode verify
python scripts/breaking_change_demo.py --table orders --column total_amount
```

## Project Structure

```
.
├── src/datamesh_cdc/              # Python services
│   ├── schema_evolution.py
│   ├── pipeline_manager.py
│   ├── iceberg_sink.py
│   └── schema_evolution_service.py
├── dbt_datamesh/                  # dbt models (bronze/silver/gold)
│   ├── models/bronze/
│   ├── models/silver/
│   ├── models/gold/
│   └── models/schema.yml          # Tests
├── kafka-connect/                 # Custom Kafka Connect image
│   ├── Dockerfile                 # Debezium + JMX Exporter
│   └── jmx_exporter_config.yml    # JMX → Prometheus mapping
├── prometheus/                    # Monitoring configuration
│   ├── prometheus.yml             # Scrape configs
│   └── alerts.yml                 # Alert rules
├── grafana/                       # Dashboards & provisioning
│   ├── dashboards/
│   │   └── datamesh-cdc.json
│   └── provisioning/
├── scripts/                       # Automation & init SQL
│   ├── setup-connectors.sh        # Registers Debezium source connectors only
│   ├── cdc_consumer.py            # ← NEW: Python consumer Kafka → DWH JSONB
│   ├── init-dwh.sql               # ← NEW: JSONB raw tables
│   ├── run_demo.py                # End-to-end demo
│   ├── data_generator.py          # Live CDC data generator
│   ├── breaking_change_demo.py    # Schema break demo (SoR behavior)
│   ├── schema_evolution_simulator.py
│   ├── init_orders.sql            # Orders DB schema + seed data
│   └── init_customers.sql         # Customers DB schema + seed data
├── logs/                          # CDC consumer logs
├── tests/                         # pytest
├── docs/                          # Documentation
├── docker-compose.yml             # Full stack
├── Makefile
└── pyproject.toml
```

## Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| `make up` hangs on "Waiting for Kafka Connect" | Connect hasn't finished starting | Wait up to 2 min on first run. Check `docker logs kafka-connect`. |
| Consumer not writing to DWH | Consumer not started or crashed | Run `make consumer` or check `tail -f logs/consumer.log` |
| `ModuleNotFoundError: confluent_kafka` | Missing Python dependency | `pip install "confluent-kafka[avro]"` |
| `Connector X may already exist` / 409 | Script is idempotent | Not an error. Run `make connectors` again safely. |
| `curl: (22) The requested URL returned error: 400` on connector registration | Invalid JSON in connector config | Check `scripts/setup-connectors.sh` for trailing commas or duplicate keys. |
| Connectors disappear after `make down` | Expected — connectors live in Kafka topics (`connect-configs`, `connect-offsets`, `connect-status`). These topics survive `make down`. | Run `make connectors` to re-register, or use `make reset` for a clean start. |
| Connectors disappear after `make reset` | `down -v` wipes Kafka data including Connect internal topics | Run `make up` — the `setup-connectors` service will auto-register them. |
| `relation "customers" does not exist` | Init SQL scripts not mounted | Check `docker-compose.yml` volumes: `scripts/init_customers.sql:/docker-entrypoint-initdb.d/init.sql:ro` |
| Prometheus not scraping Kafka Connect | JMX Exporter agent missing or `KAFKA_OPTS` misconfigured | Verify `kafka-connect/Dockerfile` includes the JAR and `KAFKA_OPTS` points to it. Check port `7071`. |
| Grafana alert not firing | Datasource or alert rule issue | Verify Prometheus is datasource in Grafana. Check alert rule evaluation interval. |
| `prometheus.yml is a directory` | File/directory name collision | `rm -rf prometheus/prometheus.yml` |
| MinIO port conflict | Existing container on port | `docker rm -f quantum-sim-minio-1` |
| Kafka `CLUSTER_ID` invalid | Wrong format | Use base64 UUID: `MkU3OEVBNTYwNTUENDI2Qg` |
| dbt test fails after breaking change demo | Schema-on-Read behavior — Silver model expects column that was dropped | This is **expected**! Restore column: `ALTER TABLE orders ADD COLUMN total_amount DECIMAL(12,2);` |

## When to Use This Approach

✅ Data warehouses (Snowflake, Databricks, BigQuery)  
✅ Exploratory analytics where schema changes frequently  
✅ When downstream consumers are SQL analysts, not microservices  
✅ Rapid prototyping  
✅ When pipeline uptime is more critical than immediate schema enforcement  

## Related Projects

- [Schema-on-Write](../datamesh-cdc-schema-on-write) — Strict Avro compatibility, DLQ-based error handling
- [Hybrid](../datamesh-cdc-hybrid) — Best of both worlds

## License

MIT
