# Data Mesh CDC Platform — Schema-on-Write (Strict)

[![CI/CD](https://github.com/yourusername/datamesh-cdc-schema-on-write/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/yourusername/datamesh-cdc-schema-on-write/actions)
[![codecov](https://codecov.io/gh/yourusername/datamesh-cdc-schema-on-write/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/datamesh-cdc-schema-on-write)

> **Approach**: Schema-on-Write with strict Avro compatibility. Breaking changes **pause** the pipeline and trigger alerts.

## Architecture

```
PostgreSQL (Domain)
    -> Debezium CDC (Source)
    -> Kafka + Schema Registry (Avro, BACKWARD)
    -> JDBC Sink Connector
    -> PostgreSQL DWH (raw.orders_cdc, raw.customers_cdc)
    -> dbt (bronze/silver/gold)
    -> [Optional] Iceberg + Trino for advanced analytics
```

## Quick Start

```bash
# 1. Install
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# 2. Start all infrastructure
make up

# 3. Verify connectors registered automatically
make connectors

# 4. Run end-to-end demo
python scripts/run_demo.py

# 5. Generate live CDC data and watch it flow to DWH
python scripts/data_generator.py --mode batch --count 20
python scripts/data_generator.py --mode continuous --interval 3

# 6. Run schema evolution simulation
make simulate

# 7. Build dbt models
make dbt-setup
make dbt-run
make dbt-test
```

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

> **Note**: Trino (`:8080`), Iceberg REST Catalog (`:8181`) and MinIO (`:9000/:9001`) are available when running the extended stack (see `docker-compose.yml` extensions or run the Iceberg-enabled profile).

## Grafana Dashboard

Pre-built dashboard with CDC, DWH and JVM monitoring:

```bash
# Dashboard is auto-imported via provisioning, or manually:
# Grafana → Dashboards → Import → upload grafana/dashboards/datamesh-cdc.json
```

**Dashboard includes:**
- **Overview**: Connector state, event totals, DWH row counts
- **CDC Source**: Events/sec, replication lag, snapshot status, poll rate
- **JDBC Sink**: Send rate, active records buffer
- **JVM**: Heap memory, GC pressure
- **PostgreSQL DWH**: Table sizes, transactions/sec, cache hit ratio, deadlocks
- **Alerts**: Firing Prometheus alerts in real-time

See [GRAFANA_SETUP.md](GRAFANA_SETUP.md) for detailed setup, alert configuration and webhook examples.

## Data Flow

```
postgres-orders     -> Debezium Source -> Kafka (Avro) -> JDBC Sink -> postgres-dwh.raw.orders_cdc
postgres-customers  -> Debezium Source -> Kafka (Avro) -> JDBC Sink -> postgres-dwh.raw.customers_cdc
```

- **Source connectors** read PostgreSQL WAL via logical replication (`pgoutput`)
- **Sink connectors** read Kafka topics and write to DWH via `upsert`
- `auto.evolve=true` — on schema evolution (ADD COLUMN) the sink automatically adds the column to DWH
- `auto.create=true` — tables in `raw.*` are created automatically if they don't exist

## Monitoring & Alerting

### How Metrics Flow to Prometheus

```
┌─────────────────┐     JMX      ┌──────────────────┐     HTTP     ┌─────────────┐
│  Kafka Connect  │ ───────────> │  JMX Exporter    │ ───────────> │  Prometheus │
│  (Debezium,     │  (port 9999) │  (port 7071)     │  /metrics    │  (port 9090)│
│   JDBC Sink)    │              │  (java agent)    │              │             │
└─────────────────┘              └──────────────────┘              └─────────────┘
                                                                         │
┌─────────────────┐     HTTP     ┌──────────────────┐                  │
│  postgres-dwh   │ ───────────> │ postgres-exporter│ ────────────────┘
│                 │  (port 9187) │  (port 9187)     │   /metrics
└─────────────────┘              └──────────────────┘
```

**Components:**
1. **JMX Exporter** — Java agent attached to Kafka Connect JVM. Exposes Debezium metrics (events/sec, lag, snapshot status) and Kafka Connect task states as Prometheus metrics on port `7071`.
2. **postgres-exporter** — Sidecar container that scrapes PostgreSQL DWH metrics (connections, slow queries, table sizes) on port `9187`.
3. **Prometheus** — Scrapes both exporters every 15s. Stores time-series data and evaluates alert rules.
4. **Grafana** — Reads Prometheus as datasource. Displays dashboards and sends alert notifications.

### Metrics Available

| Metric | Source | Description |
|--------|--------|-------------|
| `debezium_events_total` | JMX | Total CDC events captured |
| `debezium_lag_ms` | JMX | Milliseconds since last event (replication lag) |
| `debezium_snapshot_running` | JMX | Is initial snapshot in progress (0/1) |
| `kafka_connect_connector_task_state` | JMX | Task state: RUNNING=1, FAILED=0 |
| `kafka_connect_sink_record_send_rate` | JMX | Records/sec sent to DWH |
| `pg_stat_database_xact_commit` | postgres-exporter | DWH transactions committed |

### Alert Rules

Defined in `prometheus/alerts.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `CDC_Connector_Down` | Connector task not RUNNING for 30s | **critical** |
| `CDC_High_Lag` | Lag > 60 seconds for 2 minutes | warning |
| `CDC_Pipeline_Stall` | No events for 5 minutes (not snapshotting) | warning |
| `CDC_Sink_Stall` | Sink not sending records for 2 minutes | warning |

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
  orders-jdbc-sink               → RUNNING
  customers-jdbc-sink            → RUNNING

DWH (raw layer):
  raw.orders_cdc            → 24 rows
  raw.customers_cdc         → 21 rows
```

## Breaking Change Demo

Simulate a **breaking schema change** (e.g., `DROP COLUMN`) and watch the CDC pipeline fail with alerts firing in Grafana.

```bash
# Drop total_amount column from orders — breaks the Avro schema
python scripts/breaking_change_demo.py --table orders --column total_amount

# Drop email column from customers
python scripts/breaking_change_demo.py --table customers --column email
```

**What the demo does:**
1. **Baseline** — Shows all connectors RUNNING
2. **Schema** — Displays current table columns
3. **Insert** — Generates test data to establish flow
4. **💥 BREAKING CHANGE** — Executes `ALTER TABLE ... DROP COLUMN`
5. **Failure** — Next insert fails (schema mismatch)
6. **Monitor** — Polls connector status every 5s, shows `FAILED` state
7. **🚨 Alert** — Queries Prometheus for `CDC_Connector_Down` alert
8. **Recovery** — Restores column, restarts connector, verifies health

**Expected output after breaking change:**
```
Connector Status:
  orders-cdc-connector         → FAILED
    task-0 → FAILED
      Error: "org.apache.kafka.connect.errors.DataException: total_amount is not a valid field name"

🚨 ALERT FIRING!
  Alert: CDC_Connector_Down
  Connector: orders-cdc-connector
  Severity: critical
  State: firing
```

**Grafana Alert Dashboard:** http://localhost:3000 → Alerting → Alert Rules  
**Prometheus Alerts:** http://localhost:9090/alerts

## Connector Verification

After `make up`, verify all 4 connectors are active:

```bash
# List connectors
curl http://localhost:8083/connectors
# Expected: ["orders-cdc-connector", "customers-cdc-connector", "orders-jdbc-sink", "customers-jdbc-sink"]

# Check source connector status
curl http://localhost:8083/connectors/orders-cdc-connector/status
curl http://localhost:8083/connectors/customers-cdc-connector/status

# Check sink connector status
curl http://localhost:8083/connectors/orders-jdbc-sink/status
curl http://localhost:8083/connectors/customers-jdbc-sink/status

# Verify JDBC plugin is loaded
curl http://localhost:8083/connector-plugins | grep JdbcSinkConnector
```

## DWH Verification

```bash
# List tables in raw schema
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c "\dt raw.*"

# Check CDC data
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c "SELECT * FROM raw.orders_cdc LIMIT 5;"
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c "SELECT * FROM raw.customers_cdc LIMIT 5;"
```

## dbt Project

Located in `dbt_datamesh/`:

```
models/
  bronze/          -- Raw CDC views
  silver/          -- Cleaned tables (typed, joined)
  gold/            -- Business aggregates
```

**Key tests** that catch breaking changes:
- `not_null(total_amount)` — FAILS after DROP COLUMN
- `accepted_values(status)` — Validates enums
- `relationships(customer_id)` — Referential integrity

```bash
cd dbt_datamesh
dbt run   # Build models
dbt test  # Run tests (catches schema breaks!)
```

## Schema Evolution Behavior

### Scenario 1: Add optional field
```sql
ALTER TABLE orders ADD COLUMN promo_code VARCHAR(50) DEFAULT NULL;
```
| Pipeline | Mode | Result |
|----------|------|--------|
| orders-to-analytics | opt-in | PROPAGATED |
| orders-to-reporting | opt-out | CONTINUED |

### Scenario 2: Remove consumed field
```sql
ALTER TABLE orders DROP COLUMN total_amount;
```
| Pipeline | Mode | Result |
|----------|------|--------|
| orders-to-analytics | opt-in | PROPAGATED |
| orders-to-reporting | opt-out | PAUSED + ALERT |

## Useful Commands

```bash
# List connectors
curl http://localhost:8083/connectors

# Check connector status
curl http://localhost:8083/connectors/orders-cdc-connector/status

# List Schema Registry subjects
curl http://localhost:8081/subjects

# Get latest schema
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions/latest

# List Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Read CDC messages (check offset)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 --topic orders-server.public.orders --time -1

# Consume messages from CLI
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-server.public.orders \
  --from-beginning

# Live data generation & CDC monitoring
python scripts/data_generator.py --mode batch --count 20
python scripts/data_generator.py --mode verify

# Breaking change demo with alerting
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
│   ├── Dockerfile                 # Debezium + JDBC Sink + JMX Exporter
│   └── jmx_exporter_config.yml    # JMX → Prometheus mapping
├── prometheus/                    # Monitoring configuration
│   ├── prometheus.yml             # Scrape configs (JMX, postgres-exporter)
│   └── alerts.yml                 # Alert rules (CDC_Connector_Down, etc.)
├── grafana/                       # Dashboards & provisioning
│   ├── dashboards/
│   │   └── datamesh-cdc.json      # Pre-built CDC monitoring dashboard
│   └── provisioning/
├── scripts/                       # Automation & init SQL
│   ├── setup-connectors.sh        # Registers source + sink connectors
│   ├── run_demo.py                # End-to-end demo
│   ├── data_generator.py          # Live CDC data generator
│   ├── breaking_change_demo.py    # Schema break + alert demo
│   ├── schema_evolution_simulator.py
│   ├── init_orders.sql            # Orders DB schema + seed data
│   └── init_customers.sql         # Customers DB schema + seed data
├── tests/                         # pytest
├── docs/                          # Documentation
├── docker-compose.yml             # Full stack
├── Makefile
└── pyproject.toml
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `make: *** No rule to make target 'docker'` | In `Makefile`, ensure the `up:` target has **no dependencies** after the colon. All shell commands must be indented with a **tab**, not spaces. |
| `relation "customers" does not exist` | Init SQL scripts must be mounted into PostgreSQL containers. Check `docker-compose.yml` volumes: `scripts/init_customers.sql:/docker-entrypoint-initdb.d/init.sql:ro` |
| `curl: (22) The requested URL returned error: 404` (JDBC) | Do not download JDBC connector as a single JAR. Use `confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6` in the Dockerfile. |
| `JdbcSinkConnector` not in connector-plugins | The JDBC plugin is missing from the Kafka Connect image. Rebuild with the updated `kafka-connect/Dockerfile`. |
| Prometheus not scraping Kafka Connect | Ensure JMX Exporter agent is in Dockerfile and `KAFKA_OPTS` points to it. Check port `7071` is exposed. |
| Grafana alert not firing | Verify Prometheus is datasource in Grafana. Check alert rule evaluation interval. Test with `curl localhost:9090/api/v1/alerts`. See [GRAFANA_SETUP.md](GRAFANA_SETUP.md). |
| `prometheus.yml is a directory` | `rm -rf prometheus/prometheus.yml` |
| MinIO port conflict | `docker rm -f quantum-sim-minio-1` |
| Kafka `CLUSTER_ID` invalid | Use base64 UUID: `ela4zpktSX2SmBDlE9FJjA==` |
| Connector `topic.prefix` required | Replace `database.server.name` (Debezium 2.x) |
| `SchemaType` import error | Use `Schema(schema_str, schema_type='AVRO')` |
| `CompatibilityLevel` not serializable | Add `default=lambda o: o.value` to json.dumps |

## When to Use This Approach

- Streaming microservices with typed consumers
- Financial/compliance data requiring strict audit
- Data Mesh with strong inter-domain contracts
- When downstream breakage is unacceptable

## License

MIT
