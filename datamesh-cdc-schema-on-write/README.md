# Data Mesh CDC Platform — Schema-on-Write (Strict)

[![CI/CD](https://github.com/yourusername/datamesh-cdc-schema-on-write/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/yourusername/datamesh-cdc-schema-on-write/actions)
[![codecov](https://codecov.io/gh/yourusername/datamesh-cdc-schema-on-write/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/datamesh-cdc-schema-on-write)

> **Approach**: Schema-on-Write with strict Avro compatibility. Breaking changes **pause** the pipeline.

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

# 5. Run schema evolution simulation
make simulate

# 6. Build dbt models
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

## Data Flow

```
postgres-orders     -> Debezium Source -> Kafka (Avro) -> JDBC Sink -> postgres-dwh.raw.orders_cdc
postgres-customers  -> Debezium Source -> Kafka (Avro) -> JDBC Sink -> postgres-dwh.raw.customers_cdc
```

- **Source connectors** read PostgreSQL WAL via logical replication (`pgoutput`)
- **Sink connectors** read Kafka topics and write to DWH via `upsert`
- `auto.evolve=true` — on schema evolution (ADD COLUMN) the sink automatically adds the column to DWH
- `auto.create=true` — tables in `raw.*` are created automatically if they don't exist

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
│   └── Dockerfile                 # Debezium + JDBC Sink plugins
├── scripts/                       # Automation & init SQL
│   ├── setup-connectors.sh        # Registers source + sink connectors
│   ├── run_demo.py                # End-to-end demo
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
