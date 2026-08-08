# Data Mesh CDC Platform -- Schema-on-Write (Strict)

[![CI/CD](https://github.com/yourusername/datamesh-cdc-schema-on-write/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/yourusername/datamesh-cdc-schema-on-write/actions)
[![codecov](https://codecov.io/gh/yourusername/datamesh-cdc-schema-on-write/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/datamesh-cdc-schema-on-write)

> **Approach**: Schema-on-Write with strict Avro compatibility. Breaking changes **pause** the pipeline.

## Architecture

```
PostgreSQL (Domain) 
    -> Debezium CDC 
    -> Kafka + Schema Registry (Avro, BACKWARD)
    -> Schema Evolution Service (validates & pauses)
    -> Iceberg Sink (typed tables)
    -> Trino
    -> dbt (bronze/silver/gold)
```

## Quick Start

```bash
# 1. Install
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"

# 2. Start all infrastructure
make up

# 3. Register Debezium connectors
./scripts/setup-connectors.sh

# 4. Run end-to-end demo
python scripts/run_demo.py

# 5. Run schema evolution simulation
python scripts/schema_evolution_simulator.py

# 6. Build dbt models
make dbt-setup
make dbt-run
make dbt-test
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka Connect REST API | http://localhost:8083 | -- |
| Schema Registry | http://localhost:8081 | -- |
| Kafka (bootstrap) | `localhost:9092` | -- |
| MinIO Console | http://localhost:9001 | `minio` / `minio123` |
| MinIO S3 API | http://localhost:9000 | `minio` / `minio123` |
| PostgreSQL Customers | `localhost:5432` | `postgres` / `postgres` |
| PostgreSQL Orders | `localhost:5433` | `postgres` / `postgres` |
| Prometheus | http://localhost:9090 | -- |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Trino | http://localhost:8080 | -- |
| Iceberg REST Catalog | http://localhost:8181 | -- |

## Trino Queries

See [TRINO_QUERIES.md](TRINO_QUERIES.md) for complete SQL reference:
- Bronze: raw CDC exploration
- Silver: cleaned & enriched data
- Gold: business aggregates (revenue, CLV, segments)
- Time travel queries
- Schema evolution detection

Quick example:
```bash
docker exec -it trino trino --server localhost:8080 --catalog iceberg --schema raw
```

```sql
-- Daily revenue
SELECT
    DATE(created_at) AS order_date,
    COUNT(*) AS orders,
    SUM(CAST(total_amount AS DECIMAL(10,2))) AS revenue
FROM iceberg.raw.orders
GROUP BY DATE(created_at);
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
- `not_null(total_amount)` -- FAILS after DROP COLUMN
- `accepted_values(status)` -- Validates enums
- `relationships(customer_id)` -- Referential integrity

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
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell   --broker-list localhost:29092 --topic orders-server.public.orders --time -1

# Trino query
docker exec -it trino trino --catalog iceberg --schema raw   --execute "SELECT * FROM orders LIMIT 5"
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
├── tests/                         # pytest
├── scripts/                       # Automation
│   ├── setup-connectors.sh
│   ├── run_demo.py
│   └── schema_evolution_simulator.py
├── docs/                          # Documentation
├── TRINO_QUERIES.md               # SQL reference
├── docker-compose.yml             # Full stack
├── Makefile
└── pyproject.toml
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
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
