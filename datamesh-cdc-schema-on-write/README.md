# Data Mesh CDC Platform — Schema-on-Write (Strict)

[![CI/CD](https://github.com/yourusername/datamesh-cdc-schema-on-write/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/yourusername/datamesh-cdc-schema-on-write/actions)
[![codecov](https://codecov.io/gh/yourusername/datamesh-cdc-schema-on-write/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/datamesh-cdc-schema-on-write)

> **Approach**: Schema-on-Write with strict Avro compatibility. Breaking changes **pause** the pipeline.

## Architecture

```
PostgreSQL (Domain) 
    → Debezium CDC 
    → Kafka + Schema Registry (Avro, BACKWARD)
    → Schema Evolution Service (validates & pauses)
    → Iceberg Sink (typed tables)
    → Trino
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

# 4. Run schema evolution simulation
make simulate
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka Connect REST API | http://localhost:8083 | — |
| Schema Registry | http://localhost:8081 | — |
| Kafka (bootstrap) | `localhost:9092` | — |
| MinIO Console | http://localhost:9001 | `minio` / `minio123` |
| MinIO S3 API | http://localhost:9000 | `minio` / `minio123` |
| PostgreSQL Customers | `localhost:5432` | `postgres` / `postgres` |
| PostgreSQL Orders | `localhost:5433` | `postgres` / `postgres` |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Trino | http://localhost:8080 | — |
| Iceberg REST Catalog | http://localhost:8181 | — |

## Useful Commands

```bash
# List connectors
curl http://localhost:8083/connectors

# Check connector status
curl http://localhost:8083/connectors/customers-cdc-connector/status
curl http://localhost:8083/connectors/orders-cdc-connector/status

# List Schema Registry subjects
curl http://localhost:8081/subjects

# Get latest schema
curl http://localhost:8081/subjects/customers-server.public.customers-value/versions/latest

# List Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Read CDC messages (Avro) — customers
docker exec kafka kafka-avro-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic customers-server.public.customers \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081

# Read CDC messages (Avro) — orders
docker exec kafka kafka-avro-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-server.public.orders \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081
```

## Schema Evolution Behavior

### Scenario 1: Add optional field
```sql
ALTER TABLE orders ADD COLUMN promo_code VARCHAR(50) DEFAULT NULL;
```
| Pipeline | Mode | Result |
|----------|------|--------|
| orders-to-analytics | opt-in | ✅ PROPAGATED |
| orders-to-reporting | opt-out | ✅ CONTINUED (not in consumed_fields) |

### Scenario 2: Remove consumed field
```sql
ALTER TABLE orders DROP COLUMN total_amount;
```
| Pipeline | Mode | Result |
|----------|------|--------|
| orders-to-analytics | opt-in | ✅ PROPAGATED |
| orders-to-reporting | opt-out | ⏸️ PAUSED + ALERT |

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `prometheus.yml is a directory` | `rm -rf prometheus/prometheus.yml` and create a file |
| MinIO port conflict | `docker rm -f quantum-sim-minio-1` (or any old minio container) |
| Kafka `CLUSTER_ID` invalid | Use a valid base64 UUID, e.g. `ela4zpktSX2SmBDlE9FJjA==` |
| Kafka Connect `InvalidReplicationFactorException` | Set `CONNECT_*_STORAGE_REPLICATION_FACTOR: 1` and `*_PARTITIONS: 1` |
| Connector `topic.prefix` required | Replace `database.server.name` with `topic.prefix` (Debezium 2.x) |
| `SchemaType` import error | Use `Schema(schema_str, schema_type='AVRO')` instead of `SchemaType` enum |
| `CompatibilityLevel is not JSON serializable` | Add `default=lambda o: o.value if hasattr(o, 'value') else str(o)` to `json.dumps` in `pipeline_manager.py` |

## Project Structure

```
.
├── src/datamesh_cdc/
│   ├── schema_evolution.py           # Strict compatibility & opt-in/opt-out
│   ├── pipeline_manager.py           # Pipeline registry & self-serve API
│   ├── iceberg_sink.py               # Typed Iceberg sink
│   └── schema_evolution_service.py   # Main daemon & simulation
├── tests/                            # pytest suite
├── docs/                             # Per-file documentation
├── debezium/connectors/              # Debezium JSON configs
├── scripts/setup-connectors.sh       # Connector registration script
├── .github/workflows/ci-cd.yml       # Full CI/CD
├── docker-compose.yml                # Full stack
├── kafka-connect/Dockerfile          # Custom Connect with Debezium
├── Makefile
└── pyproject.toml
```

## When to Use This Approach

✅ Streaming microservices with typed consumers  
✅ Financial/compliance data requiring strict audit  
✅ Data Mesh with strong inter-domain contracts  
✅ When downstream breakage is unacceptable  

## License

MIT
