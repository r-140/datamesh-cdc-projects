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

## Key Features

- **Strict Schema Enforcement** — Schema Registry rejects incompatible schemas
- **Pipeline Pause on Breaking Change** — opt-out pipelines protect consumed fields
- **Opt-in / Opt-out Modes** — domain teams choose their evolution strategy
- **Typed Iceberg Tables** — explicit schema derived from Avro
- **Self-serve API** — domain teams manage their own pipelines
- **Full CI/CD** — lint, test, schema compat, integration tests, Docker build, security scan

## Quick Start

```bash
# Install
python3 -m venv venv
source venv/bin/activate 
pip install -e ".[dev]"

# Start infrastructure
make up

# Run simulation
make simulate
```

## Documentation

- [Architecture](docs/architecture.md)
- [schema_evolution.py](docs/schema_evolution.md) — compatibility checks, opt-in/opt-out
- [pipeline_manager.py](docs/pipeline_manager.md) — pipeline lifecycle & self-serve API
- [iceberg_sink.py](docs/iceberg_sink.md) — typed Iceberg tables with DDL evolution
- [schema_evolution_service.py](docs/schema_evolution_service.md) — main service & simulation
- [CI/CD](docs/ci-cd.md) — GitHub Actions workflow

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

## Project Structure

```
.
├── src/datamesh_cdc/
│   ├── schema_evolution.py           # Strict compatibility & opt-in/opt-out
│   ├── pipeline_manager.py           # Pipeline registry & self-serve API
│   ├── iceberg_sink.py               # Typed Iceberg sink
│   └── schema_evolution_service.py   # Main daemon
├── tests/                            # pytest suite
├── docs/                             # Per-file documentation
├── debezium/connectors/              # Debezium JSON configs
├── .github/workflows/ci-cd.yml       # Full CI/CD
├── docker-compose.yml                # Full stack
├── Dockerfile
├── Makefile
└── pyproject.toml
```

## When to Use This Approach

✅ Streaming microservices with typed consumers  
✅ Financial/compliance data requiring strict audit  
✅ Data Mesh with strong inter-domain contracts  
✅ When downstream breakage is unacceptable  

## Related Projects

- [Schema-on-Read](../datamesh-cdc-schema-on-read) — Flexible JSON/VARIANT approach
- [Hybrid](../datamesh-cdc-hybrid) — Best of both worlds

## License

MIT
