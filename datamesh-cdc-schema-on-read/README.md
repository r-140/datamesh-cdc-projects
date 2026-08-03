# Data Mesh CDC Platform — Schema-on-Read (Flexible)

> **Approach**: Bronze stores JSON/VARIANT. Schema is applied at read time in Silver/Gold. Pipeline never breaks.

## Architecture

```
PostgreSQL → Debezium CDC → Kafka (JSON) → Bronze (JSON payload)
→ Silver (explicit CAST views) → Gold (business aggregations)
```

## Key Features

- **Never-breaking pipeline** — new fields appear as NULL in Silver until explicitly added
- **Schema stored for audit** — Schema Registry tracks history, but doesn't block
- **Explicit schema at Silver** — business teams control which fields to expose
- **Easy exploration** — raw JSON available for ad-hoc analysis

## Quick Start

```bash
pip install -e ".[dev]"
make up
make simulate
```

## Schema Evolution Behavior

```sql
-- Source adds new field
ALTER TABLE orders ADD COLUMN promo_code VARCHAR(50);
```

| Layer | Behavior |
|-------|----------|
| Bronze | ✅ Appends as JSON — no schema change needed |
| Silver | `promo_code` becomes NULL until added to VIEW DDL |
| Gold | Unaffected until business decides to use the field |

## Documentation

- [Architecture](docs/architecture.md)
- [schema_evolution.py](docs/schema_evolution.md)
- [pipeline_manager.py](docs/pipeline_manager.md)
- [iceberg_sink.py](docs/iceberg_sink.md)
- [schema_evolution_service.py](docs/schema_evolution_service.md)
- [CI/CD](docs/ci-cd.md)

## When to Use

✅ Data warehouses (Snowflake, Databricks, BigQuery)  
✅ Exploratory analytics where schema changes frequently  
✅ When downstream consumers are SQL analysts, not microservices  
✅ Rapid prototyping  

## Related Projects

- [Schema-on-Write](../datamesh-cdc-schema-on-write) — Strict Avro compatibility
- [Hybrid](../datamesh-cdc-hybrid) — Best of both worlds

## License

MIT
