# Data Mesh CDC Platform — Hybrid (Best of Both)

> **Approach**: Bronze stores JSON (never breaks) + Schema Registry validates Silver DDL in CI before deployment.

## Architecture

```
PostgreSQL → Debezium CDC → Kafka (JSON) → Bronze (JSON payload)
→ Schema Registry (audit + detect changes)
→ CI validates Silver DDL against source schema
→ Silver (explicit CAST views) → Gold (business aggregations)
```

## Key Features

- **Never-breaking Bronze** — JSON payload, pipeline never stops
- **Schema audit** — Schema Registry tracks all changes for lineage
- **CI validation** — Silver SQL files validated against source schema before merge
- **Controlled evolution** — business teams decide when to add fields to Silver
- **Self-serve validation** — domain teams can validate their Silver DDL via API

## Quick Start

```bash
pip install -e ".[dev]"
make up
make simulate
```

## Validate Silver DDL

```bash
# Validate all SQL files in sql/silver/ against source schemas
python -m src.datamesh_cdc.schema_evolution_service --validate-silver sql/silver/

# Or specific directory
python -m src.datamesh_cdc.schema_evolution_service --validate-silver sql/silver/orders.sql
```

## Schema Evolution Behavior

```sql
-- Source adds new field
ALTER TABLE orders ADD COLUMN promo_code VARCHAR(50);
```

| Layer | Behavior |
|-------|----------|
| Bronze | ✅ Appends as JSON — no schema change needed |
| Schema Registry | 📝 Detects and logs change, notifies owner |
| CI (Silver validation) | ⚠️ Fails if `promo_code` added to Silver SQL but not in source |
| Silver | `promo_code` exposed only after explicit DDL update + CI pass |
| Gold | Unaffected until business decides |

## Documentation

- [Architecture](docs/architecture.md)
- [schema_evolution.py](docs/schema_evolution.md)
- [pipeline_manager.py](docs/pipeline_manager.md)
- [iceberg_sink.py](docs/iceberg_sink.md)
- [schema_evolution_service.py](docs/schema_evolution_service.md)
- [CI/CD](docs/ci-cd.md)

## When to Use

✅ Data Mesh with multiple domain teams  
✅ When you need flexibility + governance  
✅ Data warehouses with CI/CD for SQL views  
✅ When Silver layer is maintained by different team than source  

## Related Projects

- [Schema-on-Write](../datamesh-cdc-schema-on-write) — Strict Avro compatibility
- [Schema-on-Read](../datamesh-cdc-schema-on-read) — Fully flexible JSON

## License

MIT
