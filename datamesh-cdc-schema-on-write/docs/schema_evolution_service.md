# schema_evolution_service.py

## Purpose

Main entry point for the schema evolution service. Runs as a background daemon that monitors schema changes and applies strict validation.

## Modes

### Production Mode (`main()`)

- Polls Schema Registry for new schema versions
- Checks all registered pipelines
- Applies opt-in/opt-out logic
- Propagates or pauses pipelines

### Simulation Mode (`--simulate`)

Command-line demo showing two scenarios:
1. **Adding optional field** — compatible, propagated
2. **Removing consumed field** — breaking, pauses opt-out pipeline

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry endpoint |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `ICEBERG_REST_URL` | `http://localhost:8181` | Iceberg REST catalog |
| `S3_ENDPOINT` | `http://localhost:9000` | MinIO/S3 endpoint |
| `POLL_INTERVAL_SECONDS` | `30` | Schema check interval |
| `STATE_FILE` | `/tmp/datamesh_state.json` | Pipeline state file |

## Usage

```bash
# Production mode
python -m src.datamesh_cdc.schema_evolution_service

# Simulation mode
python -m src.datamesh_cdc.schema_evolution_service --simulate
```

## Demo Pipelines

The service creates three demo pipelines on startup:

| Pipeline | Domain | Mode | Consumed Fields |
|----------|--------|------|-----------------|
| `orders-to-analytics` | orders | opt-in | all |
| `orders-to-reporting` | orders | opt-out | `id`, `customer_id`, `total_amount`, `status` |
| `customers-to-analytics` | customers | opt-in | all |
