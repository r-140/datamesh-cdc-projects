# Architecture and Simulation

## 1. Overall Architecture

This project demonstrates a Data Mesh approach with CDC (Change Data Capture) and schema-on-write transformations.

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Source DB | PostgreSQL (Debezium) | Domain databases for orders and customers |
| CDC | Debezium + Kafka Connect | WAL change capture |
| Message Broker | Kafka (KRaft) | Event streaming |
| Schema Control | Confluent Schema Registry | Avro schemas, compatibility checks |
| DWH | PostgreSQL | Target database for dbt |
| Transformations | dbt (dbt-postgres) | Bronze/Silver/Gold layers |
| Monitoring | Prometheus + Grafana | Metrics and dashboards |

### Data Flow

```
App → postgres-orders → WAL → Debezium → Kafka (Avro) → Schema Registry
                                                          ↓
                                                    postgres-dwh (seed)
                                                          ↓
                                               dbt: bronze → silver → gold
```

## 2. Data Layers

### Bronze (Views)
- `raw_bronze.brz_orders` — view over `raw.orders`
- `raw_bronze.brz_customers` — view over `raw.customers`

### Silver (Tables)
- `raw_silver.slv_orders` — cleaned orders
- `raw_silver.slv_customers` — cleaned customers

### Gold (Aggregates)
- `raw_gold.fct_daily_revenue` — daily revenue
- `raw_gold.dim_customer_segments` — customer segments

## 3. Schema Evolution

### Three-Level Protection

1. **Schema Registry (hard)**
   - BACKWARD mode by default
   - Breaking changes → HTTP 409

2. **Debezium (soft)**
   - ExtractNewRecordState fills null on DROP COLUMN
   - Connector does not crash

3. **Pipeline Manager (business)**
   - Opt-in / Opt-out modes
   - PAUSED / PROPAGATED / CONTINUED

### Simulation Scenarios

| Scenario | Description | Result |
|----------|-------------|--------|
| A | Type change (double→string) | PAUSED |
| B | Field rename | PAUSED |
| C | Add required field | PAUSED |
| D | Add optional field | PROPAGATED/CONTINUED |
| E | Nested record | PAUSED |
| F | Enum instead of string | PAUSED |
| G | Field deletion (multi-pipeline) | Depends on consumed fields |
| H | FULL compatibility | PAUSED |

## 4. Running the Simulation

```bash
# 1. Infrastructure
make up

# 2. CDC connectors
./scripts/setup-connectors.sh

# 3. End-to-end demo
python scripts/run_demo.py

# 4. Schema evolution simulator
python scripts/schema_evolution_simulator.py

# 5. DBT
make dbt-setup
make dbt-run
make dbt-test
```

## 5. Checking Results

```bash
# DWH
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c "SELECT * FROM raw_gold.fct_daily_revenue;"

# Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Schema versions
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions
```

## 6. Repository Structure

```
.
├── docker-compose.yml          # Infrastructure
├── Makefile                    # Commands
├── pyproject.toml              # Dependencies
├── README.md                   # Quick start
├── .gitignore
├── kafka-connect/
│   └── Dockerfile              # Debezium Connect
├── postgres-dwh/
│   └── init.sql                # Seed data
├── scripts/
│   ├── setup-connectors.sh     # CDC registration
│   ├── run_demo.py             # End-to-end demo
│   └── schema_evolution_simulator.py
├── dbt_datamesh/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── sources.yml
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       └── schema.yml
├── docs/                       # Documentation
├── grafana/
│   └── provisioning/
└── prometheus/
```
