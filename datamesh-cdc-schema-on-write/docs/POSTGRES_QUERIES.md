# Useful Queries (PostgreSQL DWH)

## Connection

```bash
# DWH
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh

# Source databases
docker exec -it postgres-orders psql -U postgres -d orders_db
docker exec -it postgres-customers psql -U postgres -d customers_db
```

## Check DWH Schema

```sql
-- List schemas
\dn

-- Tables in raw
\dt raw.*

-- Tables in bronze
\dt raw_bronze.*

-- Tables in silver
\dt raw_silver.*

-- Tables in gold
\dt raw_gold.*
```

## Seed Data

```sql
SELECT * FROM raw.orders;
SELECT * FROM raw.customers;
```

## Bronze Layer

```sql
-- Views over source
SELECT * FROM raw_bronze.brz_orders LIMIT 5;
SELECT * FROM raw_bronze.brz_customers LIMIT 5;
```

## Silver Layer

```sql
-- Cleaned data
SELECT 
    order_id,
    customer_id,
    status,
    total_amount,
    order_date
FROM raw_silver.slv_orders
WHERE total_amount > 0;
```

## Gold Layer

```sql
-- Daily revenue
SELECT * FROM raw_gold.fct_daily_revenue ORDER BY order_date;

-- Customer segments
SELECT * FROM raw_gold.dim_customer_segments;
```

## CDC Check (source)

```sql
-- WAL level
SHOW wal_level;  -- should be 'logical'

-- Publications list
SELECT * FROM pg_publication;

-- Replication slots
SELECT * FROM pg_replication_slots;
```

## Kafka Connect

```bash
# Connector status
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/orders-cdc-connector/status
```

## Schema Registry

```bash
# Subjects
curl http://localhost:8081/subjects

# Schema versions
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions

# Specific version
curl http://localhost:8081/subjects/orders-server.public.orders-value/versions/1
```
