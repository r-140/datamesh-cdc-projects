# Data Verification — Medallion Architecture

Quick commands to inspect data at every layer of the CDC pipeline.

---

## 🥉 Bronze (Raw CDC)

Views directly over the JDBC Sink tables.

### Via Docker / psql

```bash
# Row counts
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT COUNT(*) FROM raw_raw_bronze.brz_orders;"
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT COUNT(*) FROM raw_raw_bronze.brz_customers;"

# Sample rows
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT * FROM raw_raw_bronze.brz_orders ORDER BY order_id DESC LIMIT 5;"
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT * FROM raw_raw_bronze.brz_customers ORDER BY customer_id DESC LIMIT 5;"
```

### Via dbt show

```bash
cd dbt_datamesh
dbt show --select brz_orders --limit 5
dbt show --select brz_customers --limit 5
```

**Expected:** same row count as `raw.orders_cdc` / `raw.customers_cdc`.

---

## 🥈 Silver (Cleaned & Typed)

Materialized tables with type casting, `__deleted` filter, and business rules.

### Via Docker / psql

```bash
# Row counts
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT COUNT(*) FROM raw_raw_silver.slv_orders;"
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT COUNT(*) FROM raw_raw_silver.slv_customers;"

# Typed columns & no soft-deletes
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT order_id, customer_id, total_amount, status, created_at \
   FROM raw_raw_silver.slv_orders ORDER BY order_id DESC LIMIT 5;"
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT customer_id, full_name, email, country, created_at \
   FROM raw_raw_silver.slv_customers ORDER BY customer_id DESC LIMIT 5;"
```

### Via dbt show

```bash
cd dbt_datamesh
dbt show --select slv_orders --limit 5
dbt show --select slv_customers --limit 5
```

**Expected:** same row count as bronze (unless deletes were filtered out).

---

## 🥇 Gold (Business Aggregates)

### Via Docker / psql

```bash
# Daily revenue
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT * FROM raw_raw_gold.fct_daily_revenue ORDER BY order_date DESC;"

# Customer segments by country
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT * FROM raw_raw_gold.dim_customer_segments ORDER BY customer_count DESC;"
```

### Via dbt show

```bash
cd dbt_datamesh
dbt show --select fct_daily_revenue
dbt show --select dim_customer_segments
```

**Expected:** `fct_daily_revenue` = 1 row per day with data; `dim_customer_segments` = N rows per unique country.

---

## 📊 One-Shot Row-Count Summary

```bash
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c "
SELECT 'bronze' AS layer, 'brz_orders' AS model, COUNT(*) AS rows FROM raw_raw_bronze.brz_orders
UNION ALL
SELECT 'bronze', 'brz_customers', COUNT(*) FROM raw_raw_bronze.brz_customers
UNION ALL
SELECT 'silver', 'slv_orders', COUNT(*) FROM raw_raw_silver.slv_orders
UNION ALL
SELECT 'silver', 'slv_customers', COUNT(*) FROM raw_raw_silver.slv_customers
UNION ALL
SELECT 'gold', 'fct_daily_revenue', COUNT(*) FROM raw_raw_gold.fct_daily_revenue
UNION ALL
SELECT 'gold', 'dim_customer_segments', COUNT(*) FROM raw_raw_gold.dim_customer_segments
ORDER BY layer, model;
"
```

**Expected output after `data_generator.py --mode batch --count 20`:**

```
 layer  |         model         | rows
--------+-----------------------+------
 bronze | brz_customers         |   43
 bronze | brz_orders            |   44
 gold   | dim_customer_segments |   10
 gold   | fct_daily_revenue     |    1
 silver | slv_customers         |   43
 silver | slv_orders            |   44
```

---

## 🔍 Raw CDC Tables (JDBC Sink target)

```bash
# Verify the sink actually landed data
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT COUNT(*) FROM raw.orders_cdc;"
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT COUNT(*) FROM raw.customers_cdc;"

# Peek at Debezium metadata columns
docker exec -it postgres-dwh psql -U dwh -d datamesh_dwh -c \
  "SELECT id, __deleted, created_at, updated_at FROM raw.orders_cdc ORDER BY id DESC LIMIT 3;"
```

---

## 🧪 dbt Tests

```bash
cd dbt_datamesh

# Run all tests
make dbt-test
# or
dbt test

# Run tests for a specific model
dbt test --select slv_orders

# Show test results in detail
dbt test --select slv_orders --store-failures
```

**Key tests that catch schema breaks:**
- `not_null(total_amount)` — fails after `DROP COLUMN total_amount`
- `accepted_values(status)` — validates enums
- `unique(order_id)` — ensures no duplicates
