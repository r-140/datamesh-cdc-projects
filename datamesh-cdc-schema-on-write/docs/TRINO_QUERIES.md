# Trino Queries for Iceberg Data Mesh

## Connection

```bash
docker exec -it trino trino --server localhost:8080 --catalog iceberg --schema raw
```

Or via JDBC: `jdbc:trino://localhost:8080/iceberg/raw`

---

## Bronze: Raw CDC Data

### Explore tables
```sql
SHOW TABLES FROM iceberg.raw;
DESCRIBE iceberg.raw.orders;
```

### Read raw orders
```sql
SELECT 
    id,
    customer_id,
    total_amount,
    status,
    promo_code,
    created_at
FROM iceberg.raw.orders
ORDER BY created_at DESC
LIMIT 10;
```

### Read raw customers
```sql
SELECT 
    id,
    email,
    full_name,
    country,
    created_at
FROM iceberg.raw.customers
LIMIT 10;
```

### Check for nulls (data quality)
```sql
SELECT 
    COUNT(*) FILTER (WHERE total_amount IS NULL) AS null_amount_count,
    COUNT(*) AS total_count,
    ROUND(100.0 * null_amount_count / total_count, 2) AS null_pct
FROM iceberg.raw.orders;
```

---

## Silver: Cleaned & Enriched

### Cleaned orders with customer data
```sql
WITH cleaned_orders AS (
    SELECT
        id AS order_id,
        customer_id,
        CAST(total_amount AS DECIMAL(10,2)) AS total_amount,
        status,
        promo_code,
        CASE WHEN promo_code IS NOT NULL THEN TRUE ELSE FALSE END AS has_promo,
        created_at
    FROM iceberg.raw.orders
    WHERE total_amount IS NOT NULL
)
SELECT
    o.*,
    c.email AS customer_email,
    c.country AS customer_country
FROM cleaned_orders o
LEFT JOIN iceberg.raw.customers c
    ON o.customer_id = c.id
ORDER BY o.created_at DESC
LIMIT 20;
```

### Orders by status
```sql
SELECT 
    status,
    COUNT(*) AS order_count,
    SUM(CAST(total_amount AS DECIMAL(10,2))) AS total_revenue
FROM iceberg.raw.orders
GROUP BY status
ORDER BY total_revenue DESC;
```

### Promo code analysis
```sql
SELECT 
    promo_code,
    COUNT(*) AS usage_count,
    SUM(CAST(total_amount AS DECIMAL(10,2))) AS revenue,
    AVG(CAST(total_amount AS DECIMAL(10,2))) AS avg_order
FROM iceberg.raw.orders
WHERE promo_code IS NOT NULL
GROUP BY promo_code
ORDER BY usage_count DESC;
```

---

## Gold: Business Aggregates

### Daily revenue
```sql
SELECT
    DATE(created_at) AS order_date,
    COUNT(*) AS order_count,
    SUM(CAST(total_amount AS DECIMAL(10,2))) AS total_revenue,
    AVG(CAST(total_amount AS DECIMAL(10,2))) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM iceberg.raw.orders
GROUP BY DATE(created_at)
ORDER BY order_date DESC;
```

### Revenue by country
```sql
SELECT
    c.country,
    COUNT(o.id) AS order_count,
    SUM(CAST(o.total_amount AS DECIMAL(10,2))) AS total_revenue,
    AVG(CAST(o.total_amount AS DECIMAL(10,2))) AS avg_order_value
FROM iceberg.raw.orders o
JOIN iceberg.raw.customers c ON o.customer_id = c.id
GROUP BY c.country
ORDER BY total_revenue DESC;
```

### Customer lifetime value (CLV)
```sql
SELECT
    c.id AS customer_id,
    c.email,
    c.country,
    COUNT(o.id) AS total_orders,
    SUM(CAST(o.total_amount AS DECIMAL(10,2))) AS lifetime_value,
    AVG(CAST(o.total_amount AS DECIMAL(10,2))) AS avg_order_value,
    MAX(o.created_at) AS last_order_date,
    CASE
        WHEN SUM(CAST(o.total_amount AS DECIMAL(10,2))) > 500 THEN 'VIP'
        WHEN SUM(CAST(o.total_amount AS DECIMAL(10,2))) > 100 THEN 'Regular'
        ELSE 'New'
    END AS segment
FROM iceberg.raw.customers c
LEFT JOIN iceberg.raw.orders o ON c.id = o.customer_id
GROUP BY c.id, c.email, c.country
ORDER BY lifetime_value DESC
LIMIT 20;
```

### Detect schema-breaking changes (null spike)
```sql
-- This query detects when a column suddenly becomes NULL
-- (e.g. after ALTER TABLE DROP COLUMN + Debezium null-filling)
SELECT 
    DATE(created_at) AS dt,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE total_amount IS NULL) AS null_records,
    ROUND(100.0 * null_records / total_records, 2) AS null_pct
FROM iceberg.raw.orders
GROUP BY DATE(created_at)
ORDER BY dt DESC
LIMIT 7;
```

---

## Time Travel (Iceberg feature)

```sql
-- Query data as of 1 hour ago
SELECT * FROM iceberg.raw.orders
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-08-04 15:00:00';

-- Query specific snapshot
SELECT * FROM iceberg.raw.orders
FOR SYSTEM_VERSION AS OF 1234567890;
```

---

## Schema Evolution Detection

```sql
-- Compare today's schema vs yesterday's data
-- (Useful for detecting breaking changes)
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'raw' AND table_name = 'orders'
ORDER BY ordinal_position;
```
