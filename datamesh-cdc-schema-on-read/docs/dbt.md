# dbt Project

## Overview

The dbt project (`dbt_datamesh/`) transforms raw JSONB CDC data into structured bronze, silver, and gold layers.

## Project Structure

```
dbt_datamesh/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Connection profiles
├── models/
│   ├── bronze/              # Pass-through views of raw JSONB
│   │   ├── orders.sql
│   │   └── customers.sql
│   ├── silver/              # Typed extraction with CAST
│   │   ├── orders.sql
│   │   └── customers.sql
│   ├── gold/                # Business aggregates
│   │   ├── daily_orders.sql
│   │   └── customer_summary.sql
│   └── schema.yml           # Tests and documentation
└── seeds/                   # Static reference data (optional)
```

## Layer Philosophy

### Bronze — Raw JSONB Pass-through

Bronze models are transparent views over the raw CDC tables. They add no transformation, only expose the data.

```sql
-- models/bronze/orders.sql
SELECT
    id,
    payload,
    __op,
    __source_ts_ms,
    __kafka_partition,
    __kafka_offset,
    ingested_at
FROM raw.orders_cdc
```

**Purpose**: Provide a stable interface to raw data. If raw table names change, only bronze models need updating.

### Silver — Typed Extraction

Silver models extract typed columns from JSONB using explicit `CAST`. This is where schema is enforced.

```sql
-- models/silver/orders.sql
SELECT
    id,
    (payload->>'customer_id')::bigint as customer_id,
    (payload->>'total_amount')::numeric(12,2) as total_amount,
    payload->>'status' as status,
    to_timestamp((payload->>'created_at')::bigint / 1000000.0) as created_at,
    __op,
    __source_ts_ms,
    ingested_at
FROM {{ ref('orders') }}
```

**Purpose**: Apply schema at read time. Tests catch breaking changes (e.g., dropped columns become NULL and fail `not_null` tests).

### Gold — Business Aggregates

Gold models build business-level metrics on top of Silver.

```sql
-- models/gold/daily_orders.sql
SELECT
    DATE(created_at) as order_date,
    status,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM {{ ref('orders') }}
GROUP BY 1, 2
```

```sql
-- models/gold/customer_summary.sql
SELECT
    c.id,
    c.email,
    c.name,
    COUNT(o.id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_spent,
    MAX(o.created_at) as last_order_date
FROM {{ ref('customers') }} c
LEFT JOIN {{ ref('orders') }} o ON o.customer_id = c.id
GROUP BY 1, 2, 3
```

## Schema Tests

`models/schema.yml` defines tests that catch breaking schema changes:

```yaml
version: 2

models:
  - name: orders
    columns:
      - name: id
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('customers')
              field: id
      - name: total_amount
        tests:
          - not_null
      - name: status
        tests:
          - not_null
          - accepted_values:
              values: ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']

  - name: customers
    columns:
      - name: id
        tests:
          - not_null
          - unique
      - name: email
        tests:
          - not_null
```

## How Tests Catch Breaking Changes

### Scenario: DROP COLUMN total_amount

1. Source: `ALTER TABLE orders DROP COLUMN total_amount;`
2. CDC Consumer: Continues running, writes JSONB without `total_amount` key
3. Bronze: No change (pass-through)
4. Silver: `(payload->>'total_amount')::numeric` → `NULL`
5. **dbt test**: `not_null(total_amount)` → **FAILS** ✓

This is the Schema-on-Read guarantee: pipeline never breaks, but quality is enforced downstream.

## Running dbt

```bash
# Setup (install deps, create profiles)
make dbt-setup

# Build all models
cd dbt_datamesh && dbt run

# Run tests
cd dbt_datamesh && dbt test

# Build + test
cd dbt_datamesh && dbt build

# Full refresh (rebuild incremental models)
cd dbt_datamesh && dbt run --full-refresh
```

## Profiles

```yaml
# dbt_datamesh/profiles.yml
datamesh:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5434
      user: dwh
      password: dwh
      dbname: datamesh_dwh
      schema: raw
      threads: 4
    ci:
      type: postgres
      host: localhost
      port: 5434
      user: dwh
      password: dwh
      dbname: datamesh_dwh
      schema: raw_ci
      threads: 4
```

## Materialization Strategy

| Layer | Materialization | Reason |
|-------|----------------|--------|
| Bronze | `view` | Lightweight pass-through, always current |
| Silver | `table` | Computed once, fast queries |
| Gold | `table` | Aggregations, refreshed on schedule |

Configure in `dbt_project.yml`:
```yaml
models:
  dbt_datamesh:
    bronze:
      +materialized: view
    silver:
      +materialized: table
    gold:
      +materialized: table
```

## Adding a New Field

1. **Source adds column**: `ALTER TABLE orders ADD COLUMN promo_code VARCHAR(50);`
2. **CDC Consumer**: Automatically includes new field in JSONB ✓
3. **Bronze**: No changes needed ✓
4. **Silver**: Add extraction:
   ```sql
   payload->>'promo_code' as promo_code
   ```
5. **Gold**: Use new field in aggregations if needed
6. **Tests**: Add test for new field:
   ```yaml
   - name: promo_code
     tests:
       - not_null  # optional
   ```

## dbt Docs

Generate and serve documentation:

```bash
cd dbt_datamesh
dbt docs generate
dbt docs serve
```

Open http://localhost:8080 to browse the data lineage.
