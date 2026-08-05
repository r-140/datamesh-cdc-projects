# dbt Data Mesh Project

Simple dbt project for the CDC Data Mesh pipeline.

## Structure

```
models/
  bronze/          -- Raw CDC data (views)
  silver/          -- Cleaned, typed, joined (tables)
  gold/            -- Business aggregates (tables)
```

## Setup

```bash
cd dbt_datamesh
pip install dbt-trino
dbt deps
dbt seed  # if any seeds
dbt run   -- Build all models
dbt test  -- Run tests
```

## Key Tests

| Test | Purpose |
|------|---------|
| `not_null` | Detects schema-breaking nulls (e.g. dropped column) |
| `accepted_values` | Validates status enums |
| `relationships` | Referential integrity |
| `expression_is_true` | Business rules (amount > 0) |

## Running after schema change

```bash
# After ALTER TABLE ADD COLUMN
dbt run   # New field appears in bronze automatically
dbt test  # Tests still pass if field is nullable

# After ALTER TABLE DROP COLUMN (breaking)
dbt test  # FAILS on not_null(total_amount)
```
