# Automated Hybrid Demo Guide

## Goal

The demo shows why the hybrid approach is different from both extremes: raw acquisition continues across schema changes, while incompatible records do not silently enter typed consumer tables.

## Prerequisites

- Docker with the Compose plugin;
- Python 3.10 or newer;
- project dependencies installed with `python -m pip install -e '.[dev]'`;
- `dbt-postgres`, included in the development dependencies;
- ports 5432, 5433, 5434, 8081, 8083 and 9092 available.

## Run the Demo

```bash
make up
make status
make demo
```

## Scenario 1: Baseline

The script inserts a valid order. It waits until the same event can be found in `bronze.cdc_events` and `silver.orders`.

Expected result: both checks pass.

The script then runs the complete dbt DAG. Staging and Gold models consume the valid Silver row.

## Scenario 2: Additive Evolution

The script adds `promo_code`, inserts an order using it, and checks that:

- Bronze JSONB contains `promo_code`;
- Silver still promotes the fields in its existing contract;
- governance observes the new field set;
- Silver does not expose the new field automatically.

## Scenario 3: Breaking Evolution

The script drops required `total_amount` and inserts an order without it.

Expected result:

- the event appears in Bronze;
- it does not appear in Silver;
- `governance.projection_failures` records `total_amount: missing`;
- the consumer and CDC pipeline continue running.

The script refreshes the dbt governance model. `no_unresolved_projection_failures` emits a warning, while the Gold models remain usable because they consume stable Silver rather than raw JSON.

## Scenario 4: Recovery

The script restores `total_amount` and updates the broken source record with a valid value. The corrective CDC event then upserts the row into Silver.

After the governance model is refreshed, the earlier failure is marked `resolved_by_later_event=true` and the dbt warning clears.

The dropped column is restored in a `finally` block even if the script fails. Demo rows and governance history remain for inspection.

## Inspect Results

```bash
psql postgresql://dwh:dwh@localhost:5434/datamesh_dwh
```

```sql
SELECT topic, kafka_offset, payload
FROM bronze.cdc_events
WHERE source_table = 'orders'
ORDER BY ingested_at DESC;

SELECT * FROM silver.orders ORDER BY updated_at DESC;

SELECT source_table, fields, event_count, last_seen_at
FROM governance.observed_schemas
ORDER BY last_seen_at DESC;

SELECT payload, error, failed_at
FROM governance.projection_failures
ORDER BY failed_at DESC;
```

## Repeat or Reset

The demo can be run repeatedly. It creates new records and keeps the audit history.

For a clean environment:

```bash
make reset
make up
make demo
```

`make reset` deletes the project volumes and all demo data.
