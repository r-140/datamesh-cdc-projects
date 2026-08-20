# dbt as a Consumer of the Hybrid CDC Pipeline

## Why dbt Matters Here

dbt is not merely an optional reporting layer. It represents a real downstream consumer and makes the behavioral difference among the three schema strategies visible.

| Strategy | What dbt reads | What happens after a breaking source change |
| --- | --- | --- |
| Schema-on-read | Raw JSONB | dbt performs typing; models or tests fail when required fields disappear or cannot be cast. |
| Schema-on-write | Strict typed landing tables | The sink or registry normally rejects the record before dbt sees it; dbt remains stable but may operate on stale/incomplete data. |
| Hybrid | Governed typed Silver plus governance metadata | Business models remain stable, while dbt governance models expose the quarantined record and whether a later event recovered it. |

The hybrid design therefore avoids making every business model parse JSON, but it does not hide rejected records from downstream observability.

## DAG

```text
silver.orders --------> stg_orders --------> order_status_summary
silver.customers -----> stg_customers -----> customer_country_summary
                              |                       
governance failures +---------+-----> projection_failures
governance schemas ----------------> schema_observations
```

## Models

### Staging

- `stg_orders` exposes the stable, typed orders contract and Bronze lineage coordinates.
- `stg_customers` does the same for customers.

Unlike schema-on-read, these models do not cast JSON fields. That work already happened at the controlled Silver boundary.

### Gold

- `order_status_summary` calculates current order counts, revenue and average order value by status.
- `customer_country_summary` calculates current customer counts by country.

Gold never receives an incompatible event because only successfully projected Silver records enter its DAG.

### Governance

- `schema_observations` shows every field-set fingerprint seen in Bronze.
- `projection_failures` shows rejected Silver projections and calculates `resolved_by_later_event` by checking whether the entity was subsequently promoted at a higher Kafka offset.

The singular test `no_unresolved_projection_failures` has warning severity. A quarantined event is operationally important but should not prevent unrelated, valid Silver data from producing Gold models. A production team may raise this to an error for critical domains.

## Run

```bash
make dbt-debug
make dbt-run
make dbt-test
```

Or run the full DAG and all tests:

```bash
make dbt-build
```

Generate the lineage site with:

```bash
make dbt-docs
```

## Behavior During the Automated Demo

`make demo` invokes dbt at the important boundaries:

1. Baseline: the full dbt build succeeds and Gold contains valid Silver data.
2. Breaking event: the governance model refreshes and the unresolved-projection test warns; business models remain usable.
3. Recovery: a later valid CDC event promotes the entity, the governance model marks the historical failure resolved, and the warning clears.

## Adding a Field to Gold

Adding `promo_code` to the source does not make it available to dbt business models automatically. First promote it through the Silver contract and database migration. Then:

1. add it to `stg_orders`;
2. add schema tests;
3. update the relevant Gold model;
4. run `make dbt-build`;
5. decide whether historical Bronze data needs a backfill.

This ordering prevents dbt from depending on a source field before the governed contract guarantees it.
