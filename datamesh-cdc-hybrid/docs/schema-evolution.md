# Schema Evolution in the Hybrid Approach

## Two Independent Contracts

The pipeline has two schema boundaries:

1. The Avro wire schema determines whether Kafka records can be serialized and decoded.
2. The Silver projection contract determines whether a decoded record may enter a typed consumer table.

Bronze sits between them. Once an event is decoded, its payload is preserved as JSONB even if it cannot satisfy Silver.

## Change Behavior

| Source change | Bronze | Silver | Governance |
| --- | --- | --- | --- |
| Add optional field | Stores it immediately | Ignores it until explicitly promoted | New field-set fingerprint |
| Remove required field | Stores payload | Rejects projection | Failure with `field: missing` |
| Compatible representation, such as `"12"` for an integer | Stores original value | Coerces through the contract | Existing or new fingerprint |
| Incompatible type | Stores original value | Rejects projection | Failure with conversion reason |
| Delete | Stores CDC event | Deletes typed row | Shape remains auditable |

## Silver Contracts

Contracts are defined in `src/datamesh_cdc/hybrid_projection.py`. Each field has:

- a name;
- a conversion function;
- a required/optional policy.

Unknown fields are deliberately ignored by Silver but remain available in Bronze.

## Promoting a New Field

For example, to expose `orders.promo_code`:

1. Confirm the field and its representations in `bronze.cdc_events`.
2. Decide nullability, type and business meaning.
3. Add a PostgreSQL migration for `silver.orders`.
4. Add the field to the `orders` contract.
5. Extend the orders upsert in `consumer.py`.
6. Add projection and integration tests.
7. Backfill from Bronze if historical values are required.

Do not automatically add every observed field to Silver. The explicit promotion decision is the governed schema-on-write part of the design.

## Breaking-Change Recovery

The automated demo recovers through a corrective source update:

1. The incompatible event remains in Bronze and is recorded as a projection failure.
2. The producer restores the required field and updates the record.
3. Debezium emits a new event.
4. The new event satisfies the contract and upserts Silver.

A production implementation can also provide a Bronze replay worker. Replay must define how a missing or invalid business value is repaired; blindly replaying the same incompatible payload will fail again.

## Schema Registry Is Not the Silver Contract

Schema Registry protects serialization compatibility. It cannot decide whether a new source field belongs in an analytical product, whether it is semantically valid, or which PostgreSQL type and constraints Silver should expose. Those decisions belong to the Silver contract and its migration workflow.
