# Data Mesh CDC — Hybrid Schema Evolution

This project combines the useful guarantees of the sibling demos:

- **Schema-on-read at Bronze:** every decodable CDC event is stored unchanged as JSONB. Additive or breaking source changes do not stop ingestion.
- **Schema-on-write at Silver:** explicit Python contracts coerce and validate fields before writing typed, consumer-friendly tables.
- **A controlled failure boundary:** an invalid Silver projection is recorded in `governance.projection_failures`; the original event remains queryable and replayable in Bronze.
- **Schema discovery:** each observed field set is fingerprinted in `governance.observed_schemas` so unannounced evolution is visible even when it is harmless.
- **Idempotency and lineage:** Kafka topic/partition/offset is the Bronze key and is carried into Silver.

```text
PostgreSQL -> Debezium -> Kafka/Avro -> hybrid consumer
                                      |-> bronze.cdc_events (JSONB, lossless)
                                      |-> silver.* (typed contracts)
                                      `-> governance.* (schema audit/failures)
```

PostgreSQL is intentionally used for the warehouse. The demo concerns schema-evolution boundaries, not OLTP versus OLAP storage engines.

## Run

```bash
python -m pip install -e '.[dev]'
make test
make up
make demo
make dbt-build
```

`make demo` automatically demonstrates four stages:

1. A compatible event is written to both Bronze and Silver.
2. An additive `promo_code` field is preserved immediately in Bronze while the stable Silver contract continues working.
3. Dropping required `total_amount` does not stop Bronze; the typed Silver projection is quarantined in `governance.projection_failures`.
4. Restoring the field and issuing a corrective source update promotes the record to Silver without rebuilding the pipeline.

The script restores the dropped source column even when the demo fails midway. Demo records remain in the source and warehouse so that the results can be inspected afterward.

Useful queries:

```sql
SELECT * FROM bronze.cdc_events ORDER BY ingested_at DESC;
SELECT * FROM silver.orders ORDER BY id;
SELECT * FROM governance.observed_schemas ORDER BY last_seen_at DESC;
SELECT * FROM governance.projection_failures ORDER BY failed_at DESC;
```

Connect with `psql postgresql://dwh:dwh@localhost:5434/datamesh_dwh`.

## Evolution examples

An additive `orders.promo_code` column appears immediately in Bronze and creates a new observed-schema fingerprint. Silver stays stable until the `orders` contract, DWH migration, and upsert are deliberately changed.

If `customer_id` disappears or becomes non-numeric, Bronze still accepts the event. Its Silver projection is quarantined with a concrete reason. Once the contract or producer is fixed, the immutable Bronze event can be replayed.

This is the main advantage over either extreme: source evolution cannot silently corrupt typed consumer tables, but it also cannot destroy or block raw data acquisition.

## Where to change the contract

- `src/datamesh_cdc/hybrid_projection.py`: required fields and conversions.
- `scripts/init-dwh.sql`: typed Silver table DDL and governance tables.
- `src/datamesh_cdc/consumer.py`: transactional Bronze write, Silver upsert, deletes, and quarantine.
- `debezium/connectors/`: source capture configuration.

## Documentation

- [Architecture](docs/architecture.md)
- [Schema evolution](docs/schema-evolution.md)
- [Hybrid consumer](docs/consumer.md)
- [Automated demo guide](docs/demo-guide.md)
- [Data verification queries](docs/data-verification.md)
- [dbt as a hybrid CDC consumer](docs/dbt.md)
- [CI/CD](docs/ci-cd.md)
- [Troubleshooting](docs/troubleshooting.md)

Production systems would additionally use migrations instead of init SQL, an outbox/alert for schema fingerprints, retention/compaction, metrics, and a replay command for quarantined Bronze offsets.
