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
docker compose logs -f hybrid-consumer
```

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

Production systems would additionally use migrations instead of init SQL, an outbox/alert for schema fingerprints, retention/compaction, metrics, and a replay command for quarantined Bronze offsets.
