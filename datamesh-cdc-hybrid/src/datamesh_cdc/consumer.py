"""Transactional Kafka consumer for the hybrid Bronze/Silver pipeline."""
import hashlib
import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from .hybrid_projection import ProjectionError, project

logger = logging.getLogger(__name__)
TOPICS = {"orders-server.public.orders": "orders", "customers-server.public.customers": "customers"}

def json_default(value):
    if isinstance(value, (datetime, date)): return value.isoformat()
    if isinstance(value, Decimal): return str(value)
    if isinstance(value, bytes): return value.decode(errors="replace")
    raise TypeError(type(value).__name__)

def process_message(conn, *, topic, partition, offset, key, payload):
    """Persist Bronze first and project Silver in the same DB transaction."""
    table = TOPICS[topic]
    op = payload.get("__op", "u")
    payload_json = json.dumps(payload, default=json_default)
    fields = sorted(payload.keys())
    fingerprint = hashlib.sha256(json.dumps(fields).encode()).hexdigest()
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO bronze.cdc_events
          (topic,kafka_partition,kafka_offset,record_key,source_table,operation,payload,source_ts_ms)
          VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING""",
          (topic, partition, offset, json.dumps(key, default=json_default), table, op,
           payload_json, payload.get("__source_ts_ms")))
        if cur.rowcount == 0:
            conn.commit(); return "duplicate"
        cur.execute("""INSERT INTO governance.observed_schemas(source_table,fingerprint,fields)
          VALUES (%s,%s,%s::jsonb) ON CONFLICT(source_table,fingerprint) DO UPDATE
          SET last_seen_at=now(), event_count=governance.observed_schemas.event_count+1""",
          (table, fingerprint, json.dumps(fields)))
        try:
            row = project(table, payload)
            if op in ("d", "DELETE") or payload.get("__deleted") == "true":
                cur.execute(f"DELETE FROM silver.{table} WHERE id=%s", (row["id"],))
            elif table == "orders":
                cur.execute("""INSERT INTO silver.orders
                  (id,customer_id,total_amount,status,bronze_topic,bronze_partition,bronze_offset)
                  VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET
                  customer_id=EXCLUDED.customer_id,total_amount=EXCLUDED.total_amount,
                  status=EXCLUDED.status,bronze_topic=EXCLUDED.bronze_topic,
                  bronze_partition=EXCLUDED.bronze_partition,bronze_offset=EXCLUDED.bronze_offset,
                  updated_at=now()""", (*row.values(), topic, partition, offset))
            else:
                cur.execute("""INSERT INTO silver.customers
                  (id,email,full_name,country,bronze_topic,bronze_partition,bronze_offset)
                  VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET
                  email=EXCLUDED.email,full_name=EXCLUDED.full_name,country=EXCLUDED.country,
                  bronze_topic=EXCLUDED.bronze_topic,bronze_partition=EXCLUDED.bronze_partition,
                  bronze_offset=EXCLUDED.bronze_offset,updated_at=now()""",
                  (*row.values(), topic, partition, offset))
            outcome = "promoted"
        except ProjectionError as exc:
            cur.execute("""INSERT INTO governance.projection_failures
              (topic,kafka_partition,kafka_offset,source_table,payload,error)
              VALUES (%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT DO NOTHING""",
              (topic, partition, offset, table, payload_json, str(exc)))
            outcome = "bronze_only"
    conn.commit()
    return outcome

def main():
    logging.basicConfig(level=logging.INFO)
    registry = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")})
    deserialize = AvroDeserializer(registry)
    consumer = Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                         "group.id": "hybrid-cdc-consumer", "auto.offset.reset": "earliest",
                         "enable.auto.commit": False})
    consumer.subscribe(list(TOPICS))
    conn = psycopg2.connect(os.getenv("DWH_DSN", "postgresql://dwh:dwh@postgres-dwh/datamesh_dwh"))
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None: continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF: logger.error(msg.error())
                continue
            value = deserialize(msg.value(), None)
            key = deserialize(msg.key(), None) if msg.key() else None
            process_message(conn, topic=msg.topic(), partition=msg.partition(), offset=msg.offset(), key=key, payload=value)
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close(); conn.close()

if __name__ == "__main__": main()
