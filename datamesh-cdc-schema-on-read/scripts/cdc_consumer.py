#!/usr/bin/env python3
"""
CDC Consumer — Schema-on-Read
Reads Avro from Kafka, writes JSONB to Postgres DWH.

Usage:
    python scripts/cdc_consumer.py
"""

import json
import signal
import sys
import time
from datetime import datetime, date
from decimal import Decimal

import psycopg2
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# ─── Config ──────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
GROUP_ID = "schema-on-read-consumer"

DWH_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "user": "dwh",
    "password": "dwh",
    "dbname": "datamesh_dwh",
}

TOPICS = [
    "orders-server.public.orders",
    "customers-server.public.customers",
]

TABLE_MAP = {
    "orders-server.public.orders": "raw.orders_cdc",
    "customers-server.public.customers": "raw.customers_cdc",
}

# ─── Graceful shutdown ───────────────────────────────────────────────

running = True

def signal_handler(sig, frame):
    global running
    print("\n🛑 Shutdown signal received, finishing...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ─── JSON Encoder for Avro types ──────────────────────────────────────

class AvroEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        return super().default(obj)

def to_json(value):
    return json.dumps(value, cls=AvroEncoder)

# ─── Wait for topics ────────────────────────────────────────────────

def wait_for_topics(consumer, topics, max_wait=60):
    """Wait until topics exist in Kafka."""
    print(f"⏳ Waiting for topics to be created...")
    for i in range(max_wait):
        metadata = consumer.list_topics(timeout=5)
        available = [t for t in metadata.topics.keys() if not t.startswith("__")]
        missing = [t for t in topics if t not in available]
        if not missing:
            print(f"✓ All topics available: {topics}")
            return True
        print(f"   ...{i+1}/{max_wait}: missing {missing}, retrying in 2s")
        time.sleep(2)
    print(f"⚠ Timeout waiting for topics. Available: {available}")
    return False

# ─── Main ────────────────────────────────────────────────────────────

def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    })

    if not wait_for_topics(consumer, TOPICS):
        print("⚠ Starting anyway — topics may appear shortly.")

    consumer.subscribe(TOPICS)

    conn = psycopg2.connect(**DWH_CONFIG)
    conn.autocommit = False

    print("🚀 CDC Consumer started (Schema-on-Read)")
    print(f"   Topics: {TOPICS}")
    print(f"   DWH: {DWH_CONFIG['host']}:{DWH_CONFIG['port']}/{DWH_CONFIG['dbname']}")
    print("   Press Ctrl+C to stop\n")

    total = 0
    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue
                print(f"⚠ Kafka error: {msg.error()}")
                continue

            value = avro_deserializer(msg.value(), None)
            key = avro_deserializer(msg.key(), None) if msg.key() else {"id": value.get("id")}

            table = TABLE_MAP.get(msg.topic())
            if not table:
                continue

            record_id = key.get("id") if isinstance(key, dict) else key

            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {table} (
                        id, payload, __op, __source_ts_ms,
                        __kafka_partition, __kafka_offset
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        __op = EXCLUDED.__op,
                        __source_ts_ms = EXCLUDED.__source_ts_ms,
                        __kafka_partition = EXCLUDED.__kafka_partition,
                        __kafka_offset = EXCLUDED.__kafka_offset,
                        ingested_at = NOW()
                """, (
                    record_id,
                    to_json(value),
                    value.get("__op"),
                    value.get("__source_ts_ms"),
                    msg.partition(),
                    msg.offset(),
                ))
                conn.commit()

            total += 1
            print(f"✓ {table:30s} id={record_id:4d}  offset={msg.offset():4d}  total={total}")

    finally:
        consumer.close()
        conn.close()
        print(f"\n👋 Consumer stopped. Total messages processed: {total}")


if __name__ == "__main__":
    main()
