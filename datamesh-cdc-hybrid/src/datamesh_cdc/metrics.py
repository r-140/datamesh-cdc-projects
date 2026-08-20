"""Prometheus metrics for the hybrid CDC control boundary."""

from __future__ import annotations

import time

from prometheus_client import Counter, Gauge, start_http_server

EVENTS = Counter(
    "hybrid_cdc_events_total",
    "CDC events processed by the hybrid consumer",
    ("topic", "source_table", "outcome"),
)
PROJECTION_FAILURES = Counter(
    "hybrid_cdc_projection_failures_total",
    "Events retained in Bronze but rejected by the Silver contract",
    ("source_table",),
)
LAST_PROCESSED = Gauge(
    "hybrid_cdc_last_processed_timestamp_seconds",
    "Unix timestamp of the last processed event",
    ("topic",),
)
UNRESOLVED_FAILURES = Gauge(
    "hybrid_cdc_unresolved_projection_failures",
    "Projection failures without a later successful Silver event",
)
SCHEMA_SHAPES = Gauge(
    "hybrid_cdc_observed_schema_shapes",
    "Distinct Bronze field-set fingerprints",
    ("source_table",),
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)


def record_event(topic: str, source_table: str, outcome: str) -> None:
    EVENTS.labels(topic=topic, source_table=source_table, outcome=outcome).inc()
    LAST_PROCESSED.labels(topic=topic).set(time.time())
    if outcome == "bronze_only":
        PROJECTION_FAILURES.labels(source_table=source_table).inc()


def refresh_governance_metrics(connection) -> None:
    """Refresh stateful gauges from the warehouse's authoritative state."""
    with connection.cursor() as cursor:
        cursor.execute("""SELECT source_table, count(*)
               FROM governance.observed_schemas
               GROUP BY source_table""")
        for source_table, count in cursor.fetchall():
            SCHEMA_SHAPES.labels(source_table=source_table).set(count)

        cursor.execute("""SELECT count(*)
               FROM governance.projection_failures f
               WHERE NOT (
                   (f.source_table = 'orders' AND EXISTS (
                       SELECT 1 FROM silver.orders s
                       WHERE s.id = CASE WHEN f.payload->>'id' ~ '^-?[0-9]+$'
                           THEN (f.payload->>'id')::bigint END
                         AND s.bronze_offset > f.kafka_offset
                   )) OR
                   (f.source_table = 'customers' AND EXISTS (
                       SELECT 1 FROM silver.customers s
                       WHERE s.id = CASE WHEN f.payload->>'id' ~ '^-?[0-9]+$'
                           THEN (f.payload->>'id')::bigint END
                         AND s.bronze_offset > f.kafka_offset
                   ))
               )""")
        UNRESOLVED_FAILURES.set(cursor.fetchone()[0])
    connection.commit()
