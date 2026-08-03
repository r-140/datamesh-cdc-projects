"""
Schema Evolution Service — SCHEMA-ON-READ entry point.

All events are appended to Bronze as JSON. Schema changes are detected and logged
for audit, but NEVER block the pipeline. Schema is applied at Silver/Gold layers.
"""

import os
import sys
import json
import time
import logging

from .schema_evolution import SchemaEvolutionManager
from .pipeline_manager import PipelineManager, SelfServeAPI
from .iceberg_sink import BronzeIcebergSink

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("Data Mesh CDC Platform — Schema-on-Read (Flexible)")
    logger.info("=" * 60)

    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    poll_interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
    state_file = os.environ.get("STATE_FILE", "/tmp/datamesh_state.json")

    manager = PipelineManager(schema_registry_url=sr_url, state_file=state_file)
    api = SelfServeAPI(manager)
    sink = BronzeIcebergSink()

    if not manager.pipelines:
        logger.info("Creating demo pipelines...")
        _create_demo_pipelines(manager)

    logger.info(f"Active pipelines: {len(manager.pipelines)}")
    for pid in manager.pipelines:
        logger.info(f"  - {pid}")

    logger.info("Starting schema-on-read event processing...")
    try:
        while True:
            _process_events(manager, sink)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)


def _create_demo_pipelines(manager):
    manager.create_pipeline(
        pipeline_id="orders-to-bronze", source_topic="orders-server.public.orders",
        sink_table="bronze.orders", domain="orders",
        owner_email="orders-team@example.com", alert_webhook="http://alerts.example.com/webhook"
    )
    manager.create_pipeline(
        pipeline_id="customers-to-bronze", source_topic="customers-server.public.customers",
        sink_table="bronze.customers", domain="customers",
        owner_email="customers-team@example.com"
    )


def _process_events(manager, sink):
    for pipeline_id, pipeline in manager.pipelines.items():
        # In production: consume from Kafka, parse event
        # For demo: simulate event processing
        pass


def simulate():
    manager = PipelineManager(schema_registry_url="http://localhost:8081", state_file="/tmp/datamesh_simulate.json")
    sink = BronzeIcebergSink()
    _create_demo_pipelines(manager)

    # Simulate events with evolving schemas
    event_v1 = {"id": 1, "customer_id": 100, "total_amount": 150.0, "status": "completed"}
    event_v2 = {"id": 2, "customer_id": 101, "total_amount": 299.99, "status": "pending", "promo_code": "SUMMER20"}
    event_v3 = {"id": 3, "customer_id": 102, "status": "shipped", "promo_code": "WINTER10", "discount_pct": 0.15}

    print("\n" + "=" * 70)
    print("SCENARIO: Schema-on-Read — events with different schemas")
    print("=" * 70)

    for i, event in enumerate([event_v1, event_v2, event_v3], 1):
        print(f"\nEvent {i}: {json.dumps(event)}")
        result = sink.append_event("orders", event)
        print(f"Bronze result: {result}")

    print("\n" + "=" * 70)
    print("All events appended successfully — pipeline never breaks!")
    print("=" * 70)

    print("\nSilver view SQL (schema applied at read time):")
    print("""
    CREATE VIEW silver.orders AS
    SELECT
        CAST(json_extract_scalar(_payload, '$.id') AS BIGINT) AS id,
        CAST(json_extract_scalar(_payload, '$.customer_id') AS BIGINT) AS customer_id,
        CAST(json_extract_scalar(_payload, '$.total_amount') AS DOUBLE) AS total_amount,
        CAST(json_extract_scalar(_payload, '$.status') AS VARCHAR) AS status,
        CAST(json_extract_scalar(_payload, '$.promo_code') AS VARCHAR) AS promo_code,
        CAST(json_extract_scalar(_payload, '$.discount_pct') AS DOUBLE) AS discount_pct
    FROM bronze.orders
    """)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--simulate":
        simulate()
    else:
        main()
